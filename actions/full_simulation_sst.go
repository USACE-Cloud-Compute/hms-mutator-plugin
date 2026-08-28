package actions

import (
	"errors"
	"fmt"
	"math/rand"
	"os"
	"strings"

	"time"

	"github.com/usace-cloud-compute/cc-go-sdk"
	"github.com/usace-cloud-compute/hms-mutator/utils"
)

// This file implements the "full-simulation-sst" action. Given a catalog of
// storm-typed storms, fishnets (valid placement locations per storm or storm
// type), antecedent basin model files, and per-storm-type seasonality
// distributions, it generates one record per event of a full realization:
// the selected storm, its sampled x/y placement, its sampled start date, and
// the antecedent basin file to use. The output recordset is consumed by the
// hms-runner to govern which storms are simulated.
//
// Per event, the action:
//
//  1. reads all storm names in the storm catalog (*.dss under storms_directory),
//  2. selects a storm (uniformly, or via the configured resampling method),
//  3. samples an x/y placement from the storm's fishnet,
//  4. derives the storm type from the storm name (yyyymmdd_xxhr_storm-type_storm-rank),
//  5. samples a day-of-year from the storm type's empirical seasonality distribution,
//  6. samples a year so the start date falls within the POR,
//  7. samples a calibration event name,
//  8. composes the antecedent basin path (basin_root/basin_name/calibration_event/date),
//  9. stores the results in a TileDB store, or dumps them to CSV.
//
// See full_simulation_sst.md for the attribute reference and an example
// configuration.

// FullSimulationSST is the "full-simulation-sst" action. It materializes,
// for every event of a full realization, a concrete storm, placement, storm
// start date, and antecedent basin, and writes the results to the configured
// output data source (a TileDB store, or CSV).
//
// Required action attributes:
//
//	"por_start_date", "por_end_date"                        (YYYYMMDD)
//	"output_data_source"
//	"storms_directory", "storms_store"
//	"fishnet_directory", "fishnet_store", "fishnet_type_or_name"
//	"storm_type_seasonality_distribution_directory",
//	"storm_type_seasonality_distribution_store"
//	"basin_root_directory", "basin_name"
//	"calibration_event_names"                                (string slice)
//	"seed_datasource_key", "blocks_datasource_key"
//
// Optional attributes (with defaults):
//
//	"sampling_method"  "best_estimate" | "bootstrap" | "jackknife"  (default "best_estimate")
//	"sampling_level"   "event" | "realization" | "block"            (default "realization")
//
// Note: for the "best_estimate" sampler the sampling level has no effect,
// since that sampler always draws from the full catalog.
type FullSimulationSST struct {
	action cc.Action
}

// FullSimulationResult is the set of per-event outcomes produced by
// FullSimulationSST.Compute — one record per simulated event, in event order.
type FullSimulationResult []EventResult

// EventResult is one row of the full-simulation output recordset: the storm
// chosen for a single event, where it was placed, when it begins, and which
// antecedent basin model applies. The eventstore tags define the column
// names used when the recordset is written to an event store.
type EventResult struct {
	EventNumber int64   `eventstore:"event_number"` // 1-based event index within the realization
	StormPath   string  `eventstore:"storm_path"`   // storm catalog name (file name without extension)
	X           float64 `eventstore:"x"`            // sampled fishnet x coordinate
	Y           float64 `eventstore:"y"`            // sampled fishnet y coordinate
	StormType   string  `eventstore:"storm_type"`   // storm type parsed from the storm name
	StormDate   string  `eventstore:"storm_date"`   // sampled storm start date, YYYYMMDD
	BasinPath   string  `eventstore:"basin_path"`   // antecedent basin model path
}

// InitFullRealizationSST wraps a configured cc.Action as a FullSimulationSST,
// ready for Compute to be invoked on it.
func InitFullRealizationSST(a cc.Action) *FullSimulationSST {
	return &FullSimulationSST{action: a}
}

// Compute runs the full-simulation-sst action. It reads all inputs (POR
// window, storm catalog, fishnets, seasonality distributions, basin
// configuration, calibration event names, seeds, and blocks), constructs the
// storm sampler selected by the sampling_method attribute, materializes one
// EventResult per event via compute, and writes the results to the output
// data source: a TileDB store when the output source's store name is
// "store", otherwise a CSV file through the IO manager.
func (frsst *FullSimulationSST) Compute(pm *cc.PluginManager) error {
	a := frsst.action
	//get parameters
	//time range of POR
	porStartDateString := a.Attributes.GetStringOrFail("por_start_date")
	porStartDate, err := time.Parse("20060102", porStartDateString)
	if err != nil {
		return err
	}
	porEndDateString := a.Attributes.GetStringOrFail("por_end_date")
	porEndDate, err := time.Parse("20060102", porEndDateString)
	if err != nil {
		return err
	}
	//get output datasource
	outputDataSourceKey := a.Attributes.GetStringOrFail("output_data_source")
	outputDataSource, err := a.GetOutputDataSource(outputDataSourceKey)
	if err != nil {
		return err
	}
	// Storm catalog: all *.dss files under the storm directory.
	stormDirectory := a.Attributes.GetStringOrFail("storms_directory")
	stormsStoreKey := a.Attributes.GetStringOrFail("storms_store") //expecting this to be an s3 bucket?
	stormList, err := utils.ListAllPaths(a.IOManager, stormsStoreKey, stormDirectory, "*.dss")
	if err != nil {
		return err
	}
	// Build the storm sampler from the configured method and level.
	samplingMethod := a.Attributes.GetStringOrDefault("sampling_method", "best_estimate")
	samplingLevel := a.Attributes.GetStringOrDefault("sampling_level", "realization")
	var sampler utils.StormSampler
	switch samplingMethod {
	case "best_estimate":
		sampler, err = utils.InitBestEstimateSampler(stormList, samplingLevel)
		if err != nil {
			return err
		}
	case "bootstrap":
		sampler, err = utils.InitBootstrapSampler(stormList, samplingLevel, porStartDate)
		if err != nil {
			return err
		}
	case "jackknife":
		sampler, err = utils.InitJackknifeSampler(stormList, samplingLevel, porStartDate)
		if err != nil {
			return err
		}
	default:

		return errors.New("samlper type not defined " + samplingMethod)
	}

	// Fishnets give the valid placements: a named list of coordinates,
	// currently keyed by storm name (or storm type). Fishnets are expected
	// to be unique to each storm... could be converted to be unique to each
	// storm type.
	fishnetDirectory := a.Attributes.GetStringOrFail("fishnet_directory")
	fishnetStoreKey := a.Attributes.GetStringOrFail("fishnet_store")
	fishnettypeorname := a.Attributes.GetStringOrFail("fishnet_type_or_name")
	fishnetList, err := utils.ListAllPaths(a.IOManager, fishnetStoreKey, fishnetDirectory, "*.csv")
	if err != nil {
		return err
	}
	fishNetMap, err := utils.ReadFishNets(a.IOManager, fishnetStoreKey, fishnetList, fishnetDirectory)
	if err != nil {
		return err
	}
	// Storm type seasonality distributions: empirical CDFs of day-of-year,
	// one per storm type, read from CSVs in the configured store.
	stormTypeSeasonalityDistributionDirectory := a.Attributes.GetStringOrFail("storm_type_seasonality_distribution_directory")
	stormTypeSeasonalityDistributionStoreKey := a.Attributes.GetStringOrFail("storm_type_seasonality_distribution_store")
	stormTypeDistributionList, err := utils.ListAllPaths(a.IOManager, stormTypeSeasonalityDistributionStoreKey, stormTypeSeasonalityDistributionDirectory, "*.csv")
	if err != nil {
		return err
	}
	stormTypeSeasonalityDistributionsMap, err := utils.ReadStormDistributions(a.IOManager, stormTypeSeasonalityDistributionStoreKey, stormTypeDistributionList, stormTypeSeasonalityDistributionDirectory)
	if err != nil {
		return err
	}
	// Antecedent basin models live under basin_root_directory, keyed by
	// basin name plus calibration event and date (see compute).
	basinRootDir := a.Attributes.GetStringOrFail("basin_root_directory")
	basinName := a.Attributes.GetStringOrFail("basin_name")
	// Calibration event strings: candidate antecedent conditions; one is
	// sampled uniformly per event and used in the basin path.
	calibrationEvents, err := a.Attributes.GetStringSlice("calibration_event_names")
	if err != nil {
		return err
	}
	// Per-event seeds (indexed by event number) drive all sampling, which
	// is what makes a given realization reproducible.
	seeds, err := utils.GetSeeds(a)
	if err != nil {
		return err
	}
	// Blocks partition the realization's events across compute nodes.
	blocks, err := utils.GetBlocks(pm, a)
	if err != nil {
		return err
	}
	results, err := compute(stormList, calibrationEvents, basinRootDir, basinName, fishNetMap, fishnettypeorname, stormTypeSeasonalityDistributionsMap, porStartDate, porEndDate, seeds, blocks, sampler)
	if err != nil {
		return err
	}
	//write results to data stores
	if outputDataSource.StoreName == "store" {
		return writeResultsToTileDB(pm, outputDataSource.StoreName, results, outputDataSource.Name) //update this to not referenceblock store, and also not hardcode the name to "storms"
	} else {
		return writeResultsToCSV(a.IOManager, outputDataSource, results)
	}

}

// compute walks every block, and within each block every event number from
// BlockEventStart to BlockEventEnd, materializing one EventResult per event.
//
// Sampler refresh: depending on the sampler's SamplingLevel, the storm pool
// is rebuilt per block ("block"), once per realization ("realization"), or
// per event ("event").
//
// Per event, using an RNG seeded from seeds[en-1].EventSeed (seeds are
// therefore assumed to be 1-based by event number):
//
//  1. sample a storm name from the pool,
//  2. derive the storm type from the name (third '_'-separated field),
//  3. sample a calibration event name,
//  4. resolve the fishnet key — by storm name, by storm type, or a literal
//     fishnet_type_or_name value — and sample a placement from it,
//  5. sample a day-of-year from the storm type's seasonality distribution
//     and a POR-constrained year, forming the storm start date,
//  6. compose the antecedent basin path.
//
// Known caveats (undocumented behavior worth tracking):
//   - events whose number exceeds len(seeds) are silently skipped (no output
//     row is produced) rather than raising an error;
//   - the POR-constrained year loop below can spin forever if a sampled
//     day-of-year can never satisfy the POR's start/end day-of-year bounds
//     (e.g. a sub-year POR whose seasonality distribution falls outside it);
//   - interior POR years accept any day-of-year, so sampled dates can fall
//     outside the POR even when the loop terminates.
func compute(stormNames []string, calibrationEventNames []string, basinRootDir string, basinName string, fishnets utils.FishNetMap, fishnettypeorname string, seasonalDistributions utils.StormTypeSeasonalityDistributionMap, porStart time.Time, porEnd time.Time, seeds []utils.SeedSet, blocks []utils.Block, sampler utils.StormSampler) (FullSimulationResult, error) {
	results := make(FullSimulationResult, 0)
	realizationIndex := -1
	for _, b := range blocks {
		//right here i would have logic to determine if the sampler needs to be updated for the list of storms at either the realization or block level
		if sampler.SamplingLevel() == "block" {
			sampler.SampleNames(b.BlockEventStart, int64(b.RealizationIndex), seeds) //find the right event number at the start of a block
		}
		//
		//or
		// Refresh the storm pool at the realization level: only when we
		// move onto a new realization, using that realization's first event.
		if sampler.SamplingLevel() == "realization" {
			if realizationIndex != int(b.RealizationIndex) {
				sampler.SampleNames(b.BlockEventStart, int64(b.RealizationIndex), seeds) //find the right event number at the start of a block
				realizationIndex = int(b.RealizationIndex)
			}
		}

		if b.BlockEventCount > 0 {
			for en := b.BlockEventStart; en <= b.BlockEventEnd; en++ {
				// Create a random number generator for the event, seeded by
				// that event's seed. Note: events beyond len(seeds) are
				// skipped entirely (no EventResult is emitted for them).
				if int(en) <= len(seeds) {
					enRng := rand.New(rand.NewSource(seeds[en-1].EventSeed))
					// Refresh the storm pool at the event level: a unique
					// bootstrap/jackknife/best-estimate catalog sample per
					// event number.
					if sampler.SamplingLevel() == "event" {
						sampler.SampleNames(en, int64(b.RealizationIndex), seeds) //unique bootstrap jackknife or best estimate catalog sample per event number
					}
					// Sample a storm name uniformly from the pool.
					stormName := sampler.SampleName(enRng)
					// Storm type is the third '_'-separated field of the
					// name, assuming yyyymmdd_xxhr_storm-type_storm-rank.
					stormType := strings.Split(stormName, "_")[2] //assuming yyyymmdd_xxhr_storm-type_storm-rank
					//sample calibration event
					calibrationEvent := calibrationEventNames[enRng.Intn(len(calibrationEventNames))]
					//fetch fishnet based on storm name -
					// Normalize the "st" prefix convention to "ST".
					// Note: this replaces every "st" substring, which can
					// mangle names/types containing "st" elsewhere.
					sname := strings.Split(stormName, ".")[0]
					sname = strings.Replace(sname, "st", "ST", -1) //how did this happen?//storm name just file name no extension.
					if fishnettypeorname == "type" {
						sname = strings.Replace(stormType, "st", "ST", -1)
					} else if fishnettypeorname != "name" {
						sname = fishnettypeorname //if not type or name, just use whatever they give directly.
					}
					fishnet, ok := fishnets[sname]
					if !ok {

						return results, fmt.Errorf("could not find fishnet %v in fishnet map", sname)
					}
					//sample location
					coordinate := fishnet.Coordinates[enRng.Intn(len(fishnet.Coordinates))]
					//fetch seasonal distribution based on storm type
					seasonalDistribution, ok := seasonalDistributions[stormType]
					if !ok {
						return results, fmt.Errorf("could not find the seasonal distribution for type %v", stormType)
					}
					//fetch day of year
					dayOfYear := seasonalDistribution.Sample(enRng.Float64())
					// Determine the year: sample uniformly across the POR's
					// years, requiring the first POR year's day-of-year to be
					// >= the POR start and the last POR year's to be <= the
					// POR end. Interior years accept any day-of-year.
					// Note: the loop never terminates if no year can
					// satisfy the bounds for this day-of-year.
					yearCount := porEnd.Year() - porStart.Year() //this needs to be checked on both ends for valid dates.
					dayofyearInrange := false
					year := 0
					for !dayofyearInrange {
						initalYearGuess := enRng.Intn(yearCount+1) + porStart.Year() //+1 is due to [0,n)
						if initalYearGuess == porStart.Year() {
							if dayOfYear >= porStart.YearDay() {
								dayofyearInrange = true
								year = initalYearGuess
							}
						} else if initalYearGuess == porEnd.Year() {
							if dayOfYear <= porEnd.YearDay() {
								dayofyearInrange = true
								year = initalYearGuess
							}
						} else if porStart.Year() < initalYearGuess && initalYearGuess < porEnd.Year() {
							dayofyearInrange = true
							year = initalYearGuess
						}
					}
					// Create the start date from the sampled year and
					// day-of-year (Jan 1 of the year plus dayOfYear-1 days).
					startDate := time.Date(year, 1, 1, 1, 1, 1, 1, time.Local)
					//convert day of year to duration
					sdur := fmt.Sprintf("%vh", (dayOfYear-1)*24)
					dur, err := time.ParseDuration(sdur)
					if err != nil {
						return results, err
					}
					startDate = startDate.Add(dur)
					// Compose the antecedent basin path:
					// <basin_root>/<yyyy-mm-dd>/<basin>_<calibration_event>
					event := EventResult{
						EventNumber: en,
						StormPath:   stormName,
						StormType:   stormType,
						X:           coordinate.X,
						Y:           coordinate.Y,
						StormDate:   startDate.Format("20060102"),
						BasinPath:   fmt.Sprintf("%v/%v_%v_%v", basinRootDir, startDate.Format("2006-01-02"), basinName, calibrationEvent),
					}
					results = append(results, event)
				}
			}

		}
	}
	return results, nil
}

// writeResultsToTileDB writes results into the named TileDB store as an
// event-store recordset, creating the dataset before writing.
func writeResultsToTileDB(pm *cc.PluginManager, storeKey string, results FullSimulationResult, tableName string) error {
	recordset, err := cc.NewEventStoreRecordset(pm, &results, storeKey, tableName)
	if err != nil {
		return err
	}
	err = recordset.Create()
	if err != nil {
		return err
	}
	return recordset.Write(&results)
}

// writeResultsToCSV serializes results to CSV with the header
// event_number,storm_path,x,y,storm_type,storm_date,basin_path and delivers
// them through the output data source: written to a local file for an "FS"
// store (<root>/<default path>), or uploaded via the IO manager's Put for
// object stores (e.g. S3).
func writeResultsToCSV(iomanager cc.IOManager, ds cc.DataSource, results FullSimulationResult) error {

	var sb strings.Builder

	//Write the CSV header
	sb.WriteString("event_number,storm_path,x,y,storm_type,storm_date,basin_path")

	for _, r := range results {
		sb.WriteString("\n")
		sb.WriteString(fmt.Sprintf("%v,%v,%v,%v,%v,%v,%v", r.EventNumber, r.StormPath, r.X, r.Y, r.StormType, r.StormDate, r.BasinPath))
	}

	store, err := iomanager.GetStore(ds.StoreName)
	if err != nil {
		return err
	}
	writer := strings.NewReader(sb.String())
	if store.StoreType == "FS" {
		root := store.Parameters.GetStringOrFail("root")
		if err != nil {
			return err
		}
		path := ds.Paths["default"]
		fullpath := fmt.Sprintf("%v/%v", root, path)
		os.WriteFile(fullpath, []byte(sb.String()), 0600)
		return nil
	}

	_, err = iomanager.Put(cc.PutOpInput{
		SrcReader:         writer,
		DataSourceOpInput: cc.DataSourceOpInput{DataSourceName: ds.Name, PathKey: "default"},
	})
	return err
}

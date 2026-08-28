// Package utils provides shared helpers for the hms-mutator plugin.
//
// This file implements the StormSampler family: strategies for choosing which
// storm from a storm catalog is applied to each event of a stochastic
// simulation. A sampler is used in two phases:
//
//  1. SampleNames(event, realization, seeds) (re)builds the working pool of
//     storm names for the current sampling level. It is called whenever the
//     level being simulated changes (per block, per realization, or per
//     event, as reported by SamplingLevel).
//  2. SampleName(rng) draws a single storm uniformly at random from that
//     pool, using a caller-supplied RNG so results stay reproducible.
//
// Storm names are expected to follow the layout
//
//	yyyymmdd_xxhr_storm-type_storm-rank
//
// where the leading 8 characters are the storm's date (used to bucket storms
// by water year) and field 3 (index 2 when split on '_') is the storm type.
package utils

import (
	"math/rand"
	"time"
)

// StormSampler is the interface implemented by every storm-catalog sampling
// strategy. Implementations decide how the working pool of storm names is
// assembled (SampleNames) and report the simulation level at which that pool
// should be refreshed (SamplingLevel: "block", "realization", or "event").
type StormSampler interface {
	// SampleNames rebuilds the working pool of storm names for the given
	// event/realization, seeded by seeds so the result is reproducible.
	// The event and realization arguments are only meaningful to samplers
	// that actually resample; no-op samplers may ignore them.
	SampleNames(event int64, realization int64, seeds []SeedSet) error

	// SampleName returns a single storm name chosen uniformly at random
	// from the pool established by the most recent SampleNames call.
	SampleName(rng *rand.Rand) string

	// SamplingLevel reports the level ("block", "realization", or "event")
	// at which SampleNames must be re-invoked by the caller.
	SamplingLevel() string
}

// BaseSamplerData holds the shared state for the resampling samplers
// (BootstrapSampler and JackknifeSampler): the full storm catalog, a
// year-bucketed index of that catalog, the catalog's year span, and the
// configured sampling level.
//
// yearStormMap buckets storm names by "adjusted" water year (see
// initBaseSamplerData). sampleNames is the working pool produced by the
// most recent SampleNames call.
type BaseSamplerData struct {
	StormNames    []string
	yearStormMap  map[int][]string
	sampleNames   []string
	minYear       int32
	maxYear       int32
	samplingLevel string
}

// BootstrapSampler resamples the storm catalog with replacement, grouped by
// water year, so that each simulation event works from a resampled pool that
// is the same size as the original catalog. The resample is seeded by the
// event's seed, making each event's pool deterministic.
type BootstrapSampler struct {
	*BaseSamplerData
}

// initBaseSamplerData builds a BaseSamplerData from a list of storm names.
//
// Each name's leading 8 characters are parsed as a yyyymmdd date. The storm's
// water year is the calendar year of that date, advanced by one if the
// storm's day-of-year is on/after startDate's day-of-year (so a late-season
// storm is counted toward the following water year). Storms are bucketed into
// yearStormMap by that adjusted year, and the catalog's min/max adjusted year
// are recorded.
//
// Returns an error if any name does not begin with a valid 8-digit date.
func initBaseSamplerData(Names []string, samplingLevel string, startDate time.Time) (*BaseSamplerData, error) {
	b := BaseSamplerData{}
	b.yearStormMap = make(map[int][]string)
	minYear := startDate.Year()
	maxYear := 0
	for _, n := range Names {
		// Extract the storm's date from the first 8 characters of the name.
		// Names are expected to look like: yyyymmdd_xxhr_storm-type_storm-rank
		year, err := time.Parse("20060102", n[0:8])
		if err != nil {
			return &b, err
		}
		adjustedYear := year.Year()
		// Assign the storm to the next water year if it falls on/after the
		// POR start day-of-year.
		if year.YearDay() >= startDate.YearDay() {
			adjustedYear += 1
		}
		storms, ok := b.yearStormMap[adjustedYear]
		if ok {
			storms = append(storms, n)
			b.yearStormMap[adjustedYear] = storms
		} else {
			b.yearStormMap[adjustedYear] = []string{n}
		}
		if adjustedYear > maxYear {
			maxYear = adjustedYear
		}
		if adjustedYear < minYear {
			minYear = adjustedYear
		}
	}
	b.maxYear = int32(maxYear)
	b.minYear = int32(minYear)
	b.StormNames = Names
	b.samplingLevel = samplingLevel
	return &b, nil
}

// InitBootstrapSampler constructs a BootstrapSampler over the given storm
// catalog. samplingLevel is the level at which the caller should re-run
// SampleNames; startdate is the POR start used to compute water-year
// boundaries. Returns an error if a storm name does not start with a valid
// 8-digit date.
func InitBootstrapSampler(Names []string, samplingLevel string, startdate time.Time) (*BootstrapSampler, error) {
	bdata, err := initBaseSamplerData(Names, samplingLevel, startdate)
	b := BootstrapSampler{BaseSamplerData: bdata}
	if err != nil {
		return &b, err
	}
	return &b, nil
}

// SamplingLevel returns the sampling level configured for this sampler.
func (b *BootstrapSampler) SamplingLevel() string {
	return b.samplingLevel
}

// SampleNames builds a bootstrap resample of the catalog: it repeatedly picks
// a random water year (seeded by seeds[event].EventSeed) and appends that
// year's storms until the resampled pool equals the full catalog size, i.e.
// a size-N resample with replacement.
//
// Note: delta is computed as maxYear-minYear, so the highest water year is
// not selectable (Int31n(delta) yields [minYear, maxYear-1]); and if the
// catalog spans a single year, delta is 0 and Int31n(0) panics.
func (b *BootstrapSampler) SampleNames(event int64, realization int64, seeds []SeedSet) error {
	stormCount := len(b.StormNames)
	sample := make([]string, 0)
	// Seed the RNG with this event's seed so the resample is reproducible.
	rng := rand.New(rand.NewSource(seeds[event].EventSeed)) // check with haden.
	delta := b.maxYear - b.minYear
	i := 0
	for {
		// Pick a random water year and add all of that year's storms.
		tmpStorms := b.yearStormMap[int(b.minYear+rng.Int31n(delta+1))]
		for _, s := range tmpStorms {
			sample = append(sample, s)
			i++
			if i >= stormCount {
				break
			}
		}
		if i >= stormCount {
			break
		}
	}
	b.sampleNames = sample
	return nil
}

// SampleName returns a storm name chosen uniformly at random from the pool
// established by the most recent SampleNames call.
func (b *BootstrapSampler) SampleName(rng *rand.Rand) string {
	stormCount := len(b.sampleNames)
	return b.sampleNames[rng.Int31n(int32(stormCount))]
}

// JackknifeSampler is a leave-out sampler: for a given realization it drops
// the storms belonging to a contiguous 5-water-year window (rotated by the
// realization index) and samples from the remaining catalog. Across
// realizations the omitted window slides, so every year is left out in
// turn.
type JackknifeSampler struct {
	*BaseSamplerData
}

// InitJackknifeSampler constructs a JackknifeSampler over the given storm
// catalog. samplingLevel is the level at which the caller should re-run
// SampleNames; startdate is the POR start used to compute water-year
// boundaries. Returns an error if a storm name does not start with a valid
// 8-digit date.
func InitJackknifeSampler(Names []string, samplingLevel string, startdate time.Time) (*JackknifeSampler, error) {
	// Collect names into common years and sample as year groups.
	bdata, err := initBaseSamplerData(Names, samplingLevel, startdate)
	b := JackknifeSampler{BaseSamplerData: bdata}
	if err != nil {
		return &b, err
	}
	return &b, nil
}

// SamplingLevel returns the sampling level configured for this sampler.
func (b *JackknifeSampler) SamplingLevel() string {
	return b.samplingLevel
}

// SampleNames builds the working pool by taking the full catalog and leaving
// out the storms from a contiguous window of numYearsToGroup water years.
// The window's position is rotated by realization (mod delta), so different
// realizations omit different years.
//
// Note: numYearsToGroup is currently hard-coded to 5 (consider making it a
// parameter); and if the catalog spans no more than 5 water years, delta is
// non-positive and the modulo below panics.
func (b *JackknifeSampler) SampleNames(event int64, realization int64, seeds []SeedSet) error {
	// Start with an empty pool; capacity is grown as needed.
	numYearsToGroup := 5 // consider making this a parameter
	sample := make([]string, 0)
	// Number of distinct leave-out window positions.
	delta := b.maxYear - b.minYear - int32(numYearsToGroup) + 1
	// Rotate the omitted window by the realization index.
	modReal := int32(realization) % delta
	skipYearMin := b.minYear + modReal - 1
	skipYearMax := skipYearMin + int32(numYearsToGroup)

	for year := b.minYear; year <= b.maxYear; year++ {
		// Omit the years in this realization's leave-out window.
		if year >= skipYearMin && year < skipYearMax {
			continue
		}
		tmpStorms := b.yearStormMap[int(year)]
		for _, s := range tmpStorms {
			sample = append(sample, s)
		}
	}

	b.sampleNames = sample
	return nil
}

// SampleName returns a storm name chosen uniformly at random from the pool
// established by the most recent SampleNames call.
func (b *JackknifeSampler) SampleName(rng *rand.Rand) string {
	stormCount := len(b.sampleNames)
	return b.sampleNames[rng.Int31n(int32(stormCount))]
}

// BestEstimateSampler is the simplest sampler: it does not resample at all
// and always draws from the entire storm catalog. Its SampleNames is a no-op.
type BestEstimateSampler struct {
	StormNames    []string
	samplingLevel string
}

// InitBestEstimateSampler constructs a BestEstimateSampler over the given
// storm catalog. samplingLevel is recorded but does not change behavior,
// since best-estimate sampling always uses the full catalog.
func InitBestEstimateSampler(Names []string, samplingLevel string) (*BestEstimateSampler, error) {
	b := BestEstimateSampler{}
	b.StormNames = Names
	b.samplingLevel = samplingLevel
	return &b, nil
}

// SamplingLevel returns the sampling level configured for this sampler.
func (b *BestEstimateSampler) SamplingLevel() string {
	return b.samplingLevel
}

// SampleNames is a no-op for the best-estimate sampler: the full catalog is
// always used, so there is nothing to (re)build per event/realization.
func (b *BestEstimateSampler) SampleNames(event int64, realization int64, seeds []SeedSet) error {
	return nil
}

// SampleName returns a storm name chosen uniformly at random from the full
// storm catalog.
func (b *BestEstimateSampler) SampleName(rng *rand.Rand) string {
	stormCount := len(b.StormNames)
	return b.StormNames[rng.Int31n(int32(stormCount))]
}

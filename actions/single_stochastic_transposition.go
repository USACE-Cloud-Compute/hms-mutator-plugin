package actions

import (
	"time"

	"github.com/usace-cloud-compute/cc-go-sdk"
	"github.com/usace-cloud-compute/hms-mutator/hms"
	"github.com/usace-cloud-compute/hms-mutator/transposition"
	"github.com/usace-cloud-compute/hms-mutator/utils"
)

var pluginName string = "hms-mutator"

type SingleStochasticTransposition struct {
	pm                       *cc.PluginManager
	gridFile                 hms.GridFile
	metFile                  hms.Met
	seedSet                  utils.SeedSet
	transpositionDomainBytes []byte
	watershedBytes           []byte
}
type StochasticTranspositionResult struct {
	MetBytes  []byte
	GridBytes []byte
	StormName string
}

func InitSingleStochasticTransposition(pm *cc.PluginManager, gridFile hms.GridFile, metFile hms.Met, seedSet utils.SeedSet, tbytes []byte, wbytes []byte) SingleStochasticTransposition {
	return SingleStochasticTransposition{
		pm:                       pm,
		gridFile:                 gridFile,
		metFile:                  metFile,
		seedSet:                  seedSet,
		transpositionDomainBytes: tbytes,
		watershedBytes:           wbytes,
	}
}
func (sst SingleStochasticTransposition) Compute(bootstrapCatalog bool, bootstrapCatalogLength int, normalize bool, controlStartTime time.Time, userSpecifiedOffset int) (StochasticTranspositionResult, error) {
	//initialize simulation
	var ge hms.PrecipGridEvent
	var te hms.TempGridEvent
	var m hms.Met
	var gfbytes []byte
	var originalDssPath string
	sim, err := transposition.InitTranspositionSimulation(sst.transpositionDomainBytes, sst.watershedBytes, sst.metFile, sst.gridFile)
	if err != nil {
		sst.pm.Logger.Error(err.Error())
		return StochasticTranspositionResult{}, err
	}
	//compute simulation for given seed set
	m, ge, te, err = sim.Compute(sst.seedSet.EventSeed, sst.seedSet.RealizationSeed, bootstrapCatalog, bootstrapCatalogLength)
	if err != nil {
		sst.pm.Logger.Error(err.Error())
		return StochasticTranspositionResult{}, err
	}
	originalDssPath, _ = ge.OriginalDSSFile()
	//update the dss file output to match the agreed upon convention /data/Storm.dss
	ge.UpdateDSSFile("Storm")
	te.UpdateDSSFile("Storm")
	gfbytes = sim.GetGridFileBytes(ge, te)
	geStartTime, err := time.Parse("02Jan2006:1504", ge.StartTime)
	if err != nil {
		sst.pm.Logger.Error(err.Error())
		return StochasticTranspositionResult{}, err
	}
	//get met file bytes
	m.UpdatePrecipTimeShift(normalize, controlStartTime, geStartTime, userSpecifiedOffset)
	mbytes, err := m.WriteBytes()
	if err != nil {
		sst.pm.Logger.Error(err.Error())
		return StochasticTranspositionResult{}, err
	}
	// prepare result
	result := StochasticTranspositionResult{
		MetBytes:  mbytes,
		GridBytes: gfbytes,
		StormName: originalDssPath,
	}
	return result, nil
	//find the right resource locations
}

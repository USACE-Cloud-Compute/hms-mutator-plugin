package transposition

import (
	"fmt"
	"math/rand"

	"github.com/usace-cloud-compute/hms-mutator/hms"
)

type TranspositionSimulation struct {
	transpositionModel Model
	metModel           hms.Met
	gridFile           hms.GridFile
}

func InitTranspositionSimulation(trgpkgRI []byte, wbgpkgRI []byte, metFile hms.Met, gridFile hms.GridFile) (TranspositionSimulation, error) {
	s := TranspositionSimulation{
		transpositionModel: Model{},
		metModel:           metFile,
		gridFile:           gridFile,
	}
	//initialize transposition region
	t, err := InitModel(trgpkgRI, wbgpkgRI) //TODO fix this.
	if err != nil {
		return s, err
	}
	s.transpositionModel = t
	return s, nil

}
func (s *TranspositionSimulation) Compute(eventSeed int64, realizationSeed int64, bootstrapCatalog bool, bootstrapCatalogLength int) (hms.Met, hms.PrecipGridEvent, hms.TempGridEvent, error) {
	nvrng := rand.New(rand.NewSource(eventSeed))
	stormSeed := nvrng.Int63()
	transpositionSeed := nvrng.Int63()
	if bootstrapCatalog {
		//this currently leverages the catalog and bootstraps based on the bootstrap catalog
		//length to produce a catalog of bootstrap catalog length. Discussions of an Uber catalog
		//where the catalog will be a superset of arbitrary size, where a subset of fixed
		//size would be bootstrapped can be completed if the bootstrap catalog length is smaller than the catalog itself.
		kurng := rand.New(rand.NewSource(realizationSeed))
		bootstrapSeed := kurng.Int63()
		//bootstrap catalog
		s.gridFile.Bootstrap(bootstrapSeed, bootstrapCatalogLength)
	}

	//select event
	ge, te, err := s.gridFile.SelectEvent(stormSeed)
	if err != nil {
		return s.metModel, ge, te, err
	}
	//transpose
	x, y, err := s.transpositionModel.Transpose(transpositionSeed, ge)

	if err != nil {
		return s.metModel, ge, te, err
	}

	fmt.Printf("%v,%f,%f\n", ge.Name, x, y)
	//update met storm name
	err = s.metModel.UpdateStormName(ge.Name)
	if err != nil {
		return s.metModel, ge, te, err
	}
	//update storm center
	err = s.metModel.UpdateStormCenter(fmt.Sprintf("%f", x), fmt.Sprintf("%f", y))
	if err != nil {
		return s.metModel, ge, te, err
	}

	return s.metModel, ge, te, nil
}
func (s TranspositionSimulation) GetGridFileBytes(precipevent hms.PrecipGridEvent, tempevent hms.TempGridEvent) []byte {
	return s.gridFile.ToBytes(precipevent, tempevent)
}

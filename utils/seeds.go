package utils

import (
	"encoding/json"
	"fmt"
	"io"
	"slices"

	"github.com/usace-cloud-compute/cc-go-sdk"
)

//@TODO revisit this and consider changing the interface/return types to avoid having empty close methods

const (
	seedsDatasourceName   string = "seeds"
	seedDatasourceKey     string = "seed_datasource_key"
	useTileDbStore        string = "use_tile_db"
	seedSetName           string = "hms-mutator"
	jsonDatasourcePathKey string = "default"
)

func GetSeeds(a cc.Action) ([]SeedSet, error) {

	useTileDb := a.Attributes.GetBooleanOrDefault(useTileDbStore, false)
	seedsKey := a.Attributes.GetStringOrFail(seedDatasourceKey)
	seedInput, err := a.GetInputDataSource(seedsKey) //expecting this to be a tiledb dense array
	if err != nil {
		return nil, err
	}

	if useTileDb {
		store, err := a.GetStore(seedInput.StoreName)
		if err != nil {
			return nil, err
		}
		seedReader := NewTileDbSeedReader(store, seedsDatasourceName, seedSetName)
		return seedReader.Read()
	} else {
		//use json
		reader, err := a.GetReader(cc.DataSourceOpInput{
			DataSourceName: seedsDatasourceName,
			PathKey:        jsonDatasourcePathKey,
		})
		if err != nil {
			return nil, err
		}
		seedReader := NewJsonSeedReader(reader)
		defer seedReader.Close()

		return seedReader.Read()
	}
}

// EventConfiguration is a simple structure to support consistency in cc plugins regarding the usage of seeds for natural variability and knowledge uncertainty and realization numbers for indexing
type EventConfiguration struct {
	Seeds map[string]SeedSet `json:"seeds" eventstore:"seeds"`
}
type SeedSet struct {
	EventSeed       int64 `json:"event_seed" eventstore:"event_seed"`
	BlockSeed       int64 `json:"block_seed" eventstore:"block_seed"`
	RealizationSeed int64 `json:"realization_seed" eventstore:"realization_seed"`
}

// type SeedReader interface {
// 	Read() ([]SeedSet, error)
// 	Close()
// }

type JsonSeedReader struct {
	reader io.ReadCloser
}

func NewJsonSeedReader(reader io.ReadCloser) *JsonSeedReader {
	return &JsonSeedReader{
		reader: reader,
	}
}

func (jsr *JsonSeedReader) Read() ([]SeedSet, error) {
	var eventConfigs []EventConfiguration
	err := json.NewDecoder(jsr.reader).Decode(&eventConfigs)
	if err != nil {
		return nil, err
	}
	seedSet := make([]SeedSet, len(eventConfigs))
	for i, eventConfig := range eventConfigs {
		hmsMutatorSet, ok := eventConfig.Seeds[seedSetName]
		if !ok {
			return nil, err
		}
		seedSet[i] = hmsMutatorSet
	}
	return seedSet, nil
}

func (jsr *JsonSeedReader) Close() {
	jsr.reader.Close()
}

type TileDbSeedReader struct {
	store       *cc.DataStore
	datasetName string
	seedSetName string
}

func NewTileDbSeedReader(tiledbStore *cc.DataStore, datasetName string, seedSetName string) *TileDbSeedReader {
	return &TileDbSeedReader{
		store:       tiledbStore,
		datasetName: datasetName,
		seedSetName: seedSetName,
	}
}

// seed list is a metadata record called "seed_columns" as a slice of string
// seeds live in a dense array based on "seed_name" with two dimensions, columns being plugins (from the metadata list), rows being events.
// array attributes are "realization_seed" as an int64 and "event_seed" as int64
func (reader *TileDbSeedReader) Read() ([]SeedSet, error) {
	seeds := make([]SeedSet, 0)

	tdbms, ok := reader.store.Session.(cc.MetadataStore)
	if !ok {
		return seeds, fmt.Errorf("the store named %v does not implement metadata store", reader.store.Name)
	}
	seedNames := make([]string, 0)
	err := tdbms.GetMetadata("seed_columns", &seedNames)
	if err != nil {
		return seeds, err
	}
	columnIndex := slices.Index(seedNames, reader.seedSetName)
	if columnIndex == -1 {
		return seeds, fmt.Errorf("the seed set name %v does not exist in the metadata store under seed_columns", reader.seedSetName)
	}
	//convert store to a dense array store
	tdbmdas, ok := reader.store.Session.(cc.MultiDimensionalArrayStore)
	if !ok {
		return seeds, fmt.Errorf("the store named %v does not implement multidimensional array store", reader.store.Name)
	}
	getArrayInput := cc.GetArrayInput{
		Attrs:    []string{"realization_seed", "block_seed", "event_seed"}, //does this have to be in the same order as it was written?
		DataPath: reader.datasetName,
		//BufferRange: []int64{0}, //how do i know how big of a buffer to input?
		//SearchOrder: cc.ROWMAJOR,
	}
	result, err := tdbmdas.GetArray(getArrayInput)
	if err != nil {
		return seeds, err
	}
	eventSeeds := make([]int64, 0)
	result.GetColumn(columnIndex, 2, &eventSeeds) //how do i know for certain attribute order?
	blockSeeds := make([]int64, 0)
	result.GetColumn(columnIndex, 1, &blockSeeds) //how do i know for certain attribute order?
	realizationSeeds := make([]int64, 0)
	result.GetColumn(columnIndex, 0, &realizationSeeds) //how do i know for certain attribute order?
	for i, es := range eventSeeds {
		seeds = append(seeds, SeedSet{EventSeed: es, BlockSeed: blockSeeds[i], RealizationSeed: realizationSeeds[i]})
	}
	return seeds, nil
}

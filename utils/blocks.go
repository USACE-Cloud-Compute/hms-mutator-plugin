package utils

import (
	"encoding/json"
	"io"

	"github.com/usace-cloud-compute/cc-go-sdk"
)

const (
	blocksDataSourceAttrKey string = "blocks_datasource_key"
	jsonBlocksPathKey       string = "default"
)

type Block struct {
	RealizationIndex int32 `json:"realization_index" eventstore:"realization_index"`
	BlockIndex       int32 `json:"block_index" eventstore:"block_index"`
	BlockEventCount  int32 `json:"block_event_count" eventstore:"block_event_count"`
	BlockEventStart  int64 `json:"block_event_start" eventstore:"block_event_start"` //inclusive - will be one greater than previous event end
	BlockEventEnd    int64 `json:"block_event_end" eventstore:"block_event_end"`     //inclusive - will be one less than event start if event count is 0.
}

// type BlockReader interface {
// 	Read() ([]Block, error)
// 	Close()
// }

func GetBlocks(pm *cc.PluginManager, a cc.Action) ([]Block, error) {
	var blocks []Block
	var err error

	useTileDb := a.Attributes.GetBooleanOrDefault(useTileDbStore, false)
	blocksKey := a.Attributes.GetStringOrFail(blocksDataSourceAttrKey)

	if useTileDb {
		blocksInput, err := a.GetInputDataSource(blocksKey) //expecting this to be tiledb
		if err != nil {
			return nil, err
		}
		//blocks, err = ReadBlocksFromTiledb(pm, blocksInput.StoreName, blocksInput.Name)
		blockReader := NewTileDbBlockReader(pm, blocksInput.StoreName, blocksInput.Name)
		return blockReader.Read()
	} else {
		jsonFileReader, err := a.GetReader(cc.DataSourceOpInput{
			DataSourceName: blocksKey,
			PathKey:        jsonBlocksPathKey,
		})
		if err != nil {
			return nil, err
		}
		blockReader := NewJsonBlockReader(jsonFileReader)
		defer blockReader.Close()
		return blockReader.Read()

	}

	return blocks, err
}

type JsonBlockReader struct {
	reader io.ReadCloser
}

func NewJsonBlockReader(reader io.ReadCloser) *JsonBlockReader {
	return &JsonBlockReader{
		reader: reader,
	}
}

func (jr *JsonBlockReader) Read() ([]Block, error) {
	var blocks []Block
	err := json.NewDecoder(jr.reader).Decode(&blocks)
	return blocks, err
}

func (jr *JsonBlockReader) Close() {
	jr.reader.Close()
}

type TileDbBlockReader struct {
	pm          *cc.PluginManager
	storeName   string
	datasetName string
}

func NewTileDbBlockReader(pm *cc.PluginManager, tileDbStoreName string, datasetName string) *TileDbBlockReader {
	return &TileDbBlockReader{
		pm:          pm,
		storeName:   tileDbStoreName,
		datasetName: datasetName,
	}
}

func (tr *TileDbBlockReader) Read() ([]Block, error) {
	blocks := make([]Block, 0)
	//get the recordset
	recordset, err := cc.NewEventStoreRecordset(tr.pm, &blocks, tr.storeName, tr.datasetName)
	if err != nil {
		return blocks, err
	}
	result, err := recordset.Read()
	if err != nil {
		return blocks, err
	}
	for i := 0; i < result.Size(); i++ {
		block := Block{}
		err = result.Scan(&block)
		if err != nil {
			return blocks, err
		}
		blocks = append(blocks, block)
	}
	return blocks, nil
}

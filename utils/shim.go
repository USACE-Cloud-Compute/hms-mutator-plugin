package utils

import (
	"bytes"
	"errors"
	"fmt"
	"io"

	"github.com/fema-ffrd/cc-go-sdk"
	filestore "github.com/usace-cloud-compute/filesapi"
)

func GetFile(pm cc.PluginManager, datasource cc.DataSource, index string) ([]byte, error) {
	data := make([]byte, 0)
	store, err := pm.GetStore(datasource.StoreName)
	if err != nil {
		return data, err
	}
	fmt.Printf("DEBUG GetFile: Got store, type: %T\n", store.Session)
	
	// Try BlockFS first (used for FS storage type)
	blockfsStore, ok := store.Session.(*cc.FileDataStore[filestore.BlockFS])
	if ok {
		reader, err := blockfsStore.Get(datasource.Paths[index], "")
		if err != nil {
			return data, err
		}
		data, err = io.ReadAll(reader)
		return data, err
	}
	
	// Try S3FS as fallback
	s3DataStore, ok := store.Session.(*cc.FileDataStore[filestore.S3FS])
	if ok {
		reader, err := s3DataStore.Get(datasource.Paths[index], "")
		if err != nil {
			return data, err
		}
		data, err = io.ReadAll(reader)
		return data, err
	}
	
	return data, errors.New("session is neither BlockFS nor S3FS")
}
func PutFile(data []byte, pm cc.IOManager, datasource cc.DataSource, index string) error {
	store, err := pm.GetStore(datasource.StoreName)
	if err != nil {
		return err
	}
	
	// Try BlockFS first (used for FS storage type)
	blockfsStore, ok := store.Session.(*cc.FileDataStore[filestore.BlockFS])
	if ok {
		writer := bytes.NewReader(data)
		_, err = blockfsStore.Put(writer, datasource.Paths[index], "")
		if err != nil {
			return err
		}
		return nil
	}
	
	// Try S3FS as fallback
	s3DataStore, ok := store.Session.(*cc.FileDataStore[filestore.S3FS])
	if ok {
		writer := bytes.NewReader(data)
		_, err = s3DataStore.Put(writer, datasource.Paths[index], "")
		if err != nil {
			return err
		}
		return nil
	}
	
	return errors.New("session is neither BlockFS nor S3FS")
}

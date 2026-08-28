package utils

import (
	"fmt"
	"os"
	"strings"

	"github.com/usace-cloud-compute/cc-go-sdk"
	"github.com/usace-cloud-compute/filesapi"
	filestore "github.com/usace-cloud-compute/filesapi"
)

func WriteLocalBytes(b []byte, destinationRoot string, destinationPath string) error {
	if _, err := os.Stat(destinationRoot); os.IsNotExist(err) {
		os.MkdirAll(destinationRoot, 0644) //do i need to trim filename?
	}
	return os.WriteFile(destinationPath, b, 0644)
}

func ListAllPaths(ioManager cc.IOManager, StoreKey string, DirectoryKey string, filter string) ([]string, error) {
	store, err := ioManager.GetStore(StoreKey)
	var pathList []string
	if err != nil {
		return pathList, err
	}

	var rawSession filesapi.FileStore // Adjust this type name to match whatever GetFilestore() returns
	if store.StoreType == "FS" {
		contents, err := os.ReadDir(DirectoryKey)
		if err != nil {
			return pathList, err
		}
		unstarredFilter := strings.ReplaceAll(filter, "*", "")
		for _, c := range contents {
			if strings.Contains(c.Name(), unstarredFilter) {
				pathList = append(pathList, c.Name())
			}
		}
		return pathList, nil
	}
	if s3Session, ok := store.Session.(*cc.FileDataStore[filestore.S3FS]); ok {
		rawSession = s3Session.GetFilestore()
	} else {
		return pathList, fmt.Errorf("%v was not an s3fs type or a BlockFs", StoreKey)
	}

	// 2. Paginate through the directories using the unified session
	pageIdx := 0
	for {
		input := filesapi.ListDirInput{
			Path:   filesapi.PathConfig{Path: DirectoryKey},
			Page:   pageIdx,
			Size:   filesapi.DEFAULTMAXKEYS,
			Filter: filter,
		}

		fapiresult, err := rawSession.ListDir(input)
		if err != nil {
			return pathList, err
		}

		list := *fapiresult
		for _, s := range list {
			pathList = append(pathList, s.Name)
		}

		// If we fetched fewer items than the max page limit, we reached the end
		if len(list) < int(filesapi.DEFAULTMAXKEYS) {
			break
		}
		pageIdx++
	}

	return pathList, nil
}

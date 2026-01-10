package utils

import (
	"fmt"
	"os"
	"strings"

	"github.com/fema-ffrd/cc-go-sdk"
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
	
	// Try BlockFS first (used for FS storage type)
	_, ok := store.Session.(*cc.FileDataStore[filestore.BlockFS])
	if ok {
		// For BlockFS, we can use os.ReadDir
		dirPath := fmt.Sprintf("/mnt/payload/%s", DirectoryKey)
		entries, err := os.ReadDir(dirPath)
		if err != nil {
			return pathList, err
		}
		for _, entry := range entries {
			if !entry.IsDir() {
				// Handle wildcard patterns
				if filter == "" {
					pathList = append(pathList, entry.Name())
				} else if matchesPattern(entry.Name(), filter) {
					pathList = append(pathList, entry.Name())
				}
			}
		}
		return pathList, nil
	}
	
	// Try S3FS as fallback
	session, ok := store.Session.(*cc.FileDataStore[filestore.S3FS])
	if !ok {
		return pathList, fmt.Errorf("%v was not an s3datastore or blockfs type", StoreKey)
	}
	rawSession := session.GetFilestore()
	//if !ok {
	//	return pathList, errors.New("could not convert s3datastore raw session into filestore type")
	//}
	pageIdx := 0 //does page index start with 0 or 1?
	input := filesapi.ListDirInput{
		Path:   filesapi.PathConfig{Path: DirectoryKey},
		Page:   pageIdx,
		Size:   filesapi.DEFAULTMAXKEYS,
		Filter: filter,
	}
	for {
		fapiresult, err := rawSession.ListDir(input)
		if err != nil {
			//check if there are files in the list?
			return pathList, err
		}
		list := *fapiresult
		for _, s := range list {
			pathList = append(pathList, s.Name)
		}
		if len(list) < 1000 {
			return pathList, nil
		} else {
			pageIdx++
		}
	}
}

// matchesPattern checks if a filename matches a wildcard pattern (e.g., "*.dss")
func matchesPattern(filename string, pattern string) bool {
	if pattern == "" {
		return true
	}
	if pattern == "*" {
		return true
	}
	if strings.HasPrefix(pattern, "*.") {
		// Handle *.extension patterns
		ext := pattern[1:] // Get ".extension"
		return strings.HasSuffix(filename, ext)
	}
	// For other patterns, use simple string contains as fallback
	return strings.Contains(filename, pattern)
}

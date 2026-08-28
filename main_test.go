package main

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/usace-cloud-compute/cc-go-sdk"
	tiledb "github.com/usace-cloud-compute/cc-go-sdk/tiledb-store"
	"github.com/usace-cloud-compute/hms-mutator/utils"
)

func Test_Main(t *testing.T) {
	main()
}
func Test_RenameStorms(t *testing.T) {
	dir := "/workspaces/hms-mutator/exampledata/trinity/storms"
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fail()
	}
	for _, e := range entries {
		name := e.Name()
		nameParts := strings.Split(name, "_")
		storm_type := strings.Split(nameParts[2], ".")[0]
		newName := fmt.Sprintf("%v/%v_72hr_%v_%v.dss", dir, nameParts[0], storm_type, nameParts[1])
		oldName := fmt.Sprintf("%v/%v", dir, name)
		os.Rename(oldName, newName)
	}
}
func Test_UpdateGridFileStormNames(t *testing.T) {
	gridFilePath := "/workspaces/hms-mutator/exampledata/trinity/trinity.grid"
	data, err := os.ReadFile(gridFilePath)
	if err != nil {
		t.Fail()
	}
	stringdata := string(data)
	//fmt.Print(stringdata)
	cc.DataStoreTypeRegistry.Register("TILEDB", tiledb.TileDbEventStore{})
	pm, err := cc.InitPluginManager()
	if err != nil {
		t.Fail()
	}
	a := pm.Actions[0]
	///get storms
	stormDirectory := a.Attributes.GetStringOrFail("storms_directory")
	stormsStoreKey := a.Attributes.GetStringOrFail("storms_store") //expecting this to be an s3 bucket?
	stormList, err := utils.ListAllPaths(a.IOManager, stormsStoreKey, stormDirectory, "*.dss")
	if err != nil {
		t.Fail()
	}
	//fmt.Print(stormList)

	//take the storm names, use them to find their precip and grids in the grid file
	for _, sn := range stormList {

		rank := sn[18:22]
		//sn = strings.Replace(sn, "st", "ST", -1)
		sn = sn[0 : len(sn)-4]
		fmt.Print(sn + ",")
		//fmt.Println(rank)
		pos := strings.Index(stringdata, rank)
		//add 18 to the front add 4 to the end
		startpos := pos - 18
		endpos := pos + 4
		name := stringdata[startpos:endpos]

		//fmt.Println(name)
		stringdata = strings.Replace(stringdata, name, sn, -1)
	}
	fmt.Println("")
	newdata := []byte(stringdata)
	os.WriteFile("/workspaces/hms-mutator/exampledata/trinity/trinity2.grid", newdata, 0600)
}

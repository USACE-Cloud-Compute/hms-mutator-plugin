package actions

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/usace/cc-go-sdk"
	tiledb "github.com/usace/cc-go-sdk/tiledb-store"
)

func Test_RecordSet(t *testing.T) {
	datapath := "/workspaces/hms-mutator/exampledata/trinity/storms.csv"
	data, err := os.ReadFile(datapath)
	if err != nil {
		t.Fail()
	}
	datastring := string(data)
	datalines := strings.Split(datastring, "\n")
	records := FullSimulationResult{}
	//skip header
	for i, r := range datalines {
		if i != 0 {
			elements := strings.Split(r, ",")
			en, err := strconv.Atoi(elements[0])
			if err != nil {
				t.Fail()
			}
			x, err := strconv.ParseFloat(elements[2], 64)
			if err != nil {
				t.Fail()
			}
			y, err := strconv.ParseFloat(elements[3], 64)
			if err != nil {
				t.Fail()
			}
			er := EventResult{
				EventNumber: int64(en),
				StormPath:   elements[1],
				X:           x,
				Y:           y,
				StormType:   elements[4],
				StormDate:   elements[5],
				BasinPath:   elements[6],
			}
			records = append(records, er)
		}
	}
	//register tiledb
	cc.DataStoreTypeRegistry.Register("TILEDB", tiledb.TileDbEventStore{})
	pm, err := cc.InitPluginManager()
	if err != nil {
		t.Fail()
	}
	err = writeResultsToTileDB(pm, "store", records, "storms")
	if err != nil {
		fmt.Println(err)
		t.Fail()
	}
}

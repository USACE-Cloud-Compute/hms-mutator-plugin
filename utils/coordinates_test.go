package utils

import (
	"fmt"
	"math/rand"
	"os"
	"strings"
	"testing"

	"github.com/dewberry/gdal"
	"github.com/usace/hms-mutator/hms"
)

func TestFuzzBuzz(t *testing.T) {
	input := Coordinate{X: 0, Y: 0}
	radius := 50000 //50km
	alpha := .05    //.05,.95 ci
	count := 10000
	seed := 1234
	output := CreateDensityList(input, alpha, float64(radius), count, int64(seed))
	fmt.Println(string(output.ToBytes()))
}
func TestGenerateMasterFishnet(t *testing.T) {
	path := "/workspaces/hms-mutator/exampledata/trinity/catalog_precip_and_temp.grid"
	transpositionpath := "/workspaces/hms-mutator/exampledata/trinity/transposition-domain.gpkg"
	tds := gdal.OpenDataSource(transpositionpath, 0)
	layer := tds.LayerByIndex(0)
	polygon := layer.Feature(1)
	radius := 50000 //50km
	alpha := .05    //.05,.95 ci
	count := 50
	seed := 1234
	stormTypes := []string{"ST1", "ST2", "ST3", "ST4", "ST5"}
	rng := rand.New(rand.NewSource(int64(seed)))
	bytes, err := os.ReadFile(path)
	if err != nil {
		t.Fail()
	}
	g, _ := hms.ReadGrid(bytes)
	for _, st := range stormTypes {
		originalCoordinates := make([]Coordinate, len(g.Events))
		for i, e := range g.Events {
			if strings.Contains(e.Name, st) {
				originalCoordinates[i] = Coordinate{X: e.CenterX, Y: e.CenterY}
			}
		}
		masterList := make([]Coordinate, 0)
		for _, input := range originalCoordinates {
			output := CreateDensityList(input, alpha, float64(radius), count, rng.Int63())
			//clip?
			masterList = append(masterList, output.Coordinates...)
		}

		fishnet := ClipDensityList(CoordinateList{Coordinates: masterList}, polygon)
		fishnet.Write("/workspaces/hms-mutator/exampledata/trinity/", fmt.Sprintf("%v_fishnet_50_clipped.csv", st))
	}

}

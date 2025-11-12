package actions

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/rand"
	"strconv"

	"github.com/usace-cloud-compute/cc-go-sdk"
	"github.com/usace-cloud-compute/hms-mutator/hms"
	"github.com/usace-cloud-compute/hms-mutator/utils"
)

//the objective of this action is to randomize the basin utilized in an HMS compute
//this allows the basin parameterization to be randomized and the anticedent conditions to be randomized.
//for simplicty, the process is based on an indexed list of basin files, that are selected randomly
//downloaded to the container, and then uploaded with a new name to the event ouptut destination.

const (
	selectBasinActionName string = "select-random-basin"
)

func init() {
	cc.ActionRegistry.RegisterAction(selectBasinActionName, &SelectBasinAction{})
}

// type SelectBasinsAction struct {
// 	cc.ActionRunnerBase
// }

type SelectBasinAction struct {
	//action   cc.Action
	//seedSet utils.SeedSet
	//inputDS  cc.DataSource
	//outputDS cc.DataSource
	cc.ActionRunnerBase
}

func (sba *SelectBasinAction) Run() error {
	action := sba.Action
	maxbasinid := action.Attributes.GetIntOrFail("maxBasinId")
	basinExtension := action.Attributes.GetStringOrFail("basinExtension")
	targetBasinFileName := action.Attributes.GetStringOrFail("targetBasinFileName")
	controlExtension := action.Attributes.GetStringOrFail("controlExtension")
	targetControlFileName := action.Attributes.GetStringOrFail("targetControlFileName")
	//allowing user specified start date to accommodate the inclusion of a setback period.
	updateStartDateAndTime, err := strconv.ParseBool(action.Attributes.GetStringOrFail("updateStartDateAndTime"))
	if err != nil {
		return err
	}
	hoursOffset := action.Attributes.GetIntOrDefault("startDateAndTimeOffset", 0)

	//get seeds
	var seedSet utils.SeedSet
	seedReader, err := sba.PluginManager.GetReader(cc.DataSourceOpInput{
		DataSourceName: "seeds",
		PathKey:        "default",
	})
	if err != nil {
		sba.Log("failed to read seeds", "error", err)
		return err
	}

	err = json.NewDecoder(seedReader).Decode(&seedSet)
	if err != nil {
		sba.Log("failed to parse seeds", "error", err)
	}

	//generate a natural variabiilty seed generator
	rng := rand.New(rand.NewSource(seedSet.EventSeed))

	//sample an int in the range of basin scenarios
	sampledBasinId := rng.Int31n(int32(maxbasinid)) //0 to exclusive upper bound

	//download the file from filesapi
	inDS, err := sba.PluginManager.GetInputDataSource("Input_Basin_Directory")
	if err != nil {
		sba.Log("unable to find the input basin data source")
		return err
	}

	inDSRoot := inDS.Paths["default"]
	inDS.Paths["default"] = fmt.Sprintf("%s/%d.%s", inDSRoot, sampledBasinId, basinExtension)
	basinbytes, err := sba.PluginManager.Get(cc.DataSourceOpInput{
		DataSource: &inDS,
		PathKey:    "default",
	})
	//fmt.Println(inDS.Paths["default"])
	//basinbytes, err := utils.GetFile(*pm, sba.inputDS, "default") //pm.GetFile(sba.inputDS, 0)
	if err != nil {
		sba.Log("failed to read basin input file", "error", err)
		return err
	}

	outDS, err := sba.PluginManager.GetOutputDataSource("Output_Basin_Directory")
	if err != nil {
		sba.Log("unable to find the output basin data source")
		return err
	}
	outDSRoot := outDS.Paths["default"]
	outDS.Paths["default"] = fmt.Sprintf("%s/%s.%s", outDSRoot, targetBasinFileName, basinExtension)
	_, err = sba.PluginManager.Put(cc.PutOpInput{
		SrcReader: bytes.NewReader(basinbytes),
		DataSourceOpInput: cc.DataSourceOpInput{
			DataSource: &outDS,
			PathKey:    "default",
		},
	})

	inDS.Paths["default"] = fmt.Sprintf("%s/%d.%s", inDSRoot, sampledBasinId, controlExtension)
	controlbytes, err := sba.PluginManager.Get(cc.DataSourceOpInput{
		DataSource: &inDS,
		PathKey:    "default",
	})

	control, err := hms.ReadControl(controlbytes)
	if err != nil {
		return err
	}

	controltime, err := control.StartDateAndTime()
	if err != nil {
		return err
	}

	if updateStartDateAndTime {
		controltime, err = control.AddHoursToStart(hoursOffset)
		if err != nil {
			return err
		}
		controlbytes = control.ToBytes()
	}

	//upload the file to filesapi with the appropriate new name.
	outDS.Paths["default"] = fmt.Sprintf("%s/%s.%s", outDSRoot, targetControlFileName, controlExtension)
	fmt.Println(outDS.Paths["default"])
	sba.Log("uploading random basin", "desination", outDS.Paths["default"])

	_, err = sba.PluginManager.Put(cc.PutOpInput{
		SrcReader: bytes.NewReader(controlbytes),
		DataSourceOpInput: cc.DataSourceOpInput{
			DataSource: &outDS,
			PathKey:    "default",
		},
	})
	return err
}

// func InitSelectBasinAction(action cc.Action, seedSet utils.SeedSet, inputDs cc.DataSource, outputDS cc.DataSource) *SelectBasinAction {
// 	sba := SelectBasinAction{
// 		action:   action,
// 		seedSet:  seedSet,
// 		inputDS:  inputDs,
// 		outputDS: outputDS,
// 	}
// 	return &sba
// }

// func (sba SelectBasinAction) Compute() (time.Time, error) {
// 	//get range of basin scenarios (ints between 0 and n?)
// 	maxbasinid := sba.action.Attributes.GetIntOrFail("maxBasinId")
// 	basinExtension := sba.action.Attributes.GetStringOrFail("basinExtension")
// 	targetBasinFileName := sba.action.Attributes.GetStringOrFail("targetBasinFileName")
// 	controlExtension := sba.action.Attributes.GetStringOrFail("controlExtension")
// 	targetControlFileName := sba.action.Attributes.GetStringOrFail("targetControlFileName")
// 	//allowing user specified start date to accommodate the inclusion of a setback period.
// 	updateStartDateAndTime, err := strconv.ParseBool(sba.action.Attributes.GetStringOrFail("updateStartDateAndTime"))
// 	if err != nil {
// 		return time.Now(), err
// 	}
// 	hoursOffset := sba.action.Attributes.GetIntOrDefault("startDateAndTimeOffset", 0)

// 	//generate a natural variabiilty seed generator
// 	rng := rand.New(rand.NewSource(sba.seedSet.EventSeed))

// 	//sample an int in the range of basin scenarios
// 	sampledBasinId := rng.Int31n(int32(maxbasinid)) //0 to exclusive upper bound
// 	//download the file from filesapi
// 	pm, err := cc.InitPluginManager()
// 	if err != nil {
// 		return time.Now(), err
// 	}
// 	inDS := sba.inputDS
// 	inDSRoot := inDS.Paths["default"]
// 	inDS.Paths["default"] = fmt.Sprintf("%v/%v.%v", inDSRoot, fmt.Sprint(sampledBasinId), basinExtension)
// 	//fmt.Println(inDS.Paths["default"])
// 	basinbytes, err := utils.GetFile(*pm, sba.inputDS, "default") //pm.GetFile(sba.inputDS, 0)
// 	if err != nil {
// 		return time.Now(), err
// 	}
// 	//upload the file to filesapi with the appropriate new name.
// 	outDS := sba.outputDS
// 	outDSRoot := outDS.Paths["default"]
// 	outDS.Paths["default"] = fmt.Sprintf("%v/%v.%v", outDSRoot, targetBasinFileName, basinExtension)
// 	//fmt.Println(outDS.Paths["default"])
// 	err = utils.PutFile(basinbytes, pm.IOManager, sba.outputDS, "default")
// 	if err != nil {
// 		return time.Now(), err
// 	}

// 	inDS.Paths["default"] = fmt.Sprintf("%v/%v.%v", inDSRoot, fmt.Sprint(sampledBasinId), controlExtension)
// 	//fmt.Println(inDS.Paths["default"])
// 	controlbytes, err := utils.GetFile(*pm, sba.inputDS, "default")
// 	if err != nil {
// 		return time.Now(), err
// 	}
// 	control, err := hms.ReadControl(controlbytes)
// 	if err != nil {
// 		return time.Now(), err
// 	}
// 	controltime, err := control.StartDateAndTime()
// 	if err != nil {
// 		return controltime, err
// 	}
// 	if updateStartDateAndTime {
// 		controltime, err = control.AddHoursToStart(hoursOffset)
// 		if err != nil {
// 			return time.Now(), err
// 		}
// 		controlbytes = control.ToBytes()
// 	}

// 	//upload the file to filesapi with the appropriate new name.
// 	outDS.Paths["default"] = fmt.Sprintf("%v/%v.%v", outDSRoot, targetControlFileName, controlExtension)
// 	fmt.Println(outDS.Paths["default"])
// 	err = utils.PutFile(controlbytes, pm.IOManager, sba.outputDS, "default")
// 	if err != nil {
// 		return time.Now(), err
// 	}
// 	return controltime, nil
// }

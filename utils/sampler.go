package utils

import (
	"math/rand"
	"time"
)

type StormSampler interface {
	SampleNames(event int64, realization int64, seeds []SeedSet) error
	SampleName(rng *rand.Rand) string
	SamplingLevel() string
}
type BaseSamplerData struct {
	StormNames    []string
	yearStormMap  map[int][]string
	sampleNames   []string
	minYear       int32
	maxYear       int32
	samplingLevel string
}
type BootstrapSampler struct {
	*BaseSamplerData
}

func initBaseSamplerData(Names []string, samplingLevel string, startDate time.Time) (*BaseSamplerData, error) {
	b := BaseSamplerData{}
	b.yearStormMap = make(map[int][]string)
	minYear := startDate.Year()
	maxYear := 0
	for _, n := range Names {
		//extract year from name.
		//yyyymmdd_xxhr_storm-type_storm-rank
		year, err := time.Parse("20060102", n[0:8])
		if err != nil {
			return &b, err
		}
		adjustedYear := year.Year()
		if year.YearDay() >= startDate.YearDay() {
			adjustedYear += 1
		}
		storms, ok := b.yearStormMap[adjustedYear]
		if ok {
			storms = append(storms, n)
			b.yearStormMap[adjustedYear] = storms
		} else {
			b.yearStormMap[adjustedYear] = []string{n}
		}
		if adjustedYear > maxYear {
			maxYear = adjustedYear
		}
		if adjustedYear < minYear {
			minYear = adjustedYear
		}
	}
	b.maxYear = int32(maxYear)
	b.minYear = int32(minYear)
	b.StormNames = Names
	b.samplingLevel = samplingLevel
	return &b, nil
}
func InitBootstrapSampler(Names []string, samplingLevel string, startdate time.Time) (*BootstrapSampler, error) {
	bdata, err := initBaseSamplerData(Names, samplingLevel, startdate)
	b := BootstrapSampler{BaseSamplerData: bdata}
	if err != nil {
		return &b, err
	}
	return &b, nil
}
func (b *BootstrapSampler) SamplingLevel() string {
	return b.samplingLevel
}
func (b *BootstrapSampler) SampleNames(event int64, realization int64, seeds []SeedSet) error {

	stormCount := len(b.StormNames)
	sample := make([]string, 0)
	rng := rand.New(rand.NewSource(seeds[event].EventSeed)) //check with haden.
	delta := b.maxYear - b.minYear
	i := 0
	for {
		tmpStorms := b.yearStormMap[int(b.minYear+rng.Int31n(delta))]
		for _, s := range tmpStorms {
			sample = append(sample, s)
			i++
			if i >= stormCount {
				break

			}
		}
		if i >= stormCount {
			break

		}
	}
	b.sampleNames = sample
	return nil
}
func (b *BootstrapSampler) SampleName(rng *rand.Rand) string {
	stormCount := len(b.sampleNames)
	return b.sampleNames[rng.Int31n(int32(stormCount))]
}

type JackknifeSampler struct {
	*BaseSamplerData
}

func InitJackknifeSampler(Names []string, samplingLevel string, startdate time.Time) (*JackknifeSampler, error) {
	//collect names into common years and sample as year groups
	bdata, err := initBaseSamplerData(Names, samplingLevel, startdate)
	b := JackknifeSampler{BaseSamplerData: bdata}
	if err != nil {
		return &b, err
	}
	return &b, nil
}
func (b *JackknifeSampler) SamplingLevel() string {
	return b.samplingLevel
}
func (b *JackknifeSampler) SampleNames(event int64, realization int64, seeds []SeedSet) error {

	// Initialize with 0 length but pre-allocate capacity for performance
	numYearsToGroup := 5 //consider making this a parameter
	sample := make([]string, 0)
	delta := b.maxYear - b.minYear - int32(numYearsToGroup) + 1
	modReal := int32(realization) % delta
	skipYearMin := b.minYear + modReal - 1
	skipYearMax := skipYearMin + int32(numYearsToGroup)

	for year := b.minYear; year <= b.maxYear; year++ {
		if year >= skipYearMin && year <= skipYearMax {
			continue
		}
		tmpStorms := b.yearStormMap[int(year)]
		for _, s := range tmpStorms {
			sample = append(sample, s)
		}
	}

	b.sampleNames = sample
	return nil
}
func (b *JackknifeSampler) SampleName(rng *rand.Rand) string {
	stormCount := len(b.sampleNames)
	return b.sampleNames[rng.Int31n(int32(stormCount))]
}

type BestEstimateSampler struct {
	StormNames    []string
	samplingLevel string
}

func InitBestEstimateSampler(Names []string, samplingLevel string) (*BestEstimateSampler, error) {
	b := BestEstimateSampler{}
	b.StormNames = Names
	b.samplingLevel = samplingLevel
	return &b, nil
}
func (b *BestEstimateSampler) SamplingLevel() string {
	return b.samplingLevel
}
func (b *BestEstimateSampler) SampleNames(event int64, realization int64, seeds []SeedSet) error {
	return nil
}
func (b *BestEstimateSampler) SampleName(rng *rand.Rand) string {
	stormCount := len(b.StormNames)
	return b.StormNames[rng.Int31n(int32(stormCount))]
}

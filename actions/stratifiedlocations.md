# Fishnets
The hms-mutator has a few different options for pre-generating a list of viable storm locations to be leveraged in the full_simulation_sst action.
All of these methods output a csv file of x and y coordinates. The original methods utilized uniformly spaced points in x and y, some of the more modern options are based on normal density kernals.  These lists of points are randomly generated but contain a sufficent set of points for resampling in the full_simulation_sst action. The lists are typically named by storm name so that given the selection of a storm, the points in the list are valid destinations for that storm to be moved. This is a preprocess step that can occur any time after the storm catalog has been generated (and storm typed if storm typed locations are being generated).

## Implementation details
The most basic method is to evaluate "valid" locations based on not allowing null data to cover the study area polygon. In general this approach relies on the assumption that the transposition domain represents a spatial area where any storm drawn from it is equiprobable to happen anywhere else in the transposition domain. A storm is evaluated by taking a uniform fishnet at standard spacing (4km or 1km) generated across the entire domain of the transposition region. The storm center is compared to the candidate point to evaluate an offset in x and y, the study area is shifted by the inverse of that offset and all points in the study area are evaluated to be contained by the transposition domain. if all points are contained, it is a valid placement and the next placement is evaluated. This continues for all placements for that storm, and then is performed for all storms in the database. DetermineValidStormPlacementsQUickly performs this activity in parallel to accomplish the task more quickly. 

The other options for normal density kernals and storm typed normal density kernals operate off of the storm catalog, in general the approach relies on the assumption that the structure of storm placements historically within the transposition domain is influenced by characteristics within the domain that may make the storm placements non equiprobable. So the historic storm placements are used to center the likely distribution of future placements. A normal density kernal centered on each of the original storm centers from the catalog is generated with an applied radius defined by the user. A variation on this is to allow storms of a given type to center on original placements of storms of that given type. 

## Process Flow
NOthing significant to report.

## Configuration
The configuration below is simply for the `valid_stratified_locations` action
```
{
	"stores": [{
		"name": "FFRD",
		"id": "6ba7b810-9dad-11d1-80b4-00c04fd430c8",
		"store_type": "S3",
		"profile": "FFRD",
		"params": {
			"root": "model-library/ffrd-trinity"
		}
	}],
	"attributes": {
		"scenario": "conformance",
		"outputroot": "simulations"
	},
	"inputs": [{
		"name": "HMS Model",
		"id": "6ba7b810-9dad-11d1-80b4-00c04fd430c8",
		"paths": {
			"grid":"{ATTR::scenario}/storm-catalog/catalog.grid",
		},
		"store_name": "FFRD"
	},{
		"name": "TranspositionRegion",
		"id": "6ba7b810-9dad-11d1-80b4-00c04fd430c8",
		"paths": {
			"default":"{ATTR::scenario}/storm-catalog/transposition-domain.gpkg"
		},
		"store_name": "FFRD"
	},{
		"name": "WatershedBoundary",
		"id": "6ba7b810-9dad-11d1-80b4-00c04fd430c8",
		"paths": {
			"default":"{ATTR::scenario}/storm-catalog/watershed-boundary.gpkg"
		},
		"store_name": "FFRD"
	}],
	"outputs": [{
		"name": "ValidLocations",
		"id": "6ba7b810-9dad-11d1-80b4-00c04fd430c8",
		"paths": {"default":"{ATTR::scenario}/storm-catalog/fishnets-fast"},
		"store_name": "FFRD"
	}],
	"actions": [{
		"type": "valid_stratified_locations",
		"description": "create stratified fishnets for storms",
		"attributes": {
			"_comment": "4km in meters",
			"spacing": 4000
		}
	}]
}
```

### Inputs

#### Environment
This is a preprocess, no environment variables are needed except for those associated with the cc_store and any other store in the manifest.
#### Attributes
If the user uses attribute substitution in paths in the datasources, those attributes should be defined in the global attributes. All action specific attributes must be in the action.
##### Global

##### Action
- spacing: the spacing in kilometers, should be consistent with the spacing of the input precipitation grids in the catalog. For AORC data it is typically 4km or 1km.
#### Data Sources
Three input datasources are required, and they are required as payload level datasources.
- the HMS grid file. The hms grid file is required because it contains all storm names, and their original x y coordinates. The input datasource should be defined named `HMS Model` with a datasource path of `grid` and the path must have an extension of `.grid`
- the transposition domain. The transposition domain is a geopackage that must be in the same coordinate system as the grid file grid coordinates. The geopackage can only have one geometry. The datasource name must be "TranspositionRegion" with a path of `default`
- the watershed domain. The watershed domain is a geopackage that must be in the same coordinate system as the grid file grid coordinates, it must also by definition be fully contained by the transposition domain. The geopackage can only have one geometry. The datasource name must be "WatershedBoundary" with a path of `default`
### Outputs
There is one required output datasource:
- The validplacements directory. The name must be `ValidLocations` and there should be a path for where all named csv files will be dumped in the `default` path.

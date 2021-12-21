## Changes in 0.9.4 (in development)

## Changes in 0.9.3

* Always show time bounds as coordinate, not as data variable
* Prevent IndexError when requesting data with a time range

## Changes in 0.9.2

* Fixed issue where opening datasets in a daily resolution without a delimiting
  time range would cause memory errors 
  [#56](https://github.com/dcs4cop/xcube-cci/issues/56).

## Changes in 0.9.1
* Fixed issue that datasets with spatial bounds could not be opened.
* Show version as `__version__`
* The interface of the method `search_data` of the CciOdpDataStore 
  has been changed. Search parameters `ecv`, `frequency` `institute`,
  `processing_level`, `product_string`, `product_version`, `data_type`,
  `sensor`, and `platform` may now be passed in a dictionary parameter named 
  `cci_attrs`. This makes it possible again to use the parameter `data_type` 
  [#54](https://github.com/dcs4cop/xcube-cci/issues/54).

## Changes in 0.9.0
* Version 0.9 now requires xcube 0.9 because of incompatible API changes in the 
  xcube data store framework.
* CciOdpCubeOpener has been removed.
* CciOdpDatasetOpener and CciOdpDataStore now have a new constructor parameter 
  `normalize`, that may be used to apply normalization steps to the CCI 
  datasets.
* Set coordinates correctly. All coordinates are present in data descriptions
  and opened datasets, no coordinates appear as data variables 
  [#42](https://github.com/dcs4cop/xcube-cci/issues/42).
* CRS are supported correctly. CRS variables are present in datasets in case 
  the CRS is different from 'WGS84', the CRS information is provided by the
  data descriptor [#50](https://github.com/dcs4cop/xcube-cci/issues/50).

## Changes in 0.8.1

* Fixed an issue that caused that occasionally values returned by open_data consisted 
  of random numbers where a fill value would have been expected. (#47) 
* DataDescriptors contain coords
* Internal virtual file system is built lazily, so opening datasets has become faster.
* Store parameter method `get_search_params_schema` has been revised to correctly support 
  all parameter values.
* Support more datasets from ODP.
* Fixed support of `user_agent` parameter
* Added CCI Zarr Store as convenience store to access cci zarr datasets

## Changes in 0.8.0

* Added `user_agent` as additional optional store parameter.
* Provided xcube data store framework interface compatibility with 
  breaking changes in xcube 0.8.0 (see https://github.com/dcs4cop/xcube/issues/420).

## Changes in 0.7.0
* Removed constant-valued parameters from opener schema
* Renamed store parameters `opensearch_url` and `opensearch_description_url` to
  `endpoint_url` and `endpoint_description_url`, respectively.
* Chunkstore considers bounding box when accessing data. Less data is accessed and normalized. (#33)
* Fixed time range detection for datasets with daily time frequency.
* Fixed problem with the encoding of a dataset's coordinate variables that occurs 
  when using `xcube_cci` with xcube 0.6.0. (#27)
* Removed CLI

## Changes in 0.6.0.
* Support type specifiers [#18](https://github.com/dcs4cop/xcube-cci/issues/18). 
The CCI Store supports type specifiers `dataset` and `dataset[cube]`
* Descriptions of variables and dimensions are different for the same dataset, 
depending on what type specifier is set.
* There are now two DataOpeners: The CciOdpDatasetOpener and the CciOdpCubeOpener.
Both openers are used by the CciOdpDataStore, with the CciOdpDatasetOpener being the default.
The CciOdpDatasetOpener will open any data sets from the CCI Open Data Portal without changing their dimensions.
The CciOdpCubeOpener will normalize datasets to have dimensions `lat`, `lon`, `time` (and possibly others).
Subsetting is only supported for data cubes. 
As not all datasets can be normalized to cubes, the CciOdpCubeOpener supports a subset of the datasets that can be accessed with the CciOdpDatasetOpener.
* Establish common data store conventions ([#10](https://github.com/dcs4cop/xcube-cci/issues/10)
* xcube-cci can now get the time ranges for satellite-orbit-frequency datasets available via opensearch 
* Introduced new optional parameters to CciStore:
    - enable_warnings
    - num_retries
    - _retry_backoff_max
    - _retry_backoff_base
* Updated setup.py [#16](https://github.com/dcs4cop/xcube-cci/issues/16)
* Added opener parameters `time_range` and `spatial_res`
* String-encoded variables are added to datasets as metadata
* Updated example notebooks

## Changes in 0.5.0.
 
Initial version. 
This version has been designed to work with the `xcube` store framework that has been introduced with
`xcube` v0.5.0.
It affords
- a CciOdpDataOpener Implementaton for opening datasets from the ESA CCI Open Data Portal. 
The Opener has open parameters `variable_names`, `time_period`, `bbox`, and `crs`.
- a CciStore Implementation that uses and extends the aforementioned opener and allows for searching 
the ESA CCI Open Data Portal

## Changes in 0.7.1 (in development)

* Provided xcube data store framework interface compatibility with 
  minor changes in xcube 0.7.1 (see https://github.com/dcs4cop/xcube/issues/420).

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

## Changes in 0.6.0.
* Establish common data store conventions ([#10](https://github.com/dcs4cop/xcube-cci/issues/10)
* xcube-cci can now get the time ranges for satellite-orbit-frequency datasets available via opensearch 
* Introduced new optional parameters to CciStore:
    - enable_warnings
    - num_retries
    - _retry_backoff_max
    - _retry_backoff_base
* Descriptions of variables and dimensions are different when data is normalized. 
* In case the CciOdpDataStore or the CciOdpDataOpener is initialized with `normalize_data` set to True, 
dimensions will be normalized to `lat`, `lon`, `time` and possibly additional dimensions. 
Variables that cannot be normalized to use these dimensions will not be shown.
If `normalize_data` is False, dimensions will not be changed and all data variables will be shown 
(i.e., all variables that are not dimensionns, that are numeric and that have more than one dimension).
* Updated setup.py [#16]()https://github.com/dcs4cop/xcube-cci/issues/16)
* Added opener parameters `time_range` and `spatial_res`

## Changes in 0.5.0.
 
Initial version. 
This version has been designed to work with the `xcube` store framework that has been introduced with
`xcube` v0.5.0.
It affords
- a CciOdpDataOpener Implementaton for opening datasets from the ESA CCI Open Data Portal. 
The Opener has open parameters `variable_names`, `time_period`, `bbox`, and `crs`.
- a CciStore Implementation that uses and extends the aforementioned opener and allows for searching 
the ESA CCI Open Data Portal

## Changes in 0.6.0. dev

- xcube-cci can now get the time ranges for satellite-orbit-frequency datasets available via opensearch 

## Changes in 0.5.0.
 
Initial version. 
This version has been designed to work with the `xcube` store framework that has been introduced with
`xcube` v0.5.0.
It affords
- a CciOdpDataOpener Implementaton for opening datasets from the ESA CCI Open Data Portal. 
The Opener has open parameters `variable_names`, `time_period`, `bbox`, and `crs`.
- a CciStore Implementation that uses and extends the aforementioned opener and allows for searching 
the ESA CCI Open Data Portal

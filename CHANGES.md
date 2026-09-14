## Changes in 0.14.3 (in development)

## Changes in 0.14.2

* Avoid redundant requests to the open data portal, 
  thereby preventing time out errors and improving performance
* Updated list of data states

## Changes in 0.14.1

* Chunks are cached with the dataset id as key (avoiding issue when using the same store
  to open multiple datasets)   
* Improved error messages when data cannot be accessed from opendap by also stating the url
* Avoid access errors when accessing all features over opendap (usually an issue when determining time steps)

## Changes in 0.14

  * Improved error messages when data cannot be accessed from opendap
  * added caching of chunks using a Least-Recently-Used-Cache
    Cache Size in MB can be set for the store using the newly added parameter `cache_size`
  * avoid concurrent calls of the same request
  * Ensure classes `SessionExecutor` and `CciOdp` can be serialised and de-serialised. 
    This is necessary for support of dask clusters.
  * Updated list of data states

## Changes in 0.13.1

* Updated list of dataset states

## Changes in 0.13

* Class `SessionExecutor` now runs all asynchronous tasks in a single dedicated loop. This change
  * increases performance of the `cciodp` store
  * causes that the package `nest-asyncio` is no longer needed
  * ensures compatibility with python 3.14
* Read CRS information from tif files

## Changes in 0.12.1
* Avoid FileNotFoundError when opening store [#70](https://github.com/dcs4cop/xcube-cci/issues/70)

## Changes in 0.12
* Updated list of kerchunk datasets
* Added xarray Data Trees as fourth data model. Data Trees provide a tree-like 
  hierarchical structure of datasets, where single nodes can be accessed by an
  identifier. This is a preferable approach to splitting datasets into sub-datasets, 
  as. e.g., "esacci.FIRE.mon.L3S.BA.MSI-(Sentinel-2).Sentinel-2A.MSI.2-0.pixel~h41v14-fv2.0-JD"
  The solution has been made available for FIRE, LC, and VEGETATION datasets. 
  A full list of available datasets can be retrieved with the store method
  `get_data_ids(data_type="datatree")`.
  To open a data tree with a subset of datasets, pass `place_names` as parameter 
  to the store method `open_params()`. You can get a list of available place names 
  from `get_open_data_params_schema()`.
* In concordance with the change above, the support for the dataset 
  "esacci.RD.satellite-orbit-frequency.L3S.WL.multi-sensor.multi-platform.MERGED.v1-1.r1" 
  has been modified so it is no longer split up into single datasets as, e.g. 
  "esacci.RD.satellite-orbit-frequency.L3S.WL.multi-sensor.multi-platform.MERGED.v1-1.r1~AMAZON_NEGRO_SAO-FELIPE".
  To open the dataset with a subset of places, pass `place_names` as parameter 
  to the store method `open_params()`. You can get a list of available place names 
  from `get_open_data_params_schema()`.
* We fixed an issue with geodataframes which could cause that not all data were read in.
* Fixed issues that data could not be accessed from ICESHEETS datasets with multiple time steps in base files.
  Solves [#69](https://github.com/dcs4cop/xcube-cci/issues/69)

## Changes in 0.11.10
* Revised warnings
* Removed redundant attempt loops, resulting in faster opening of some datasets (e.g., FIRE)

## Changes in 0.11.9
* Enabled support for BIOMASS change data v6.0
* Updated list of dataset states

## Changes in 0.11.8
* Read RIVER DISCHARGE data as geodataframes
* Avoid ClientPayloadResponseErrors
* Avoid warning about duplicate fill values
* Do not convert integer values to unsigned integer values
* Allow use of environment variables to set data location

## Changes in 0.11.7
* Improved determination of coordinate reference systems
* Restricted version of external pydap package to avoid unstable environments

## Changes in 0.11.6
* Fixed bug causing some ICESHEETS datasets where the time dimension is named
  differently than the time coordinate to not be opened
* Improved chunking behaviour which caused some datasets to take very long 
  to open and others to not open at all
* updated dataset states (supporting more datasets from the odp)

## Changes in 0.11.5
* Improved support of dataset with unsigned integers
* Improved determination of chunks (and thereby handling of, e.g., PERMAFROST datasets)
* The `get_data_ids()` method in `CciOdpDataStore`, `CciZarrDataStore` 
  and `CciKerchunkDataStore` has an enhanced include_attrs parameter.
  Previously accepting only Container[str], it now also supports a bool value.
  Setting include_attrs to True retrieves all attributes of the data_ids. 

## Changes in 0.11.4
* Maintenance release to handle changes from external package

## Changes in 0.11.3
* Fixed retrieval of data sets with unsigned data types 
  (this affected SNOW datasets)
* Increased list of supported kerchunk datasets

## Changes in 0.11.2
* Fixed retrieval of time steps of BIOMASS datasets

## Changes in 0.11.1
* Improved support of vector data cubes
* Improved support of FIRE ECV datasets

## Changes in 0.11

* Migrated code from `ESA CCI Toolbox`. This encompasses the following changes:
  * Added store aliases `esa-cdc` and  `esa-climate-data-centre`, `esa-cci` 
    and `esa-cci-zarr`
  * Added new data store `esa-cci-kc` 
    (and corresponding xcube data store `esa-cci-kc`) that allows performant 
    accessing of selected datasets of the ESA Climate Data Centre using a 
    Zarr view of the original NetCDF files. This approach is made possible by 
    using the [kerchunk](https://fsspec.github.io/kerchunk/) package. Also 
    added new Notebook that demonstrates usage of the new data store.
  * Added opener `"datafame:geojson:esa-cdc"` to open data as data frames
  * Added opener `"vectordatacube::esa-cdc"` to open data as vector data cubes
  * Updated list of dataset states
* Set up pipeline to publish package on pypi

## Changes in 0.10.2

* Fixed support for climatology datasets

## Changes in 0.10.1

* Fixed support for SST climatology dataset 

## Changes in 0.10

* Support Climatology Datasets
* Ensure compatiblity with Python versions > 3.10. This concerns the way 
  new event loops are created within threads. A deprecated event loop 
  policy has been removed. 
  Solves issues [#61](https://github.com/dcs4cop/xcube-cci/issues/61)
  and [#64](https://github.com/dcs4cop/xcube-cci/issues/64)

## Changes in 0.9.9

* Removed cache
* Pinned pydap version
* Improved determination of CCI data chunks

## Changes in 0.9.8

* Consider environment parameter `READ_CEDA_CATALOGUE` to trigger whether 
  metadata from the ceda catalogue shall be read. Enabled by default.
* Updated list of supported datasets. 
  No odp-provided datasets are excluded anymore.  

## Changes in 0.9.7

* Zarr Datastore reads data ids from json file if provided
* Updated CCI Store to set zarr store (from here on, xcube 0.12.1 is required) 
  [#62](https://github.com/dcs4cop/xcube-cci/issues/62)

## Changes in 0.9.6

* Updated Example Notebooks
* Extended support of datasets. Previously, datasets which had a chunking of 
  the time dimension larger than one were not handled properly. This affected 
  OZONE in a monthly resolution, SEALEVEL in a monthly resolution, and several
  ICESHEETS datasets. [#12](https://github.com/dcs4cop/xcube-cci/issues/12)
* Chunk sizes may now also be decreased, in order to achieve optimal chunkings.
  For some datasets (e.g., BIOMASS), this increases the performance and helps
  avoiding memory issues. [#48](https://github.com/dcs4cop/xcube-cci/issues/48)

## Changes in 0.9.5

* Allow reading of datasets that do not specify chunk sizes (e.g., FIRE) 
  [Cate #1033](https://github.com/CCI-Tools/cate/issues/1033).

## Changes in 0.9.4

* Ensure compatibility with xarray >= 0.20.2 and python >= 3.10
* Ensure compatibility with expected odp update 

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

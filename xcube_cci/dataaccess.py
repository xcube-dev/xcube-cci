# The MIT License (MIT)
# Copyright (c) 2020 by the xcube development team and contributors
#
# Permission is hereby granted, free of charge, to any person obtaining a copy of
# this software and associated documentation files (the "Software"), to deal in
# the Software without restriction, including without limitation the rights to
# use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
# of the Software, and to permit persons to whom the Software is furnished to do
# so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

from abc import abstractmethod
from typing import Any, Iterator, List, Tuple, Optional

import xarray as xr
import zarr

from xcube.core.normalize import normalize_dataset
from xcube.core.store import TYPE_SPECIFIER_CUBE
from xcube.core.store import TYPE_SPECIFIER_DATASET
from xcube.core.store import TypeSpecifier
from xcube.core.store import DataDescriptor
from xcube.core.store import DataOpener
from xcube.core.store import DataStore
from xcube.core.store import DataStoreError
from xcube.core.store import DatasetDescriptor
from xcube.core.store import VariableDescriptor
from xcube.util.jsonschema import JsonArraySchema
from xcube.util.jsonschema import JsonBooleanSchema
from xcube.util.jsonschema import JsonDateSchema
from xcube.util.jsonschema import JsonIntegerSchema
from xcube.util.jsonschema import JsonNumberSchema
from xcube.util.jsonschema import JsonObjectSchema
from xcube.util.jsonschema import JsonStringSchema
from xcube_cci.cciodp import CciOdp
from xcube_cci.chunkstore import CciChunkStore
from xcube_cci.constants import CCI_ODD_URL
from xcube_cci.constants import CUBE_OPENER_ID
from xcube_cci.constants import DATASET_OPENER_ID
from xcube_cci.constants import DEFAULT_NUM_RETRIES
from xcube_cci.constants import DEFAULT_RETRY_BACKOFF_BASE
from xcube_cci.constants import DEFAULT_RETRY_BACKOFF_MAX
from xcube_cci.constants import OPENSEARCH_CEDA_URL
from xcube_cci.normalize import normalize_cci_dataset
from xcube_cci.normalize import normalize_dims_description
from xcube_cci.normalize import normalize_variable_dims_description
from xcube_cci.subsetting import subset_spatial

CCI_ID_PATTERN = 'esacci\..+\..+\..+\..+\..+\..+\..+\..+\..+'
CRS_PATTERN = 'http://www.opengis.net/def/crs/EPSG/0/[0-9]{4,5}'
WKT_PATTERN = '[A-Z]*\(\([0-9 0-9,*]+\)\)'
TIME_PERIOD_PATTERN = '[0-9]+[Y|M|W|D|T|S|L|U|N|days|day|hours|hour|hr|h|minutes|minute|min|m|seconds|second|sec|' \
                      'milliseconds|millisecond|millis|milli|microseconds|microsecond|micros|micro|' \
                      'nanoseconds|nanosecond|nanos|nano|ns]'
_FREQUENCY_TO_ADJECTIVE = {
    'mon': 'Monthly',
    'day': 'Daily',
    'satellite-orbit-frequency': '',
    '5-days': '5 day',
    '8-days': '8 day',
    '10-days': '10 day',
    'climatology': '',
    '13-yrs': '13 year',
    '15-days': '15 day',
    '5-yrs': '5 year',
    'yr': 'year',
    'unspecified': ''
}
_RELEVANT_METADATA_ATTRIBUTES = ['ecv', 'institute', 'processing_level', 'product_string',
                                 'product_version', 'data_type', 'abstract', 'title', 'licences',
                                 'publication_date', 'catalog_url', 'sensor_id', 'platform_id',
                                 'cci_project', 'description', 'project', 'references', 'source',
                                 'history', 'comment']


def _normalize_dataset(ds: xr.Dataset) -> xr.Dataset:
    ds = normalize_cci_dataset(ds)
    ds = normalize_dataset(ds)
    return ds


def _get_temporal_resolution_from_id(data_id: str) -> Optional[str]:
    data_time_res = data_id.split('.')[2]
    time_res_items = dict(D=['days', 'day'],
                          M=['months', 'mon', 'climatology'],
                          Y=['yrs', 'yr', 'year'])
    for time_res_pandas_id, time_res_ids_list in time_res_items.items():
        for i, time_res_id in enumerate(time_res_ids_list):
            if time_res_id in data_time_res:
                if i == 0:
                    return f'{data_time_res.split("-")[0]}{time_res_pandas_id}'
                return f'1{time_res_pandas_id}'


class CciOdpDataOpener(DataOpener):

    def __init__(self, cci_odp: CciOdp, id:str, type_specifier: TypeSpecifier):
        self._cci_odp = cci_odp
        self._id = id
        self._type_specifier = type_specifier

    @property
    def dataset_names(self) -> List[str]:
        return self._cci_odp.dataset_names

    def _describe_data(self, data_ids: List[str]) -> List[DatasetDescriptor]:
        ds_metadata_list = self._cci_odp.get_datasets_metadata(data_ids)
        data_descriptors = []
        for i, ds_metadata in enumerate(ds_metadata_list):
            data_descriptors.append(self._get_data_descriptor_from_metadata(data_ids[i], ds_metadata))
        return data_descriptors

    def describe_data(self, data_id: str) -> DataDescriptor:
        self._assert_valid_data_id(data_id)
        try:
            ds_metadata = self._cci_odp.get_dataset_metadata(data_id)
            return self._get_data_descriptor_from_metadata(data_id, ds_metadata)
        except ValueError:
            raise DataStoreError(f'Cannot describe metadata. "{data_id}" does not seem to be a valid identifier.')

    # noinspection PyArgumentList
    def _get_data_descriptor_from_metadata(self, data_id: str, metadata: dict) -> DatasetDescriptor:
        ds_metadata = metadata.copy()
        dims = self._normalize_dims(ds_metadata.get('dimensions', {}))
        if 'time' not in dims:
            dims['time'] = ds_metadata.get('time_dimension_size')
        else:
            dims['time'] *= ds_metadata.get('time_dimension_size')
        temporal_resolution = _get_temporal_resolution_from_id(data_id)
        dataset_info = self._cci_odp.get_dataset_info(data_id, ds_metadata)
        spatial_resolution = dataset_info['lat_res']
        if spatial_resolution <= 0:
            spatial_resolution = None
        bbox = dataset_info['bbox']
        # only use date parts of times
        temporal_coverage = (dataset_info['temporal_coverage_start'].split('T')[0],
                             dataset_info['temporal_coverage_end'].split('T')[0])
        var_descriptors = {}
        var_infos = ds_metadata.get('variable_infos', {})
        var_names = dataset_info['var_names']
        for var_name in var_names:
            if var_name in var_infos:
                var_info = var_infos[var_name]
                var_dtype = var_info['data_type']
                var_dims = self._normalize_var_dims(var_info['dimensions'])
                if var_dims:
                    var_descriptors[var_name] = \
                        VariableDescriptor(var_name, var_dtype, var_dims, var_info)
            else:
                var_descriptors[var_name] = VariableDescriptor(var_name, '', '')
        if 'variables' in ds_metadata:
            ds_metadata.pop('variables')
        ds_metadata.pop('dimensions')
        ds_metadata.pop('variable_infos')
        attrs = ds_metadata.get('attributes', {}).get('NC_GLOBAL', {})
        ds_metadata.pop('attributes')
        attrs.update(ds_metadata)
        self._remove_irrelevant_metadata_attributes(attrs)
        descriptor = DatasetDescriptor(data_id=data_id, type_specifier=self._type_specifier,
                                       dims=dims, data_vars=var_descriptors, attrs=attrs,
                                       bbox=bbox, spatial_res=spatial_resolution,
                                       time_range=temporal_coverage,
                                       time_period=temporal_resolution)
        data_schema = self._get_open_data_params_schema(descriptor)
        descriptor.open_params_schema = data_schema
        return descriptor

    @staticmethod
    def _remove_irrelevant_metadata_attributes(attrs: dict):
        to_remove_list = []
        for attribute in attrs:
            if attribute not in _RELEVANT_METADATA_ATTRIBUTES:
                to_remove_list.append(attribute)
        for to_remove in to_remove_list:
            attrs.pop(to_remove)

    def search_data(self, **search_params) -> Iterator[DatasetDescriptor]:
        search_result = self._cci_odp.search(**search_params)
        data_descriptors = self._describe_data(search_result)
        return iter(data_descriptors)

    def get_open_data_params_schema(self, data_id: str = None) -> JsonObjectSchema:
        if data_id is None:
            return self._get_open_data_params_schema()
        self._assert_valid_data_id(data_id)
        dsd = self.describe_data(data_id)
        return self._get_open_data_params_schema(dsd)

    @staticmethod
    def _get_open_data_params_schema(dsd: DataDescriptor=None):
        min_date = dsd.time_range[0] if dsd and dsd.time_range else None
        max_date = dsd.time_range[1] if dsd and dsd.time_range else None
        # noinspection PyUnresolvedReferences
        cube_params = dict(
            variable_names=JsonArraySchema(items=JsonStringSchema(
                enum=dsd.data_vars.keys() if dsd and dsd.data_vars else None)),
            time_range=JsonDateSchema.new_range(min_date, max_date)
        )
        min_lon = dsd.bbox[0] if dsd and dsd.bbox else -180
        min_lat = dsd.bbox[1] if dsd and dsd.bbox else -90
        max_lon = dsd.bbox[2] if dsd and dsd.bbox else 180
        max_lat = dsd.bbox[3] if dsd and dsd.bbox else 90
        subsetting_params = dict(
            bbox=JsonArraySchema(items=(
                JsonNumberSchema(minimum=min_lon, maximum=max_lon),
                JsonNumberSchema(minimum=min_lat, maximum=max_lat),
                JsonNumberSchema(minimum=min_lon, maximum=max_lon),
                JsonNumberSchema(minimum=min_lat, maximum=max_lat))),
        )
        cci_schema = JsonObjectSchema(
            properties=dict(**cube_params,
                            **subsetting_params
                            ),
            required=[
            ],
            additional_properties=False
        )
        return cci_schema

    def open_data(self, data_id: str, **open_params) -> Any:
        cci_schema = self.get_open_data_params_schema(data_id)
        cci_schema.validate_instance(open_params)
        cube_kwargs, open_params = cci_schema.process_kwargs_subset(open_params, (
            'variable_names',
            'time_range'
        ))
        max_cache_size: int = 2 ** 30
        chunk_store = CciChunkStore(self._cci_odp, data_id, cube_kwargs)
        if max_cache_size:
            chunk_store = zarr.LRUStoreCache(chunk_store, max_cache_size)
        ds = xr.open_zarr(chunk_store)
        ds = self._normalize_dataset(ds, cci_schema, **open_params)
        return ds

    def _assert_valid_data_id(self, data_id: str):
        if data_id not in self.dataset_names:
            raise DataStoreError(f'Cannot describe metadata of data resource "{data_id}", '
                                 f'as it cannot be accessed by data accessor "{self._id}".')

    @abstractmethod
    def _get_subsetting_params(self, min_lon:float, min_lat:float, max_lon:float, max_lat:float):
        pass

    @abstractmethod
    def _normalize_dataset(self, ds: xr.Dataset, cci_schema: JsonObjectSchema, **open_params) -> xr.Dataset:
        pass

    @abstractmethod
    def _normalize_dims(self, dims: dict) -> dict:
        pass

    @abstractmethod
    def _normalize_var_dims(self, var_dims: List[str]) -> Optional[List[str]]:
        pass


class CciOdpDatasetOpener(CciOdpDataOpener):

    def __init__(self, **store_params):
        super().__init__(CciOdp(only_consider_cube_ready=False, **store_params), DATASET_OPENER_ID, TYPE_SPECIFIER_DATASET)

    def _get_subsetting_params(self, min_lon:float, min_lat:float, max_lon:float, max_lat:float):
        # no subsetting allowed on non-cubes
        return dict(
            bbox=JsonArraySchema(items=(
                JsonNumberSchema(minimum=min_lon, maximum=min_lon),
                JsonNumberSchema(minimum=min_lat, maximum=min_lat),
                JsonNumberSchema(minimum=max_lon, maximum=max_lon),
                JsonNumberSchema(minimum=max_lat, maximum=max_lat)))
        )

    def _normalize_dataset(self, ds: xr.Dataset, cci_schema: JsonObjectSchema, **open_params) -> xr.Dataset:
        return ds

    def _normalize_dims(self, dims: dict) -> dict:
        return dims

    def _normalize_var_dims(self, var_dims: List[str]) -> Optional[List[str]]:
        new_var_dims = var_dims.copy()
        if not 'time' in new_var_dims:
            new_var_dims.append('time')
        return new_var_dims


class CciOdpCubeOpener(CciOdpDataOpener):

    def __init__(self, **store_params):
        super().__init__(CciOdp(only_consider_cube_ready=True, **store_params), CUBE_OPENER_ID, TYPE_SPECIFIER_CUBE)

    def _get_subsetting_params(self, min_lon:float, min_lat:float, max_lon:float, max_lat:float):
        return dict(
            bbox=JsonArraySchema(items=(
                JsonNumberSchema(minimum=min_lon, maximum=max_lon),
                JsonNumberSchema(minimum=min_lat, maximum=max_lat),
                JsonNumberSchema(minimum=min_lon, maximum=max_lon),
                JsonNumberSchema(minimum=min_lat, maximum=max_lat)))
        )

    def _normalize_dataset(self, ds: xr.Dataset, cci_schema: JsonObjectSchema, **open_params) -> xr.Dataset:
        ds = normalize_cci_dataset(ds)
        ds = normalize_dataset(ds)
        subsetting_kwargs, open_params = cci_schema.process_kwargs_subset(open_params, (
            'bbox',
        ))
        if 'bbox' in subsetting_kwargs:
            ds = subset_spatial(ds, **subsetting_kwargs)
        return ds

    def _normalize_dims(self, dims: dict) -> dict:
        return normalize_dims_description(dims)

    def _normalize_var_dims(self, var_dims: List[str]) -> Optional[List[str]]:
        return normalize_variable_dims_description(var_dims)


class CciOdpDataStore(DataStore):

    def __init__(self, **store_params):
        cci_schema = self.get_data_store_params_schema()
        cci_schema.validate_instance(store_params)
        store_kwargs, store_params = cci_schema.process_kwargs_subset(store_params, (
            'endpoint_url',
            'endpoint_description_url',
            'enable_warnings',
            'num_retries',
            'retry_backoff_max',
            'retry_backoff_base',
        ))
        self._dataset_opener = CciOdpDatasetOpener(**store_params)
        self._cube_opener = CciOdpCubeOpener(**store_params)

    @classmethod
    def get_data_store_params_schema(cls) -> JsonObjectSchema:
        cciodp_params = dict(
            endpoint_url=JsonStringSchema(default=OPENSEARCH_CEDA_URL),
            endpoint_description_url=JsonStringSchema(default=CCI_ODD_URL),
            enable_warnings=JsonBooleanSchema(default=False, title='Whether to output warnings'),
            num_retries=JsonIntegerSchema(default=DEFAULT_NUM_RETRIES, minimum=0,
                                            title='Number of retries when requesting data fails'),
            retry_backoff_max=JsonIntegerSchema(default=DEFAULT_RETRY_BACKOFF_MAX, minimum=0),
            retry_backoff_base=JsonNumberSchema(default=DEFAULT_RETRY_BACKOFF_BASE, exclusive_minimum=1.0)
        )
        return JsonObjectSchema(
            properties=dict(**cciodp_params),
            required=None,
            additional_properties=False
        )

    @classmethod
    def get_type_specifiers(cls) -> Tuple[str, ...]:
        return TYPE_SPECIFIER_DATASET, TYPE_SPECIFIER_CUBE

    def get_type_specifiers_for_data(self, data_id: str) -> Tuple[str, ...]:
        if self.has_data(data_id, type_specifier=str(TYPE_SPECIFIER_CUBE)):
            return str(TYPE_SPECIFIER_DATASET), str(TYPE_SPECIFIER_CUBE)
        if self.has_data(data_id, type_specifier=str(TYPE_SPECIFIER_DATASET)):
            return str(TYPE_SPECIFIER_DATASET),
        raise DataStoreError(f'Data resource "{data_id}" does not exist in store')

    def _get_opener(self, opener_id: str = None, type_specifier: str = None) -> CciOdpDataOpener:
        self._assert_valid_opener_id(opener_id)
        self._assert_valid_type_specifier(type_specifier)
        if type_specifier:
            if TYPE_SPECIFIER_CUBE.is_satisfied_by(type_specifier):
                type_opener_id = CUBE_OPENER_ID
            else:
                type_opener_id = DATASET_OPENER_ID
            if opener_id and opener_id != type_opener_id:
                raise DataStoreError(f'Invalid combination of opener_id "{opener_id}" '
                                     f'and type_specifier "{type_specifier}"')
            opener_id = type_opener_id
        if opener_id == CUBE_OPENER_ID:
            return self._cube_opener
        return self._dataset_opener

    def get_data_ids(self, type_specifier: str = None, include_titles = True) -> Iterator[Tuple[str, str]]:
        data_ids = self._get_opener(type_specifier=type_specifier).dataset_names
        if include_titles:
            tuples = ((data_id, self._create_human_readable_title_from_data_id(data_id)) for data_id in data_ids)
        else:
            tuples = ((data_id, None) for data_id in data_ids)
        return iter(tuples)

    @staticmethod
    def _create_human_readable_title_from_data_id(data_id: str) -> str:
        split_id = data_id.split('.')
        version = split_id[-2]
        if not version.startswith('v'):
            version = f'v{version}'
        version = version.replace('-', '.')
        return f'{split_id[1]} CCI: {_FREQUENCY_TO_ADJECTIVE[split_id[2]]} {split_id[5]} {split_id[3]} ' \
               f'{split_id[7]} {split_id[4]}, {version}'

    def has_data(self, data_id: str, type_specifier: str=None) -> bool:
        return data_id in self._get_opener(type_specifier=type_specifier).dataset_names

    def describe_data(self, data_id: str, type_specifier: str=None) -> DataDescriptor:
        return self._get_opener(type_specifier=type_specifier).describe_data(data_id)

    @classmethod
    def get_search_params_schema(cls, type_specifier:str=None) -> JsonObjectSchema:
        cls._assert_valid_type_specifier(type_specifier)
        search_params = dict(
            start_date=JsonStringSchema(format='date-time'),
            end_date=JsonStringSchema(format='date-time'),
            bbox=JsonArraySchema(items=(JsonNumberSchema(),
                                        JsonNumberSchema(),
                                        JsonNumberSchema(),
                                        JsonNumberSchema())),
            ecv=JsonStringSchema(enum=[
                'ICESHEETS', 'AEROSOL', 'OC', 'GHG', 'OZONE', 'SEAICE', 'SST', 'CLOUD', 'SOILMOISTURE', 'FIRE', 'LC',
                'SEASTATE', 'SEASURFACESALINITY', 'GLACIERS', 'SEALEVEL']),
            frequency=JsonStringSchema(enum=[
                'month', 'day', 'satellite orbit frequency' '5 days', '8 days', 'climatology', '13 years', '15 days',
                '5 years', 'year']),
            institute=JsonStringSchema(enum=[
                'Plymouth Marine Laboratory',
                'Alfred-Wegener-Institut Helmholtz-Zentrum für Polar- und Meeresforschung',
                'ENVironmental Earth Observation IT GmbH', 'multi-institution', 'DTU Space',
                'Vienna University of Technology', 'Deutscher Wetterdienst', 'Netherlands Institute for Space Research',
                'Technische Universität Dresden', 'Institute of Environmental Physics',
                'Rutherford Appleton Laboratory', 'Universite Catholique de Louvain', 'University of Alcala',
                'University of Leicester', 'Norwegian Meteorological Institute', 'University of Bremen',
                'Belgian Institute for Space Aeronomy', 'Deutsches Zentrum fuer Luft- und Raumfahrt',
                'Freie Universitaet Berlin', 'Royal Netherlands Meteorological Institute',
                'The Geological Survey of Denmark and Greenland']),
            processing_level=JsonStringSchema(enum=['L3S', 'L3C', 'L2P', 'L4', 'L2' 'L3', 'L3U']),
            product_string=JsonStringSchema(enum=[
                'MERGED (20)', 'ADV ', 'ORAC', 'SU', 'AATSR', 'ATSR1', 'ATSR2', 'AVHRR07_G', 'AVHRR09_G', 'AVHRR11_G',
                'AVHRR12_G', 'AVHRR14_G', 'AVHRR15_G', 'AVHRR16_G', 'AVHRR17_G', 'AVHRR18_G', 'AVHRR19_G', 'AVHRRMTA_G',
                'MODIS_TERRA', 'Map', 'OSTIA', 'ACTIVE', 'COMBINED', 'EMMA', 'MERIS_ENVISAT', 'MSI', 'OCFP', 'PASSIVE',
                'SRFP', 'WFMD', 'ACE_FTS_SCISAT', 'AERGOM', 'AMSR_25kmEASE2', 'AMSR_50kmEASE2', 'ATSR2-AATSR',
                'ATSR2_ERS2', 'AVHRR-AM', 'AVHRR-PM', 'BESD', 'Envisat', 'GFO', 'GOMOS_ENVISAT', 'IMAP',
                'MERGED_OI_7DAY_RUNNINGMEAN_DAILY_25km', 'MERGED_OI_Monthly_CENTRED_15Day_25km', 'MERIS-AATSR',
                'MIPAS_ENVISAT', 'MODIS_AQUA', 'MZM', 'OCPR', 'OSIRIS_ODIN', 'SCIAMACHY_ENVISAT', 'SMM', 'SMR_ODIN',
                'SRPR', 'Saral']),
            product_version=JsonStringSchema(enum=[
                '1.1', '2.0', 'esp 2.1', '2.1', '4.0', '3.1', 'undefined', 'v0001', '1.2', 'v1.0', '03.02', '2.30',
                '4.21', '04.4', '04.5', '1.0', '2.2', 'v1.1', 'v1.2', 'v2.3.8', '01.08', 'v0002', 'v1.3', 'v4.0', '0.1',
                '1.3', '1.5.7', '1.6.1', '2.0.7', '2.19', '3.0', '4-0', '4.21u', 'ch4_v1.2', 'fv0002', 'fv0100', 'v0.1',
                'v02.01.02', 'v1.4', 'v1.5', 'v2-1', 'v2.0', 'v2.2', 'v2.2a', 'v2.2b', 'v2.2c', 'v3.0', 'v5.1',
                'v7-0-1', 'v7.0', 'v7.2']),
            data_type=JsonStringSchema(enum=[
                'IV', 'AER_PRODUCTS', 'LP', 'SITHICK', 'CH4', 'CLD_PRODUCTS', 'GMB', 'SSTskin', 'CO2', 'BA', 'CHLOR_A',
                'K_490', 'SSMV', 'IOP', 'OC_PRODUCTS', 'RRS', 'SEC', 'SSTdepth', 'SWH', 'AAI', 'AOD', 'GLL', 'LCCS',
                'SICONC', 'SSMS', 'SSS', 'AEX', 'NP', 'TC', 'WB']),
            sensor=JsonStringSchema(enum=[
                'ATSR-2', 'AATSR', 'TANSO-FTS', 'RA-2', 'SCIAMACHY', 'SIRAL', 'MODIS', 'ATSR', 'AVHRR-2', 'AVHRR-3',
                'GOMOS', 'MERIS', 'RA', 'SMR', 'ACE-FTS', 'AMI-SCAT', 'ASAR', 'AltiKa', 'GFO-RA', 'MIPAS', 'OSIRIS',
                'Poseidon-2', 'Poseidon-3']),
            platform=JsonStringSchema(enum=[
                'Envisat', 'ERS-2', 'GOSAT', 'CryoSat-2', 'GRACE', 'ERS-1', 'Metop-A', 'NOAA-11', 'NOAA-12', 'NOAA-14',
                'NOAA-15', 'NOAA-16', 'NOAA-17', 'NOAA-18', 'NOAA-19', 'NOAA-7', 'NOAA-9', 'ODIN', 'Terra', 'GFO',
                'Jason-1', 'Jason-2', 'SARAL', 'Aqua', 'RadarSat-2'])
        )
        search_schema = JsonObjectSchema(
            properties=dict(**search_params),
            additional_properties=False)
        return search_schema

    def search_data(self, type_specifier: str = None, **search_params) -> Iterator[DatasetDescriptor]:
        if not self._is_valid_type_specifier(type_specifier):
            return iter([])
        search_schema = self.get_search_params_schema()
        search_schema.validate_instance(search_params)
        opener = self._get_opener(type_specifier=type_specifier)
        return opener.search_data(**search_params)

    def get_data_opener_ids(self, data_id: str = None, type_specifier: str = None, ) -> Tuple[str, ...]:
        self._assert_valid_type_specifier(type_specifier)
        if data_id is not None and not self.has_data(data_id):
            raise DataStoreError(f'Data Resource "{data_id}" is not available.')
        may_be_cube = data_id is None or self.has_data(data_id, str(TYPE_SPECIFIER_CUBE))
        if type_specifier:
            if TYPE_SPECIFIER_CUBE.is_satisfied_by(type_specifier):
                if not may_be_cube:
                    raise DataStoreError(f'Data Resource "{data_id}" is not available '
                                         f'as specified type "{type_specifier}".')
                return CUBE_OPENER_ID,
        if may_be_cube:
            return DATASET_OPENER_ID, CUBE_OPENER_ID
        return DATASET_OPENER_ID,

    def get_open_data_params_schema(self, data_id: str = None, opener_id: str = None) -> JsonObjectSchema:
        return self._get_opener(opener_id=opener_id).get_open_data_params_schema(data_id)

    def open_data(self, data_id: str, opener_id: str = None, **open_params) -> xr.Dataset:
        return self._get_opener(opener_id=opener_id).open_data(data_id, **open_params)

    #############################################################################
    # Implementation helpers

    @classmethod
    def _is_valid_type_specifier(cls, type_specifier: str) -> bool:
        return type_specifier is None or TYPE_SPECIFIER_CUBE.satisfies(type_specifier)

    @classmethod
    def _assert_valid_type_specifier(cls, type_specifier):
        if not cls._is_valid_type_specifier(type_specifier):
            raise DataStoreError(
                f'Type Specifier must be "{TYPE_SPECIFIER_DATASET}" or "{TYPE_SPECIFIER_CUBE}", '
                f'but got "{type_specifier}"')

    def _assert_valid_opener_id(self, opener_id):
        if opener_id is not None and opener_id != DATASET_OPENER_ID and opener_id != CUBE_OPENER_ID:
            raise DataStoreError(f'Data opener identifier must be "{DATASET_OPENER_ID}" or "{CUBE_OPENER_ID}",'
                                 f'but got "{opener_id}"')

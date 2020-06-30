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

import re
from typing import Any, Iterator, List, Tuple

import xarray as xr
import zarr

from xcube.core.store import DataDescriptor
from xcube.core.store import DataOpener
from xcube.core.store import DataStore
from xcube.core.store import DataStoreError
from xcube.core.store import DatasetDescriptor
from xcube.core.store import VariableDescriptor
from xcube.util.jsonschema import JsonArraySchema
from xcube.util.jsonschema import JsonBooleanSchema
from xcube.util.jsonschema import JsonNumberSchema
from xcube.util.jsonschema import JsonObjectSchema
from xcube.util.jsonschema import JsonStringSchema
from xcube_cci.cciodp import CciOdp
from xcube_cci.chunkstore import CciChunkStore
from xcube_cci.constants import CCI_ODD_URL
from xcube_cci.constants import DATA_OPENER_ID
from xcube_cci.constants import DEFAULT_CRS
from xcube_cci.constants import OPENSEARCH_CEDA_URL

DATASET_DATA_TYPE = 'dataset'

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
    'climatology': '',
    '13-yrs': '13 year',
    '15-days': '15 day',
    '5-yrs': '5 year',
    'yr': 'year'
}



class CciOdpDataOpener(DataOpener):

    def __init__(self, cci_odp: CciOdp = None):
        self._cci_odp = cci_odp

    def _describe_data(self, data_ids: List[str]) -> List[DataDescriptor]:
        ds_metadata_list = self._cci_odp.get_dataset_metadata(data_ids)
        data_descriptors = []
        for i, ds_metadata in enumerate(ds_metadata_list):
            data_descriptors.append(self._get_data_descriptor_from_metadata(data_ids[i], ds_metadata))
        return data_descriptors

    def describe_data(self, data_id: str) -> DataDescriptor:
        try:
            ds_metadata = self._cci_odp.get_dataset_metadata([data_id])[0]
            return self._get_data_descriptor_from_metadata(data_id, ds_metadata)
        except ValueError:
            raise DataStoreError(f'Cannot describe metadata. "{data_id}" does not seem to be a valid identifier.')

    def _get_data_descriptor_from_metadata(self, data_id: str, ds_metadata: dict) -> DatasetDescriptor:
        dims = ds_metadata.get('dimensions', {})
        attrs = ds_metadata.get('attributes', {}).get('NC_GLOBAL', {})
        temporal_resolution = attrs.get('time_coverage_resolution', 'P')[1:]
        if re.match(TIME_PERIOD_PATTERN, temporal_resolution) is None:
            temporal_resolution = None
        dataset_info = self._cci_odp.get_dataset_info(data_id, ds_metadata)
        spatial_resolution = dataset_info['lat_res']
        bbox = dataset_info['bbox']
        temporal_coverage = (dataset_info['temporal_coverage_start'], dataset_info['temporal_coverage_end'])
        var_descriptors = []
        var_infos = ds_metadata.get('variable_infos', {})
        var_names = dataset_info['var_names']
        for var_name in var_names:
            if var_name in var_infos:
                var_info = var_infos[var_name]
                var_dtype = var_info['data_type']
                var_dims = var_info['dimensions']
                var_descriptors.append(VariableDescriptor(var_name,
                                                          var_dtype,
                                                          var_dims,
                                                          var_info))
            else:
                var_descriptors.append(VariableDescriptor(var_name, '', ''))
        descriptor = DatasetDescriptor(data_id=data_id, dims=dims, data_vars=var_descriptors, attrs=attrs, bbox=bbox,
                                       spatial_res=spatial_resolution, time_range=temporal_coverage,
                                       time_period=temporal_resolution)
        data_schema = self._get_open_data_params_schema(descriptor)
        descriptor.open_params_schema = data_schema
        return descriptor

    def get_open_data_params_schema(self, data_id: str = None, opener_id: str = None) -> JsonObjectSchema:
        dsd = self.describe_data(data_id) if data_id else None
        return self._get_open_data_params_schema(dsd)

    def _get_open_data_params_schema(self, dsd: DataDescriptor):
        cube_params = dict(
            variable_names=JsonArraySchema(items=JsonStringSchema(
                enum=[v.name for v in dsd.data_vars] if dsd and dsd.data_vars else None)),
            time_range=JsonArraySchema(items=(JsonStringSchema(format='date-time'),
                                              JsonStringSchema(format='date-time')))
        )
        normalization_params = dict(
        )
        cci_schema = JsonObjectSchema(
            properties=dict(**cube_params,
                            **normalization_params
                            ),
            required=[
                # cube_params
                'variable_names',
                'time_range',
            ],
            additional_properties=False
        )
        return cci_schema

    def open_data(self, data_id: str, opener_id: str = None, **open_params) -> Any:
        cci_schema = self.get_open_data_params_schema(data_id)
        cci_schema.validate_instance(open_params)
        cube_kwargs, open_params = cci_schema.process_kwargs_subset(open_params, (
            'variable_names',
            # 'chunk_sizes',
            'time_range'
        ))
        max_cache_size: int = 2 ** 30
        cci_odp = CciOdp()
        chunk_store = CciChunkStore(cci_odp, data_id, cube_kwargs)
        if max_cache_size:
            chunk_store = zarr.LRUStoreCache(chunk_store, max_cache_size)
        raw_ds = xr.open_zarr(chunk_store)
        normalization_kwargs, open_params = cci_schema.process_kwargs_subset(open_params, (
            'bbox',
            # 'geometry_wkt',
            'spatial_res',
            'crs',
            'time_period'
        ))
        return raw_ds
        # return cci_normalize(raw_ds, dataset_id, cube_params, cci_odp)


class CciOdpDataStore(CciOdpDataOpener, DataStore):

    def __init__(self, **store_params):
        cci_schema = self.get_data_store_params_schema()
        cci_schema.validate_instance(store_params)
        store_kwargs, store_params = cci_schema.process_kwargs_subset(store_params, (
            'opensearch_url',
            'opensearch_description_url'
        ))
        normalize_kwargs, store_params = cci_schema.process_kwargs_subset(store_params, (
            'normalize_params'
        ))
        super().__init__(CciOdp(**store_kwargs))

    @property
    def description(self) -> dict:
        all_data = list(self.search_data())
        datasets = []
        for data in all_data:
            datasets.append(data.to_dict())
        description = dict(store_id='cciodp',
                           description='ESA CCI Open Data Portal',
                           datasets=datasets
                           )
        return description

    @classmethod
    def get_data_store_params_schema(cls) -> JsonObjectSchema:
        cciodp_params = dict(
            opensearch_url=JsonStringSchema(default=OPENSEARCH_CEDA_URL),
            opensearch_description_url=JsonStringSchema(default=CCI_ODD_URL)
        )
        normalization_params = dict(
            normalize_params=JsonBooleanSchema(default=False)
        )
        return JsonObjectSchema(
            properties=dict(**cciodp_params,
                            **normalization_params
                            ),
            required=None,
            additional_properties=False
        )

    @classmethod
    def get_type_ids(cls) -> Tuple[str, ...]:
        return DATASET_DATA_TYPE,

    def get_data_ids(self, type_id: str = None) -> Iterator[Tuple[str, str]]:
        self._assert_valid_type_id(type_id)
        data_ids = self._cci_odp.dataset_names
        tuples = ((data_id, self._create_human_readable_title_from_id(data_id)) for data_id in data_ids)
        return iter(tuples)

    def _create_human_readable_title_from_id(self, id: str) -> str:
        split_id = id.split('.')
        version = split_id[-2]
        if not version.startswith('v'):
            version = f'v{version}'
        version = version.replace('-', '.')
        return f'{split_id[1]} CCI: {_FREQUENCY_TO_ADJECTIVE[split_id[2]]} {split_id[5]} {split_id[3]} ' \
               f'{split_id[7]} {split_id[4]}, {version}'


    def has_data(self, data_id: str) -> bool:
        return data_id in self._cci_odp.dataset_names

    @classmethod
    def get_search_params_schema(cls) -> JsonObjectSchema:
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

    def search_data(self, type_id: str = None, **search_params) -> Iterator[DatasetDescriptor]:
        self._assert_valid_type_id(type_id)
        search_schema = self.get_search_params_schema()
        search_schema.validate_instance(search_params)
        search_result = self._cci_odp.search(**search_params)
        data_descriptors = []
        for data_id in search_result:
            data_descriptors.append(self.describe_data(data_id))
        return iter(data_descriptors)

    def get_data_opener_ids(self, data_id: str = None, type_id: str = None, ) -> Tuple[str, ...]:
        self._assert_valid_type_id(type_id)
        return DATA_OPENER_ID,

    def get_open_data_params_schema(self, data_id: str = None, opener_id: str = None) -> JsonObjectSchema:
        self._assert_valid_opener_id(opener_id)
        return super().get_open_data_params_schema(data_id)

    def open_data(self, data_id: str, opener_id: str = None, **open_params) -> xr.Dataset:
        self._assert_valid_opener_id(opener_id)
        return super().open_data(data_id, **open_params)

    #############################################################################
    # Implementation helpers

    def _assert_valid_type_id(self, type_id):
        if type_id is not None and type_id != DATASET_DATA_TYPE:
            raise DataStoreError(f'Data type identifier must be "{DATASET_DATA_TYPE}", but got "{type_id}"')

    def _assert_valid_opener_id(self, opener_id):
        if opener_id is not None and opener_id != DATA_OPENER_ID:
            raise DataStoreError(f'Data opener identifier must be "{DATA_OPENER_ID}", but got "{opener_id}"')

# The MIT License (MIT)
# Copyright (c) 2023 ESA Climate Change Initiative
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
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
import asyncio
import bisect
import copy
import io
import json
import math
import os
import re
import tarfile
import urllib.parse
from datetime import datetime
from typing import Dict, List, Mapping, Optional, Tuple, Union
from urllib.parse import quote

import geopandas as gpd
import lxml.etree as etree
import numcodecs
import numpy as np
import pandas as pd
import pyproj
import rioxarray
from dateutil.relativedelta import relativedelta
from pydap.handlers.dap import BaseProxyDap2, SequenceProxy, unpack_dap2_data
from pydap.lib import BytesReader, combine_slices, fix_slice, hyperslab, walk
from pydap.model import BaseType, GridType, SequenceType
from pydap.parsers import parse_ce
from pydap.parsers.das import add_attributes, parse_das
from pydap.parsers.dds import dds_to_dataset
from shapely import Point
from shapely.geometry import mapping
from six.moves.urllib.parse import urlsplit, urlunsplit
from xcube.core.store import DataStoreError, DATASET_TYPE, GEO_DATA_FRAME_TYPE

from xcube_cci.timeutil import get_timestrings_from_string

from .constants import (CCI_ODD_URL, COMMON_TIME_COORD_VAR_NAMES,
                        DATASET_STATES_FILE, DEFAULT_NUM_RETRIES,
                        DEFAULT_RETRY_BACKOFF_BASE, DEFAULT_RETRY_BACKOFF_MAX,
                        GEODATAFRAME_STATES_FILE, LOG, OPENSEARCH_CEDA_URL,
                        TIFF_VARS, TIMESTAMP_FORMAT,
                        VECTORDATACUBE_STATES_FILE)
from .odpconnector import OdpConnector
from .sessionexecutor import SessionExecutor
from .vdcaccess import VECTOR_DATA_CUBE_TYPE

DATA_TYPE_TO_FILE_NAME = {
    DATASET_TYPE.alias: DATASET_STATES_FILE,
    GEO_DATA_FRAME_TYPE.alias: GEODATAFRAME_STATES_FILE,
    VECTOR_DATA_CUBE_TYPE.alias: VECTORDATACUBE_STATES_FILE
}

DESC_NS = {'gmd': 'http://www.isotc211.org/2005/gmd',
           'gml': 'http://www.opengis.net/gml/3.2',
           'gco': 'http://www.isotc211.org/2005/gco',
           'gmx': 'http://www.isotc211.org/2005/gmx',
           'xlink': 'http://www.w3.org/1999/xlink'
           }

_FEATURE_LIST_LOCK = asyncio.Lock()

_EARLY_START_TIME = '1000-01-01T00:00:00'
_LATE_END_TIME = '3000-12-31T23:59:59'

_RE_TO_DATETIME_FORMATS = \
    [(re.compile(14 * '\\d'), '%Y%m%d%H%M%S', relativedelta()),
     (re.compile(12 * '\\d'), '%Y%m%d%H%M',
      relativedelta(minutes=1, seconds=-1)),
     (re.compile(8 * '\\d'), '%Y%m%d', relativedelta(days=1, seconds=-1)),
     (re.compile(4 * '\\d' + '-' + 2 * '\\d' + '-' + 2 * '\\d'), '%Y-%m-%d',
      relativedelta(days=1, seconds=-1)),
     (re.compile(6 * '\\d'), '%Y%m', relativedelta(months=1, seconds=-1)),
     (re.compile(4 * '\\d'), '%Y', relativedelta(years=1, seconds=-1))]

_DTYPES_TO_DTYPES_WITH_MORE_BYTES = {
    'int8': 'int16',
    'int16': 'int32',
    'int32': 'int64',
    'uint8': 'uint16',
    'uint16': 'uint32',
    'uint32': 'uint64',
    'float32': 'float32',
    'float64': 'float64'
}
_VECTOR_DATACUBE_CHUNKING = 50


def _convert_time_from_drs_id(time_value: str) -> str:
    if time_value == 'mon':
        return 'month'
    if time_value == 'yr':
        return 'year'
    if time_value == '5-days':
        return '5 days'
    if time_value == '8-days':
        return '8 days'
    if time_value == '15-days':
        return '15 days'
    if time_value == '13-yrs':
        return '13 years'
    return time_value

def _get_feature_dict_from_feature(feature: dict) -> Optional[dict]:
    fc_props = feature.get("properties", {})
    feature_dict = {'uuid': feature.get("id", "").split("=")[-1],
                    'title': fc_props.get("title", "")}
    variables = _get_variables_from_feature(feature)
    feature_dict['variables'] = variables
    fc_props_links = fc_props.get("links")
    if fc_props_links is not None:
        search = fc_props_links.get("search")
        if search:
            odd_url = search[0].get('href')
            if odd_url:
                feature_dict['odd_url'] = odd_url
        described_by = fc_props_links.get("describedby")
        if described_by:
            for entry in described_by:
                if entry.get('title', '') == 'ISO19115':
                    metadata_url = entry.get("href")
                    if metadata_url:
                        feature_dict['metadata_url'] = metadata_url
                elif entry.get('title', '') == 'Dataset Information':
                    catalogue_url = entry.get("href")
                    if catalogue_url:
                        feature_dict['catalog_url'] = catalogue_url
        via = fc_props_links.get("via")
        if via and len(via) > 0:
            if via[0].get('title') == 'Dataset Manifest':
                feature_dict['variable_manifest'] = via[0].get('href')
    return feature_dict


def _get_variables_from_feature(feature: dict) -> List:
    feature_props = feature.get("properties", {})
    variables = feature_props.get("variables", [])
    variable_dicts = []
    for variable in variables:
        variable_dict = {
            'var_id': variable.get("var_id", None),
            'units': variable.get("units", ""),
            'long_name': variable.get("long_name", None)}
        variable_dicts.append(variable_dict)
    return variable_dicts


def _harmonize_info_field_names(
        catalogue: dict, single_field_name: str, multiple_fields_name: str,
        multiple_items_name: Optional[str] = None
):
    if single_field_name in catalogue and multiple_fields_name in catalogue:
        if len(multiple_fields_name) == 0:
            catalogue.pop(multiple_fields_name)
        elif len(catalogue[multiple_fields_name]) == 1:
            if catalogue[multiple_fields_name][0] \
                    is catalogue[single_field_name]:
                catalogue.pop(multiple_fields_name)
            else:
                catalogue[multiple_fields_name].append(
                    catalogue[single_field_name]
                )
                catalogue.pop(single_field_name)
        else:
            if catalogue[single_field_name] not in \
                    catalogue[multiple_fields_name] \
                    and (
                    multiple_items_name is None
                    or catalogue[single_field_name] != multiple_items_name
            ):
                catalogue[multiple_fields_name].append(
                    catalogue[single_field_name]
                )
            catalogue.pop(single_field_name)


def _extract_metadata_from_descxml(descxml: etree.XML) -> dict:
    metadata = {}
    metadata_elems = {
        'abstract': 'gmd:identificationInfo/gmd:MD_DataIdentification/'
                    'gmd:abstract/gco:CharacterString',
        'title': 'gmd:identificationInfo/gmd:MD_DataIdentification/'
                 'gmd:citation/gmd:CI_Citation/gmd:title/gco:CharacterString',
        'licences': 'gmd:identificationInfo/gmd:MD_DataIdentification/'
                    'gmd:resourceConstraints/gmd:MD_Constraints/'
                    'gmd:useLimitation/gco:CharacterString',
        'bbox_minx': 'gmd:identificationInfo/gmd:MD_DataIdentification/'
                     'gmd:extent/gmd:EX_Extent/gmd:geographicElement/'
                     'gmd:EX_GeographicBoundingBox/gmd:westBoundLongitude/'
                     'gco:Decimal',
        'bbox_miny': 'gmd:identificationInfo/gmd:MD_DataIdentification/'
                     'gmd:extent/gmd:EX_Extent/gmd:geographicElement/'
                     'gmd:EX_GeographicBoundingBox/gmd:southBoundLatitude/'
                     'gco:Decimal',
        'bbox_maxx': 'gmd:identificationInfo/gmd:MD_DataIdentification/'
                     'gmd:extent/gmd:EX_Extent/gmd:geographicElement/'
                     'gmd:EX_GeographicBoundingBox/gmd:eastBoundLongitude/'
                     'gco:Decimal',
        'bbox_maxy': 'gmd:identificationInfo/gmd:MD_DataIdentification/'
                     'gmd:extent/gmd:EX_Extent/gmd:geographicElement/'
                     'gmd:EX_GeographicBoundingBox/gmd:northBoundLatitude/'
                     'gco:Decimal',
        'temporal_coverage_start':
            'gmd:identificationInfo/gmd:MD_DataIdentification/gmd:extent/'
            'gmd:EX_Extent/gmd:temporalElement/gmd:EX_TemporalExtent/'
            'gmd:extent/gml:TimePeriod/gml:beginPosition',
        'temporal_coverage_end':
            'gmd:identificationInfo/gmd:MD_DataIdentification/gmd:extent/'
            'gmd:EX_Extent/gmd:temporalElement/gmd:EX_TemporalExtent/'
            'gmd:extent/gml:TimePeriod/gml:endPosition'
    }
    for identifier in metadata_elems:
        content = _get_element_content(descxml, metadata_elems[identifier])
        if content:
            metadata[identifier] = content
    metadata_elems_with_replacement = {'file_formats': [
        'gmd:identificationInfo/gmd:MD_DataIdentification/gmd:resourceFormat/'
        'gmd:MD_Format/gmd:name/gco:CharacterString',
        'Data are in NetCDF format', '.nc'
    ]}
    for metadata_elem in metadata_elems_with_replacement:
        content = _get_replaced_content_from_descxml_elem(
            descxml, metadata_elems_with_replacement[metadata_elem]
        )
        if content:
            metadata[metadata_elem] = content
    metadata_linked_elems = {
        'publication_date':
            ['gmd:identificationInfo/gmd:MD_DataIdentification/gmd:citation/'
             'gmd:CI_Citation/gmd:date/gmd:CI_Date/gmd:dateType/'
             'gmd:CI_DateTypeCode', 'publication',
             '../../gmd:date/gco:DateTime'],
        'creation_date':
            ['gmd:identificationInfo/gmd:MD_DataIdentification/gmd:citation/'
             'gmd:CI_Citation/gmd:date/gmd:CI_Date/gmd:dateType/'
             'gmd:CI_DateTypeCode', 'creation', '../../gmd:date/gco:DateTime']
    }
    for identifier in metadata_linked_elems:
        content = _get_linked_content_from_descxml_elem(
            descxml, metadata_linked_elems[identifier]
        )
        if content:
            metadata[identifier] = content
    return metadata


def _get_element_content(
        descxml: etree.XML, path: str
) -> Optional[Union[str, List[str]]]:
    elements = descxml.findall(path, namespaces=DESC_NS)
    if not elements:
        return None
    if len(elements) == 1:
        return elements[0].text
    return [elem.text for elem in elements]


def _get_replaced_content_from_descxml_elem(
        descxml: etree.XML, paths: List[str]
) -> Optional[str]:
    descxml_elem = descxml.find(paths[0], namespaces=DESC_NS)
    if descxml_elem is None:
        return None
    if descxml_elem.text == paths[1]:
        return paths[2]


def _get_linked_content_from_descxml_elem(
        descxml: etree.XML, paths: List[str]
) -> Optional[str]:
    descxml_elements = descxml.findall(paths[0], namespaces=DESC_NS)
    if descxml is None:
        return None
    for descxml_elem in descxml_elements:
        if descxml_elem.text == paths[1]:
            return _get_element_content(descxml_elem, paths[2])


def find_datetime_format(
        filename: str
) -> Tuple[Optional[str], int, int, relativedelta]:
    for regex, time_format, timedelta in _RE_TO_DATETIME_FORMATS:
        searcher = regex.search(filename)
        if searcher:
            p1, p2 = searcher.span()
            return time_format, p1, p2, timedelta
    return None, -1, -1, relativedelta()


def _extract_feature_info(feature: dict) -> List:
    feature_props = feature.get("properties", {})
    filename = feature_props.get("title", "")
    date = feature_props.get("date")
    start_time = ""
    end_time = ""
    if date is not None and "/" in date:
        start_time, end_time = date.split("/")
    elif filename:
        time_format, p1, p2, timedelta = find_datetime_format(filename)
        if time_format:
            start_time = datetime.strptime(filename[p1:p2], time_format)
            end_time = start_time + timedelta
            # Convert back to text, so we can JSON-encode it
            start_time = datetime.strftime(start_time, TIMESTAMP_FORMAT)
            end_time = datetime.strftime(end_time, TIMESTAMP_FORMAT)
    file_size = feature_props.get("filesize", 0)
    related_links = feature_props.get("links", {}).get("related", [])
    urls = {}
    for related_link in related_links:
        urls[related_link.get("title")] = related_link.get("href")
    return [filename, start_time, end_time, file_size, urls]


def get_res(nc_attrs: dict, dim: str) -> float:
    if dim == 'lat':
        attr_name = 'geospatial_lat_resolution'
        index = 0
    else:
        attr_name = 'geospatial_lon_resolution'
        index = -1
    for name in [attr_name, 'resolution', 'spatial_resolution']:
        if name in nc_attrs:
            res_attr = nc_attrs[name]
            try:
                if type(res_attr) is float:
                    return res_attr
                elif type(res_attr) is int:
                    return float(res_attr)
                # as we now expect to deal with a string, we try to parse a
                # float for that, we remove any trailing units and consider
                # that lat and lon might be given, separated by an 'x'
                return float(res_attr.split('(')[0].split('x')[index].
                             split('deg')[0].split('degree')[0].split('km')[0].
                             split('m')[0])
            except ValueError:
                continue
    return -1.0


def _determine_fill_value(dtype):
    if np.issubdtype(dtype, np.integer):
        return np.iinfo(dtype).max
    if np.issubdtype(dtype, np.inexact):
        return np.nan


class CciOdpWarning(Warning):
    pass


class CciOdp:
    """
    Represents the ESA CCI Climate Data Centre

    :param endpoint_url: The base URL to the opensearch service
    :param endpoint_description_url: The URL to a document describing
    the capabilities of the opensearch service
    """

    def __init__(self,
                 endpoint_url: str = OPENSEARCH_CEDA_URL,
                 endpoint_description_url: str = CCI_ODD_URL,
                 enable_warnings: bool = False,
                 num_retries: int = DEFAULT_NUM_RETRIES,
                 retry_backoff_max: int = DEFAULT_RETRY_BACKOFF_MAX,
                 retry_backoff_base: float = DEFAULT_RETRY_BACKOFF_BASE,
                 user_agent: str = None,
                 data_type: str = 'dataset',
                 drs_ids: List[str] = None
                 ):
        self._opensearch_url = endpoint_url
        self._opensearch_description_url = endpoint_description_url
        self._enable_warnings = enable_warnings
        self._num_retries = num_retries
        self._retry_backoff_max = retry_backoff_max
        self._retry_backoff_base = retry_backoff_base
        self._task_cache = {}
        self._data_type = data_type
        self._data_sources = {}
        self._features = {}
        self._result_dicts = {}
        self._session_executor = SessionExecutor(user_agent)
        self._odp_connector = OdpConnector(user_agent, endpoint_description_url)
        self._vector_offsets = {}
        self._tar_to_tif = {}
        self._tif_to_array = {}
        self._drs_ids = drs_ids
        if not drs_ids:
            filename = DATA_TYPE_TO_FILE_NAME.get(self._data_type)
            states_file = os.path.join(
                os.path.dirname(os.path.abspath(__file__)),
                f'data/{filename}'
            )
            with open(states_file, 'r') as fp:
                states = json.load(fp)
            self._drs_ids = list(states.keys())


    def get_data_type(self) -> str:
        return self._data_type

    def close(self):
        pass

    @property
    def dataset_names(self) -> List[str]:
        return self._drs_ids

    def get_kerchunk_files(self) -> List[str]:
        return self._session_executor.run_with_session(self._get_kerchunk_files)

    def get_dataset_info(
            self, dataset_id: str, dataset_metadata: dict = None
    ) -> dict:
        data_info = {}
        if not dataset_metadata:
            dataset_metadata = self.get_dataset_metadata(dataset_id)
        nc_attrs = dataset_metadata.get('attributes', {}).get('NC_GLOBAL', {})
        data_info['crs'] = \
            self._get_crs(dataset_metadata.get('variable_infos', {}))
        data_info['y_res'] = get_res(nc_attrs, 'lat')
        if data_info['y_res'] == -1:
            data_info['y_res'] = get_res(dataset_metadata.get('attributes', {}), 'lat')
        data_info['x_res'] = get_res(nc_attrs, 'lon')
        if data_info['x_res'] == -1:
            data_info['x_res'] = get_res(dataset_metadata.get('attributes', {}), 'lon')
        float(dataset_metadata.get('attributes', {}).get('bbox_minx', dataset_metadata.get('bbox_minx', np.nan)))
        data_info['bbox'] = \
            (float(dataset_metadata.get('attributes', {}).get(
                'bbox_minx', dataset_metadata.get('bbox_minx', np.nan))
            ),
             float(dataset_metadata.get('attributes', {}).get(
                 'bbox_miny', dataset_metadata.get('bbox_miny', np.nan))
             ),
             float(dataset_metadata.get('attributes', {}).get(
                 'bbox_maxx', dataset_metadata.get('bbox_maxx', np.nan))
             ),
             float(dataset_metadata.get('attributes', {}).get(
                 'bbox_maxy', dataset_metadata.get('bbox_maxy', np.nan))
             )
            )
        if np.isnan(data_info['bbox']).all():
            data_info['bbox'] = None
        data_info['temporal_coverage_start'] = \
            dataset_metadata.get('temporal_coverage_start')
        data_info['temporal_coverage_end'] = \
            dataset_metadata.get('temporal_coverage_end')
        if not data_info['temporal_coverage_start'] and \
                not data_info['temporal_coverage_end']:
            time_ranges = self.get_time_ranges_from_data(dataset_id)
            if len(time_ranges) > 0:
                data_info['temporal_coverage_start'] = \
                    time_ranges[0][0].isoformat()
                data_info['temporal_coverage_end'] = \
                    time_ranges[-1][1].isoformat()
        data_info['var_names'], data_info['coord_names'] = \
            self.var_and_coord_names(dataset_id)
        return data_info

    @staticmethod
    def _get_crs(variable_infos: dict) -> str:
        crs = None
        for var_attrs in variable_infos.values():
            if 'grid_mapping_name' in var_attrs:
                try:
                    crs = pyproj.crs.CRS.from_cf(var_attrs)
                    break
                except pyproj.crs.CRSError:
                    LOG.info(f'Could not convert grid mapping '
                                  f'"{var_attrs["grid_mapping_name"]}" '
                                  f'into CRS')
                    return var_attrs["grid_mapping_name"]
        if crs:
            if crs.name != 'undefined':
                return crs.name.strip('\\\"')
            crs_authority = crs.to_authority()
            if crs_authority:
                return f'{crs_authority[0]}:{crs_authority[1]}'
            if crs.coordinate_operation:
                if crs.coordinate_operation.method_name:
                    return crs.coordinate_operation.method_name
                if crs.coordinate_operation.method_auth_name and \
                        crs.coordinate_operation.method_code:
                    return f'{crs.coordinate_operation.method_auth_name}:' \
                           f'{crs.coordinate_operation.method_code}'
        return 'WGS84'

    def get_dataset_metadata(self, dataset_id: str) -> dict:
        return self.get_datasets_metadata([dataset_id])[0]

    def get_datasets_metadata(self, dataset_ids: List[str]) -> List[dict]:
        assert isinstance(dataset_ids, list)
        self._session_executor.run_with_session(
            self._ensure_all_info_in_data_sources, dataset_ids
        )
        metadata = []
        for dataset_id in dataset_ids:
            metadata.append(self._data_sources[dataset_id])
        return metadata

    async def _get_kerchunk_files(self, session):
        dataset_names = self._drs_ids
        kerchunk_urls = []
        await self._read_all_data_sources(session)
        for i, dataset_name in enumerate(dataset_names):
            LOG.debug(
                f"Attempting to retrieve kerchunk url for dataset {dataset_name} "
                f"({i + 1}/{len(dataset_names)})"
            )
            dataset_id = await self._get_dataset_id(session, dataset_name)
            data_source = self._data_sources[dataset_name]
            feature, _ = \
                await self._fetch_feature_and_num_nc_files_at(
                    session,
                    self._opensearch_url,
                    dict(parentIdentifier=dataset_id,
                         startDate=data_source.get("temporal_coverage_start"),
                         endDate = data_source.get("temporal_coverage_end"),
                         drsId=dataset_name),
                    1
                )
            if feature is not None:
                feature_info = _extract_feature_info(feature)
                kerchunk_url = f"{feature_info[4].get('Kerchunk')}"
                if kerchunk_url is not None and kerchunk_url not in kerchunk_urls:
                    kerchunk_urls.append(kerchunk_url)
        return kerchunk_urls

    async def _create_data_source(
            self, session, json_dict: dict, datasource_id: str
    ):
        meta_info = await self._fetch_meta_info(
            session,
            datasource_id,
            json_dict.get('odd_url'),
            json_dict.get('metadata_url')
        )
        drs_ids = self._get_as_list(meta_info, 'drs_id', 'drs_ids')
        for drs_id in drs_ids:
            # create list of entries in self._drs_ids starting with drs_id
            self_drs_ids = [e for e in self._drs_ids if e.startswith(drs_id)]
            if len(self_drs_ids) == 0:
                continue

            drs_meta_info = copy.deepcopy(meta_info)
            drs_variables = drs_meta_info.get('variables', {}).get(drs_id, None)
            drs_meta_info.update(json_dict)
            if drs_variables:
                drs_meta_info['variables'] = drs_variables
            drs_uuid = drs_meta_info.get('uuids', {}).get(drs_id, None)
            if drs_uuid:
                drs_meta_info['uuid'] = drs_uuid
            self._adjust_json_dict(drs_meta_info, drs_id)
            for variable in drs_meta_info.get('variables', []):
                variable['var_id'] = variable['var_id'].replace('.', '_')
            drs_meta_info['cci_project'] = drs_meta_info['ecv']
            drs_meta_info['fid'] = datasource_id
            drs_meta_info['num_files'] = drs_meta_info['num_files'][drs_id]
            for sdi in self_drs_ids:
                self._data_sources[sdi] = copy.deepcopy(drs_meta_info)

    def _adjust_json_dict(self, json_dict: dict, drs_id: str):
        values = drs_id.split('.')
        self._adjust_json_dict_for_param(
            json_dict, 'time_frequency', 'time_frequencies',
            _convert_time_from_drs_id(values[2])
        )
        self._adjust_json_dict_for_param(
            json_dict, 'processing_level', 'processing_levels', values[3]
        )
        self._adjust_json_dict_for_param(
            json_dict, 'data_type', 'data_types', values[4]
        )
        self._adjust_json_dict_for_param(
            json_dict, 'sensor_id', 'sensor_ids', values[5]
        )
        self._adjust_json_dict_for_param(
            json_dict, 'platform_id', 'platform_ids', values[6]
        )
        self._adjust_json_dict_for_param(
            json_dict, 'product_string', 'product_strings', values[7]
        )
        self._adjust_json_dict_for_param(
            json_dict, 'product_version', 'product_versions', values[8]
        )

    @staticmethod
    def _adjust_json_dict_for_param(
            json_dict: dict, single_name: str, list_name: str, param_value: str
    ):
        json_dict[single_name] = param_value
        if list_name in json_dict:
            json_dict.pop(list_name)

    @staticmethod
    def _get_as_list(meta_info: dict, single_name: str, list_name: str) -> List:
        if single_name in meta_info:
            return [meta_info[single_name]]
        if list_name in meta_info:
            return meta_info[list_name]
        return []

    def var_and_coord_names(
            self, dataset_name: str
    ) -> Tuple[List[str], List[str]]:
        self._session_executor.run_with_session(
            self._ensure_all_info_in_data_sources, [dataset_name]
        )
        return self._get_data_var_and_coord_names(
            self._data_sources[dataset_name]
        )

    async def _ensure_all_info_in_data_sources(
            self, session, dataset_names: List[str]
    ):
        await self._ensure_in_data_sources(session, dataset_names)
        all_info_tasks = []
        for dataset_name in dataset_names:
            all_info_tasks.append(
                self._ensure_all_info_in_data_source(session, dataset_name)
            )
        await asyncio.gather(*all_info_tasks)

    async def _ensure_all_info_in_data_source(self, session, dataset_name: str):
        data_source = self._data_sources[dataset_name]
        if 'dimensions' in data_source \
                and 'variable_infos' in data_source \
                and 'attributes' in data_source:
            return
        data_fid = await self._get_dataset_id(session, dataset_name)
        await self._set_variable_infos(
            self._opensearch_url, data_fid, dataset_name, session, data_source
        )

    def _get_data_var_and_coord_names(self, data_source) \
            -> Tuple[List[str], List[str]]:
        names_of_dims = list(data_source.get('dimensions', {}).keys())
        variable_infos = data_source['variable_infos']
        variables = []
        coords = []
        for variable_name, variable_info in variable_infos.items():
            if variable_name in names_of_dims:
                coords.append(variable_name)
            elif variable_name.endswith('bounds') or variable_name.endswith('bnds'):
                coords.append(variable_name)
            elif variable_name in COMMON_TIME_COORD_VAR_NAMES:
                coords.append(variable_name)
            elif variable_name == 'geometry':
                coords.append(variable_name)
            elif variable_info.get('data_type', '') == 'bytes1024' \
                    and len(variable_info['dimensions']) > 0:
                # add as neither coordinate nor variable
                continue
            else:
                variables.append(variable_name)
        if self._data_type == "vectordatacube" and "geometry" not in coords:
            coords.append("geometry")
        return variables, coords

    def search(self,
               start_date: Optional[str] = None,
               end_date: Optional[str] = None,
               bbox: Optional[Tuple[float, float, float, float]] = None,
               cci_attrs: Optional[Mapping[str, str]] = None) -> List[str]:
        if cci_attrs is None:
            cci_attrs = {}
        candidate_names = []
        if not self._data_sources and 'ecv' not in cci_attrs \
                and 'frequency' not in cci_attrs \
                and 'processing_level' not in cci_attrs \
                and 'data_type' not in cci_attrs \
                and 'product_string' not in cci_attrs \
                and 'product_version' not in cci_attrs:
            if self._data_type == "dataset":
                self._session_executor.run_with_session(self._read_all_data_sources)
            candidate_names = self.dataset_names
        else:
            for dataset_name in self.dataset_names:
                _, ecv, frequency, processing_level, data_type, sensor, \
                platform, product_string, product_version, _ = \
                    dataset_name.split("~")[0].split('.')
                if cci_attrs.get('ecv', ecv) != ecv:
                    continue
                if cci_attrs.get('processing_level', processing_level) \
                        != processing_level:
                    continue
                if cci_attrs.get('data_type', data_type) != data_type:
                    continue
                if cci_attrs.get('product_string', product_string) != \
                        product_string:
                    continue
                product_version = product_version.replace('-', '.')
                if cci_attrs.get('product_version', product_version) \
                        != product_version:
                    continue
                converted_time = _convert_time_from_drs_id(frequency)
                if cci_attrs.get('frequency', converted_time) != converted_time:
                    continue
                candidate_names.append(dataset_name)
            if len(candidate_names) == 0:
                return []
        if not start_date and not end_date and not bbox \
                and 'institute' not in cci_attrs \
                and 'sensor' not in cci_attrs \
                and 'platform' not in cci_attrs:
            return candidate_names
        results = []
        if start_date:
            converted_start_date = self._get_datetime_from_string(start_date)
        if end_date:
            converted_end_date = self._get_datetime_from_string(end_date)
        self._session_executor.run_with_session(self._ensure_in_data_sources, candidate_names)
        for candidate_name in candidate_names:
            data_source_info = self._data_sources.get(candidate_name)
            if data_source_info is None:
                continue
            institute = cci_attrs.get('institute')
            if institute is not None and \
                    ('institute' not in data_source_info or
                     institute != data_source_info['institute']):
                continue
            if cci_attrs.get('sensor', data_source_info['sensor_id']) \
                    != data_source_info.get('sensor_id'):
                continue
            if cci_attrs.get('platform', data_source_info['platform_id']) \
                    != data_source_info.get('platform_id'):
                continue
            if bbox:
                if float(data_source_info.get('bbox_minx', np.inf)) > bbox[2]:
                    continue
                if float(data_source_info.get('bbox_maxx', -np.inf)) < bbox[0]:
                    continue
                if float(data_source_info.get('bbox_miny', np.inf)) > bbox[3]:
                    continue
                if float(data_source_info.get('bbox_maxy', -np.inf)) < bbox[1]:
                    continue
            if start_date:
                data_source_end = datetime.strptime(
                    data_source_info['temporal_coverage_end'],
                    TIMESTAMP_FORMAT
                )
                # noinspection PyUnboundLocalVariable
                if converted_start_date > data_source_end:
                    continue
            if end_date:
                data_source_start = datetime.strptime(
                    data_source_info['temporal_coverage_start'],
                    TIMESTAMP_FORMAT
                )
                # noinspection PyUnboundLocalVariable
                if converted_end_date < data_source_start:
                    continue
            results.append(candidate_name)
        return results

    async def _read_all_data_sources(self, session):
        catalogue = await self._fetch_data_source_list_json(
            session, self._opensearch_url, dict(parentIdentifier='cci')
        )
        if catalogue:
            tasks = []
            for catalogue_item in catalogue:
                tasks.append(self._create_data_source(
                    session, catalogue[catalogue_item], catalogue_item)
                )
            await asyncio.gather(*tasks)

    async def _ensure_in_data_sources(self, session, dataset_names: List[str]):
        dataset_names_to_check = []
        for dataset_name in dataset_names:
            if dataset_name not in self._data_sources:
                dataset_names_to_check.append(dataset_name)
        if len(dataset_names_to_check) == 0:
            return
        fetch_fid_tasks = []
        catalogue = {}
        for dataset_name in dataset_names_to_check:
            fetch_fid_tasks.append(
                self._update_catalogue_with_data_source_list(
                    session, catalogue, dataset_name
                )
            )
        await asyncio.gather(*fetch_fid_tasks)
        create_source_tasks = []
        for catalogue_item in catalogue:
            create_source_tasks.append(self._create_data_source(
                session, catalogue[catalogue_item], catalogue_item)
            )
        await asyncio.gather(*create_source_tasks)

    async def _update_catalogue_with_data_source_list(
            self, session, catalogue: dict, dataset_name: str
    ):
        dataset_catalogue = await self._fetch_data_source_list_json(
            session, self._opensearch_url, dict(
                parentIdentifier='cci', drsId=dataset_name)
        )
        catalogue.update(dataset_catalogue)

    @staticmethod
    def _get_datetime_from_string(time_as_string: str) -> datetime:
        time_format, start, end, timedelta = \
            find_datetime_format(time_as_string)
        return datetime.strptime(time_as_string[start:end], time_format)

    def get_variable_data(self, dataset_name: str,
                          variable_dict: Dict[str, List[int]],
                          start_time: str = None,
                          end_time: str = None):
        dimension_data = self._session_executor.run_with_session(
            self._get_var_data, dataset_name, variable_dict,
            start_time, end_time
        )
        return dimension_data

    async def _get_var_data(self,
                            session,
                            dataset_name: str,
                            variable_dict: Dict[str, List[int]],
                            start_time: str = None,
                            end_time: str = None):
        await self._ensure_in_data_sources(session, [dataset_name])
        dataset_id = await self._get_dataset_id(session, dataset_name)
        request_start_time = start_time or self._data_sources[dataset_name]["temporal_coverage_start"]
        request_end_time = end_time or self._data_sources[dataset_name]["temporal_coverage_end"]
        request = dict(parentIdentifier=dataset_id,
                       startDate=request_start_time,
                       endDate=request_end_time,
                       drsId=dataset_name
                       )
        orig_request = copy.deepcopy(request)
        opendap_url = await self._get_opendap_url(session, request)
        var_data = {}
        if not opendap_url:
            request = copy.deepcopy(orig_request)
            tar_url = await self._get_tar_url(session, request)
            if tar_url is not None:
                tif_files = await self._get_tif_files_from_tar_url(tar_url, session)
                if len(tif_files) > 0:
                    array = rioxarray.open_rasterio(
                        f"tar+{tar_url}!{tif_files[0]}", chunks=dict(x=512, y=512)
                    )
                    for var_name in variable_dict:
                        data = array[var_name].values
                        var_data[var_name] = dict(size=array[var_name].size,
                                                  shape=array[var_name].shape,
                                                  chunkSize=array[var_name].shape,
                                                  data=list(data))
            else:
                request = copy.deepcopy(orig_request)
                tif_url = await self._get_tif_url(session, request)
                if tif_url is not None:
                    array = rioxarray.open_rasterio(tif_url, chunks=dict(x=512, y=512))
                    for var_name in variable_dict:
                        data = array[var_name].values
                        var_data[var_name] = dict(size=array[var_name].size,
                                                  shape=array[var_name].shape,
                                                  chunkSize=array[var_name].shape,
                                                  data=list(data))
            return var_data
        dataset = await self._get_opendap_dataset(session, opendap_url)
        if not dataset:
            return var_data
        for var_name in variable_dict:
            if var_name in dataset:
                var_data[var_name] = dict(
                    size=dataset[var_name].size,
                    shape=dataset[var_name].shape,
                    chunkSize=dataset[var_name].attributes.get(
                        '_ChunkSizes', list(dataset[var_name].shape)
                    )
                )
                if dataset[var_name].size < 512 * 512:
                    data = await self._get_data_from_opendap_dataset(
                        dataset,
                        session,
                        var_name,
                        (slice(None, None, None),))
                    if data is None:
                        var_data[var_name]['data'] = []
                    else:
                        var_data[var_name]['data'] = data
                else:
                    var_data[var_name]['data'] = []
            elif var_name == "geometry" and self._data_type == "vectordatacube":
                size = 1
                for dim_size in variable_dict[var_name]:
                    size *= dim_size
                var_data[var_name] = dict(
                    size=size,
                    chunkSize=_VECTOR_DATACUBE_CHUNKING,
                    data=[]
                )
            else:
                size = 1
                for dim_size in variable_dict[var_name]:
                    size *= dim_size
                var_data[var_name] = dict(
                    size=size,
                    chunkSize=variable_dict[var_name],
                    data=list(range(size))
                )
        return var_data

    async def _get_feature_list(self, session, request, file_format):
        async with _FEATURE_LIST_LOCK:
            request["fileFormat"] = file_format
            extender = self._extract_times_and_opendap_url
            if file_format != ".nc":
                extender = self._extract_times_and_download_url
            ds_id = request['drsId']
            sdrsid = ds_id.split("~")
            ds_id = sdrsid[0]
            name_filter = ""
            if len(sdrsid) > 1:
                request['drsId'] = ds_id
                name_filter = sdrsid[1]
            start_date_str = request['startDate']
            try:
                start_date = datetime.strptime(start_date_str, TIMESTAMP_FORMAT)
            except (TypeError, IndexError, ValueError, KeyError):
                start_date = int(start_date_str)
            end_date_str = request['endDate']
            try:
                end_date = datetime.strptime(end_date_str, TIMESTAMP_FORMAT)
            except (TypeError, IndexError, ValueError, KeyError):
                end_date = int(end_date_str)
            feature_list = []
            if ds_id not in self._features:
                self._features[ds_id] = {}
            if len(self._features[ds_id].get(file_format, {})) == 0:
                self._features[ds_id][file_format] = []
                await self._fetch_opensearch_feature_list(
                    session, self._opensearch_url, feature_list,
                    extender, request, ""
                )
                if len(feature_list) == 0:
                    # try without dates. For some data sets, this works better
                    if 'startDate' in request:
                        request.pop('startDate')
                    if 'endDate' in request:
                        request.pop('endDate')
                    await self._fetch_opensearch_feature_list(
                        session, self._opensearch_url, feature_list,
                        extender, request, ""
                    )
                feature_list.sort(key=lambda x: x[0])
                self._features[ds_id][file_format] = feature_list
            else:
                if start_date < self._features[ds_id][file_format][0][0]:
                    request['endDate'] = datetime.strftime(
                        self._features[ds_id][file_format][0][0], TIMESTAMP_FORMAT
                    )
                    await self._fetch_opensearch_feature_list(
                        session, self._opensearch_url, feature_list,
                        extender, request, ""
                    )
                    if len(feature_list) > 0:
                        feature_list.sort(key=lambda x: x[0])
                        end_offset = -1
                        while feature_list[end_offset] in self._features[ds_id][file_format] \
                                and end_offset > 0:
                            end_offset -= 1
                        self._features[ds_id][file_format] = \
                            feature_list[:end_offset] + self._features[ds_id][file_format]
                if end_date > self._features[ds_id][file_format][-1][1]:
                    request['startDate'] = datetime.strftime(
                        self._features[ds_id][file_format][-1][1], TIMESTAMP_FORMAT
                    )
                    request['endDate'] = end_date_str
                    await self._fetch_opensearch_feature_list(
                        session, self._opensearch_url, feature_list,
                        extender, request, ""
                    )
                    if len(feature_list) > 0:
                        feature_list.sort(key=lambda x: x[0])
                        end_offset = 0
                        while feature_list[end_offset] in self._features[ds_id][file_format] \
                                and end_offset < len(feature_list) - 1:
                            end_offset += 1
                        if feature_list[end_offset] not in self._features[ds_id][file_format]:
                            self._features[ds_id][file_format] = \
                                self._features[ds_id][file_format] + feature_list[end_offset:]
            sub_feature_list = [f for f in self._features[ds_id][file_format] if name_filter in f[2]]
            start = bisect.bisect_left(
                [feature[1] for feature in sub_feature_list], start_date
            )
            end = bisect.bisect_right(
                [feature[0] for feature in sub_feature_list], end_date
            )
            return sub_feature_list[start:end]

    @staticmethod
    def _extract_times_and_opendap_url(
            features: List[Tuple], feature_list: List[Dict], name_filter: str
    ):
        CciOdp._extract_times_and_url(features, feature_list, "Opendap", name_filter)

    @staticmethod
    def _extract_times_and_download_url(
            features: List[Tuple], feature_list: List[Dict], name_filter: str
    ):
        CciOdp._extract_times_and_url(features, feature_list, "Download", name_filter)

    @staticmethod
    def _extract_times_and_url(
            features: List[Tuple], feature_list: List[Dict], url_type: str,
            name_filter: str
    ):
        for feature in feature_list:
            start_time = None
            end_time = None
            properties = feature.get('properties', {})
            url = None
            links = properties.get('links', {}).get('related', {})
            for link in links:
                if link.get('title', '') == url_type:
                    url = link.get('href', None)
                    url = url if name_filter in url else None
            if not url:
                continue
            uuid = properties.get("identifier")
            date_property = properties.get('date')
            if date_property is not None:
                split_date = date_property.split('/')
                # remove trailing symbols from times
                start_time = datetime.strptime(
                    split_date[0].split('.')[0].split('+')[0], TIMESTAMP_FORMAT
                )
                end_time = datetime.strptime(
                    split_date[1].split('.')[0].split('+')[0], TIMESTAMP_FORMAT
                )
            else:
                title = properties.get('title')
                if title is not None:
                    start_time, end_time = get_timestrings_from_string(title)
                    if start_time:
                        try:
                            start_time = datetime.strptime(
                                start_time, TIMESTAMP_FORMAT
                            )
                        except TypeError:
                            # just use the previous start value
                            pass
                    if end_time:
                        try:
                            end_time = datetime.strptime(
                                end_time, TIMESTAMP_FORMAT
                            )
                        except TypeError:
                            # just use the previous end value
                            pass
                    else:
                        end_time = start_time
            if start_time:
                try:
                    start_time = pd.Timestamp(datetime.strftime(
                        start_time, TIMESTAMP_FORMAT)
                    )
                    end_time = pd.Timestamp(datetime.strftime(
                        end_time, TIMESTAMP_FORMAT)
                    )
                except (TypeError, IndexError, ValueError, KeyError):
                    # just use the previous values
                    pass
                features.append((start_time, end_time, url, uuid))

    async def get_time_chunking(self, session, dataset_name: str):
        await self._ensure_all_info_in_data_source(session, dataset_name)
        return self._data_sources[dataset_name].get("time_chunking", 1)

    def get_time_ranges_from_data(self, dataset_name: str,
                                  start_time: str = _EARLY_START_TIME,
                                  end_time: str = _LATE_END_TIME
                                  ) -> List[Tuple[datetime, datetime]]:
        return self._session_executor.run_with_session(self._get_time_ranges_from_data,
                                      dataset_name,
                                      start_time,
                                      end_time)

    async def _get_time_ranges_from_data(
            self, session, dataset_name: str, start_time: str, end_time: str
    ) -> List[Tuple[datetime, datetime]]:
        dataset_id = await self._get_dataset_id(session, dataset_name)
        request = dict(parentIdentifier=dataset_id,
                       startDate=start_time,
                       endDate=end_time,
                       drsId=dataset_name)

        feature_list = await self._get_feature_list(session, request, '.nc')
        if len(feature_list) == 0:
            request = dict(parentIdentifier=dataset_id,
                           startDate=start_time,
                           endDate=end_time,
                           drsId=dataset_name,
                           fileFormat='.shp')

            feature_list = await self._get_feature_list(session, request, '.shp')
        if len(feature_list) == 0:
            request = dict(parentIdentifier=dataset_id,
                           startDate=start_time,
                           endDate=end_time,
                           drsId=dataset_name)
            feature_list = await self._get_feature_list(session, request, '.tif')
        try:
            time_chunking = await self.get_time_chunking(session, dataset_name)
        except ValueError as ve:
            raise DataStoreError(str(ve))
        ds = self._data_sources[dataset_name]
        # TODO find better criterion
        if time_chunking > 1 and (ds["ecv"] == "BIOMASS" or ds["ecv"] == "ICESHEETS"):
            time_bnds_dim_name = "time_bnds"
            time_dim_name = ds.get("time_coord_name", "time")
            request_time_ranges = []
            for feature in feature_list:
                start_time = datetime.strftime(feature[0], TIMESTAMP_FORMAT)
                end_time = datetime.strftime(feature[1], TIMESTAMP_FORMAT)
                time_bnds_size = ds.get("variable_infos", {}).get(time_bnds_dim_name, {}).get(
                    "shape", ds.get("variable_infos", {}).get(time_bnds_dim_name, {}).get("chunk_sizes"))
                if time_bnds_size is not None:
                    time_bnds_data = await self._get_var_data(
                        session, dataset_name, {time_bnds_dim_name: time_bnds_size},
                        start_time, end_time
                    )
                    for time_bound in time_bnds_data[time_bnds_dim_name]["data"]:
                        start_time_value = self._convert_time_value(ds, time_bound[0])
                        end_time_value = self._convert_time_value(ds, time_bound[1])
                        request_time_ranges.append((start_time_value, end_time_value))
                else:
                    time_data = await self._get_var_data(
                        session, dataset_name, {time_dim_name: [time_chunking]},
                        start_time, end_time
                    )
                    for raw_time_value in time_data[time_dim_name]["data"]:
                        time_value = self._convert_time_value(ds, raw_time_value)
                        time_step_range = time_value, time_value
                        request_time_ranges.append(time_step_range)
        else:
            request_time_ranges = [feature[0:2] for feature in feature_list]
        return request_time_ranges

    @staticmethod
    def _convert_time_value(data_source, raw_time_value):
        time_coord_name = data_source.get("time_coord_name", "time")
        time_units = (data_source.get("variable_infos", {}).get(time_coord_name, {}).
                      get("units"))
        if "since" in time_units:
            time_unit, t_offset = time_units.split("since")
            time_offset = pd.Timestamp(t_offset)
            time_unit = time_unit.strip().lower()
            return time_offset + pd.Timedelta(raw_time_value, unit=time_unit)
        else:
            return pd.Timestamp(raw_time_value)

    def get_dataset_id(self, dataset_name: str) -> str:
        return self._session_executor.run_with_session(self._get_dataset_id, dataset_name)

    async def _get_dataset_id(self, session, dataset_name: str) -> str:
        await self._ensure_in_data_sources(session, [dataset_name])
        return self._data_sources[dataset_name].get(
            'uuid', self._data_sources[dataset_name]['fid']
        )

    async def _get_shapefile_url(self, session, request: Dict):
        request['fileFormat'] = '.shp'
        feature_list = await self._get_feature_list(session, request, '.shp')
        if len(feature_list) == 0:
            return
        return feature_list[0][2]

    async def _get_tif_url(self, session, request: Dict):
        feature_list = await self._get_feature_list(session, request, '.tif')
        if len(feature_list) == 0:
            return
        return feature_list[0][2]

    async def _get_tar_url(self, session, request: Dict):
        feature_list = await self._get_feature_list(session, request, '.gz')
        if len(feature_list) == 0:
            return
        return feature_list[0][2]

    async def _get_opendap_url(self, session, request: Dict):
        feature_list = await self._get_feature_list(session, request, '.nc')
        if len(feature_list) == 0:
            return
        return feature_list[0][2]

    def get_data_chunk(
            self, request: Dict, dim_indexes: Tuple, to_bytes: bool = True
    ) -> Optional[Union[bytes, np.array]]:
        data_chunk = self._session_executor.run_with_session(
            self._get_data_chunk, request, dim_indexes, to_bytes
        )
        return data_chunk

    async def _get_data_chunk(
            self, session, request: Dict, dim_indexes: Tuple, to_bytes: bool = True
    ) -> Optional[bytes]:
        key = (
            json.dumps(request, sort_keys=True),
            dim_indexes,
            to_bytes,
        )
        try:
            return await self._task_cache[key]
        except KeyError:
            pass
        if self._data_type == "vectordatacube":
            task = asyncio.create_task(self._get_vectordatacube_chunk(
                session, request, dim_indexes, to_bytes
            ))
        else:
            task = asyncio.create_task(
                self._get_dataset_chunk(session, request, dim_indexes, to_bytes)
            )
        self._task_cache[key] = task
        try:
            return await task
        finally:
            self._task_cache.pop(key, None)

    async def get_geometry_data(
            self, session, dataset, data_source, geom_var_name, ds_dim_indexes
    ):
        var_name = data_source[geom_var_name]
        data_type = [d for d in data_source.get('variables', {})
                     if d["var_id"] == var_name][0].get("data_type")
        data = await self._get_data_from_opendap_dataset(
            dataset, session, var_name, ds_dim_indexes
        )
        return np.asarray(data, dtype=data_type)

    async def _get_vectordatacube_chunk(
            self, session, request: Dict, dim_indexes: Tuple, to_bytes: bool = True
    ) -> Optional[bytes]:
        var_name = request.get('varNames')[0]
        drsId = request.get('drsId')
        vector_offsets = self._vector_offsets[drsId]
        await self._ensure_all_info_in_data_sources(session, [drsId])
        data_source = self._data_sources[drsId]
        dimensions = (data_source.get('variable_infos', {}).
                      get(var_name, {}).get('file_dimensions'))
        geometry_index = dimensions.index(data_source.get("geometry_dimension"))
        geom_start_index = dim_indexes[geometry_index].start
        geom_stop_index = dim_indexes[geometry_index].stop
        start = bisect.bisect_right(
            [vo[0] for vo in vector_offsets], geom_start_index
        ) - 1
        end = bisect.bisect_right(
            [vo[0] for vo in vector_offsets], geom_stop_index
        ) - 1
        res = None
        for i in range(start, end + 1):
            vo = vector_offsets[i]
            geom_dim_start_index = max(0, geom_start_index - vo[0])
            geom_dim_stop_index = min(geom_stop_index, vo[1]) - vo[0]
            if geom_dim_start_index == geom_dim_stop_index:
                continue
            opendap_url = vo[2]
            dataset = await self._get_opendap_dataset(session, opendap_url)
            ds_dim_index_list = []
            for i, di in enumerate(dim_indexes):
                if i == geometry_index:
                    ds_dim_index_list.append(
                        slice(geom_dim_start_index, geom_dim_stop_index)
                    )
                else:
                    ds_dim_index_list.append(di)
            ds_dim_indexes = tuple(ds_dim_index_list)
            if var_name == "geometry":
                lat_data = await self.get_geometry_data(
                    session, dataset, data_source, "lat_var", ds_dim_indexes
                )
                lon_data = await self.get_geometry_data(
                    session, dataset, data_source, "lon_var", ds_dim_indexes
                )
                lat_lon_data = np.array((lon_data, lat_data)).T
                geometry_data = [mapping(Point(ll)) for ll in lat_lon_data]
                np_array = np.asarray(geometry_data, dtype=object)
            else:
                data_type = (data_source.get('variable_infos', {}).get(var_name, {}).
                             get('data_type'))
                data = await self._get_data_from_opendap_dataset(
                    dataset, session, var_name, ds_dim_indexes
                )
                np_array = np.asarray(data, dtype=data_type)
            if res is None:
                res = np_array
            else:
                res = np.append(res, np_array)
        if to_bytes:
            if var_name == "geometry":
                if len(res) < _VECTOR_DATACUBE_CHUNKING:
                    to_fill = _VECTOR_DATACUBE_CHUNKING - len(res)
                    nones = [None] * to_fill
                    res = np.append(res, np.array(nones))
                codec = numcodecs.JSON()
                return codec.encode(res)
            else:
                if res.shape[geometry_index] < _VECTOR_DATACUBE_CHUNKING:
                    fill_size = _VECTOR_DATACUBE_CHUNKING - res.shape[geometry_index]
                    padding = []
                    for i in range(len(res.shape)):
                        if i == geometry_index:
                            padding.append((0, fill_size))
                        else:
                            padding.append((0, 0))
                    padding = tuple(padding)
                    fill_value = (
                        data_source.get("variable_infos", {}).get(var_name, {}).
                        get("fill_value", np.nan)
                    )
                    res = np.pad(res, pad_width=padding, constant_values=fill_value)
                return res.flatten().tobytes()
        return res

    async def _get_dataset_chunk(
            self, session, request: Dict, dim_indexes: Tuple, to_bytes: bool = True
    ) -> Optional[bytes]:
        drs_id = request.get("drsId")
        orig_request = copy.deepcopy(request)
        var_name = request.pop('varNames')[0]
        opendap_url = await self._get_opendap_url(session, request)
        await self._ensure_all_info_in_data_sources(
            session, [drs_id]
        )
        data_type = self._data_sources[drs_id].\
            get('variable_infos', {}).get(var_name, {}).get('data_type')
        if not opendap_url:
            request = copy.deepcopy(orig_request)
            tar_url = await self._get_tar_url(session, request)
            if tar_url is not None:
                dims = self._data_sources[drs_id].get("variable_infos", {}).\
                    get(var_name, {}).get("dimensions")
                chunks = {}
                sel_chunks = {}
                offset = len(dim_indexes)  - len(dims)
                for i in range(len(dims)):
                    di = dim_indexes[offset + i]
                    chunks[dims[i]] = di.stop - di.start
                    sel_chunks[dims[i]] = di
                tif_files = await self._get_tif_files_from_tar_url(tar_url, session)
                tif_files = [tf for tf in tif_files if var_name in tf]
                if len(tif_files) > 0:
                    file_path = f"tar+{tar_url}!{tif_files[0]}"
                    if file_path not in self._tif_to_array:
                        array = rioxarray.open_rasterio(file_path, chunks=chunks)
                        self._tif_to_array[file_path] = array
                    array = self._tif_to_array[file_path]
                    data = array.isel(sel_chunks)
                    data = np.asarray(data, dtype=data_type)
                    if to_bytes:
                        return data.flatten().tobytes()
                    return data
            else:
                request = copy.deepcopy(orig_request)
                tif_url = await self._get_tif_url(session, request)
                if tif_url is not None:
                    dims = self._data_sources[drs_id].get("variable_infos", {}). \
                        get(var_name, {}).get("dimensions")
                    chunks = {}
                    sel_chunks = {}
                    offset = len(dim_indexes) - len(dims)
                    for i in range(len(dims)):
                        di = dim_indexes[offset + i]
                        chunks[dims[i]] = di.stop - di.start
                        sel_chunks[dims[i]] = di
                    band_index = self._data_sources[drs_id].get("variable_infos", {}).get(var_name, {}).get("band_index")
                    if band_index is not None:
                        sel_chunks["band"] = band_index
                    if tif_url not in self._tif_to_array:
                        array = rioxarray.open_rasterio(tif_url, chunks=chunks)
                        self._tif_to_array[tif_url] = array
                    array = self._tif_to_array[tif_url]
                    data = array.isel(sel_chunks)
                    data = np.asarray(data, dtype=data_type)
                    if to_bytes:
                        return data.flatten().tobytes()
                    return data
            return None
        dataset = await self._get_opendap_dataset(session, opendap_url)
        if not dataset:
            return None
        data = await self._get_data_from_opendap_dataset(
            dataset, session, var_name, dim_indexes
        )
        if data is None:
            return None
        if data_type == 'bytes1024':
            if data.size > 1:
                data = [d.decode() for d in data]
        else:
            data = np.asarray(data, dtype=data_type)
        if to_bytes:
            return data.flatten().tobytes()
        return data

    def get_geodataframe_from_shapefile(
            self, request: Dict
    ) -> Optional[gpd.GeoDataFrame]:
        gdf = self._session_executor.run_with_session(
            self._get_geodataframe_from_shapefile, request
        )
        return gdf

    async def _get_geodataframe_from_shapefile(
            self, session, request: Dict
    ) -> Optional[gpd.GeoDataFrame]:
        var_names = request['varNames']
        shapefile_url = await self._get_shapefile_url(session, request)
        if not shapefile_url:
            return None
        gdf = gpd.read_file(shapefile_url)
        gdf = gdf[var_names]
        return gdf

    async def _fetch_data_source_list_json(self, session, base_url, query_args,
                                           max_wanted_results=100000) -> Dict:
        def _extender(
                inner_catalogue: dict, feature_list: List[Dict], name_filter: str
        ):
            for fc in feature_list:
                fc_props = fc.get("properties", {})
                fc_id = fc_props.get("identifier", None)
                fc_title = fc_props.get("title", "")
                if not fc_id or name_filter not in fc_title:
                    continue
                inner_catalogue[fc_id] = _get_feature_dict_from_feature(fc)
        catalogue = {}
        name_filter = ""
        if "drsId" in query_args:
            sdrsid = query_args.get("drsId").split("~")
            query_args["drsId"] = sdrsid[0]
            if query_args.get("parentIdentifier", "cci") != "cci":
                name_filter = sdrsid[1] if len(sdrsid) > 1 else ""
        await self._fetch_opensearch_feature_list(
            session, base_url, catalogue, _extender, query_args, name_filter,
            max_wanted_results
        )
        return catalogue

    async def _fetch_opensearch_feature_list(
            self, session, base_url, extension, extender, query_args, name_filter,
            max_wanted_results=100000
    ):
        """
        Return JSON value read from Opensearch web service.
        :return:
        """
        start_page = 1
        initial_maximum_records = min(1000, max_wanted_results)
        total_results = await self._fetch_opensearch_feature_part_list(
            session, base_url, query_args, start_page, initial_maximum_records,
            extension, extender, None, None, name_filter
        )
        if total_results < initial_maximum_records or max_wanted_results < 1000:
            return
        num_results = 0
        extension.clear()
        while num_results < total_results:
            if 'startDate' in query_args and 'endDate' in query_args:
                # we have to clear the extension of any previous values
                # to avoid duplicate values extension.clear()
                start_time = datetime.strptime(
                    query_args.pop('startDate'), TIMESTAMP_FORMAT
                )
                start_time = datetime(start_time.year, month=1, day=1)
                end_time = datetime.strptime(
                    query_args.pop('endDate'), TIMESTAMP_FORMAT
                )
                end_time = datetime(end_time.year, month=12, day=31, hour=23, minute=59, second=59)
                one_year = relativedelta(years=1, seconds=-1)
                one_second = relativedelta(seconds=1)
                current_time = start_time
                while current_time < end_time:
                    task_start = current_time.strftime(TIMESTAMP_FORMAT)
                    current_time += one_year
                    if current_time > end_time:
                        current_time = end_time
                    task_end = current_time.strftime(TIMESTAMP_FORMAT)
                    await self._fetch_opensearch_feature_part_list(
                        session, base_url, query_args, start_page,
                        None, extension, extender,
                        task_start, task_end, name_filter
                    )
                    current_time += one_second
                num_results = total_results
            else:
                await self._fetch_opensearch_feature_part_list(
                    session, base_url, query_args, start_page,
                    None, extension, extender, None, None, name_filter)
                num_results = len(extension)

    async def _fetch_opensearch_feature_part_list(
            self, session, base_url, query_args, start_page, max_records,
            extension, extender, start_date, end_date, name_filter
    ) -> int:
        paging_query_args = dict(query_args or {})
        maximum_records = max_records if max_records else 10000
        paging_query_args.update(startPage=start_page,
                                 maximumRecords=maximum_records,
                                 httpAccept='application/geo+json')
        if start_date:
            paging_query_args.update(startDate=start_date)
        if end_date:
            paging_query_args.update(endDate=end_date)
        total_results = 0
        url = base_url + '?' + urllib.parse.urlencode(paging_query_args)
        while url is not None:
            resp_content = await self._session_executor.get_response_content_from_session(
                session, url
            )
            url = None
            if resp_content:
                json_dict = json.loads(resp_content.decode('utf-8'))
                next_links = json_dict.get("links", {}).get("next")
                url = next_links[0].get("href") if next_links is not None else None
                if extender:
                    feature_list = json_dict.get("features", [])
                    extender(extension, feature_list, name_filter)
                total_results = json_dict['totalResults']
                if max_records is not None and max_records <= total_results:
                    break
        return total_results

    async def _set_variable_infos(self, opensearch_url: str, dataset_id: str,
                                  dataset_name: str, session, data_source):
        attributes = {}
        dimensions = {}
        variable_infos = {}
        if data_source.get('variable_manifest'):
            resp_content = await self._session_executor.get_response_content_from_session(
                session, data_source.get('variable_manifest')
            )
            if resp_content:
                json_dict = json.loads(resp_content.decode('utf-8'))
                data_source['variables'] = json_dict.get(dataset_name, [])

        start_time = data_source.get("temporal_coverage_start")
        end_time = data_source.get("temporal_coverage_end")

        feature, num_shapefiles = \
            await self._fetch_feature_from_shapefile(
                session,
                opensearch_url,
                dict(parentIdentifier=dataset_id,
                     startDate=start_time,
                     endDate=end_time,
                     drsId=dataset_name),
                1
            )
        if feature is not None:
            variable_infos, attributes = \
                await self._get_variable_infos_from_shapefile_feature(feature)
            attributes["shapefile"] = True
            dimensions = {}
        else:
            feature, time_dimension_size = \
                await self._fetch_feature_and_num_nc_files_at(
                    session,
                    opensearch_url,
                    dict(parentIdentifier=dataset_id,
                         startDate=start_time,
                         endDate=end_time,
                         drsId=dataset_name),
                    1
                )
            if feature is None:
                feature, time_dimension_size = \
                    await self._fetch_feature_and_num_tar_files_at(
                        session,
                        opensearch_url,
                        dict(parentIdentifier=dataset_id,
                             startDate=start_time,
                             endDate=end_time,
                             drsId=dataset_name),
                        1
                    )
            if feature is None:
                feature, time_dimension_size = \
                    await self._fetch_feature_and_num_tif_files_at(
                        session,
                        opensearch_url,
                        dict(parentIdentifier=dataset_id,
                             startDate=start_time,
                             endDate=end_time,
                             drsId=dataset_name),
                        1
                    )
            if feature is not None:
                variable_infos, attributes = \
                    await self._get_variable_infos_from_feature(feature, session)
                attributes["shapefile"] = False
                for variable_info in variable_infos:
                    for index, dimension in enumerate(
                            variable_infos[variable_info]['dimensions']
                    ):
                        if dimension not in dimensions:
                            dimensions[dimension] = \
                                variable_infos[variable_info]['shape'][index]
                time_dim_name = "time"
                if 'AEROSOL.climatology' in dataset_name:
                    time_dim_name = 'month'
                if "Time" in dimensions:
                    time_dim_name = "Time"
                if "nbmonth" in dimensions:
                    time_dim_name = "nbmonth"
                    time_dimension_size = 1
                data_source["time_chunking"] = dimensions.get(time_dim_name, 1)
                if data_source["ecv"] == "VEGETATION" and data_source["time_frequency"] == "5days":
                    data_source["time_chunking"] = 73
                time_coord_name = time_dim_name
                if time_coord_name not in variable_infos.keys():
                    potential_time_names = ["time", "t", "Time", "T"]
                    for potential_time_name in potential_time_names:
                        if (potential_time_name in variable_infos.keys() and
                                variable_infos[potential_time_name].get(
                                    "dimensions", []) == [time_dim_name]):
                            time_coord_name = potential_time_name
                            break
                data_source["time_coord_name"] = time_coord_name
                dimensions[time_dim_name] = (
                        time_dimension_size * data_source["time_chunking"])
                for variable_info in variable_infos.values():
                    if time_dim_name in variable_info['dimensions']:
                        time_index = variable_info['dimensions'].index(time_dim_name)
                        if 'shape' in variable_info:
                            variable_info['shape'][time_index] = \
                                dimensions[time_dim_name]
                            variable_info['size'] = np.prod(variable_info['shape'])
        data_source['dimensions'] = dimensions
        data_source['variable_infos'] = variable_infos
        data_source['attributes'] = attributes
        if self._data_type == "vectordatacube":
            start_time = data_source.get("temporal_coverage_start")
            end_time = data_source.get("temporal_coverage_end")
            non_time_dimension = [dim for dim in dimensions if not dim == time_dim_name][0]
            num_geometries = await self._count_geometries(
                session, dataset_id, dataset_name, start_time, end_time,
                non_time_dimension
            )
            data_source["dimensions"][non_time_dimension] = num_geometries
            data_source["geometry_dimension"] = non_time_dimension
            for variable_name, var_dict in variable_infos.items():
                if non_time_dimension in var_dict.get("dimensions"):
                    index = var_dict.get("dimensions").index(non_time_dimension)
                    if "shape" in var_dict:
                        data_source["variable_infos"][variable_name]["shape"][index] \
                            = num_geometries
                        data_source["variable_infos"][variable_name]["size"] = \
                            sum(data_source["variable_infos"][variable_name]["shape"])
                    if len(var_dict.get("dimensions")) > 1:
                        data_source["variable_infos"][variable_name]["chunk_sizes"][index] \
                            = _VECTOR_DATACUBE_CHUNKING
                        data_source["variable_infos"][variable_name]["file_chunk_sizes"][index] \
                            = _VECTOR_DATACUBE_CHUNKING
                    else:
                        data_source["variable_infos"][variable_name]["chunk_sizes"] \
                                = _VECTOR_DATACUBE_CHUNKING
                        data_source["variable_infos"][variable_name]["file_chunk_sizes"] \
                                = _VECTOR_DATACUBE_CHUNKING
            lat_lons = [("lat", "lon"), ("latitude", "longitude")]
            for lat_lon in lat_lons:
                data_source["lat_var"] = lat_lon[0]
                data_source["lon_var"] = lat_lon[1]
                if lat_lon[0] in variable_infos.keys() \
                        and lat_lon[1] in variable_infos.keys():
                    var_info = variable_infos.pop(lat_lon[0])
                    variable_infos["geometry"] = dict(
                        standard_name="geometry",
                        long_name="geometry",
                        dimensions=var_info.get('dimensions'),
                        file_dimensions=var_info.get('file_dimensions'),
                        size=var_info.get('size'),
                        shape=var_info.get('shape'),
                        chunk_sizes=[_VECTOR_DATACUBE_CHUNKING],
                        file_chunk_sizes=[_VECTOR_DATACUBE_CHUNKING],
                        data_type="object"
                    )
                    variable_infos.pop(lat_lon[1])
                    break

    async def _count_geometries(
            self, session, dataset_id, dataset_name, start_time, end_time,
            non_time_dimension
    ):
        request = dict(parentIdentifier=dataset_id,
                       startDate=start_time,
                       endDate=end_time,
                       drsId=dataset_name,
                       fileFormat='.nc')
        feature_list = await self._get_feature_list(session, request, '.nc')
        search_string = f"{non_time_dimension} = "
        offsets = []
        offset = 0
        for feature in feature_list:
            opendap_url = feature[2]
            res_dict = {}
            await self._get_content_from_opendap_url(
                opendap_url, 'dds', res_dict, session
            )
            index = res_dict.get("dds").index(search_string)
            sub_dds = res_dict.get("dds")[index + len(search_string):]
            next_offset = offset + int(sub_dds[:sub_dds.index("]")])
            offsets.append([offset, next_offset, opendap_url])
            offset = next_offset
        self._vector_offsets[dataset_name] = offsets
        return offset

    async def _fetch_feature_and_num_nc_files_at(
            self, session, base_url, query_args, index
    ) -> Tuple[Optional[Dict], int]:
        return await self._fetch_feature_and_num_files_at(
            session, base_url, query_args, index, '.nc'
        )

    async def _fetch_feature_and_num_tar_files_at(
            self, session, base_url, query_args, index
    ) -> Tuple[Optional[Dict], int]:
        return await self._fetch_feature_and_num_files_at(
            session, base_url, query_args, index, '.gz'
        )

    async def _fetch_feature_and_num_tif_files_at(
            self, session, base_url, query_args, index
    ) -> Tuple[Optional[Dict], int]:
        return await self._fetch_feature_and_num_files_at(
            session, base_url, query_args, index, '.tif'
        )

    async def _fetch_feature_and_num_files_at(
            self, session, base_url, query_args, index, file_format
    ) -> Tuple[Optional[Dict], int]:
        paging_query_args = dict(query_args or {})
        drs_id = paging_query_args.get("drsId", "")
        sdrsid = drs_id.split("~")
        if len(sdrsid) == 1:
            paging_query_args.update(
                startPage=index, maximumRecords=5, httpAccept='application/geo+json', fileFormat=file_format
            )
            if "startDate" in paging_query_args:
                paging_query_args.pop("startDate")
            if "endDate" in paging_query_args:
                paging_query_args.pop("endDate")
            url = base_url + '?' + urllib.parse.urlencode(paging_query_args)
            resp_content = await self._session_executor.get_response_content_from_session(
                session, url
            )
            if resp_content:
                json_dict = json.loads(resp_content.decode('utf-8'))
                feature_list = json_dict.get("features", [])
                if len(feature_list) > 0:
                    index = math.floor(len(feature_list) / 2)
                    total_num_files = json_dict.get("totalResults", 0)
                    return feature_list[index], total_num_files
        elif len(sdrsid) == 2:
            await self._ensure_in_data_sources(session, [drs_id])
            data_source = self._data_sources[drs_id]
            paging_query_args.update(
                startDate = data_source.get("temporal_coverage_start"),
                endDate = data_source.get("temporal_coverage_end")
            )
            feature_list = await self._get_feature_list(session, paging_query_args, file_format)
            if len(feature_list) > 0:
                index = math.floor(len(feature_list) / 2)
                uuid = feature_list[index][3]
                paging_query_args.update(
                    httpAccept='application/geo+json', uuid=uuid
                )
                if "startDate" in paging_query_args:
                    paging_query_args.pop("startDate")
                if "endDate" in paging_query_args:
                    paging_query_args.pop("endDate")
                url = base_url + '?' + urllib.parse.urlencode(paging_query_args)
                resp_content = await self._session_executor.get_response_content_from_session(
                    session, url
                )
                if resp_content:
                    json_dict = json.loads(resp_content.decode('utf-8'))
                    new_feature_list = json_dict.get("features", [])
                    if len(new_feature_list) > 0:
                        return new_feature_list[0], len(feature_list)
            return None, len(feature_list)
        return None, 0

    async def _fetch_feature_from_shapefile(
            self, session, base_url, query_args, index
    ) -> Tuple[Optional[Dict], int]:
        paging_query_args = dict(query_args or {})
        paging_query_args.update(startPage=index,
                                 maximumRecords=5,
                                 httpAccept='application/geo+json',
                                 fileFormat='.shp')
        if "startDate" in paging_query_args:
            paging_query_args.pop("startDate")
        if "endDate" in paging_query_args:
            paging_query_args.pop("endDate")
        url = base_url + '?' + urllib.parse.urlencode(paging_query_args)
        resp_content = await self._session_executor.get_response_content_from_session(
            session, url
        )
        if resp_content:
            json_dict = json.loads(resp_content.decode('utf-8'))
            feature_list = json_dict.get("features", [])
            # we try not to take the first feature,
            # as the last and the first one may have different time chunkings
            if len(feature_list) > 0:
                index = math.floor(len(feature_list) / 2)
                return feature_list[index], json_dict.get("totalResults", 0)
        return None, 0

    async def _fetch_meta_info(self,
                               session,
                               datasource_id: str,
                               odd_url: str,
                               metadata_url: str) -> Dict:
        meta_info_dict = {}
        if odd_url:
            meta_info_dict = self._odp_connector.extract_metadata_from_odd_url(odd_url)
        read_ceda_catalogue = os.environ.get("READ_CEDA_CATALOGUE", "1")
        if metadata_url and read_ceda_catalogue != '0':
            desc_metadata = await self._extract_metadata_from_descxml_url(
                session, metadata_url
            )
            for item in desc_metadata:
                if item not in meta_info_dict:
                    meta_info_dict[item] = desc_metadata[item]
        _harmonize_info_field_names(
            meta_info_dict, 'file_format', 'file_formats'
        )
        _harmonize_info_field_names(
            meta_info_dict, 'platform_id', 'platform_ids'
        )
        _harmonize_info_field_names(
            meta_info_dict, 'sensor_id', 'sensor_ids'
        )
        _harmonize_info_field_names(
            meta_info_dict, 'processing_level', 'processing_levels'
        )
        _harmonize_info_field_names(
            meta_info_dict, 'time_frequency', 'time_frequencies'
        )
        return meta_info_dict

    async def _extract_metadata_from_descxml_url(
            self, session, descxml_url: str = None
    ) -> dict:
        if not descxml_url:
            return {}
        resp_content = await self._session_executor.get_response_content_from_session(
            session, descxml_url
        )
        if resp_content:
            descxml = etree.XML(resp_content)
            try:
                return _extract_metadata_from_descxml(descxml)
            except etree.ParseError:
                LOG.info(f'Cannot read metadata from {descxml_url} '
                          f'due to parsing error.')
        return {}

    @staticmethod
    async def _get_variable_infos_from_shapefile_feature(
            feature: dict
    ) -> (dict, dict):
        feature_info = _extract_feature_info(feature)
        shapefile_url = f"{feature_info[4].get('Download')}"
        if shapefile_url == 'None':
            LOG.info('Shapefile is not accessible')
            return {}, {}
        geodataframe = gpd.read_file(shapefile_url)
        variable_infos = {}
        for column in geodataframe.columns:
            variable_infos[column] = {}
            variable_infos[column]["name"] = geodataframe[column].name
            variable_infos[column]["dtype"] = geodataframe[column].dtype
        return variable_infos, geodataframe.attrs

    @staticmethod
    def _get_kerchunk_url_from_feature(feature: dict) -> str:
        feature_info = _extract_feature_info(feature)
        kerchunk_url = f"{feature_info[4].get('Kerchunk')}"
        return kerchunk_url

    async def _get_variable_infos_from_feature(self,
                                               feature: dict,
                                               session) -> (dict, dict):
        feature_info = _extract_feature_info(feature)
        opendap_url = f"{feature_info[4].get('Opendap')}"
        if opendap_url == 'None':
            download_url = f"{feature_info[4].get('Download', '')}"
            if download_url.endswith(".tar.gz"):
                return await self._get_variable_infos_from_tar_feature(feature, session)
            elif download_url.endswith(".tif"):
                return await self._get_variable_infos_from_tif_feature(feature, session)
            raise DataStoreError("Dataset is not accessible via Opendap or Download")
        dataset = await self._get_opendap_dataset(session, opendap_url)
        if not dataset:
            raise DataStoreError(f"Could not extract information about variables and attributes from {opendap_url}")
        variable_infos = {}
        time_set_as_dim = False
        for key in dataset.keys():
            fixed_key = key.replace('%2E', '_').replace('.', '_')
            data_type = dataset[key].dtype.name
            var_attrs = copy.deepcopy(dataset[key].attributes)
            var_attrs['orig_data_type'] = data_type
            if '_FillValue' in var_attrs:
                var_attrs['fill_value'] = var_attrs['_FillValue']
                if (var_attrs.get("orig_data_type", "").startswith("u")
                        and var_attrs['fill_value'] < 0):
                    if var_attrs["orig_data_type"][-2:].isdigit():
                        factor = int(var_attrs["orig_data_type"][-2:])
                        var_attrs['fill_value'] += 2 ** factor
                    elif var_attrs["orig_data_type"][-1].isdigit():
                        factor = int(var_attrs["orig_data_type"][-1])
                        var_attrs['fill_value'] += 2 ** factor
                    else:
                        LOG.info(
                            f'Variable "{fixed_key}" has negative fill value, '
                            f'but unsigned data type "{var_attrs["orig_data_type"]}"',
                            category=CciOdpWarning
                        )
                var_attrs.pop('_FillValue')
                if "missing_value" in var_attrs:
                    var_attrs.pop('missing_value')
            else:
                if data_type in _DTYPES_TO_DTYPES_WITH_MORE_BYTES:
                    data_type = _DTYPES_TO_DTYPES_WITH_MORE_BYTES[data_type]
                    var_attrs['fill_value'] = \
                        _determine_fill_value(np.dtype(data_type))
                elif dataset[key].size > 1:
                    LOG.info(f'Variable "{fixed_key}" has no fill value, '
                             f'cannot set one. For parts where no data is '
                             f'available you will see random values. This '
                             f'is usually the case when data is missing '
                             f'for a time step.',
                             category=CciOdpWarning)
            var_attrs['size'] = dataset[key].size
            var_attrs['shape'] = list(dataset[key].shape)
            if len(var_attrs['shape']) == 0:
                var_attrs['shape'] = [var_attrs['size']]
            if '_ChunkSizes' in var_attrs and 'DODS' not in var_attrs:
                var_attrs['chunk_sizes'] = var_attrs['_ChunkSizes']
                var_attrs.pop('_ChunkSizes')
            else:
                var_attrs['chunk_sizes'] = var_attrs['shape']
            # do this to ensure that chunk size is never bigger than shape
            if isinstance(var_attrs['chunk_sizes'], List):
                for i, chunksize in enumerate(var_attrs['chunk_sizes']):
                    var_attrs['chunk_sizes'][i] = min(chunksize,
                                                      var_attrs['shape'][i])
            else:
                var_attrs['chunk_sizes'] = min(var_attrs['chunk_sizes'],
                                               var_attrs['shape'][0])
            if type(var_attrs['chunk_sizes']) is int:
                var_attrs['file_chunk_sizes'] = var_attrs['chunk_sizes']
            else:
                var_attrs['file_chunk_sizes'] = \
                    copy.deepcopy(var_attrs['chunk_sizes'])
            var_attrs['data_type'] = data_type
            var_attrs['dimensions'] = list(dataset[key].dimensions)
            if "time" in var_attrs['dimensions']:
                time_set_as_dim = True
            var_attrs['file_dimensions'] = \
                copy.deepcopy(var_attrs['dimensions'])
            variable_infos[fixed_key] = var_attrs
        if variable_infos.get("time", {}).get("size", 1) > 1 and not time_set_as_dim:
            variable_infos.pop("time")
        return variable_infos, dataset.attributes

    async def _get_tif_files_from_tar_url(self, tar_url: str, session) -> List[str]:
        if tar_url not in self._tar_to_tif:
            resp_content = await self._session_executor.get_response_content_from_session(
                session, tar_url
            )
            tif_files = []
            if resp_content:
                tar_file = io.BytesIO(resp_content)
                tar = tarfile.open(fileobj=tar_file, mode="r:gz")
                content = tar.getnames()
                tif_files = [c for c in content if c.endswith(".tif")]
            self._tar_to_tif[tar_url] = tif_files
        return self._tar_to_tif[tar_url]

    async def _get_variable_infos_from_tar_feature(
            self, feature: dict, session
    ) -> (dict, dict):
        feature_info = _extract_feature_info(feature)
        download_url = f"{feature_info[4].get('Download')}"
        if download_url == 'None':
            LOG.warning('Dataset is not accessible via Download')
            return {}, {}
        tif_files = await self._get_tif_files_from_tar_url(download_url, session)
        var_dict = {}
        var_infos = {}
        attributes = {}
        for file in tif_files:
            array = rioxarray.open_rasterio(
                f"tar+{download_url}!{file}", chunks=dict(x=512, y=512)
            )
            var_name = file.split(".tif")[0].split("-")[-1]
            self._put_variable_info_from_tif_file_var_infos_attributes(
                array, var_name, var_dict, var_infos, attributes
            )
        return var_infos, attributes

    async def _get_variable_infos_from_tif_feature(
            self, feature: dict, session
    ) -> (dict, dict):
        feature_info = _extract_feature_info(feature)
        download_url = f"{feature_info[4].get('Download')}"
        if download_url == 'None':
            LOG.warning('Dataset is not accessible via Download')
            return {}, {}
        var_infos = {}
        attributes = {}
        array = rioxarray.open_rasterio(download_url, chunks=dict(x=512, y=512))
        var_dicts = self._get_var_names_from_download_url(download_url)
        for var_name, var_dict in var_dicts.items():
            self._put_variable_info_from_tif_file_var_infos_attributes(
                array, var_name, var_dict, var_infos, attributes
            )
        return var_infos, attributes

    @staticmethod
    def _get_var_names_from_download_url(download_url: str) -> Dict[str, Dict]:
        file_name = download_url.split("/")[-1].split(".tif")[0]
        for var_name, var_dict in TIFF_VARS.items():
            if var_name in file_name:
                return var_dict

    @staticmethod
    def _put_variable_info_from_tif_file_var_infos_attributes(
            array, var_name, var_dict, var_infos, attributes
    ):
        var_infos[var_name] = {}
        if array.rio.crs:
            var_infos[var_name]["crs_wkt"] = array.rio.crs.wkt
        band_dim = -1
        divisor = 1
        if "band" in array.dims:
            band_dim = list(array.dims).index("band")
            divisor = array["band"].size
        dims = [d for d in array.dims if d != "band"]
        var_infos[var_name]["dimensions"] = dims
        var_infos[var_name]["file_dimensions"] = dims
        var_infos[var_name]["size"] = array.size / divisor
        var_infos[var_name]["shape"] = \
            [s for i, s in enumerate(array.shape) if i != band_dim]
        var_infos[var_name]["data_type"] = array.dtype.name
        if "band_index" in var_dict:
            var_infos[var_name]["band_index"] = var_dict["band_index"]
        if "description" in var_dict:
            var_infos[var_name]["description"] = var_dict["description"]
        if "min_value" in var_dict:
            var_infos[var_name]["min_value"] = var_dict["min_value"]
        if "max_value" in var_dict:
            var_infos[var_name]["max_value"] = var_dict["max_value"]
        if "fill_value" in var_dict:
            var_infos[var_name]["fill_value"] = var_dict["fill_value"]
        preferred_chunks = array.encoding.get("preferred_chunks", {})
        chunk_sizes = []
        for dim in dims:
            if dim in preferred_chunks:
                chunk_sizes.append(preferred_chunks.get(dim))
        if len(chunk_sizes) == len(dims):
            var_infos[var_name]["chunk_sizes"] = chunk_sizes
            var_infos[var_name]["file_chunk_sizes"] = chunk_sizes
        for dim in dims:
            if dim in var_infos:
                continue
            var_infos[dim] = {}
            var_infos[dim]["dimensions"] = dim
            var_infos[dim]["file_dimensions"] = dim
            var_infos[dim]["size"] = array[dim].size
            var_infos[dim]["shape"] = array[dim].shape
            var_infos[dim]["data_type"] = array[dim].dtype.name
            var_infos[dim]["chunk_sizes"] = array[dim].shape
            var_infos[dim]["file_chunk_sizes"] = array[dim].shape
            if dim == "x":
                diff = array.x.diff(dim="x")
                if np.allclose(diff[0], array.x.diff(dim="x"), rtol=1e-8):
                    attributes["geospatial_lon_resolution"] = \
                        float(diff[0].values)
                attributes["bbox_minx"] = \
                    float(array.x[0].values -
                          attributes["geospatial_lon_resolution"])
                attributes["bbox_maxx"] = \
                    float(array.x[-1].values +
                          attributes["geospatial_lon_resolution"])
            if dim == "y":
                diff = array.y.diff(dim="y")
                if np.allclose(diff[0], array.y.diff(dim="y"), rtol=1e-8):
                    attributes["geospatial_lat_resolution"] = \
                        abs(float(diff[0].values))
                if diff[0] > 0:
                    min = 0
                    max = -1
                else:
                    min = -1
                    max = 0
                attributes["bbox_miny"] = \
                    float(array.y[min].values -
                          attributes["geospatial_lat_resolution"])
                attributes["bbox_maxy"] = \
                    float(array.y[max].values +
                          attributes["geospatial_lat_resolution"])

    def get_opendap_dataset(self, url: str):
        return self._session_executor.run_with_session(self._get_opendap_dataset, url)

    async def _get_result_dict(self, session, url: str):
        key = url
        try:
            return await self._task_cache[key]
        except KeyError:
            pass
        task = asyncio.create_task(
            self._get_result_dict_impl(session, url)
        )
        self._task_cache[key] = task
        try:
            return await task
        finally:
            self._task_cache.pop(key, None)

    async def _get_result_dict_impl(self, session, url: str):
        if url in self._result_dicts:
            return self._result_dicts[url]
        tasks = []
        res_dict = {}
        tasks.append(self._get_content_from_opendap_url(
            url, 'dds', res_dict, session)
        )
        tasks.append(self._get_content_from_opendap_url(
            url, 'das', res_dict, session)
        )
        await asyncio.gather(*tasks)
        if 'das' in res_dict:
            res_dict['das'] = res_dict['das'].replace(
                '        Float32 valid_min -Infinity;\n', ''
            )
            res_dict['das'] = res_dict['das'].replace(
                '        Float32 valid_max Infinity;\n', ''
            )
            if 'dds' in res_dict:
                self._result_dicts[url] = res_dict
        return res_dict

    async def _get_opendap_dataset(self, session, url: str):
        res_dict = await self._get_result_dict(session, url)
        if 'dds' not in res_dict or 'das' not in res_dict:
            LOG.warning(
                'Could not open opendap url. No dds or das file provided.'
            )
            return
        if res_dict['dds'] == '':
            LOG.warning('Could not open opendap url. dds file is empty.')
            return
        dataset = dds_to_dataset(res_dict['dds'])
        add_attributes(dataset, parse_das(res_dict['das']))

        # remove any projection from the url, leaving selections
        scheme, netloc, path, query, fragment = urlsplit(url)
        projection, selection = parse_ce(query)
        url = urlunsplit((scheme, netloc, path, '&'.join(selection), fragment))

        # now add data proxies
        for var in walk(dataset, BaseType):
            var.data = BaseProxyDap2(url, var.id, var.dtype, var.shape)
        for var in walk(dataset, SequenceType):
            template = copy.copy(var)
            var.data = SequenceProxy(url, template)

        # apply projections
        for var in projection:
            target = dataset
            while var:
                token, index = var.pop(0)
                target = target[token]
                if isinstance(target, BaseType):
                    target.data.slice = fix_slice(index, target.shape)
                elif isinstance(target, GridType):
                    index = fix_slice(index, target.array.shape)
                    target.array.data.slice = index
                    for s, child in zip(index, target.maps):
                        target[child].data.slice = (s,)
                elif isinstance(target, SequenceType):
                    target.data.slice = index

        # retrieve only main variable for grid types:
        for var in walk(dataset, GridType):
            var.set_output_grid(True)

        return dataset

    async def _get_content_from_opendap_url(
            self, url: str, part: str, res_dict: dict, session
    ):
        scheme, netloc, path, query, fragment = urlsplit(url)
        url = urlunsplit((scheme, netloc, path + f'.{part}', query, fragment))
        resp_content = await self._session_executor.get_response_content_from_session(
            session, url
        )
        if resp_content:
            res_dict[part] = str(resp_content, 'utf-8')

    async def _get_data_from_opendap_dataset(
            self, dataset, session, variable_name, slices
    ):
        proxy = dataset[variable_name].data
        if type(proxy) is list:
            proxy = proxy[0]
        # build download url
        index = combine_slices(proxy.slice, fix_slice(slices, proxy.shape))
        scheme, netloc, path, query, fragment = urlsplit(proxy.baseurl)
        url = urlunsplit((
            scheme, netloc, path + '.dods',
            quote(proxy.id) + hyperslab(index) + '&' + query,
            fragment)).rstrip('&')
        # download and unpack data
        resp_content = await self._session_executor.get_response_content_from_session(
            session, url
        )
        if not resp_content:
            LOG.warning(f'Could not read response from "{url}"')
            return None
        dds, data = resp_content.split(b'\nData:\n', 1)
        dds = str(dds, 'utf-8')
        # Parse received dataset:
        dataset = dds_to_dataset(dds)
        try:
            dataset.data = unpack_dap2_data(BytesReader(data), dataset)
        except ValueError:
            LOG.warning(f'Could not read data from "{url}"')
            return None
        return dataset[proxy.id].data

    def __getstate__(self):
        # overwrite to allow for serialisation
        state = self.__dict__.copy()
        state["_task_cache"] = None
        return state


    def __setstate__(self, state):
        # overwrite to set clean state after de-serialisation
        self.__dict__.update(state)
        self._task_cache = {}


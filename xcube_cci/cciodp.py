# The MIT License (MIT)
# Copyright (c) 2019 by the xcube development team and contributors
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

import aiohttp
import asyncio
import bisect
import copy
import json
import logging
import lxml.etree as etree
import numpy as np
import os
import random
import re
import pandas as pd
import time
import urllib.parse
import warnings
from datetime import datetime
from dateutil.relativedelta import relativedelta
from typing import List, Any, Dict, Tuple, Optional, Union, Sequence
from urllib.parse import quote
from xcube_cci.constants import CCI_ODD_URL
from xcube_cci.constants import DEFAULT_NUM_RETRIES
from xcube_cci.constants import DEFAULT_RETRY_BACKOFF_MAX
from xcube_cci.constants import DEFAULT_RETRY_BACKOFF_BASE
from xcube_cci.constants import OPENSEARCH_CEDA_URL
from xcube_cci.timeutil import get_timestrings_from_string

from pydap.client import Functions
from pydap.handlers.dap import BaseProxy
from pydap.handlers.dap import SequenceProxy
from pydap.handlers.dap import unpack_data
from pydap.lib import BytesReader
from pydap.lib import combine_slices
from pydap.lib import fix_slice
from pydap.lib import hyperslab
from pydap.lib import walk
from pydap.model import BaseType, SequenceType, GridType
from pydap.parsers import parse_ce
from pydap.parsers.dds import build_dataset
from pydap.parsers.das import parse_das, add_attributes
from six.moves.urllib.parse import urlsplit, urlunsplit

_LOG = logging.getLogger('xcube')
ODD_NS = {'os': 'http://a9.com/-/spec/opensearch/1.1/',
          'param': 'http://a9.com/-/spec/opensearch/extensions/parameters/1.0/'}
DESC_NS = {'gmd': 'http://www.isotc211.org/2005/gmd',
           'gml': 'http://www.opengis.net/gml/3.2',
           'gco': 'http://www.isotc211.org/2005/gco',
           'gmx': 'http://www.isotc211.org/2005/gmx',
           'xlink': 'http://www.w3.org/1999/xlink'
           }

_TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S"

_RE_TO_DATETIME_FORMATS = patterns = [(re.compile(14 * '\\d'), '%Y%m%d%H%M%S', relativedelta()),
                                      (re.compile(12 * '\\d'), '%Y%m%d%H%M', relativedelta(minutes=1, seconds=-1)),
                                      (re.compile(8 * '\\d'), '%Y%m%d', relativedelta(days=1, seconds=-1)),
                                      (re.compile(4 * '\\d' + '-' + 2 * '\\d' + '-' + 2 * '\\d'), '%Y-%m-%d',
                                       relativedelta(days=1, seconds=-1)),
                                      (re.compile(6 * '\\d'), '%Y%m', relativedelta(months=1, seconds=-1)),
                                      (re.compile(4 * '\\d'), '%Y', relativedelta(years=1, seconds=-1))]


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


def _run_with_session(function, *params):
    # See https://github.com/aio-libs/aiohttp/blob/master/docs/client_advanced.rst#graceful-shutdown
    loop = asyncio.get_event_loop()
    result = loop.run_until_complete(_run_with_session_executor(function, *params))
    # Zero-sleep to allow underlying connections to close
    loop.run_until_complete(asyncio.sleep(0))
    return result


async def _run_with_session_executor(function, *params):
    async with aiohttp.ClientSession() as session:
        return await function(session, *params)


def _get_feature_dict_from_feature(feature: dict) -> Optional[dict]:
    fc_props = feature.get("properties", {})
    feature_dict = {}
    feature_dict['uuid'] = feature.get("id", "").split("=")[-1]
    feature_dict['title'] = fc_props.get("title", "")
    variables = _get_variables_from_feature(feature)
    feature_dict['variables'] = variables
    fc_props_links = fc_props.get("links", None)
    if fc_props_links:
        search = fc_props_links.get("search", None)
        if search:
            odd_url = search[0].get('href', None)
            if odd_url:
                feature_dict['odd_url'] = odd_url
        described_by = fc_props_links.get("describedby", None)
        if described_by:
            metadata_url = described_by[0].get("href", None)
            if metadata_url:
                feature_dict['metadata_url'] = metadata_url
    return feature_dict


def _get_variables_from_feature(feature: dict) -> List:
    feature_props = feature.get("properties", {})
    variables = feature_props.get("variables", [])
    variable_dicts = []
    for variable in variables:
        variable_dict = {
            'name': variable.get("var_id", None),
            'units': variable.get("units", ""),
            'long_name': variable.get("long_name", None)}
        variable_dicts.append(variable_dict)
    return variable_dicts


def _harmonize_info_field_names(catalogue: dict, single_field_name: str, multiple_fields_name: str,
                                multiple_items_name: Optional[str] = None):
    if single_field_name in catalogue and multiple_fields_name in catalogue:
        if len(multiple_fields_name) == 0:
            catalogue.pop(multiple_fields_name)
        elif len(catalogue[multiple_fields_name]) == 1:
            if catalogue[multiple_fields_name][0] is catalogue[single_field_name]:
                catalogue.pop(multiple_fields_name)
            else:
                catalogue[multiple_fields_name].append(catalogue[single_field_name])
                catalogue.pop(single_field_name)
        else:
            if catalogue[single_field_name] not in catalogue[multiple_fields_name] \
                    and (multiple_items_name is None or catalogue[single_field_name] != multiple_items_name):
                catalogue[multiple_fields_name].append(catalogue[single_field_name])
            catalogue.pop(single_field_name)


def _extract_metadata_from_descxml(descxml: etree.XML) -> dict:
    metadata = {}
    metadata_elems = {
        'abstract': 'gmd:identificationInfo/gmd:MD_DataIdentification/gmd:abstract/gco:CharacterString',
        'title': 'gmd:identificationInfo/gmd:MD_DataIdentification/gmd:citation/gmd:CI_Citation/gmd:title/'
                 'gco:CharacterString',
        'licences': 'gmd:identificationInfo/gmd:MD_DataIdentification/gmd:resourceConstraints/gmd:MD_Constraints/'
                    'gmd:useLimitation/gco:CharacterString',
        'bbox_minx': 'gmd:identificationInfo/gmd:MD_DataIdentification/gmd:extent/gmd:EX_Extent/'
                     'gmd:geographicElement/gmd:EX_GeographicBoundingBox/gmd:westBoundLongitude/gco:Decimal',
        'bbox_miny': 'gmd:identificationInfo/gmd:MD_DataIdentification/gmd:extent/gmd:EX_Extent/'
                     'gmd:geographicElement/gmd:EX_GeographicBoundingBox/gmd:southBoundLatitude/gco:Decimal',
        'bbox_maxx': 'gmd:identificationInfo/gmd:MD_DataIdentification/gmd:extent/gmd:EX_Extent/'
                     'gmd:geographicElement/gmd:EX_GeographicBoundingBox/gmd:eastBoundLongitude/gco:Decimal',
        'bbox_maxy': 'gmd:identificationInfo/gmd:MD_DataIdentification/gmd:extent/gmd:EX_Extent/'
                     'gmd:geographicElement/gmd:EX_GeographicBoundingBox/gmd:northBoundLatitude/gco:Decimal',
        'temporal_coverage_start': 'gmd:identificationInfo/gmd:MD_DataIdentification/gmd:extent/gmd:EX_Extent/'
                                   'gmd:temporalElement/gmd:EX_TemporalExtent/gmd:extent/gml:TimePeriod/'
                                   'gml:beginPosition',
        'temporal_coverage_end': 'gmd:identificationInfo/gmd:MD_DataIdentification/gmd:extent/gmd:EX_Extent/'
                                 'gmd:temporalElement/gmd:EX_TemporalExtent/gmd:extent/gml:TimePeriod/gml:endPosition'
    }
    for identifier in metadata_elems:
        content = _get_element_content(descxml, metadata_elems[identifier])
        if content:
            metadata[identifier] = content
    metadata_elems_with_replacement = {'file_formats': [
        'gmd:identificationInfo/gmd:MD_DataIdentification/gmd:resourceFormat/gmd:MD_Format/gmd:name/'
        'gco:CharacterString', 'Data are in NetCDF format', '.nc']
    }
    for metadata_elem in metadata_elems_with_replacement:
        content = _get_replaced_content_from_descxml_elem(descxml, metadata_elems_with_replacement[metadata_elem])
        if content:
            metadata[metadata_elem] = content
    metadata_linked_elems = {
        'publication_date': ['gmd:identificationInfo/gmd:MD_DataIdentification/gmd:citation/'
                             'gmd:CI_Citation/gmd:date/gmd:CI_Date/gmd:dateType/gmd:CI_DateTypeCode', 'publication',
                             '../../gmd:date/gco:DateTime'],
        'creation_date': ['gmd:identificationInfo/gmd:MD_DataIdentification/gmd:citation/'
                          'gmd:CI_Citation/gmd:date/gmd:CI_Date/gmd:dateType/gmd:CI_DateTypeCode', 'creation',
                          '../../gmd:date/gco:DateTime']
    }
    for identifier in metadata_linked_elems:
        content = _get_linked_content_from_descxml_elem(descxml, metadata_linked_elems[identifier])
        if content:
            metadata[identifier] = content
    return metadata


def _get_element_content(descxml: etree.XML, path: str) -> Optional[Union[str, List[str]]]:
    elems = descxml.findall(path, namespaces=DESC_NS)
    if not elems:
        return None
    if len(elems) == 1:
        return elems[0].text
    return [elem.text for elem in elems]


def _get_replaced_content_from_descxml_elem(descxml: etree.XML, paths: List[str]) -> Optional[str]:
    descxml_elem = descxml.find(paths[0], namespaces=DESC_NS)
    if descxml_elem is None:
        return None
    if descxml_elem.text == paths[1]:
        return paths[2]


def _get_linked_content_from_descxml_elem(descxml: etree.XML, paths: List[str]) -> Optional[str]:
    descxml_elems = descxml.findall(paths[0], namespaces=DESC_NS)
    if descxml is None:
        return None
    for descxml_elem in descxml_elems:
        if descxml_elem.text == paths[1]:
            return _get_element_content(descxml_elem, paths[2])


def find_datetime_format(filename: str) -> Tuple[Optional[str], int, int, relativedelta]:
    for regex, time_format, timedelta in _RE_TO_DATETIME_FORMATS:
        searcher = regex.search(filename)
        if searcher:
            p1, p2 = searcher.span()
            return time_format, p1, p2, timedelta
    return None, -1, -1, relativedelta()


def _extract_metadata_from_odd(odd_xml: etree.XML) -> dict:
    metadata = {}
    metadata_names = {'ecv': ['ecv', 'ecvs'], 'frequency': ['time_frequency', 'time_frequencies'],
                      'institute': ['institute', 'institutes'],
                      'processingLevel': ['processing_level', 'processing_levels'],
                      'productString': ['product_string', 'product_strings'],
                      'productVersion': ['product_version', 'product_versions'],
                      'dataType': ['data_type', 'data_types'], 'sensor': ['sensor_id', 'sensor_ids'],
                      'platform': ['platform_id', 'platform_ids'], 'fileFormat': ['file_format', 'file_formats'],
                      'drsId': ['drs_id', 'drs_ids']}
    for param_elem in odd_xml.findall('os:Url/param:Parameter', namespaces=ODD_NS):
        if param_elem.attrib['name'] in metadata_names:
            param_content = _get_from_param_elem(param_elem)
            if param_content:
                if type(param_content) == str:
                    metadata[metadata_names[param_elem.attrib['name']][0]] = param_content
                else:
                    metadata[metadata_names[param_elem.attrib['name']][1]] = param_content
    return metadata


def _get_from_param_elem(param_elem: etree.Element) -> Optional[Union[str, List[str]]]:
    options = param_elem.findall('param:Option', namespaces=ODD_NS)
    if not options:
        return None
    if len(options) == 1:
        return options[0].get('value')
    return [option.get('value') for option in options]


def _extract_feature_info(feature: dict) -> List:
    feature_props = feature.get("properties", {})
    filename = feature_props.get("title", "")
    date = feature_props.get("date", None)
    start_time = ""
    end_time = ""
    if date and "/" in date:
        start_time, end_time = date.split("/")
    elif filename:
        time_format, p1, p2, timedelta = find_datetime_format(filename)
        if time_format:
            start_time = datetime.strptime(filename[p1:p2], time_format)
            end_time = start_time + timedelta
            # Convert back to text, so we can JSON-encode it
            start_time = datetime.strftime(start_time, _TIMESTAMP_FORMAT)
            end_time = datetime.strftime(end_time, _TIMESTAMP_FORMAT)
    file_size = feature_props.get("filesize", 0)
    related_links = feature_props.get("links", {}).get("related", [])
    urls = {}
    for related_link in related_links:
        urls[related_link.get("title")] = related_link.get("href")
    return [filename, start_time, end_time, file_size, urls]


class CciOdp:
    """
    Represents the ESA CCI Open Data Portal

    :param opensearch_url: The base URL to the opensearch service
    :param opensearch_description_url: The URL to a document describing the capabilities of the opensearch service
    """

    def __init__(self,
                 opensearch_url: str = OPENSEARCH_CEDA_URL,
                 opensearch_description_url: str = CCI_ODD_URL,
                 enable_warnings: bool = False,
                 num_retries: int = DEFAULT_NUM_RETRIES,
                 retry_backoff_max: int = DEFAULT_RETRY_BACKOFF_MAX,
                 retry_backoff_base: float = DEFAULT_RETRY_BACKOFF_BASE,
                 only_consider_cube_ready = False
                 ):
        self._opensearch_url = opensearch_url
        self._opensearch_description_url = opensearch_description_url
        self._enable_warnings = enable_warnings
        self._num_retries = num_retries
        self._retry_backoff_max = retry_backoff_max
        self._retry_backoff_base = retry_backoff_base
        self._drs_ids = None
        self._data_sources = {}
        self._features = {}
        eds_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data/excluded_data_sources')
        with open(eds_file, 'r') as eds:
            self._excluded_data_sources = eds.read().split('\n')
        if only_consider_cube_ready:
            ncds_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data/non_cube_data_sources')
            with open(ncds_file, 'r') as ncds:
                non_cube_ready_data_sources = ncds.read().split('\n')
                self._excluded_data_sources += non_cube_ready_data_sources

    def close(self):
        pass

    @property
    def token_info(self) -> Dict[str, Any]:
        return {}

    @property
    def dataset_names(self) -> List[str]:
        return _run_with_session(self._fetch_dataset_names)

    @property
    def description(self) -> dict:
        _run_with_session(self._ensure_all_info_in_data_sources, self.dataset_names)
        datasets = []
        for data_source in self._data_sources:
            metadata = self._data_sources[data_source]
            var_names = self._get_data_var_names(metadata.get('variable_infos', {}))
            variables = []
            for variable in var_names:
                variable_dict = {}
                var_metadata_list = [dict for dict in metadata['variables'] if dict['name'] == variable]
                var_metadata = var_metadata_list[0]
                variable_dict['id'] = var_metadata.get('name', '')
                variable_dict['name'] = var_metadata.get('name', '')
                variable_dict['units'] = var_metadata.get('units', '')
                variable_dict['description'] = var_metadata.get('description', '')
                variable_dict['dtype'] = metadata.get('variable_infos', {}).get(variable, {}).get('data_type', '')
                variable_dict['spatialRes'] = metadata.get('attributes', {}).get('NC_GLOBAL', {}). \
                    get('geospatial_lat_resolution', '')
                variable_dict['spatialLatRes'] = metadata.get('attributes', {}).get('NC_GLOBAL', {}). \
                    get('geospatial_lat_resolution', '')
                variable_dict['spatialLonRes'] = metadata.get('attributes', {}).get('NC_GLOBAL', {}). \
                    get('geospatial_lon_resolution', '')
                variable_dict['temporal_coverage_start'] = metadata.get('temporal_coverage_start', '')
                variable_dict['temporal_coverage_end'] = metadata.get('temporal_coverage_end', '')
                variable_dict['temporalRes'] = metadata.get('attributes', {}).get('NC_GLOBAL', {}). \
                    get('time_coverage_resolution', '')
                variables.append(variable_dict)
            dataset_dict = dict(id=data_source,
                                name=self._shorten_dataset_name(metadata['title']),
                                variables=variables
                                )
            datasets.append(dataset_dict)
        description = dict(id='cciodp',
                           name='ESA CCI Open Data Portal',
                           datasets=datasets)
        return description

    def _shorten_dataset_name(self, dataset_name: str) -> str:
        if re.match('.*[(].*[)].*[:].*', dataset_name) is None:
            return dataset_name
        split_name = dataset_name.split(':')
        cci_name = split_name[0][split_name[0].index('(') + 1:split_name[0].index(')')]
        set_name = split_name[1].replace('Level ', 'L').replace(', version ', ' v').replace(', Version ', ' v')
        return f'{cci_name}:{set_name}'

    def get_dataset_info(self, dataset_id: str, dataset_metadata: dict = None) -> dict:
        data_info = {}
        if not dataset_metadata:
            dataset_metadata = self.get_dataset_metadata(dataset_id)
        nc_attrs = dataset_metadata.get('attributes', {}).get('NC_GLOBAL', {})
        data_info['lat_res'] = self._get_res(nc_attrs, 'lat')
        data_info['lon_res'] = self._get_res(nc_attrs, 'lon')
        data_info['bbox'] = (float(dataset_metadata['bbox_minx']), float(dataset_metadata['bbox_miny']),
                             float(dataset_metadata['bbox_maxx']), float(dataset_metadata['bbox_maxy']))
        data_info['temporal_coverage_start'] = dataset_metadata.get('temporal_coverage_start', '')
        data_info['temporal_coverage_end'] = dataset_metadata.get('temporal_coverage_end', '')
        data_info['var_names'] = self.var_names(dataset_id)
        return data_info

    def _get_res(self, nc_attrs: dict, dim: str) -> float:
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
                    if type(res_attr) == float:
                        return res_attr
                    elif type(res_attr) == int:
                        return float(res_attr)
                    return float(res_attr.split('(')[0].split('x')[index].split('deg')[0].split('degree')[0].
                                 split('km')[0].split('m')[0])
                except ValueError:
                    continue
        return -1.0

    def get_dataset_metadata(self, dataset_id: str) -> dict:
        return self.get_datasets_metadata([dataset_id])[0]

    def get_datasets_metadata(self, dataset_ids: List[str]) -> List[dict]:
        assert isinstance(dataset_ids, list)
        _run_with_session(self._ensure_all_info_in_data_sources, dataset_ids)
        metadata = []
        for dataset_id in dataset_ids:
            metadata.append(self._data_sources[dataset_id])
        return metadata

    async def _fetch_dataset_names(self, session):
        if self._drs_ids:
            return self._drs_ids
        meta_info_dict = await self._extract_metadata_from_odd_url(session, self._opensearch_description_url)
        if 'drs_ids' in meta_info_dict:
            self._drs_ids = meta_info_dict['drs_ids']
            for excluded_data_source in self._excluded_data_sources:
                if excluded_data_source in self._drs_ids:
                    self._drs_ids.remove(excluded_data_source)
            return self._drs_ids
        if not self._data_sources:
            self._data_sources = {}
            catalogue = await self._fetch_data_source_list_json(session, self._opensearch_url, dict(parentIdentifier='cci'))
            if catalogue:
                tasks = []
                for catalogue_item in catalogue:
                    tasks.append(self._create_data_source(session, catalogue[catalogue_item], catalogue_item))
                await asyncio.gather(*tasks)
        return list(self._data_sources.keys())

    async def _create_data_source(self, session, json_dict: dict, datasource_id: str):
        meta_info = await self._fetch_meta_info(session,
                                                json_dict.get('odd_url', None),
                                                json_dict.get('metadata_url', None))
        drs_ids = self._get_as_list(meta_info, 'drs_id', 'drs_ids')
        for excluded_data_source in self._excluded_data_sources:
            if excluded_data_source in drs_ids:
                    drs_ids.remove(excluded_data_source)
        for drs_id in drs_ids:
            meta_info = meta_info.copy()
            meta_info.update(json_dict)
            self._adjust_json_dict(meta_info, drs_id)
            for variable in meta_info.get('variables', []):
                variable['name'] = variable['name'].replace('.', '_')
            meta_info['cci_project'] = meta_info['ecv']
            meta_info['fid'] = datasource_id
            self._data_sources[drs_id] = meta_info

    def _adjust_json_dict(self, json_dict: dict, drs_id: str):
        values = drs_id.split('.')
        self._adjust_json_dict_for_param(json_dict, 'time_frequency', 'time_frequencies',
                                         _convert_time_from_drs_id(values[2]))
        self._adjust_json_dict_for_param(json_dict, 'processing_level', 'processing_levels', values[3])
        self._adjust_json_dict_for_param(json_dict, 'data_type', 'data_types', values[4])
        self._adjust_json_dict_for_param(json_dict, 'sensor_id', 'sensor_ids', values[5])
        self._adjust_json_dict_for_param(json_dict, 'platform_id', 'platform_ids', values[6])
        self._adjust_json_dict_for_param(json_dict, 'product_string', 'product_strings', values[7])
        self._adjust_json_dict_for_param(json_dict, 'product_version', 'product_versions', values[8])

    def _adjust_json_dict_for_param(self, json_dict: dict, single_name: str, list_name: str, param_value: str):
        json_dict[single_name] = param_value
        if list_name in json_dict:
            json_dict.pop(list_name)

    def _get_pretty_id(self, json_dict: dict, value_tuple: Tuple, drs_id: str) -> str:
        pretty_values = []
        for value in value_tuple:
            pretty_values.append(self._make_string_pretty(value))
        return f'esacci.{json_dict["ecv"]}.{".".join(pretty_values)}.{drs_id}'

    def _make_string_pretty(self, string: str):
        string = string.replace(" ", "-")
        if string.startswith("."):
            string = string[1:]
        if string.endswith("."):
            string = string[:-1]
        if "." in string:
            string = string.replace(".", "-")
        return string

    def _get_as_list(self, meta_info: dict, single_name: str, list_name: str) -> List:
        if single_name in meta_info:
            return [meta_info[single_name]]
        if list_name in meta_info:
            return meta_info[list_name]
        return []

    def var_names(self, dataset_name: str) -> List:
        _run_with_session(self._ensure_all_info_in_data_sources, [dataset_name])
        return self._get_data_var_names(self._data_sources[dataset_name]['variable_infos'])

    async def _ensure_all_info_in_data_sources(self, session, dataset_names: List[str]):
        await self._ensure_in_data_sources(session, dataset_names)
        all_info_tasks = []
        for dataset_name in dataset_names:
            all_info_tasks.append(self._ensure_all_info_in_data_source(session, dataset_name))
        await asyncio.gather(*all_info_tasks)

    async def _ensure_all_info_in_data_source(self, session, dataset_name: str):
        data_source = self._data_sources[dataset_name]
        if 'dimensions' in data_source and 'variable_infos' in data_source and 'attributes' in data_source:
            return
        data_fid = await self._get_fid_for_dataset(session, dataset_name)
        data_source['dimensions'], data_source['variable_infos'], data_source['attributes'],\
            data_source['time_dimension_size'] = \
            await self._fetch_variable_infos(self._opensearch_url, data_fid, session)

    def _get_data_var_names(self, variable_infos) -> List:
        variables = []
        names_of_dims = ['period', 'hist1d_cla_vis006_bin_centre', 'lon_bnds', 'air_pressure',
                         'field_name_length', 'lon', 'view', 'hist2d_cot_bin_centre',
                         'hist1d_cer_bin_border', 'altitude', 'vegetation_class',
                         'hist1d_cla_vis006_bin_border', 'time_bnds', 'hist1d_ctp_bin_border',
                         'hist1d_cot_bin_centre', 'hist1d_cot_bin_border',
                         'hist1d_cla_vis008_bin_centre', 'lat_bnds', 'hist1d_cwp_bin_border',
                         'layers', 'hist1d_cer_bin_centre', 'aerosol_type',
                         'hist1d_ctt_bin_border', 'hist1d_ctp_bin_centre', 'fieldsp1', 'time',
                         'hist_phase', 'hist1d_cwp_bin_centre', 'hist2d_ctp_bin_border', 'lat',
                         'fields', 'hist2d_cot_bin_border', 'hist2d_ctp_bin_centre',
                         'hist1d_ctt_bin_centre', 'hist1d_cla_vis008_bin_border', 'crs']
        for variable in variable_infos:
            if variable in names_of_dims:
                continue
            if len(variable_infos[variable]['dimensions']) == 0:
                continue
            if variable_infos[variable].get('data_type', '') not in \
                    ['uint8', 'uint16', 'uint32', 'int8', 'int16', 'int32', 'float32', 'float64']:
                continue
            variables.append(variable)
        return variables

    def search(self,
               start_date: Optional[str] = None,
               end_date: Optional[str] = None,
               bbox: Optional[Tuple[float, float, float, float]] = None,
               ecv: Optional[str] = None,
               frequency: Optional[str] = None,
               institute: Optional[str] = None,
               processing_level: Optional[str] = None,
               product_string: Optional[str] = None,
               product_version: Optional[str] = None,
               data_type: Optional[str] = None,
               sensor: Optional[str] = None,
               platform: Optional[str] = None) -> List[str]:
        candidate_names = []
        if not self._data_sources and not ecv and not frequency and not processing_level and not data_type and \
                not product_string and not product_version:
            _run_with_session(self._read_all_data_sources)
            candidate_names = self.dataset_names
        else:
            for dataset_name in self.dataset_names:
                split_dataset_name = dataset_name.split('.')
                if ecv is not None and ecv != split_dataset_name[1]:
                    continue
                if frequency is not None and frequency != _convert_time_from_drs_id(split_dataset_name[2]):
                    continue
                if processing_level is not None and processing_level != split_dataset_name[3]:
                    continue
                if data_type is not None and data_type != split_dataset_name[4]:
                    continue
                if product_string is not None and product_string != split_dataset_name[7]:
                    continue
                if product_version is not None and product_version.replace('.', '-') != split_dataset_name[8]:
                    continue
                candidate_names.append(dataset_name)
            if len(candidate_names) == 0:
                return []
        if not start_date and not end_date and not institute and not sensor and not platform and not bbox:
            return candidate_names
        results = []
        if start_date:
            converted_start_date = self._get_datetime_from_string(start_date)
        if end_date:
            converted_end_date = self._get_datetime_from_string(end_date)
        _run_with_session(self._ensure_in_data_sources, candidate_names)
        for candidate_name in candidate_names:
            data_source_info = self._data_sources.get(candidate_name, None)
            if not data_source_info:
                continue
            if institute is not None and ('institute' not in data_source_info or
                                          institute != data_source_info['institute']):
                continue
            if sensor is not None and sensor != data_source_info['sensor_id']:
                continue
            if platform is not None and platform != data_source_info['platform_id']:
                continue
            if bbox:
                if float(data_source_info['bbox_minx']) > bbox[2]:
                    continue
                if float(data_source_info['bbox_maxx']) < bbox[0]:
                    continue
                if float(data_source_info['bbox_miny']) > bbox[3]:
                    continue
                if float(data_source_info['bbox_maxy']) < bbox[1]:
                    continue
            if start_date:
                data_source_end = datetime.strptime(data_source_info['temporal_coverage_end'], _TIMESTAMP_FORMAT)
                if converted_start_date > data_source_end:
                    continue
            if end_date:
                data_source_start = datetime.strptime(data_source_info['temporal_coverage_start'], _TIMESTAMP_FORMAT)
                if converted_end_date < data_source_start:
                    continue
            results.append(candidate_name)
        return results

    async def _read_all_data_sources(self, session):
        catalogue = await self._fetch_data_source_list_json(session,
                                                       self._opensearch_url,
                                                       dict(parentIdentifier='cci'))
        if catalogue:
            tasks = []
            for catalogue_item in catalogue:
                tasks.append(self._create_data_source(session, catalogue[catalogue_item], catalogue_item))
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
                self._fetch_data_source_list_json_and_add_to_catalogue(session, catalogue, dataset_name)
            )
        await asyncio.gather(*fetch_fid_tasks)
        create_source_tasks = []
        for catalogue_item in catalogue:
            create_source_tasks.append(self._create_data_source(session, catalogue[catalogue_item], catalogue_item))
        await asyncio.gather(*create_source_tasks)

    async def _fetch_data_source_list_json_and_add_to_catalogue(self, session, catalogue: dict, dataset_name: str):
        dataset_catalogue = await self._fetch_data_source_list_json(session,
                                                                    self._opensearch_url,
                                                                    dict(parentIdentifier='cci',
                                                                    drsId=dataset_name))
        catalogue.update(dataset_catalogue)

    def _get_datetime_from_string(self, time_as_string: str) -> datetime:
        time_format, start, end, timedelta = find_datetime_format(time_as_string)
        return datetime.strptime(time_as_string[start:end], time_format)

    def get_variable_data(self, dataset_name: str,
                          dimension_names: Dict[str, int],
                          start_time: str = '1900-01-01T00:00:00',
                          end_time: str = '3001-12-31T00:00:00'):
        dimension_data = _run_with_session(self._get_var_data, dataset_name, dimension_names, start_time, end_time)
        return dimension_data

    async def _get_var_data(self, session, dataset_name: str, variable_names: Dict[str, int],
                            start_time: str, end_time: str):
        fid = await self._get_fid_for_dataset(session, dataset_name)
        request = dict(parentIdentifier=fid,
                       startDate=start_time,
                       endDate=end_time,
                       drsId=dataset_name
                       )
        opendap_url = await self._get_opendap_url(session, request)
        var_data = {}
        if not opendap_url:
            return var_data
        dataset = await self._get_opendap_dataset(session, opendap_url)
        if not dataset:
            return var_data
        for var_name in variable_names:
            if var_name in dataset:
                var_data[var_name] = dict(size=dataset[var_name].size,
                                          chunkSize=dataset[var_name].attributes.get('_ChunkSizes'))
                if dataset[var_name].size < 512 * 512:
                    data = await self._get_data_from_opendap_dataset(dataset, session, var_name,
                                                                     (slice(None, None, None),))
                    if data is None:
                        var_data[var_name]['data'] = []
                    else:
                        var_data[var_name]['data'] = data
                else:
                    var_data[var_name]['data'] = []
            else:
                var_data[var_name] = dict(size=variable_names[var_name],
                                          chunkSize=variable_names[var_name],
                                          data=list(range(variable_names[var_name])))
        return var_data

    def get_earliest_start_date(self, dataset_name: str, start_time: str, end_time: str, frequency: str) -> \
            Optional[datetime]:
        return _run_with_session(self._get_earliest_start_date, dataset_name, start_time, end_time, frequency)

    async def _get_earliest_start_date(self, session, dataset_name: str, start_time: str, end_time: str,
                                       frequency: str) -> Optional[datetime]:
        fid = await self._get_fid_for_dataset(session, dataset_name)
        query_args = dict(parentIdentifier=fid,
                          startDate=start_time,
                          endDate=end_time,
                          frequency=frequency,
                          drsId=dataset_name,
                          fileFormat='.nc')
        opendap_url = await self._get_opendap_url(session, query_args, get_earliest=True)
        if opendap_url:
            dataset = await self._get_opendap_dataset(session, opendap_url)
            start_time_attributes = ['time_coverage_start', 'start_date']
            attributes = dataset.attributes.get('NC_GLOBAL', {})
            for start_time_attribute in start_time_attributes:
                start_time_string = attributes.get(start_time_attribute, '')
                time_format, start, end, timedelta = find_datetime_format(start_time_string)
                if time_format:
                    start_time = datetime.strptime(start_time_string[start:end], time_format)
                    return start_time
        return None

    async def _get_feature_list(self, session, request):
        ds_id = request['drsId']
        start_date_str = request['startDate']
        start_date = datetime.strptime(start_date_str, _TIMESTAMP_FORMAT)
        end_date_str = request['endDate']
        end_date = datetime.strptime(end_date_str, _TIMESTAMP_FORMAT)
        feature_list = []
        if ds_id not in self._features or len(self._features[ds_id]) == 0:
            self._features[ds_id] = []
            await self._fetch_opensearch_feature_list(session, self._opensearch_url, feature_list,
                                                      self._extract_times_and_opendap_url, request)
            if len(feature_list) == 0:
                # try without dates. For some data sets, this works better
                request.pop('startDate')
                request.pop('endDate')
                await self._fetch_opensearch_feature_list(session, self._opensearch_url, feature_list,
                                                          self._extract_times_and_opendap_url, request)
                feature_list.sort(key=lambda x: x[0])
            self._features[ds_id] = feature_list
            return self._features[ds_id]
        else:
            if start_date < self._features[ds_id][0][0]:
                request['endDate'] = datetime.strftime(self._features[ds_id][0][0], _TIMESTAMP_FORMAT)
                await self._fetch_opensearch_feature_list(session, self._opensearch_url, feature_list,
                                                          self._extract_times_and_opendap_url, request)
                feature_list.sort(key=lambda x: x[0])
                self._features[ds_id] = feature_list + self._features[ds_id]
            if end_date > self._features[ds_id][-1][1]:
                request['startDate'] = datetime.strftime(self._features[ds_id][-1][1], _TIMESTAMP_FORMAT)
                request['endDate'] = end_date_str
                await self._fetch_opensearch_feature_list(session, self._opensearch_url, feature_list,
                                                          self._extract_times_and_opendap_url, request)
                feature_list.sort(key=lambda x: x[0])
                self._features[ds_id] = self._features[ds_id] + feature_list
        start = bisect.bisect_left([feature[0] for feature in self._features[ds_id]], start_date)
        end = bisect.bisect_right([feature[1] for feature in self._features[ds_id]], end_date)
        return self._features[ds_id][start:end]

    def _extract_times_and_opendap_url(self, features: List[Tuple], feature_list: List[Dict]):
        for feature in feature_list:
            start_time = None
            end_time = None
            properties = feature.get('properties', {})
            opendap_url = None
            links = properties.get('links', {}).get('related', {})
            for link in links:
                if link.get('title', '') == 'Opendap':
                    opendap_url = link.get('href', None)
            if not opendap_url:
                continue
            date_property = properties.get('date', None)
            if date_property:
                start_time = datetime.strptime(date_property.split('/')[0].split('.')[0].split('+')[0], _TIMESTAMP_FORMAT)
                end_time = datetime.strptime(date_property.split('/')[1].split('.')[0].split('+')[0], _TIMESTAMP_FORMAT)
            else:
                title = properties.get('title', None)
                if title:
                    start_time, end_time = get_timestrings_from_string(title)
                    if start_time:
                        start_time = datetime.strptime(start_time, _TIMESTAMP_FORMAT)
                    if end_time:
                        end_time = datetime.strptime(end_time, _TIMESTAMP_FORMAT)
                    else:
                        end_time = start_time
            if start_time:
                start_time = pd.Timestamp(datetime.strftime(start_time, _TIMESTAMP_FORMAT))
                end_time = pd.Timestamp(datetime.strftime(end_time, _TIMESTAMP_FORMAT))
                features.append((start_time, end_time, opendap_url))

    def get_time_ranges_from_data(self, dataset_name: str, start_time: str, end_time: str) -> \
            List[Tuple[datetime, datetime]]:
        return _run_with_session(self._get_time_ranges_from_data, dataset_name, start_time, end_time)

    async def _get_time_ranges_from_data(self, session,
                                                         dataset_name: str,
                                                         start_time: str,
                                                         end_time: str) -> List[Tuple[datetime, datetime]]:
        fid = await self._get_fid_for_dataset(session, dataset_name)
        request = dict(parentIdentifier=fid,
                       startDate=start_time,
                       endDate=end_time,
                       drsId=dataset_name,
                       fileFormat='.nc')

        feature_list = await self._get_feature_list(session, request)
        request_time_ranges = [feature[0:2] for feature in feature_list]
        return request_time_ranges

    def get_fid_for_dataset(self, dataset_name: str) -> str:
        return _run_with_session(self._get_fid_for_dataset, dataset_name)

    async def _get_fid_for_dataset(self, session, dataset_name: str) -> str:
        await self._ensure_in_data_sources(session, [dataset_name])
        return self._data_sources[dataset_name]['fid']

    async def _get_opendap_url(self, session, request: Dict, get_earliest: bool = False, get_latest: bool = False):
        if get_earliest and get_latest:
            raise ValueError('Not both get_earliest and get_latest may be set to true')
        request['fileFormat'] = '.nc'
        feature_list = await self._get_feature_list(session, request)
        if len(feature_list) == 0:
            return
        if get_latest:
            return feature_list[-1][2]
        return feature_list[0][2]

    def get_data(self, request: Dict, bbox: Tuple[float, float, float, float], dim_indexes: dict, dim_flipped: dict) \
            -> Optional[bytes]:
        return _run_with_session(self._get_data, request, bbox, dim_indexes, dim_flipped)

    async def _get_data(self, session, request: Dict, bbox: Tuple[float, float, float, float], dim_indexes: dict,
                        dim_flipped: dict) -> Optional[bytes]:
        start_date = datetime.strptime(request['startDate'], _TIMESTAMP_FORMAT)
        end_date = datetime.strptime(request['endDate'], _TIMESTAMP_FORMAT)
        var_names = request['varNames']
        opendap_url = await self._get_opendap_url(session, request)
        if not opendap_url:
            return None
        dataset = await self._get_opendap_dataset(session, opendap_url)
        # todo support more dimensions
        supported_dimensions = ['lat', 'lon', 'time', 'latitude', 'longitude']
        result = bytearray()
        for i, var in enumerate(var_names):
            indexes = []
            for dimension in dataset[var].dimensions:
                if dimension not in dim_indexes:
                    if dimension not in supported_dimensions:
                        raise ValueError(f'Variable {var} has unsupported dimension {dimension}. '
                                         f'Cannot retrieve this variable.')
                    dim_indexes[dimension] = self._get_indexing(dataset, dimension, bbox, start_date, end_date)
                indexes.append(dim_indexes[dimension])
            variable_data = np.array(dataset[var][tuple(indexes)].data[0], dtype=dataset[var].dtype.type)
            for i, dimension in enumerate(dataset[var].dimensions):
                if dim_flipped.get(dimension, False):
                    variable_data = np.flip(variable_data, axis=i)
            result += variable_data.flatten().tobytes()
        return result

    def get_data_chunk(self, request: Dict, dim_indexes: Tuple) -> Optional[bytes]:
        data_chunk = _run_with_session(self._get_data_chunk, request, dim_indexes)
        return data_chunk

    async def _get_data_chunk(self, session, request: Dict, dim_indexes: Tuple) -> Optional[bytes]:
        var_name = request['varNames'][0]
        opendap_url = await self._get_opendap_url(session, request)
        if not opendap_url:
            return None
        dataset = await self._get_opendap_dataset(session, opendap_url)
        if not dataset:
            return None
        data = await self._get_data_from_opendap_dataset(dataset, session, var_name, dim_indexes)
        variable_data = np.array(data, dtype=dataset[var_name].dtype.type)
        result = variable_data.flatten().tobytes()
        return result

    def _get_indexing(self, dataset, dimension: str, bbox: (float, float, float, float),
                      start_date: datetime, end_date: datetime):
        if dimension == 'lat' or dimension == 'latitude':
            return self._get_dim_indexing(dataset[dimension].data[:], bbox[1], bbox[3])
        if dimension == 'lon' or dimension == 'longitude':
            return self._get_dim_indexing(dataset[dimension].data[:], bbox[0], bbox[2])
        if dimension == 'time':
            time_units = dataset.time.attributes.get('units', '')
            time_data = self._convert_time_data(dataset[dimension].data[:], time_units)
            return self._get_dim_indexing(time_data, start_date, end_date)
        else:
            return 0

    def _convert_time_data(self, time_data: np.array, units: str):
        converted_time = []
        format, start, end, timedelta = find_datetime_format(units)
        if format:
            start_time = datetime.strptime(units[start:end], format)
            for time in time_data:
                if units.startswith('days'):
                    converted_time.append(start_time + relativedelta(days=int(time), hours=12))
                elif units.startswith('seconds'):
                    converted_time.append(start_time + relativedelta(seconds=int(time)))
        else:
            for time in time_data:
                converted_time.append(time)
        return converted_time

    def _get_dim_indexing(self, data, min, max):
        if len(data) == 1:
            return 0
        start_index = bisect.bisect_right(data, min)
        end_index = bisect.bisect_right(data, max)
        if start_index != end_index:
            return slice(start_index, end_index)
        return start_index

    async def _fetch_data_source_list_json(self, session, base_url, query_args) -> Sequence:
        def _extender(catalogue: dict, feature_list: List[Dict]):
            for fc in feature_list:
                fc_props = fc.get("properties", {})
                fc_id = fc_props.get("identifier", None)
                if not fc_id:
                    continue
                catalogue[fc_id] = _get_feature_dict_from_feature(fc)
        catalogue = {}
        await self._fetch_opensearch_feature_list(session, base_url, catalogue, _extender, query_args)
        return catalogue

    async def _fetch_opensearch_feature_list(self, session, base_url, extension, extender, query_args):
        """
        Return JSON value read from Opensearch web service.
        :return:
        """
        start_page = 1
        maximum_records = 10000
        total_results = await self._fetch_opensearch_feature_part_list(session, base_url, query_args,
                                                                       start_page, maximum_records,
                                                                       extension, extender)
        num_results = maximum_records
        while num_results < total_results:
            tasks = []
            while len(tasks) < 4 and num_results < total_results:
                start_page += 1
                tasks.append(self._fetch_opensearch_feature_part_list(session, base_url, query_args,
                                                                      start_page, maximum_records, extension, extender))
                num_results += maximum_records
            await asyncio.gather(*tasks)

    async def _fetch_opensearch_feature_part_list(self, session, base_url, query_args, start_page,
                                                  maximum_records, extension, extender) -> int:
        paging_query_args = dict(query_args or {})
        paging_query_args.update(startPage=start_page, maximumRecords=maximum_records,
                                 httpAccept='application/geo+json')
        url = base_url + '?' + urllib.parse.urlencode(paging_query_args)
        resp = await self.get_response(session, url)
        if resp:
            json_text = await resp.read()
            _LOG.debug(f'Read page {start_page}')
            json_dict = json.loads(json_text.decode('utf-8'))
            if extender:
                feature_list = json_dict.get("features", [])
                extender(extension, feature_list)
            return json_dict['totalResults']
        _LOG.debug(f'Did not read page {start_page}')

    async def _fetch_variable_infos(self, opensearch_url: str, dataset_id: str, session):
        attributes = {}
        dimensions = {}
        variable_infos = {}
        feature, time_dimension_size = \
            await self._fetch_feature_and_num_nc_files_at(session,
                                                          opensearch_url,
                                                          dict(parentIdentifier=dataset_id),
                                                          1)
        if feature is not None:
            variable_infos, attributes = \
                await self._get_variable_infos_from_feature(feature, session)
            for variable_info in variable_infos:
                for dimension in variable_infos[variable_info]['dimensions']:
                    if not dimension in dimensions:
                        if dimension == 'bin_index':
                            dimensions[dimension] = variable_infos[variable_info]['size']
                        elif not dimension in variable_infos and \
                                variable_info.split('_')[-1] == 'bnds':
                            dimensions[dimension] = 2
                        else:
                            if dimension not in variable_infos and \
                                    len(variable_infos[variable_info]['dimensions']):
                                dimensions[dimension] = variable_infos[variable_info]['size']
                            else:
                                dimensions[dimension] = variable_infos[dimension]['size']
            if 'time' in dimensions:
                time_dimension_size *= dimensions['time']
        return dimensions, variable_infos, attributes, time_dimension_size

    async def _fetch_feature_and_num_nc_files_at(self, session, base_url, query_args, index) -> \
            Tuple[Optional[Dict], int]:
        paging_query_args = dict(query_args or {})
        maximum_records = 1
        paging_query_args.update(startPage=index,
                                 maximumRecords=maximum_records,
                                 httpAccept='application/geo+json',
                                 fileFormat='.nc')
        url = base_url + '?' + urllib.parse.urlencode(paging_query_args)
        resp = await self.get_response(session, url)
        if resp:
            json_text = await resp.read()
            json_dict = json.loads(json_text.decode('utf-8'))
            feature_list = json_dict.get("features", [])
            if len(feature_list) > 0:
                return feature_list[0], json_dict.get("totalResults", 0)
        return None, 0

    async def _fetch_meta_info(self, session, odd_url: str, metadata_url: str) -> Dict:
        meta_info_dict = {}
        if odd_url:
            meta_info_dict = await self._extract_metadata_from_odd_url(session, odd_url)
        if metadata_url:
            desc_metadata = await self._extract_metadata_from_descxml_url(session, metadata_url)
            for item in desc_metadata:
                if not item in meta_info_dict:
                    meta_info_dict[item] = desc_metadata[item]
        _harmonize_info_field_names(meta_info_dict, 'file_format', 'file_formats')
        _harmonize_info_field_names(meta_info_dict, 'platform_id', 'platform_ids')
        _harmonize_info_field_names(meta_info_dict, 'sensor_id', 'sensor_ids')
        _harmonize_info_field_names(meta_info_dict, 'processing_level', 'processing_levels')
        _harmonize_info_field_names(meta_info_dict, 'time_frequency', 'time_frequencies')
        return meta_info_dict

    async def _extract_metadata_from_descxml_url(self, session, descxml_url: str = None) -> dict:
        if not descxml_url:
            return {}
        resp = await self.get_response(session, descxml_url)
        if resp:
            descxml = etree.XML(await resp.read())
            try:
                return _extract_metadata_from_descxml(descxml)
            except etree.ParseError:
                _LOG.info(f'Cannot read metadata from {descxml_url} due to parsing error.')
        return {}

    async def _extract_metadata_from_odd_url(self, session: aiohttp.ClientSession, odd_url: str = None) -> dict:
        if not odd_url:
            return {}
        resp = await self.get_response(session, odd_url)
        if not resp:
            return {}
        xml_text = await resp.read()
        return _extract_metadata_from_odd(etree.XML(xml_text))

    async def _get_variable_infos_from_feature(self, feature: dict, session) -> (dict, dict):
        feature_info = _extract_feature_info(feature)
        opendap_url = f"{feature_info[4]['Opendap']}"
        dataset = await self._get_opendap_dataset(session, opendap_url)
        if not dataset:
            _LOG.warning(f'Could not extract information about variables and attributes from {opendap_url}')
            return {}, {}
        variable_infos = {}
        for key in dataset.keys():
            fixed_key = key.replace('%2E', '_').replace('.', '_')
            variable_infos[fixed_key] = dataset[key].attributes
            if '_FillValue' in variable_infos[fixed_key]:
                variable_infos[fixed_key]['fill_value'] = variable_infos[fixed_key]['_FillValue']
                variable_infos[fixed_key].pop('_FillValue')
            if '_ChunkSizes' in variable_infos[fixed_key]:
                variable_infos[fixed_key]['chunk_sizes'] = variable_infos[fixed_key]['_ChunkSizes']
                variable_infos[fixed_key].pop('_ChunkSizes')
            variable_infos[fixed_key]['data_type'] = dataset[key].dtype.name
            variable_infos[fixed_key]['dimensions'] = list(dataset[key].dimensions)
            variable_infos[fixed_key]['size'] = dataset[key].size
        return variable_infos, dataset.attributes

    def get_opendap_dataset(self, url: str):
        return _run_with_session(self._get_opendap_dataset, url)

    async def _get_opendap_dataset(self, session, url: str):
        tasks = []
        res_dict = {}
        tasks.append(self._get_content_from_opendap_url(url, 'dds', res_dict, session))
        tasks.append(self._get_content_from_opendap_url(url, 'das', res_dict, session))
        await asyncio.gather(*tasks)
        if 'dds' not in res_dict or 'das' not in res_dict:
            _LOG.warning('Could not open opendap url. No dds or das file provided.')
            return
        if res_dict['dds'] == '':
            _LOG.warning('Could not open opendap url. dds file is empty.')
            return
        dataset = build_dataset(res_dict['dds'])
        res_dict['das'] = res_dict['das'].replace('        Float32 valid_min -Infinity;\n', '')
        res_dict['das'] = res_dict['das'].replace('        Float32 valid_max Infinity;\n', '')
        add_attributes(dataset, parse_das(res_dict['das']))

        # remove any projection from the url, leaving selections
        scheme, netloc, path, query, fragment = urlsplit(url)
        projection, selection = parse_ce(query)
        url = urlunsplit((scheme, netloc, path, '&'.join(selection), fragment))

        # now add data proxies
        for var in walk(dataset, BaseType):
            var.data = BaseProxy(url, var.id, var.dtype, var.shape)
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

        dataset.functions = Functions(url)

        return dataset

    async def _get_content_from_opendap_url(self, url: str, part: str, res_dict: dict, session):
        scheme, netloc, path, query, fragment = urlsplit(url)
        url = urlunsplit((scheme, netloc, path + f'.{part}', query, fragment))
        resp = await self.get_response(session, url)
        if resp:
            res_dict[part] = await resp.read()
            res_dict[part] = str(res_dict[part], 'utf-8')

    async def _get_data_from_opendap_dataset(self, dataset, session, variable_name, slices):
        proxy = dataset[variable_name].data
        if type(proxy) == list:
            proxy = proxy[0]
        # build download url
        index = combine_slices(proxy.slice, fix_slice(slices, proxy.shape))
        scheme, netloc, path, query, fragment = urlsplit(proxy.baseurl)
        url = urlunsplit((
            scheme, netloc, path + '.dods',
            quote(proxy.id) + hyperslab(index) + '&' + query,
            fragment)).rstrip('&')
        # download and unpack data
        resp = await self.get_response(session, url)
        if not resp:
            return None
        content = await resp.read()
        dds, data = content.split(b'\nData:\n', 1)
        dds = str(dds, 'utf-8')
        # Parse received dataset:
        dataset = build_dataset(dds)
        dataset.data = unpack_data(BytesReader(data), dataset)
        return dataset[proxy.id].data

    async def get_response(self, session: aiohttp.ClientSession, url: str) -> Optional[aiohttp.ClientResponse]:
        num_retries = self._num_retries
        retry_backoff_max = self._retry_backoff_max  # ms
        retry_backoff_base = self._retry_backoff_base
        response = None
        for i in range(num_retries):
            resp = await session.request(method='GET', url=url)
            if resp.status == 200:
                return resp
            elif 500 <= resp.status < 600:
                # Retry (immediately) on 5xx errors
                continue
            elif resp.status == 429:
                # Retry after 'Retry-After' with exponential backoff
                retry_min = int(response.headers.get('Retry-After', '100'))
                retry_backoff = random.random() * retry_backoff_max
                retry_total = retry_min + retry_backoff
                if self._enable_warnings:
                    retry_message = f'Error 429: Too Many Requests. ' \
                                    f'Attempt {i + 1} of {num_retries} to retry after ' \
                                    f'{"%.2f" % retry_min} + {"%.2f" % retry_backoff} = {"%.2f" % retry_total} ms...'
                    warnings.warn(retry_message)
                time.sleep(retry_total / 1000.0)
                retry_backoff_max *= retry_backoff_base
            else:
                break
        return None

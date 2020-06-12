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
import re
import urllib.parse
from datetime import datetime
from dateutil.relativedelta import relativedelta
from typing import List, Any, Dict, Tuple, Optional, Union, Sequence, Callable

from pydap.client import Functions
from pydap.handlers.dap import BaseProxy, SequenceProxy
from pydap.lib import fix_slice, walk
from pydap.model import BaseType, SequenceType, GridType
from pydap.parsers import parse_ce
from pydap.parsers.dds import build_dataset
from pydap.parsers.das import parse_das, add_attributes
from six.moves.urllib.parse import urlsplit, urlunsplit

_LOG = logging.getLogger('xcube')
_OPENSEARCH_CEDA_URL = "http://opensearch-test.ceda.ac.uk/opensearch/request"
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
    if time_value == 'month':
        return 'month'
    if time_value == 'yr':
        return 'year'
    if time_value == 'day':
        return 'day'
    if time_value == 'satellite-orbit-frequency':
        return 'satellite-orbit-frequency'
    if time_value == '5-days':
        return '5 days'
    if time_value == '8-days':
        return '8 days'
    if time_value == '15-days':
        return '15 days'
    if time_value == '13-yrs':
        return '13 years'
    if time_value == 'climatology':
        return 'climatology'
    raise ValueError('Unknown time frequency format')

async def _fetch_fid_and_uuid(dataset_id: str) -> (str, str):
    ds_components = dataset_id.split('.')
    query_args = dict(parentIdentifier='cci',
                      ecv=ds_components[1],
                      frequency=_convert_time_from_drs_id(ds_components[2]),
                      processingLevel=ds_components[3],
                      dataType=ds_components[4],
                      sensor=ds_components[5],
                      platform=ds_components[6],
                      productString=ds_components[7],
                      productVersion=ds_components[8].replace('-', '.')
                      )
    feature_list = await _fetch_opensearch_feature_list(_OPENSEARCH_CEDA_URL, query_args)
    if len(feature_list) == 0:
        query_args = dict(parentIdentifier='cci',
                          ecv=ds_components[1],
                          frequency=_convert_time_from_drs_id(ds_components[2]),
                          processingLevel=ds_components[3],
                          dataType=ds_components[4],
                          sensor=ds_components[5],
                          platform=ds_components[6],
                          productString=ds_components[7]
                          )
        feature_list = await _fetch_opensearch_feature_list(_OPENSEARCH_CEDA_URL, query_args)
        if len(feature_list) == 0:
            raise ValueError(f'Could not find any fid for dataset {dataset_id}')
    if len(feature_list) > 1:
        for feature in feature_list:
            title = feature.get("properties", {}).get("title", '')
            if ds_components[9] in title:
                fid = feature.get("properties", {}).get("identifier", None)
                uuid = feature.get("id", "uuid=").split("uuid=")[-1]
                return fid, uuid
        raise ValueError(f'Could not unambiguously determine fid for dataset {dataset_id}')
    fid = feature_list[0].get("properties", {}).get("identifier", None)
    uuid = feature_list[0].get("id", "uuid=").split("uuid=")[-1]
    return fid, uuid


async def _fetch_dataset_metadata(dataset_id: str) -> dict:
    fid, uuid = await _fetch_fid_and_uuid(dataset_id)
    query_args = dict(parentIdentifier='cci', uuid=uuid)
    feature_list = await _fetch_opensearch_feature_list(_OPENSEARCH_CEDA_URL, query_args)
    if len(feature_list) < 1:
        return {}
    feature_metadata = _get_feature_dict_from_feature(feature_list[0])
    fid = feature_list[0].get("properties", {}).get("identifier", None)
    return await _fetch_meta_info(fid, feature_metadata['odd_url'], feature_metadata['metadata_url'],
                                  feature_metadata['variables'], True)


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


async def _fetch_data_source_list_json(base_url, query_args) -> Sequence:
    feature_collection_list = await _fetch_opensearch_feature_list(base_url, query_args)
    catalogue = {}
    for fc in feature_collection_list:
        fc_props = fc.get("properties", {})
        fc_id = fc_props.get("identifier", None)
        if not fc_id:
            continue
        catalogue[fc_id] = _get_feature_dict_from_feature(fc)
    return catalogue


async def _fetch_opensearch_feature_list(base_url, query_args) -> List:
    """
    Return JSON value read from Opensearch web service.
    :return:
    """
    start_page = 1
    maximum_records = 1000
    full_feature_list = []
    async with aiohttp.ClientSession() as session:
        while True:
            paging_query_args = dict(query_args or {})
            paging_query_args.update(startPage=start_page, maximumRecords=maximum_records,
                                     httpAccept='application/geo+json')
            url = base_url + '?' + urllib.parse.urlencode(paging_query_args)
            resp = await session.request(method='GET', url=url)
            resp.raise_for_status()
            json_text = await resp.read()
            json_dict = json.loads(json_text.decode('utf-8'))
            feature_list = json_dict.get("features", [])
            full_feature_list.extend(feature_list)
            if len(feature_list) < maximum_records:
                break
            start_page += 1
    return full_feature_list


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


async def _fetch_feature_at(session, base_url, query_args, index) -> Optional[Dict]:
    paging_query_args = dict(query_args or {})
    maximum_records = 1
    paging_query_args.update(startPage=index, maximumRecords=maximum_records, httpAccept='application/geo+json',
                             fileFormat='.nc')
    url = base_url + '?' + urllib.parse.urlencode(paging_query_args)
    resp = await session.request(method='GET', url=url)
    resp.raise_for_status()
    json_text = await resp.read()
    json_dict = json.loads(json_text.decode('utf-8'))
    feature_list = json_dict.get("features", [])
    if len(feature_list) > 0:
        return feature_list[0]
    return None


def _replace_ending_number_brackets(das: str) -> str:
    number_signs = ['1', '2', '3', '4', '5', '6', '7', '8', '9', '0']
    for number_sign in number_signs:
        das = das.replace(f'{number_sign};\n', f'{number_sign}],\n')
    return das


async def _get_content_from_opendap_url(url: str, part: str, res_dict: dict, session):
    scheme, netloc, path, query, fragment = urlsplit(url)
    url = urlunsplit((scheme, netloc, path + f'.{part}', query, fragment))
    resp = await session.request(method='GET', url=url)
    if resp.status >= 400:
        resp.release()
        _LOG.warning(f"Could not open {url}: {resp.status}")
    else:
        res_dict[part] = await resp.read()


def get_opendap_dataset(url: str):
    return asyncio.run(_wrap_get_opendap_dataset(url))

async def _wrap_get_opendap_dataset(url: str):
    async with aiohttp.ClientSession() as session:
        return await _get_opendap_dataset(url, session)


async def _get_opendap_dataset(url: str, session):
    tasks = []
    res_dict = {}
    tasks.append(_get_content_from_opendap_url(url, 'dds', res_dict, session))
    tasks.append(_get_content_from_opendap_url(url, 'das', res_dict, session))
    await asyncio.gather(*tasks)
    if 'dds' not in res_dict or 'das' not in res_dict:
        return
    dataset = build_dataset(str(res_dict['dds'], 'utf-8'))
    add_attributes(dataset, parse_das(str(res_dict['das'], 'utf-8')))

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


async def _get_variable_infos_from_feature(feature: dict, session) -> (dict, dict):
    feature_info = _extract_feature_info(feature)
    opendap_url = f"{feature_info[4]['Opendap']}"
    dataset = await _get_opendap_dataset(opendap_url, session)
    variable_infos = {}
    for key in dataset.keys():
        variable_infos[key] = dataset[key].attributes
        if '_FillValue' in variable_infos[key]:
            variable_infos[key]['fill_value'] = variable_infos[key]['_FillValue']
            variable_infos[key].pop('_FillValue')
        if '_ChunkSizes' in variable_infos[key]:
            variable_infos[key]['chunk_sizes'] = variable_infos[key]['_ChunkSizes']
            variable_infos[key].pop('_ChunkSizes')
        variable_infos[key]['data_type'] = dataset[key].dtype.name
        variable_infos[key]['dimensions'] = list(dataset[key].dimensions)
        variable_infos[key]['size'] = dataset[key].size
    return variable_infos, dataset.attributes


def _retrieve_attribute_info_from_das(das: str) -> dict:
    if len(das) == 0:
        return {}
    das = das.replace(' "', '": "')
    json_das = _handle_numeric_attribute_names(das)
    json_das = json_das.replace('Attributes', '{\n  "Attributes').replace(' {\n    ', '": {\n    "') \
        .replace('{\n    "    ', '{\n      "').replace(';\n        ', '",\n      "').replace('}\n    ', '},\n    '). \
        replace(',\n    },\n    ', '\n    },\n    "').replace('""', '"'). \
        replace(';\n    }\n    ', '"\n    },\n    "').replace(';\n    }\n}', '\n    }\n  }\n}')
    json_das = prettify_dict_names(json_das)
    dict = json.loads(json_das)
    return dict['Attributes']


def _handle_numeric_attribute_names(das: str) -> str:
    numeric_attribute_names = ['_ChunkSizes', 'min', 'max', 'resolution']
    for numeric_attribute_name in numeric_attribute_names:
        appearances = set(re.findall(numeric_attribute_name + ' [-|0-9|.|, ]+;\n[ ]*', das))
        for appearance in appearances:
            fixed_appearance = appearance.replace(f'{numeric_attribute_name} ', f'{numeric_attribute_name}": [')
            fixed_appearance = fixed_appearance.replace(';', '],')
            fixed_appearance = f'{fixed_appearance}"'
            das = das.replace(appearance, fixed_appearance)
    das = das.replace('"}', '}')
    fill_value_appearances = set(re.findall('_FillValue [-|0-9|.|NaN]+;\n {8}', das))
    for fill_value_appearance in fill_value_appearances:
        fixed_appearance = fill_value_appearance.replace('_FillValue ', '_FillValue": "')
        fixed_appearance = fixed_appearance.replace(';\n        ', '",\n      "')
        das = das.replace(fill_value_appearance, fixed_appearance)
    return das


def prettify_dict_names(das: str) -> str:
    ugly_keys = set(re.findall('"[A-Z]+[a-z]+[0-9]* [a-zA-Z_]+":', das))
    for ugly_key in ugly_keys:
        das = das.replace(ugly_key, f'"{ugly_key.split(" ")[1]}')
    das = das.replace('_ChunkSizes', 'chunk_sizes')
    das = das.replace('_FillValue', 'fill_value')
    return das


async def _get_infos_from_feature(session, feature: dict) -> tuple:
    feature_info = _extract_feature_info(feature)
    opendap_dds_url = f"{feature_info[4]['Opendap']}.dds"
    resp = await session.request(method='GET', url=opendap_dds_url)
    if resp.status >= 400:
        resp.release()
        _LOG.warning(f"Could not open {opendap_dds_url}: {resp.status}")
        return {}, {}
    content = await resp.read()
    return _retrieve_infos_from_dds(str(content, 'utf-8').split('\n'))


def _retrieve_infos_from_dds(dds_lines: List) -> tuple:
    dimensions = {}
    variable_infos = {}
    dim_info_pattern = '[a-zA-Z0-9_]+ [a-zA-Z0-9_]+[\[\w* = \d{1,7}\]]*;'
    type_and_name_pattern = '[a-zA-Z0-9_]+'
    dimension_pattern = '\[[a-zA-Z]* = \d{1,7}\]'
    for dds_line in dds_lines:
        if type(dds_line) is bytes:
            dds_line = str(dds_line, 'utf-8')
        dim_info_search_res = re.search(dim_info_pattern, dds_line)
        if dim_info_search_res is None:
            continue
        type_and_name = re.findall(type_and_name_pattern, dim_info_search_res.string)
        if type_and_name[1] not in variable_infos:
            dimension_names = []
            variable_dimensions = re.findall(dimension_pattern, dim_info_search_res.string)
            for variable_dimension in variable_dimensions:
                dimension_name, dimension_size = variable_dimension[1:-1].split(' = ')
                dimension_names.append(dimension_name)
                if dimension_name not in dimensions:
                    dimensions[dimension_name] = int(dimension_size)
            variable_infos[type_and_name[1]] = {'data_type': type_and_name[0], 'dimensions': dimension_names}
    return dimensions, variable_infos


async def _fetch_meta_info(dataset_id: str, odd_url: str, metadata_url: str, variables: List, read_dimensions: bool) \
        -> Dict:
    async with aiohttp.ClientSession() as session:
        meta_info_dict = {}
        if odd_url:
            meta_info_dict = await _extract_metadata_from_odd_url(session, odd_url)
        if metadata_url:
            desc_metadata = await _extract_metadata_from_descxml_url(session, metadata_url)
            for item in desc_metadata:
                if not item in meta_info_dict:
                    meta_info_dict[item] = desc_metadata[item]
        if read_dimensions and len(variables) > 0:
            meta_info_dict['dimensions'], meta_info_dict['variable_infos'], meta_info_dict['attributes'] = \
                await _fetch_variable_infos(dataset_id, session)
        _harmonize_info_field_names(meta_info_dict, 'file_format', 'file_formats')
        _harmonize_info_field_names(meta_info_dict, 'platform_id', 'platform_ids')
        _harmonize_info_field_names(meta_info_dict, 'sensor_id', 'sensor_ids')
        _harmonize_info_field_names(meta_info_dict, 'processing_level', 'processing_levels')
        _harmonize_info_field_names(meta_info_dict, 'time_frequency', 'time_frequencies')
        return meta_info_dict


async def _fetch_variable_infos(dataset_id: str, session):
    attributes = {}
    dimensions = {}
    variable_infos = {}
    feature = await _fetch_feature_at(session, _OPENSEARCH_CEDA_URL, dict(parentIdentifier=dataset_id), 1)
    if feature is not None:
        variable_infos, attributes = await _get_variable_infos_from_feature(feature, session)
        for variable_info in variable_infos:
            for dimension in variable_infos[variable_info]['dimensions']:
                if not dimension in dimensions:
                    if dimension == 'bin_index':
                        dimensions[dimension] = variable_infos[variable_info]['size']
                    elif not dimension in variable_infos and variable_info.split('_')[-1] == 'bnds':
                        dimensions[dimension] = 2
                    else:
                        dimensions[dimension] = variable_infos[dimension]['size']
    return dimensions, variable_infos, attributes


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


async def _extract_metadata_from_descxml_url(session=None, descxml_url: str = None) -> dict:
    if session is None:
        session = aiohttp.ClientSession()
    if not descxml_url:
        return {}
    resp = await session.request(method='GET', url=descxml_url)
    resp.raise_for_status()
    descxml = etree.XML(await resp.read())
    try:
        return _extract_metadata_from_descxml(descxml)
    except etree.ParseError:
        _LOG.info(f'Cannot read metadata from {descxml_url} due to parsing error.')
        return {}


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


async def _extract_metadata_from_odd_url(session=None, odd_url: str = None) -> dict:
    if session is None:
        session = aiohttp.ClientSession()
    if not odd_url:
        return {}
    resp = await session.request(method='GET', url=odd_url)
    resp.raise_for_status()
    xml_text = await resp.read()
    return _extract_metadata_from_odd(etree.XML(xml_text))


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
    Represents the ESA CCI Open Data Portal.

    :param error_policy: "raise" or "warn". If "raise" an exception is raised on failed API requests.
    :param error_handler: An optional function called with the response from a failed API request.
    :param enable_warnings: Allow emitting warnings on failed API requests.
    """

    def __init__(self,
                 enable_warnings: bool = False,
                 error_policy: str = 'fail',
                 error_handler: Callable[[Any], None] = None):
        self.error_policy = error_policy or 'fail'
        self.error_handler = error_handler
        self.enable_warnings = enable_warnings
        self._data_sources = None

    def close(self):
        pass

    @property
    def token_info(self) -> Dict[str, Any]:
        return {}

    @property
    def dataset_names(self) -> List[str]:
        return asyncio.run(self._fetch_dataset_names())

    @property
    def description(self) -> dict:
        asyncio.run(self._ensure_data_sources_read(read_dimensions=True))
        datasets = []
        for data_source in self._data_sources:
            metadata = self._data_sources[data_source]
            var_names = self._get_var_names(metadata.get('variable_infos', {}))
            variables = []
            for variable in var_names:
                variable_dict = {}
                var_metadata = [dict for dict in metadata['variables'] if dict['name'] == variable][0]
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

    def get_dataset_info(self, dataset_id: str, dataset_metadata: dict=None) -> dict:
        data_info = {}
        if not dataset_metadata:
            dataset_metadata = self.get_dataset_metadata(dataset_id)
        nc_attrs = dataset_metadata.get('attributes', {}).get('NC_GLOBAL', {})
        data_info['lat_res'] = self._get_res(nc_attrs, 'lat')
        data_info['lon_res'] = self._get_res(nc_attrs, 'lon')
        data_info['bbox'] = (float(dataset_metadata['bbox_minx']), float(dataset_metadata['bbox_miny']),
                             float(dataset_metadata['bbox_maxx']), float(dataset_metadata['bbox_maxy']))
        data_info['temporal_coverage_start'] = '2000-02-01T00:00:00'
        data_info['temporal_coverage_end'] = '2014-12-31T23:59:59'
        data_info['var_names'] = self.var_names(dataset_id)
        return data_info

    def _get_res(self, nc_attrs: dict, dim: str) -> float:
        if dim == 'lat':
            attr_name = 'geospatial_lat_resolution'
            index = 0
        else:
            attr_name = 'geospatial_lon_resolution'
            index = -1
        for name in [attr_name, 'resolution']:
            if name in nc_attrs:
                res_attr = nc_attrs[name]
                if type(res_attr) == float:
                    return res_attr
                return float(res_attr.split('x')[index].split('deg')[0].split('degree')[0])
        return -1.0

    def get_dataset_metadata(self, dataset_id: str) -> dict:
        return asyncio.run(_fetch_dataset_metadata(dataset_id))

    async def _ensure_data_sources_read(self, read_dimensions: bool = False):
        if not self._data_sources:
            self._data_sources = {}
            catalogue = await _fetch_data_source_list_json(_OPENSEARCH_CEDA_URL, dict(parentIdentifier='cci'))
            if catalogue:
                tasks = []
                for catalogue_item in catalogue:
                    tasks.append(self._create_data_source(catalogue[catalogue_item], catalogue_item, read_dimensions))
                await asyncio.gather(*tasks)

    async def _fetch_dataset_names(self):
        if not self._data_sources:
            self._data_sources = {}
            catalogue = await _fetch_data_source_list_json(_OPENSEARCH_CEDA_URL, dict(parentIdentifier='cci'))
            if catalogue:
                tasks = []
                for catalogue_item in catalogue:
                    tasks.append(self._create_data_source(catalogue[catalogue_item], catalogue_item))
                await asyncio.gather(*tasks)
        return list(self._data_sources.keys())

    async def _create_data_source(self, json_dict: dict, datasource_id: str, read_dimensions: bool = False):
        meta_info = await _fetch_meta_info(datasource_id, json_dict['odd_url'], json_dict['metadata_url'],
                                           json_dict['variables'], read_dimensions)
        drs_ids = self._get_as_list(meta_info, 'drs_id', 'drs_ids')
        for drs_id in drs_ids:
            meta_info = meta_info.copy()
            meta_info.update(json_dict)
            self._adjust_json_dict(meta_info, drs_id)
            meta_info['cci_project'] = meta_info['ecv']
            meta_info['fid'] = datasource_id
            self._data_sources[drs_id] = meta_info

    def _adjust_json_dict(self, json_dict: dict, drs_id: str):
        values = drs_id.split('.')
        self._adjust_json_dict_for_param(json_dict, 'time_frequency', 'time_frequencies',
                                         self._convert_time_from_drs_id(values[2]))
        self._adjust_json_dict_for_param(json_dict, 'processing_level', 'processing_levels', values[3])
        self._adjust_json_dict_for_param(json_dict, 'data_type', 'data_types', values[4])
        self._adjust_json_dict_for_param(json_dict, 'sensor_id', 'sensor_ids', values[5])
        self._adjust_json_dict_for_param(json_dict, 'platform_id', 'platform_ids', values[6])
        self._adjust_json_dict_for_param(json_dict, 'product_string', 'product_strings', values[7])
        self._adjust_json_dict_for_param(json_dict, 'product_version', 'product_versions', values[8])

    def _convert_time_from_drs_id(self, time_value: str) -> str:
        if time_value == 'mon' or time_value == 'month':
            return 'month'
        if time_value == 'yr' or time_value == 'year':
            return 'year'
        if time_value == 'day':
            return 'day'
        if time_value == 'satellite-orbit-frequency':
            return 'satellite-orbit-frequency'
        if time_value == '5-days':
            return '5 days'
        if time_value == '8-days':
            return '8 days'
        if time_value == '15-days':
            return '15 days'
        if time_value == '13-yrs':
            return '13 years'
        if time_value == 'climatology':
            return 'climatology'
        raise ValueError('Unknown time frequency format')

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
        return asyncio.run(self._var_names(dataset_name))

    async def _var_names(self, dataset_name) -> List:
        async with aiohttp.ClientSession() as session:
            fid, uuid = await _fetch_fid_and_uuid(dataset_name)
            dimensions, variable_infos, attributes = await _fetch_variable_infos(fid, session)
            return self._get_var_names(variable_infos)

    def _get_var_names(self, variable_infos) -> List:
        # todo support variables with other dimensions
        variables = []
        for variable in variable_infos:
            dimensions = variable_infos[variable]['dimensions']
            if ('lat' not in dimensions and 'latitude' not in dimensions) or \
                    ('lon' not in dimensions and 'longitude' not in dimensions) or \
                    (len(dimensions) > 2 and 'time' not in dimensions):
                continue
            variables.append(variable)
        return variables

    def get_dimension_data(self, dataset_name: str, dimension_names: List[str]):
        fid, uuid = asyncio.run(_fetch_fid_and_uuid(dataset_name))
        request = dict(parentIdentifier=fid,
                       startDate='1900-01-01T00:00:00',
                       endDate='3001-12-31T00:00:00')
        opendap_url = self._get_opendap_url(request)
        dim_data = {}
        if opendap_url:
            dataset = get_opendap_dataset(opendap_url)
            for dim in dimension_names:
                if dim in dataset:
                    dim_data[dim] = dict(size=dataset[dim].size,
                                         chunkSize=dataset[dim].attributes.get('_ChunkSizes'))
                    if dataset[dim].size < 512 * 512:
                        dim_data[dim]['data'] = dataset[dim].data[:].tolist()
                    else:
                        dim_data[dim]['data'] = []
                else:
                    dim_data[dim] = dict(size=dimension_names[dim],
                                         chunkSize=dimension_names[dim],
                                         data=list(range(dimension_names[dim]))
                                         )
        return dim_data

    def get_earliest_start_date(self, dataset_name: str, start_time: str, end_time: str, frequency: str) -> \
            Optional[datetime]:
        query_args = dict(parentIdentifier=self.get_fid_for_dataset(dataset_name),
                          startDate=start_time,
                          endDate=end_time,
                          frequency=frequency,
                          fileFormat='.nc')
        opendap_url = self._get_opendap_url(query_args, get_earliest=True)
        if opendap_url:
            dataset = get_opendap_dataset(opendap_url)
            start_time_attributes = ['time_coverage_start', 'start_date']
            attributes = dataset.attributes.get('NC_GLOBAL', {})
            for start_time_attribute in start_time_attributes:
                start_time_string = attributes[start_time_attribute]
                time_format, start, end, timedelta = find_datetime_format(start_time_string)
                if time_format:
                    start_time = datetime.strptime(start_time_string[start:end], time_format)
                    return start_time
        return None

    def get_fid_for_dataset(self, dataset_name: str) -> str:
        fid, uuid = asyncio.run(_fetch_fid_and_uuid(dataset_name))
        return fid

    def _get_opendap_url(self, request: Dict, get_earliest: bool = False):
        start_date = datetime.strptime(request['startDate'], _TIMESTAMP_FORMAT)
        end_date = datetime.strptime(request['endDate'], _TIMESTAMP_FORMAT)
        request['fileFormat'] = '.nc'
        feature_list = asyncio.run(_fetch_opensearch_feature_list(_OPENSEARCH_CEDA_URL, request))
        if len(feature_list) == 0:
            # try without dates. For some data sets, this works better
            request.pop('startDate')
            request.pop('endDate')
            feature_list = asyncio.run(_fetch_opensearch_feature_list(_OPENSEARCH_CEDA_URL, request))
        opendap_url = None
        earliest_date = datetime(2999, 12, 31)
        for feature in feature_list:
            feature_info = _extract_feature_info(feature)
            feature_start_date = datetime.strptime(feature_info[1], _TIMESTAMP_FORMAT)
            feature_end_date = datetime.strptime(feature_info[2], _TIMESTAMP_FORMAT)
            if feature_end_date < start_date or feature_start_date > end_date or feature_start_date > earliest_date:
                continue
            feature_opendap_url = feature_info[4].get('Opendap', None)
            if feature_opendap_url:
                earliest_date = feature_start_date
                opendap_url = feature_opendap_url
                if not get_earliest:
                    return opendap_url
        return opendap_url

    def get_data(self, request: Dict, bbox: Tuple[float, float, float, float], dim_indexes: dict, dim_flipped: dict) \
            -> Optional[bytes]:
        start_date = datetime.strptime(request['startDate'], _TIMESTAMP_FORMAT)
        end_date = datetime.strptime(request['endDate'], _TIMESTAMP_FORMAT)
        var_names = request['varNames']
        opendap_url = self._get_opendap_url(request)
        if not opendap_url:
            return None
        dataset = get_opendap_dataset(opendap_url)
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
        var_name = request['varNames'][0]
        opendap_url = self._get_opendap_url(request)
        if not opendap_url:
            return None
        dataset = get_opendap_dataset(opendap_url)
        variable_data = np.array(dataset[var_name][dim_indexes].data[0], dtype=dataset[var_name].dtype.type)
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

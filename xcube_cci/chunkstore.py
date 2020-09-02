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

from datetime import datetime
from dateutil.relativedelta import relativedelta
import itertools
import json
import time
import warnings
from abc import abstractmethod, ABCMeta
from collections import MutableMapping
from typing import Iterator, Any, List, Dict, Tuple, Callable, Iterable, KeysView, Mapping, Union

import numpy as np
import pandas as pd
import re

from .cciodp import CciOdp

_TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S"


def _dict_to_bytes(d: Dict):
    return _str_to_bytes(json.dumps(d, indent=2))


def _str_to_bytes(s: str):
    return bytes(s, encoding='utf-8')


# todo move this to xcube
class RemoteChunkStore(MutableMapping, metaclass=ABCMeta):
    """
    A remote Zarr Store.

    :param data_id: The identifier of the
    :param cube_params: A mapping containing additional parameters to define the cube.
    :param observer: An optional callback function called when remote requests are mode: observer(**kwargs).
    :param trace_store_calls: Whether store calls shall be printed (for debugging).
    """

    def __init__(self,
                 data_id: str,
                 cube_params: Mapping[str, Any] = None,
                 observer: Callable = None,
                 trace_store_calls=False):
        if not cube_params:
            cube_params = {}
        self._variable_names = cube_params.get('variable_names', self.get_all_variable_names())
        self._observers = [observer] if observer is not None else []
        self._trace_store_calls = trace_store_calls

        self._dataset_name = data_id
        self._time_ranges = self.get_time_ranges(data_id, cube_params)

        if not self._time_ranges:
            raise ValueError('Could not determine any valid time stamps')

        t_array = np.array([s + 0.5 * (e - s) for s, e in self._time_ranges]).astype('datetime64[s]').astype(np.int64)
        t_bnds_array = np.array(self._time_ranges).astype('datetime64[s]').astype(np.int64)
        time_coverage_start = self._time_ranges[0][0]
        time_coverage_end = self._time_ranges[-1][1]
        cube_params['time_range'] = (self._extract_time_range_as_strings(
            cube_params.get('time_range', self.get_default_time_range(data_id))))

        self._vfs = {
        }

        self._dimension_data = self.get_dimension_data(data_id)
        for dimension_name in self._dimension_data:
            if dimension_name == 'time':
                self._dimension_data['time']['size'] = len(t_array)
                self._dimension_data['time']['data'] = t_array
                continue
            dim_attrs = self.get_attrs(dimension_name)
            dim_attrs['_ARRAY_DIMENSIONS'] = dimension_name
            dimension_data = self._dimension_data[dimension_name]['data']
            if len(dimension_data) > 0:
                dim_array = np.array(dimension_data)
                self._add_static_array(dimension_name, dim_array, dim_attrs)
            else:
                size = self._dimension_data[dimension_name]['size']
                chunk_size = self._dimension_data[dimension_name]['chunkSize']
                encoding = self.get_encoding(dimension_name)
                self._add_remote_array(dimension_name, [size], [chunk_size], encoding, dim_attrs)

        time_attrs = {
            "_ARRAY_DIMENSIONS": ['time'],
            "units": "seconds since 1970-01-01T00:00:00Z",
            "calendar": "proleptic_gregorian",
            "standard_name": "time",
            "bounds": "time_bnds",
        }
        time_bnds_attrs = {
            "_ARRAY_DIMENSIONS": ['time', 'bnds'],
            "units": "seconds since 1970-01-01T00:00:00Z",
            "calendar": "proleptic_gregorian",
            "standard_name": "time",
        }

        self._add_static_array('time', t_array, time_attrs)
        self._add_static_array('time_bnds', t_bnds_array, time_bnds_attrs)

        self._time_indexes = {}
        for variable_name in self._variable_names:
            var_encoding = self.get_encoding(variable_name)
            var_attrs = self.get_attrs(variable_name)
            dimensions = var_attrs.get('dimensions', None)
            if not dimensions:
                warnings.warn(f'Could not find dimensions of variable {variable_name}. '
                              f'Will omit this one from the dataset.')
                self._variable_names.remove(variable_name)
                continue
            var_attrs.update(_ARRAY_DIMENSIONS=dimensions)
            chunk_sizes = var_attrs.get('chunk_sizes', [-1] * len(dimensions))
            if isinstance(chunk_sizes, int):
                chunk_sizes = [chunk_sizes]
            sizes = []
            self._time_indexes[variable_name] = -1
            for i, dimension_name in enumerate(dimensions):
                sizes.append(self._dimension_data[dimension_name]['size'])
                if dimension_name == 'time':
                    chunk_sizes[i] = 1
                    self._time_indexes[variable_name] = i
                if chunk_sizes[i] == -1:
                    chunk_sizes[i] = sizes[i]
            var_attrs['chunk_sizes'] = chunk_sizes
            self._add_remote_array(variable_name,
                                   sizes,
                                   chunk_sizes,
                                   var_encoding,
                                   var_attrs)
        cube_params['variable_names'] = self._variable_names
        global_attrs = dict(
            Conventions='CF-1.7',
            coordinates='time_bnds',
            title=f'{data_id} Data Cube',
            history=[
                dict(
                    program=f'{self._class_name}',
                    cube_params=cube_params
                )
            ],
            date_created=pd.Timestamp.now().isoformat(),
            processing_level=self._dataset_name.split('.')[3],
            time_coverage_start=time_coverage_start.isoformat(),
            time_coverage_end=time_coverage_end.isoformat(),
            time_coverage_duration=(time_coverage_end - time_coverage_start).isoformat(),
        )
        # setup Virtual File System (vfs)
        self._vfs['.zgroup'] = _dict_to_bytes(dict(zarr_format=2))
        self._vfs['.zattrs'] = _dict_to_bytes(global_attrs)

    @classmethod
    def _adjust_size(cls, size: int, tile_size: int) -> int:
        if size > tile_size:
            num_tiles = cls._safe_int_div(size, tile_size)
            size = num_tiles * tile_size
        return size

    @classmethod
    def _safe_int_div(cls, x: int, y: int) -> int:
        return (x + y - 1) // y

    @abstractmethod
    def get_time_ranges(self, cube_id: str, cube_params: Mapping[str, Any]) -> List[Tuple]:
        pass

    @abstractmethod
    def get_default_time_range(self, ds_id: str) -> Tuple[str, str]:
        return '', ''

    @abstractmethod
    def get_all_variable_names(self) -> List[str]:
        pass

    # def get_spatial_lat_res(self):
    #     return self._cube_config.spatial_res

    # def get_spatial_lon_res(self):
    #     return self._cube_config.spatial_res

    @abstractmethod
    def get_dimension_data(self, dataset_id: str) -> dict:
        pass

    def add_observer(self, observer: Callable):
        """
        Add a request observer.

        :param observer: A callback function called when remote requests are mode: observer(**kwargs).
        """
        self._observers.append(observer)

    @abstractmethod
    def get_encoding(self, band_name: str) -> Dict[str, Any]:
        """
        Get the encoding settings for band (variable) *band_name*.
        Must at least contain "dtype" whose value is a numpy array-protocol type string.
        Refer to https://docs.scipy.org/doc/numpy/reference/arrays.interface.html#arrays-interface
        and zarr format 2 spec.
        """

    @abstractmethod
    def get_attrs(self, band_name: str) -> Dict[str, Any]:
        """
        Get any metadata attributes for band (variable) *band_name*.
        """

    def request_bbox(self, x_tile_index: int, y_tile_index: int) -> Tuple[float, float, float, float]:
        x_index = x_tile_index * self._tile_width
        y_index = y_tile_index * self._tile_height

        x01, _, _, y02 = self.cube_config.geometry
        spatial_lat_res = self.get_spatial_lat_res()
        spatial_lon_res = self.get_spatial_lon_res()

        x1 = x01 + spatial_lon_res * x_index
        x2 = x01 + spatial_lon_res * (x_index + self._tile_width)
        y1 = y02 - spatial_lat_res * (y_index + self._tile_height)
        y2 = y02 - spatial_lat_res * y_index

        return x1, y1, x2, y2

    def request_time_range(self, time_index: int) -> Tuple[pd.Timestamp, pd.Timestamp]:
        start_time, end_time = self._time_ranges[time_index]
        return start_time, end_time

    def _add_static_array(self, name: str, array: np.ndarray, attrs: Dict):
        shape = list(map(int, array.shape))
        dtype = str(array.dtype.str)
        array_metadata = {
            "zarr_format": 2,
            "chunks": shape,
            "shape": shape,
            "dtype": dtype,
            "fill_value": None,
            "compressor": None,
            "filters": None,
            "order": "C",
        }
        self._vfs[name] = _str_to_bytes('')
        self._vfs[name + '/.zarray'] = _dict_to_bytes(array_metadata)
        self._vfs[name + '/.zattrs'] = _dict_to_bytes(attrs)
        self._vfs[name + '/' + ('.'.join(['0'] * array.ndim))] = bytes(array)

    def _add_remote_array(self,
                          name: str,
                          shape: List[int],
                          chunks: List[int],
                          encoding: Dict[str, Any],
                          attrs: Dict):
        array_metadata = dict(zarr_format=2,
                              shape=shape,
                              chunks=chunks,
                              compressor=None,
                              fill_value=None,
                              filters=None,
                              order='C')
        array_metadata.update(encoding)
        self._vfs[name] = _str_to_bytes('')
        self._vfs[name + '/.zarray'] = _dict_to_bytes(array_metadata)
        self._vfs[name + '/.zattrs'] = _dict_to_bytes(attrs)
        nums = np.array(shape) // np.array(chunks)
        indexes = itertools.product(*tuple(map(range, map(int, nums))))
        for index in indexes:
            filename = '.'.join(map(str, index))
            # noinspection PyTypeChecker
            self._vfs[name + '/' + filename] = name, index

    def _fetch_chunk(self, var_name: str, chunk_index: Tuple[int, ...]) -> bytes:
        request_time_range = self.request_time_range(self._time_indexes[var_name])

        t0 = time.perf_counter()
        try:
            exception = None
            chunk_data = self.fetch_chunk(var_name,
                                          chunk_index,
                                          # bbox=request_bbox,
                                          time_range=request_time_range)
        except Exception as e:
            exception = e
            chunk_data = None
        duration = time.perf_counter() - t0

        for observer in self._observers:
            observer(band_name=var_name,
                     chunk_index=chunk_index,
                     # bbox=request_bbox,
                     time_range=request_time_range,
                     duration=duration,
                     exception=exception)

        if exception:
            raise exception

        return chunk_data

    @abstractmethod
    def fetch_chunk(self,
                    var_name: str,
                    chunk_index: Tuple[int, ...],
                    time_range: Tuple[pd.Timestamp, pd.Timestamp]
                    ) -> bytes:
        """
        Fetch chunk data from remote.

        :param var_name: Variable name
        :param chunk_index: 3D chunk index (time, y, x)
        :param bbox: Requested bounding box in coordinate units of the CRS
        :param time_range: Requested time range
        :return: chunk data as raw bytes
        """
        pass

    @property
    def _class_name(self):
        return self.__module__ + '.' + self.__class__.__name__

    ###############################################################################
    # Zarr Store (MutableMapping) implementation
    ###############################################################################

    def keys(self) -> KeysView[str]:
        if self._trace_store_calls:
            print(f'{self._class_name}.keys()')
        return self._vfs.keys()

    def listdir(self, key: str) -> Iterable[str]:
        if self._trace_store_calls:
            print(f'{self._class_name}.listdir(key={key!r})')
        if key == '':
            return list((k for k in self._vfs.keys() if '/' not in k))
        else:
            prefix = key + '/'
            start = len(prefix)
            return list((k for k in self._vfs.keys() if k.startswith(prefix) and k.find('/', start) == -1))

    def getsize(self, key: str) -> int:
        if self._trace_store_calls:
            print(f'{self._class_name}.getsize(key={key!r})')
        return len(self._vfs[key])

    def __iter__(self) -> Iterator[str]:
        if self._trace_store_calls:
            print(f'{self._class_name}.__iter__()')
        return iter(self._vfs.keys())

    def __len__(self) -> int:
        if self._trace_store_calls:
            print(f'{self._class_name}.__len__()')
        return len(self._vfs.keys())

    def __contains__(self, key) -> bool:
        if self._trace_store_calls:
            print(f'{self._class_name}.__contains__(key={key!r})')
        return key in self._vfs

    def __getitem__(self, key: str) -> bytes:
        if self._trace_store_calls:
            print(f'{self._class_name}.__getitem__(key={key!r})')
        value = self._vfs[key]
        if isinstance(value, tuple):
            return self._fetch_chunk(*value)
        return value

    def __setitem__(self, key: str, value: bytes) -> None:
        if self._trace_store_calls:
            print(f'{self._class_name}.__setitem__(key={key!r}, value={value!r})')
        raise TypeError(f'{self._class_name} is read-only')

    def __delitem__(self, key: str) -> None:
        if self._trace_store_calls:
            print(f'{self._class_name}.__delitem__(key={key!r})')
        raise TypeError(f'{self._class_name} is read-only')


class CciChunkStore(RemoteChunkStore):
    """
    A remote Zarr Store using the ESA CCI Open Data Portal as backend.

    :param cci_odp: CCI ODP instance.
    :param cube_config: Cube configuration.
    :param observer: An optional callback function called when remote requests are mode: observer(**kwargs).
    :param trace_store_calls: Whether store calls shall be printed (for debugging).
    """

    _SAMPLE_TYPE_TO_DTYPE = {
        # Note: Sentinel Hub currently only supports unsigned
        # integer values therefore requesting INT8 or INT16
        # will return the same as UINT8 or UINT16 respectively.
        'uint8': '|u1',
        'uint16': '<u2',
        'uint32': '<u4',
        'int8': '|u1',
        'int16': '<u2',
        'int32': '<u4',
        'float32': '<f4',
        'float64': '<f8',
    }

    def __init__(self,
                 cci_odp: CciOdp,
                 dataset_id: str,
                 cube_params: Mapping[str, Any] = None,
                 observer: Callable = None,
                 trace_store_calls=False):
        self._cci_odp = cci_odp
        if dataset_id not in self._cci_odp.dataset_names:
            raise ValueError(f'Data ID {dataset_id} not provided by ODP.')
        self._metadata = self._cci_odp.get_dataset_metadata(dataset_id)
        super().__init__(dataset_id,
                         cube_params,
                         observer=observer,
                         trace_store_calls=trace_store_calls)

    @classmethod
    def _extract_time_as_string(cls, time: Union[pd.Timestamp, str]) -> str:
        if isinstance(time, str):
            time = pd.to_datetime(time, utc=True)
        return time.tz_localize(None).isoformat()

    @classmethod
    def _extract_time_range_as_strings(cls, time_range: Union[Tuple, List]) -> (str, str):
        if isinstance(time_range, tuple):
            time_start, time_end = time_range
        else:
            time_start = time_range[0]
            time_end = time_range[1]
        return cls._extract_time_as_string(time_start), cls._extract_time_as_string(time_end)

    def _extract_time_range_as_datetime(self, time_range: Union[Tuple, List]) -> (datetime, datetime, str, str):
        iso_start_time, iso_end_time = self._extract_time_range_as_strings(time_range)
        start_time = datetime.strptime(iso_start_time, _TIMESTAMP_FORMAT)
        end_time = datetime.strptime(iso_end_time, _TIMESTAMP_FORMAT)
        return start_time, end_time, iso_start_time, iso_end_time

    def get_time_ranges(self, dataset_id: str, cube_params: Mapping[str, Any]) -> List[Tuple]:
        start_time, end_time, iso_start_time, iso_end_time = \
            self._extract_time_range_as_datetime(cube_params.get('time_range', self.get_default_time_range(dataset_id)))
        time_period = dataset_id.split('.')[2]
        if time_period == 'day':
            start_time = datetime(year=start_time.year, month=start_time.month, day=start_time.day)
            end_time = datetime(year=end_time.year, month=end_time.month, day=end_time.day)
            delta = relativedelta(days=1)
        elif time_period == 'month' or time_period == 'mon':
            start_time = datetime(year=start_time.year, month=start_time.month, day=1)
            end_time = datetime(year=end_time.year, month=end_time.month, day=1)
            delta = relativedelta(months=+1)
            end_time += delta
        elif time_period == 'year' or time_period == 'yr':
            start_time = datetime(year=start_time.year, month=1, day=1)
            end_time = datetime(year=end_time.year, month=12, day=31)
            delta = relativedelta(years=1)
        elif re.compile('[0-9]*-days').search(time_period):
            num_days = int(time_period.split('-')[0])
            temp_start_time = datetime(start_time.year, start_time.month, start_time.day)
            temp_start_time -= relativedelta(days=num_days - 1)
            temp_start_time = self._cci_odp.get_earliest_start_date(dataset_id,
                                                                    datetime.strftime(temp_start_time,
                                                                                      _TIMESTAMP_FORMAT),
                                                                    iso_end_time,
                                                                    f'{num_days} days')
            if temp_start_time:
                start_time = temp_start_time
            else:
                start_time = datetime(start_time.year, start_time.month, start_time.day)
            start_time_ordinal = start_time.toordinal()
            end_time_ordinal = end_time.toordinal()
            end_time_ordinal = start_time_ordinal + int(np.ceil((end_time_ordinal - start_time_ordinal) /
                                                                float(num_days)) * num_days)
            end_time = datetime.fromordinal(end_time_ordinal)
            end_time += relativedelta(days=1)
            delta = relativedelta(days=num_days)
        elif re.compile('[0-9]*-yrs').search(time_period):
            num_years = int(time_period.split('-')[0])
            temp_start_time = datetime(start_time.year, start_time.month, start_time.day)
            temp_start_time -= relativedelta(years=num_years - 1)
            temp_start_time = self._cci_odp.get_earliest_start_date(dataset_id,
                                                                    datetime.strftime(temp_start_time,
                                                                                      _TIMESTAMP_FORMAT),
                                                                    iso_end_time,
                                                                    f'{num_years} years')
            if temp_start_time:
                start_time = temp_start_time
            else:
                start_time = datetime(start_time.year, start_time.month, start_time.day)
            start_time_ordinal = start_time.toordinal()
            end_time_ordinal = end_time.toordinal()
            end_time_ordinal = start_time_ordinal + int(np.ceil((end_time_ordinal - start_time_ordinal) /
                                                                float(num_years)) * num_years)
            end_time = datetime.fromordinal(end_time_ordinal)
            end_time += relativedelta(years=1)
            delta = relativedelta(years=num_years)
        elif time_period == 'satellite-orbit-frequency':
            time_range = (cube_params.get('time_range')[0],
                          cube_params.get('time_range')[1].replace(hour=23, minute=59, second=59))
            start_time, end_time, iso_start_time, iso_end_time = \
                self._extract_time_range_as_datetime(time_range)
            request_time_ranges = self._cci_odp.get_time_ranges_satellite_orbit_frequency(dataset_id,
                                                                                          iso_start_time,
                                                                                          iso_end_time)
            return request_time_ranges
        else:
            return []
        request_time_ranges = []
        this = start_time
        while this < end_time:
            next = this + delta
            pd_this = pd.Timestamp(datetime.strftime(this, _TIMESTAMP_FORMAT))
            pd_next = pd.Timestamp(datetime.strftime(next, _TIMESTAMP_FORMAT))
            request_time_ranges.append((pd_this, pd_next))
            this = next
        return request_time_ranges

    def get_default_time_range(self, ds_id: str):
        temporal_start = self._metadata.get('temporal_coverage_start', None)
        if not temporal_start:
            time_frequency = self._get_time_frequency(ds_id.split('.')[2])
            temporal_start = self._cci_odp.get_earliest_start_date(ds_id, '1000-01-01', '3000-12-31', time_frequency)
            if not temporal_start:
                raise ValueError("Could not determine temporal start of dataset. Please use 'time_range' parameter.")
            temporal_start = datetime.strftime(temporal_start, _TIMESTAMP_FORMAT)
        temporal_end = self._metadata.get('temporal_coverage_end', None)
        if not temporal_end:
            time_frequency = self._get_time_frequency(ds_id.split('.')[2])
            temporal_end = self._cci_odp.get_latest_end_date(ds_id, '1000-01-01', '3000-12-31', time_frequency)
            if not temporal_end:
                raise ValueError("Could not determine temporal end of dataset. Please use 'time_range' parameter.")
            temporal_end = datetime.strftime(temporal_end, _TIMESTAMP_FORMAT)
        return (temporal_start, temporal_end)

    def _get_time_frequency(self, time_period: str):
        if time_period == 'mon':
            return 'month'
        elif time_period == 'yr':
            return 'year'
        elif re.compile('[0-9]*-days').search(time_period):
            num_days = int(time_period.split('-')[0])
            return f'{num_days} days'
        elif re.compile('[0-9]*-yrs').search(time_period):
            num_years = int(time_period.split('-')[0])
            return f'{num_years} years'
        else:
            return time_period

    def _get_time_range_for_num_days(self, num_days: int, start_time: datetime, end_time: datetime):
        temp_start_time = datetime(start_time.year, start_time.month, start_time.day)
        temp_start_time -= relativedelta(days=num_days - 1)
        temp_start_time = self._cci_odp.get_earliest_start_date(self.cube_config.dataset_name,
                                                                datetime.strftime(temp_start_time, _TIMESTAMP_FORMAT),
                                                                datetime.strftime(end_time, _TIMESTAMP_FORMAT),
                                                                f'{num_days} days')
        if temp_start_time:
            start_time = temp_start_time
        else:
            start_time = datetime(start_time.year, start_time.month, start_time.day)
        start_time_ordinal = start_time.toordinal()
        end_time_ordinal = end_time.toordinal()
        end_time_ordinal = start_time_ordinal + int(np.ceil((end_time_ordinal - start_time_ordinal) /
                                                            float(num_days)) * num_days)
        end_time = datetime.fromordinal(end_time_ordinal)
        end_time += relativedelta(days=1)
        delta = relativedelta(days=num_days)
        return start_time, end_time, delta

    def get_all_variable_names(self) -> List[str]:
        return [variable['name'] for variable in self._metadata['variables']]

    def get_dimension_data(self, dataset_id: str):
        dimension_names = self._metadata['dimensions']
        return self._cci_odp.get_dimension_data(dataset_id, dimension_names)

    def get_encoding(self, var_name: str) -> Dict[str, Any]:
        encoding_dict = {}
        encoding_dict['fill_value'] = self._metadata.get('variable_infos', {}).get(var_name, {}).get('fill_value')
        encoding_dict['dtype'] = self._metadata.get('variable_infos', {}).get(var_name, {}).get('data_type')
        return encoding_dict

    def get_attrs(self, var_name: str) -> Dict[str, Any]:
        return self._metadata.get('variable_infos', {}).get(var_name, {})

    def fetch_chunk(self,
                    var_name: str,
                    chunk_index: Tuple[int, ...],
                    time_range: Tuple[pd.Timestamp, pd.Timestamp]) -> bytes:

        start_time, end_time = time_range
        identifier = self._cci_odp.get_fid_for_dataset(self._dataset_name)
        iso_start_date = start_time.tz_localize(None).isoformat()
        iso_end_date = end_time.tz_localize(None).isoformat()
        dim_indexes = self._get_dimension_indexes_for_chunk(var_name, chunk_index)
        request = dict(parentIdentifier=identifier,
                       varNames=[var_name],
                       startDate=iso_start_date,
                       endDate=iso_end_date,
                       fileFormat='.nc'
                       )
        data = self._cci_odp.get_data_chunk(request, dim_indexes)
        if not data:
            data = bytearray()
            var_info = self._metadata.get('variable_infos', {}).get(var_name, {})
            length = 1
            for chunk_size in var_info.get('chunk_sizes', {}):
                length *= chunk_size
            dtype = np.dtype(self._SAMPLE_TYPE_TO_DTYPE[var_info['data_type']])
            var_array = np.full(shape=length, fill_value=var_info['fill_value'], dtype=dtype)
            data += var_array.tobytes()
        return data

    def _get_dimension_indexes_for_chunk(self, var_name: str, chunk_index: Tuple[int, ...]) -> tuple:
        dim_indexes = []
        var_dimensions = self._metadata.get('variable_infos', {}).get(var_name, {}).get('dimensions', [])
        chunk_sizes = self.get_attrs(var_name).get('chunk_sizes', [])
        for i, var_dimension in enumerate(var_dimensions):
            if var_dimension == 'time':
                dim_indexes.append(slice(None, None, None))
                continue
            dim_size = self._metadata.get('dimensions', {}).get(var_dimension, -1)
            if dim_size < 0:
                raise ValueError(f'Could not determine size of dimension {var_dimension}')
            start = chunk_index[i] * chunk_sizes[i]
            end = min(start + chunk_sizes[i], dim_size)
            dim_indexes.append(slice(start, end))
        return tuple(dim_indexes)

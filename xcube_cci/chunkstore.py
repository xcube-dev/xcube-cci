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
import bisect
import itertools
import json
import logging
import time
import warnings
from abc import abstractmethod, ABCMeta
from collections.abc import MutableMapping
from numcodecs import Blosc
from typing import Iterator, Any, List, Dict, Tuple, Callable, Iterable, KeysView, Mapping, Union

import numpy as np
import pandas as pd
import re

from .cciodp import CciOdp

_STATIC_ARRAY_COMPRESSOR_PARAMS = dict(cname='zstd', clevel=1, shuffle=Blosc.SHUFFLE, blocksize=0)
_STATIC_ARRAY_COMPRESSOR_CONFIG = dict(id='blosc', **_STATIC_ARRAY_COMPRESSOR_PARAMS)
_STATIC_ARRAY_COMPRESSOR = Blosc(**_STATIC_ARRAY_COMPRESSOR_PARAMS)

_LOG = logging.getLogger()
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
        logging.debug('Determined time ranges')
        if not self._time_ranges:
            raise ValueError('Could not determine any valid time stamps')

        t_array = [s.to_pydatetime() + 0.5 * (e.to_pydatetime() - s.to_pydatetime())
                   for s, e in self._time_ranges]
        t_array = np.array(t_array).astype('datetime64[s]').astype(np.int64)
        t_bnds_array = np.array(self._time_ranges).astype('datetime64[s]').astype(np.int64)
        time_coverage_start = self._time_ranges[0][0]
        time_coverage_end = self._time_ranges[-1][1]
        cube_params['time_range'] = (self._extract_time_range_as_strings(
            cube_params.get('time_range', self.get_default_time_range(data_id))))

        self._vfs = {
        }

        bbox = cube_params.get('bbox', None)
        lon_size = -1
        lat_size = -1
        self._dimension_chunk_offsets = {}
        self._dimension_data = self.get_dimension_data(data_id)
        logging.debug('Determined dimensionalities')
        self._dimension_data['time'] = {}
        self._dimension_data['time']['size'] = len(t_array)
        self._dimension_data['time']['data'] = t_array
        for dimension_name in self._dimension_data:
            if dimension_name == 'time':
                continue
            dim_attrs = self.get_attrs(dimension_name)
            dim_attrs['_ARRAY_DIMENSIONS'] = dimension_name
            dimension_data = self._dimension_data[dimension_name]['data']
            if bbox and dimension_name == 'lon' or dimension_name == 'longitude':
                if dimension_data[0] < dimension_data[-1]:
                    min_lat_offset = bisect.bisect_left(dimension_data, bbox[0])
                    max_lat_offset = bisect.bisect_right(dimension_data, bbox[2])
                else:
                    min_lat_offset = bisect.bisect_left(dimension_data, bbox[2])
                    max_lat_offset = bisect.bisect_right(dimension_data, bbox[0])
                dimension_data = self._adjust_dimension_data(dimension_name, min_lat_offset,
                                                             max_lat_offset, dimension_data,
                                                             dim_attrs)
            elif bbox and dimension_name == 'lat' or dimension_name == 'latitude':
                min_lat_offset = bisect.bisect_left(dimension_data, bbox[1])
                max_lat_offset = bisect.bisect_right(dimension_data, bbox[3])
                dimension_data = self._adjust_dimension_data(dimension_name, min_lat_offset,
                                                             max_lat_offset, dimension_data,
                                                             dim_attrs)
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

        global_attrs = dict(
            Conventions='CF-1.7',
            coordinates='time_bnds',
            title=f'{data_id} Data Cube',
            date_created=pd.Timestamp.now().isoformat(),
            processing_level=self._dataset_name.split('.')[3],
            time_coverage_start=time_coverage_start.isoformat(),
            time_coverage_end=time_coverage_end.isoformat(),
            time_coverage_duration=(time_coverage_end - time_coverage_start).isoformat(),
        )

        self._time_indexes = {}
        remove = []
        logging.debug('Adding variables to dataset ...')
        for variable_name in self._variable_names:
            if variable_name in self._dimension_data or variable_name == 'time_bnds':
                remove.append(variable_name)
                continue
            var_encoding = self.get_encoding(variable_name)
            var_attrs = self.get_attrs(variable_name)
            dimensions = var_attrs.get('dimensions', None)
            if not dimensions:
                warnings.warn(f"Could not find dimensions of variable '{variable_name}'. "
                              f"Will omit it from the dataset.")
                remove.append(variable_name)
                continue
            self._maybe_adjust_attrs(lon_size, lat_size, var_attrs)
            chunk_sizes = var_attrs.get('chunk_sizes', [-1] * len(dimensions))
            if isinstance(chunk_sizes, int):
                chunk_sizes = [chunk_sizes]
            if 'time' not in dimensions:
                dimensions.insert(0, 'time')
                chunk_sizes.insert(0, 1)
            var_attrs.update(_ARRAY_DIMENSIONS=dimensions)
            sizes = []
            self._time_indexes[variable_name] = -1
            time_dimension = -1
            for i, dimension_name in enumerate(dimensions):
                sizes.append(self._dimension_data[dimension_name]['size'])
                if dimension_name == 'time':
                    self._time_indexes[variable_name] = i
                    time_dimension = i
                if chunk_sizes[i] == -1:
                    chunk_sizes[i] = sizes[i]
            if var_encoding.get('dtype', '') == 'bytes1024':
                if len(dimensions) == 1 and sizes[0] < 512 * 512:
                    _LOG.info(f"Variable '{variable_name}' is encoded as string. "
                              f"Will convert it to metadata.")
                    variable = {variable_name: sizes[0]}
                    var_data = self.get_variable_data(data_id, variable)
                    global_attrs[variable_name] = \
                        [var.decode('utf-8') for var in var_data[variable_name]['data']]
                else:
                    warnings.warn(f"Variable '{variable_name}' is encoded as string. "
                                  f"Will omit it from the dataset.")
                remove.append(variable_name)
                continue
            chunk_sizes = self._adjust_chunk_sizes(chunk_sizes, sizes, time_dimension)
            var_attrs['chunk_sizes'] = chunk_sizes
            if len(var_attrs['file_dimensions']) < len(dimensions):
                var_attrs['file_chunk_sizes'] = chunk_sizes[1:]
            else:
                var_attrs['file_chunk_sizes'] = chunk_sizes
            self._add_remote_array(variable_name,
                                   sizes,
                                   chunk_sizes,
                                   var_encoding,
                                   var_attrs)
        logging.debug(f"Added a total of {len(self._variable_names)} variables to the data set")
        for r in remove:
            self._variable_names.remove(r)
        cube_params['variable_names'] = self._variable_names
        global_attrs['history'] = [dict(
            program=f'{self._class_name}',
            cube_params=cube_params
        )]
        # setup Virtual File System (vfs)
        self._vfs['.zgroup'] = _dict_to_bytes(dict(zarr_format=2))
        self._vfs['.zattrs'] = _dict_to_bytes(global_attrs)

    def _adjust_dimension_data(self, dimension_name: str, min_offset:int, max_offset: int,
                               dimension_data, dim_attrs: dict):
        self._dimension_chunk_offsets[dimension_name] = min_offset
        dimension_data = dimension_data[min_offset:max_offset]
        lon_size = len(dimension_data)
        dim_attrs['chunk_sizes'] = min(dim_attrs.get('chunk_sizes', 1000000), lon_size)
        dim_attrs['file_chunk_sizes'] = \
            min(dim_attrs.get('file_chunk_sizes', 1000000), lon_size)
        dim_attrs['size'] = lon_size
        if 'shape' in dim_attrs:
            dim_attrs['shape'][0] = lon_size
        self._metadata['dimensions'][dimension_name] = lon_size
        self._dimension_data[dimension_name]['size'] = lon_size
        self._dimension_data[dimension_name]['chunkSize'] = \
            min(dim_attrs['chunk_sizes'], lon_size)
        self._dimension_data[dimension_name]['data'] = dimension_data
        return dimension_data

    @classmethod
    def _maybe_adjust_attrs(cls, lon_size, lat_size, var_attrs):
        cls._maybe_adjust_to('lat', 'latitude', lat_size, var_attrs)
        cls._maybe_adjust_to('lon', 'longitude', lon_size, var_attrs)

    @classmethod
    def _maybe_adjust_to(cls, first_name, second_name, adjusted_size, var_attrs):
        if adjusted_size == -1:
            return
        try:
            index = var_attrs['dimensions'].index(first_name)
        except ValueError:
            try:
                index = var_attrs['dimensions'].index(second_name)
            except ValueError:
                index = -1
        if index > 0:
            var_attrs['shape'][index] = adjusted_size
            if 'chunk_sizes' in var_attrs:
                var_attrs['chunk_sizes'][index] = \
                    min(var_attrs['chunk_sizes'][index], adjusted_size)
                var_attrs['file_chunk_sizes'][index] = \
                    min(var_attrs['file_chunk_sizes'][index], adjusted_size)

    @classmethod
    def _adjust_chunk_sizes(cls, chunks, sizes, time_dimension):
        # check if we can actually read in everything as one big chunk
        sum_sizes = np.product(sizes)
        if time_dimension >= 0:
            sum_sizes = sum_sizes / sizes[time_dimension] * chunks[time_dimension]
            if sum_sizes < 1000000:
                best_chunks = sizes.copy()
                best_chunks[time_dimension] = chunks[time_dimension]
                return best_chunks
        if sum_sizes < 1000000:
            return sizes
        # determine valid values for a chunk size. A value is valid if the size can be divided by it without remainder
        valid_chunk_sizes = []
        for i, chunk, size in zip(range(len(chunks)), chunks, sizes):
            # do not rechunk time dimension
            if i == time_dimension:
                valid_chunk_sizes.append([chunk])
                continue
            #handle case that the size cannot be divided evenly by the chunk
            if size % chunk > 0:
                if np.product(chunks) / chunk * size < 1000000:
                    # if the size is small enough to be ingested in single chunk, take it
                    valid_chunk_sizes.append([size])
                else:
                    # otherwise, give in to that we cannot chunk the data evenly
                    valid_chunk_sizes.append(list(range(chunk, size + 1, chunk)))
                continue
            valid_dim_chunk_sizes = []
            for r in range(chunk, size + 1, chunk):
                if size % r == 0:
                    valid_dim_chunk_sizes.append(r)
            valid_chunk_sizes.append(valid_dim_chunk_sizes)
        # recursively determine the chunking with the biggest size smaller than 1000000
        chunks, chunk_size = cls._get_best_chunks(chunks, valid_chunk_sizes, chunks.copy(), 0, 0, time_dimension)
        return chunks

    @classmethod
    def _get_best_chunks(cls, chunks, valid_sizes, best_chunks, best_chunk_size, index, time_dimension):
        for valid_size in valid_sizes[index]:
            test_chunks = chunks.copy()
            test_chunks[index] = valid_size
            if index < len(chunks) - 1:
                test_chunks, test_chunk_size = \
                    cls._get_best_chunks(test_chunks, valid_sizes, best_chunks, best_chunk_size, index + 1,
                                         time_dimension)
            else:
                test_chunk_size = np.product(test_chunks)
                if test_chunk_size > 1000000:
                    break
            if test_chunk_size > best_chunk_size:
                best_chunk_size = test_chunk_size
                best_chunks = test_chunks.copy()
            elif test_chunk_size == best_chunk_size:
                # in case two chunkings have the same size, choose the one where values are more similar
                where = np.full(len(test_chunks), fill_value=True)
                where[time_dimension] = False
                test_min_chunk = np.max(test_chunks, initial=0, where=where)
                best_min_chunk = np.max(best_chunks, initial=0, where=where)
                if best_min_chunk > test_min_chunk:
                    best_chunk_size = test_chunk_size
                    best_chunks = test_chunks.copy()
        return best_chunks, best_chunk_size

    @classmethod
    def _adjust_size(cls, size: int, tile_size: int) -> int:
        if size > tile_size:
            num_tiles = cls._safe_int_div(size, tile_size)
            size = num_tiles * tile_size
        return size

    @classmethod
    def _safe_int_div(cls, x: int, y: int) -> int:
        return (x + y - 1) // y

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

    @abstractmethod
    def get_variable_data(self, dataset_id: str, variable_names: Dict[str, int]):
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
        order = "C"
        array_metadata = {
            "zarr_format": 2,
            "chunks": shape,
            "shape": shape,
            "dtype": dtype,
            "fill_value": None,
            "compressor": _STATIC_ARRAY_COMPRESSOR_CONFIG,
            "filters": None,
            "order": order,
        }
        chunk_key = '.'.join(['0'] * array.ndim)
        self._vfs[name] = _str_to_bytes('')
        self._vfs[name + '/.zarray'] = _dict_to_bytes(array_metadata)
        self._vfs[name + '/.zattrs'] = _dict_to_bytes(attrs)
        self._vfs[name + '/' + chunk_key] = \
            _STATIC_ARRAY_COMPRESSOR.encode(array.tobytes(order=order))


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

    def _fetch_chunk(self, key: str, var_name: str, chunk_index: Tuple[int, ...]) -> bytes:
        request_time_range = self.request_time_range(chunk_index[self._time_indexes[var_name]])

        t0 = time.perf_counter()
        try:
            exception = None
            chunk_data = self.fetch_chunk(key, var_name, chunk_index, request_time_range)
        except Exception as e:
            exception = e
            chunk_data = None
        duration = time.perf_counter() - t0

        for observer in self._observers:
            observer(var_name=var_name,
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
                    key: str,
                    var_name: str,
                    chunk_index: Tuple[int, ...],
                    time_range: Tuple[pd.Timestamp, pd.Timestamp]
                    ) -> bytes:
        """
        Fetch chunk data from remote.

        :param key: The original chunk key being retrieved.
        :param var_name: Variable name
        :param chunk_index: chunk index
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
            return self._fetch_chunk(key, *value)
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

    def _extract_time_range_as_datetime(self, time_range: Union[Tuple, List]) -> (datetime, datetime, str, str):
        iso_start_time, iso_end_time = self._extract_time_range_as_strings(time_range)
        start_time = datetime.strptime(iso_start_time, _TIMESTAMP_FORMAT)
        end_time = datetime.strptime(iso_end_time, _TIMESTAMP_FORMAT)
        return start_time, end_time, iso_start_time, iso_end_time

    def get_time_ranges(self, dataset_id: str, cube_params: Mapping[str, Any]) -> List[Tuple]:
        start_time, end_time, iso_start_time, iso_end_time = \
            self._extract_time_range_as_datetime(
                cube_params.get('time_range', self.get_default_time_range(dataset_id)))
        time_period = dataset_id.split('.')[2]
        if time_period == 'day':
            start_time = datetime(year=start_time.year, month=start_time.month, day=start_time.day)
            end_time = datetime(year=end_time.year, month=end_time.month, day=end_time.day,
                                hour=23, minute=59, second=59)
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
        else:
            end_time = end_time.replace(hour=23, minute=59, second=59)
            end_time_str = datetime.strftime(end_time, _TIMESTAMP_FORMAT)
            iso_end_time = self._extract_time_as_string(end_time_str)
            request_time_ranges = self._cci_odp.get_time_ranges_from_data(dataset_id, iso_start_time, iso_end_time)
            return request_time_ranges
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
        temporal_end = self._metadata.get('temporal_coverage_end', None)
        if not temporal_start or not temporal_end:
            time_ranges = self._cci_odp.get_time_ranges_from_data(ds_id, '1000-01-01', '3000-12-31')
            if not temporal_start:
                if len(time_ranges) == 0:
                    raise ValueError(
                        "Could not determine temporal start of dataset. Please use 'time_range' parameter.")
                temporal_start = time_ranges[0][0]
            if not temporal_end:
                if len(time_ranges) == 0:
                    raise ValueError(
                        "Could not determine temporal end of dataset. Please use 'time_range' parameter.")
                temporal_end = time_ranges[-1][1]
        return (temporal_start, temporal_end)

    def get_all_variable_names(self) -> List[str]:
        return [variable['name'] for variable in self._metadata['variables']]

    def get_dimension_data(self, dataset_id: str):
        dimensions = self._metadata['dimensions']
        dimension_data = self.get_variable_data(dataset_id, dimensions)
        if len(dimension_data) == 0:
            # no valid data found in indicated time range, let's set this broader
            dimension_data = self._cci_odp.get_variable_data(dataset_id, dimensions)
        return dimension_data

    def get_variable_data(self, dataset_id: str, variables: Dict[str, int]):
        return self._cci_odp.get_variable_data(dataset_id,
                                               variables,
                                               self._time_ranges[0][0].strftime(_TIMESTAMP_FORMAT),
                                               self._time_ranges[0][1].strftime(_TIMESTAMP_FORMAT))

    def get_encoding(self, var_name: str) -> Dict[str, Any]:
        encoding_dict = {}
        encoding_dict['fill_value'] = self._metadata.get('variable_infos', {}).get(var_name, {}).get('fill_value')
        encoding_dict['dtype'] = self._metadata.get('variable_infos', {}).get(var_name, {}).get('data_type')
        return encoding_dict

    def get_attrs(self, var_name: str) -> Dict[str, Any]:
        return self._metadata.get('variable_infos', {}).get(var_name, {})

    def fetch_chunk(self,
                    key: str,
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
                       drsId=self._dataset_name,
                       fileFormat='.nc'
                       )
        data = self._cci_odp.get_data_chunk(request, dim_indexes)
        if not data:
            raise KeyError(f'{key}: cannot fetch chunk for variable {var_name!r} '
                           f'and time_range {time_range!r}.')
        _LOG.info(f'Fetched chunk for ({chunk_index})"{var_name}"')
        return data

    def _get_dimension_indexes_for_chunk(self, var_name: str, chunk_index: Tuple[int, ...]) -> tuple:
        dim_indexes = []
        var_dimensions = self.get_attrs(var_name).get('file_dimensions', [])
        chunk_sizes = self.get_attrs(var_name).get('file_chunk_sizes', [])
        offset = 0
        # dealing with the case that time has been added as additional first dimension
        if len(chunk_index) > len(chunk_sizes):
            offset = 1
        for i, var_dimension in enumerate(var_dimensions):
            if var_dimension == 'time':
                dim_indexes.append(slice(None, None, None))
                continue
            dim_size = self._metadata.get('dimensions', {}).get(var_dimension, -1)
            if dim_size < 0:
                raise ValueError(f'Could not determine size of dimension {var_dimension}')
            data_offset = self._dimension_chunk_offsets.get(var_dimension, 0)
            start = data_offset + chunk_index[i + offset] * chunk_sizes[i]
            end = min(start + chunk_sizes[i], data_offset + dim_size)
            dim_indexes.append(slice(start, end))
        return tuple(dim_indexes)

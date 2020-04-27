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

import itertools
import json
import time
from abc import abstractmethod, ABCMeta
from collections import MutableMapping
from typing import Iterator, Any, List, Dict, Tuple, Callable, Iterable, KeysView, Union

import numpy as np
import pandas as pd

from .config import CubeConfig
from .constants import DATA_ARRAY_NAME
from .cciodp import CciOdp


def _dict_to_bytes(d: Dict):
    return _str_to_bytes(json.dumps(d, indent=2))


def _str_to_bytes(s: str):
    return bytes(s, encoding='utf-8')


# todo move this to xcube
class RemoteStore(MutableMapping, metaclass=ABCMeta):
    """
    A remote Zarr Store.

    :param cube_config: Cube configuration.
    :param observer: An optional callback function called when remote requests are mode: observer(**kwargs).
    :param trace_store_calls: Whether store calls shall be printed (for debugging).
    """

    def __init__(self,
                 cube_config: CubeConfig,
                 observer: Callable = None,
                 trace_store_calls=False):

        self._cube_config = cube_config
        self._observers = [observer] if observer is not None else []
        self._trace_store_calls = trace_store_calls
        self._time_ranges = self.get_time_ranges()

        if not self._time_ranges:
            raise ValueError('Could not determine any valid time stamps')

        width, height = self._cube_config.size
        spatial_res = self._cube_config.spatial_res
        x1, y1, x2, y2 = self._cube_config.geometry
        x_array = np.linspace(x1 + spatial_res / 2, x2 - spatial_res / 2, width, dtype=np.float64)
        y_array = np.linspace(y2 - spatial_res / 2, y1 + spatial_res / 2, height, dtype=np.float64)

        t_array = np.array([s + 0.5 * (e - s) for s, e in self._time_ranges]).astype('datetime64[s]').astype(np.int64)
        t_bnds_array = np.array(self._time_ranges).astype('datetime64[s]').astype(np.int64)

        time_coverage_start = self._time_ranges[0][0]
        time_coverage_end = self._time_ranges[-1][1]
        #todo add processing_level from metadata
        global_attrs = dict(
            Conventions='CF-1.7',
            coordinates='time_bnds',
            title=f'{self._cube_config.dataset_name} Data Cube Subset',
            history=[
                dict(
                    program=f'{self._class_name}',
                    cube_config=self._cube_config.as_dict(),
                )
            ],
            date_created=pd.Timestamp.now().isoformat(),
            time_coverage_start=time_coverage_start.isoformat(),
            time_coverage_end=time_coverage_end.isoformat(),
            time_coverage_duration=(time_coverage_end - time_coverage_start).isoformat(),
        )
        if self._cube_config.time_period:
            global_attrs.update(time_coverage_resolution=self._cube_config.time_period.isoformat())

        if self._cube_config.is_wgs84_crs:
            x1, y2, x2, y2 = self._cube_config.geometry
            global_attrs.update(geospatial_lon_min=x1,
                                geospatial_lat_min=y1,
                                geospatial_lon_max=x2,
                                geospatial_lat_max=y2)

        # setup Virtual File System (vfs)
        self._vfs = {
            '.zgroup': _dict_to_bytes(dict(zarr_format=2)),
            '.zattrs': _dict_to_bytes(global_attrs)
        }

        if self._cube_config.is_wgs84_crs:
            x_name, y_name = 'lon', 'lat'
            x_attrs, y_attrs = ({
                                    "_ARRAY_DIMENSIONS": ['lon'],
                                    "units": "decimal_degrees",
                                    "long_name": "longitude",
                                    "standard_name": "longitude",
                                }, {
                                    "_ARRAY_DIMENSIONS": ['lat'],
                                    "units": "decimal_degrees",
                                    "long_name": "longitude",
                                    "standard_name": "latitude",
                                })
        else:
            x_name, y_name = 'x', 'y'
            x_attrs, y_attrs = ({
                                    "_ARRAY_DIMENSIONS": ['x'],
                                    "long_name": "x coordinate of projection",
                                    "standard_name": "projection_x_coordinate",
                                }, {
                                    "_ARRAY_DIMENSIONS": ['y'],
                                    "long_name": "y coordinate of projection",
                                    "standard_name": "projection_y_coordinate",
                                })

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

        self._add_static_array(x_name, x_array, x_attrs)
        self._add_static_array(y_name, y_array, y_attrs)
        self._add_static_array('time', t_array, time_attrs)
        self._add_static_array('time_bnds', t_bnds_array, time_bnds_attrs)

        if self._cube_config.four_d:
            if self._cube_config.is_wgs84_crs:
                var_array_dimensions = ['time', 'lat', 'lon', 'var']
            else:
                var_array_dimensions = ['time', 'y', 'x', 'var']
            tile_width, tile_height = self._cube_config.tile_size
            num_vars = len(self._cube_config.variable_names)
            self._add_static_array('variable',
                                   np.array(self._cube_config.variable_names),
                                   attrs=dict(_ARRAY_DIMENSIONS=['variable']))
            var_encoding = self.get_encoding(DATA_ARRAY_NAME)
            var_attrs = self.get_attrs(DATA_ARRAY_NAME)
            var_attrs.update(_ARRAY_DIMENSIONS=var_array_dimensions, var_names=self._cube_config.variable_names)
            self._add_remote_array(DATA_ARRAY_NAME,
                                   [t_array.size, height, width, num_vars],
                                   [1, tile_height, tile_width, num_vars],
                                   var_encoding,
                                   var_attrs)
        else:
            if self._cube_config.is_wgs84_crs:
                band_array_dimensions = ['time', 'lat', 'lon']
            else:
                band_array_dimensions = ['time', 'y', 'x']
            tile_width, tile_height = self._cube_config.tile_size
            for variable_name in self._cube_config.variable_names:
                var_encoding = self.get_encoding(variable_name)
                var_attrs = self.get_attrs(variable_name)
                var_attrs.update(_ARRAY_DIMENSIONS=band_array_dimensions)
                self._add_remote_array(variable_name,
                                       [t_array.size, height, width],
                                       [1, tile_height, tile_width],
                                       var_encoding,
                                       var_attrs)

    def get_time_ranges(self):
        time_start, time_end = self._cube_config.time_range
        time_period = self._cube_config.time_period
        request_time_ranges = []
        time_now = time_start
        while time_now <= time_end:
            time_next = time_now + time_period
            request_time_ranges.append((time_now, time_next))
            time_now = time_next
        return request_time_ranges

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
        x_tile_size, y_tile_size = self.cube_config.tile_size

        x_index = x_tile_index * x_tile_size
        y_index = y_tile_index * y_tile_size

        x01, _, _, y02 = self.cube_config.geometry
        spatial_res = self.cube_config.spatial_res

        x1 = x01 + spatial_res * x_index
        x2 = x01 + spatial_res * (x_index + x_tile_size)
        y1 = y02 - spatial_res * (y_index + y_tile_size)
        y2 = y02 - spatial_res * y_index

        return x1, y1, x2, y2

    def request_time_range(self, time_index: int) -> Tuple[pd.Timestamp, pd.Timestamp]:
        start_time, end_time = self._time_ranges[time_index]
        # if self.cube_config.time_tolerance:
        #     start_time -= self.cube_config.time_tolerance
        #     end_time += self.cube_config.time_tolerance
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

    @property
    def cube_config(self) -> CubeConfig:
        return self._cube_config

    def _fetch_chunk(self, band_name: str, chunk_index: Tuple[int, ...]) -> bytes:
        if len(chunk_index) == 4:
            time_index, y_chunk_index, x_chunk_index, band_index = chunk_index
        else:
            time_index, y_chunk_index, x_chunk_index = chunk_index

        request_bbox = self.request_bbox(x_chunk_index, y_chunk_index)
        request_time_range = self.request_time_range(time_index)

        t0 = time.perf_counter()
        try:
            exception = None
            chunk_data = self.fetch_chunk(band_name,
                                          chunk_index,
                                          bbox=request_bbox,
                                          time_range=request_time_range)
        except Exception as e:
            exception = e
            chunk_data = None
        duration = time.perf_counter() - t0

        for observer in self._observers:
            observer(band_name=band_name,
                     chunk_index=chunk_index,
                     bbox=request_bbox,
                     time_range=request_time_range,
                     duration=duration,
                     exception=exception)

        if exception:
            raise exception

        return chunk_data

    @abstractmethod
    def fetch_chunk(self,
                    band_name: str,
                    chunk_index: Tuple[int, ...],
                    bbox: Tuple[float, float, float, float],
                    time_range: Tuple[pd.Timestamp, pd.Timestamp]) -> bytes:
        """
        Fetch chunk data from remote.

        :param band_name: Band name
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


class CciStore(RemoteStore):
    """
    A remote Zarr Store using the ESA CCI Open Data Portal as backend.

    :param cci_odp: CCI ODP instance.
    :param cube_config: Cube configuration.
    :param observer: An optional callback function called when remote requests are mode: observer(**kwargs).
    :param trace_store_calls: Whether store calls shall be printed (for debugging).
    """

    def __init__(self,
                 cci_odp: CciOdp,
                 cube_config: CubeConfig,
                 observer: Callable = None,
                 trace_store_calls=False):
        self._cci_odp = cci_odp
        self._metadata = None
        super().__init__(cube_config,
                         observer=observer,
                         trace_store_calls=trace_store_calls)

    def ensure_metadata_read(self):
        if self._metadata is None:
            self._metadata = self._cci_odp.get_dataset_metadata(self._cube_config.fid)

    def get_encoding(self, var_name: str) -> Dict[str, Any]:
        self.ensure_metadata_read()
        #todo use actual values from the metadata
        encoding_dict = {}
        encoding_dict['fill_value'] = 'NaN'
        encoding_dict['dtype'] = 'Float32'
        return encoding_dict

    def get_attrs(self, var_name: str) -> Dict[str, Any]:
        self.ensure_metadata_read()
        # todo use actual values from the metadata
        var_metadata = dict(standard_name = 'surface_air_pressure',
                            long_name = 'Pressure at the bottom of the atmosphere.',
                            units = 'hPa',
                            fill_value = 'NaN',
                            chunk_sizes = [1, 180, 360],
                            data_type = 'Float32',
                            dimensions = ['time', 'lat', 'lon']
                            )
        return var_metadata

    def fetch_chunk(self,
                    var_name: str,
                    chunk_index: Tuple[int, ...],
                    bbox: Tuple[float, float, float, float],
                    time_range: Tuple[pd.Timestamp, pd.Timestamp]) -> bytes:

        start_time, end_time = time_range
        if var_name == 'var_data':
            var_names = self.cube_config.variable_names
        else:
            var_names = [var_name]
        request = dict(parentIdentifier=self.cube_config.fid,
                       varNames=var_names,
                       startDate=start_time.tz_localize(None).isoformat(),
                       endDate=end_time.tz_localize(None).isoformat(),
                       bbox=bbox,
                       fileFormat='.nc'
                       )
        return self._cci_odp.get_data(request)

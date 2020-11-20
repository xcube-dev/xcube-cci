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

import numpy as np
import pandas as pd
import xarray as xr
import warnings

from typing import List, Optional

from xcube_cci.timeutil import get_timestamps_from_string


def normalize_cci_dataset(ds: xr.Dataset) -> xr.Dataset:
    ds = _normalize_zonal_lat_lon(ds)
    ds = _normalize_missing_time(ds)
    return ds


def _normalize_zonal_lat_lon(ds: xr.Dataset) -> xr.Dataset:
    """
    In case that the dataset only contains lat_centers and is a zonal mean dataset,
    the longitude dimension created and filled with the variable value of certain latitude.
    :param ds: some xarray dataset
    :return: a normalized xarray dataset
    """

    if 'latitude_centers' not in ds.coords or 'lon' in ds.coords:
        return ds

    ds_zonal = ds.copy()
    resolution = (ds.latitude_centers.values[1] - ds.latitude_centers.values[0])
    ds_zonal = ds_zonal.assign_coords(lon=[i + (resolution / 2) for i in np.arange(-180.0, 180.0, resolution)])

    for var in ds_zonal.data_vars:
        if sorted([dim for dim in ds_zonal[var].dims]) == sorted([coord for coord in ds.coords]):
            ds_zonal[var] = xr.concat([ds_zonal[var] for _ in ds_zonal.lon], 'lon')
            ds_zonal[var]['lon'] = ds_zonal.lon
    ds_zonal = ds_zonal.rename_dims({'latitude_centers': 'lat'})
    ds_zonal = ds_zonal.assign_coords(lat=ds.latitude_centers.values)
    ds_zonal = ds_zonal.drop('latitude_centers')
    ds_zonal = ds_zonal.transpose(..., 'lat', 'lon')

    has_lon_bnds = 'lon_bnds' in ds_zonal.coords or 'lon_bnds' in ds_zonal
    if not has_lon_bnds:
        ds_zonal = ds_zonal.assign_coords(
            lon_bnds=xr.DataArray([[i - (resolution / 2), i + (resolution / 2)] for i in ds_zonal.lon.values],
                                  dims=['lon', 'bnds']))
    has_lat_bnds = 'lat_bnds' in ds_zonal.coords or 'lat_bnds' in ds_zonal
    if not has_lat_bnds:
        ds_zonal = ds_zonal.assign_coords(
            lat_bnds=xr.DataArray([[i - (resolution / 2), i + (resolution / 2)] for i in ds_zonal.lat.values],
                                  dims=['lat', 'bnds']))

    ds_zonal.lon.attrs['bounds'] = 'lon_bnds'
    ds_zonal.lon.attrs['long_name'] = 'longitude'
    ds_zonal.lon.attrs['standard_name'] = 'longitude'
    ds_zonal.lon.attrs['units'] = 'degrees_east'

    ds_zonal.lat.attrs['bounds'] = 'lat_bnds'
    ds_zonal.lat.attrs['long_name'] = 'latitude'
    ds_zonal.lat.attrs['standard_name'] = 'latitude'
    ds_zonal.lat.attrs['units'] = 'degrees_north'

    return ds_zonal


def _get_time_coverage_from_ds(ds: xr.Dataset) -> (pd.Timestamp, pd.Timestamp):
    time_coverage_start = ds.attrs.get('time_coverage_start')
    if time_coverage_start is not None:
        # noinspection PyBroadException
        try:
            time_coverage_start = pd.to_datetime(time_coverage_start)
        except BaseException:
            pass

    time_coverage_end = ds.attrs.get('time_coverage_end')
    if time_coverage_end is not None:
        # noinspection PyBroadException
        try:
            time_coverage_end = pd.to_datetime(time_coverage_end)
        except BaseException:
            pass
    if time_coverage_start or time_coverage_end:
        return time_coverage_start, time_coverage_end
    filename = ds.encoding.get('source', '').split('/')[-1]
    return get_timestamps_from_string(filename)


def _normalize_missing_time(ds: xr.Dataset) -> xr.Dataset:
    """
    Add a time coordinate variable and their associated bounds coordinate variables
    if either temporal CF attributes ``time_coverage_start`` and ``time_coverage_end``
    are given or time information can be extracted from the file name but the time dimension is missing.

    The new time coordinate variable will be named ``time`` with dimension ['time'] and shape [1].
    The time bounds coordinates variable will be named ``time_bnds`` with dimensions ['time', 'bnds'] and shape [1,2].
    Both are of data type ``datetime64``.

    :param ds: Dataset to adjust
    :return: Adjusted dataset
    """
    time_coverage_start, time_coverage_end = _get_time_coverage_from_ds(ds)

    if not time_coverage_start and not time_coverage_end:
        # Can't do anything
        return ds

    if 'time' in ds:
        time = ds.time
        if not time.dims:
            ds = ds.drop_vars('time')
        elif len(time.dims) == 1:
            time_dim_name = time.dims[0]
            is_time_used_as_dim = any([(time_dim_name in ds[var_name].dims) for var_name in ds.data_vars])
            if is_time_used_as_dim:
                # It seems we already have valid time coordinates
                return ds
            time_bnds_var_name = time.attrs.get('bounds')
            if time_bnds_var_name in ds:
                ds = ds.drop_vars(time_bnds_var_name)
            ds = ds.drop_vars('time')
            ds = ds.drop_vars([var_name for var_name in ds.coords if time_dim_name in ds.coords[var_name].dims])

    try:
        ds = ds.expand_dims('time')
    except BaseException as e:
        warnings.warn(f'failed to add time dimension: {e}')
    if time_coverage_start and time_coverage_end:
        time_value = time_coverage_start + 0.5 * (time_coverage_end - time_coverage_start)
    else:
        time_value = time_coverage_start or time_coverage_end
    new_coord_vars = dict(time=xr.DataArray([time_value], dims=['time']))
    if time_coverage_start and time_coverage_end:
        has_time_bnds = 'time_bnds' in ds.coords or 'time_bnds' in ds
        if not has_time_bnds:
            new_coord_vars.update(time_bnds=xr.DataArray([[time_coverage_start, time_coverage_end]],
                                                         dims=['time', 'bnds']))
    ds = ds.assign_coords(**new_coord_vars)
    ds.coords['time'].attrs['long_name'] = 'time'
    ds.coords['time'].attrs['standard_name'] = 'time'
    ds.coords['time'].encoding['units'] = 'days since 1970-01-01'
    if 'time_bnds' in ds.coords:
        ds.coords['time'].attrs['bounds'] = 'time_bnds'
        ds.coords['time_bnds'].attrs['long_name'] = 'time'
        ds.coords['time_bnds'].attrs['standard_name'] = 'time'
        ds.coords['time_bnds'].encoding['units'] = 'days since 1970-01-01'

    return ds


def normalize_dims_description(dims: dict) -> dict:
    new_dims = dims.copy()
    if 'latitude' in new_dims:
        new_dims['lat'] = new_dims.pop('latitude')
    if 'longitude' in new_dims:
        new_dims['lon'] = new_dims.pop('longitude')
    if 'latitude_centers' in new_dims:
        new_dims['lat'] = new_dims.pop('latitude_centers')
        new_dims['lon'] = new_dims['lat'] * 2
    return new_dims


def normalize_variable_dims_description(var_dims: List[str]) -> Optional[List[str]]:
    if ('lat' in var_dims and 'lon' in var_dims) or \
            ('latitude' in var_dims and 'longitude' in var_dims) or \
            ('latitude_centers' in var_dims):
        # dataset cannot be normalized
        # return None
        default_dims = ['time', 'lat', 'lon', 'latitude', 'longitude', 'latitude_centers']
        if var_dims != ('time', 'lat', 'lon'):
            other_dims = []
            for dim in var_dims:
                if dim not in default_dims:
                    other_dims.append(dim)
            new_dims = ['time', 'lat', 'lon']
            for i in range(len(other_dims)):
                new_dims.insert(i + 1, other_dims[i])
            var_dims = tuple(new_dims)
        return var_dims

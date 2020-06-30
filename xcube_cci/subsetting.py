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

# The MIT License (MIT)
# Copyright (c) 2016, 2017 by the ESA CCI Toolbox development team and contributors
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

from typing import Sequence, Tuple

import xarray as xr
from shapely.geometry import box
from shapely.geometry import LineString
from shapely.geometry import Polygon

from xcube.core.store.error import DataStoreError
from xcube_cci.constants import DEFAULT_CRS

__author__ = "Janis Gailis (S[&]T Norway)" \
             "Norman Fomferra (Brockmann Consult GmbH)"


def _lat_inverted(lat: xr.DataArray) -> bool:
    """
    Determine if the latitude is inverted
    """
    if lat.values[0] > lat.values[-1]:
        return True

    return False


def _pad_extents(ds: xr.Dataset, extents: Sequence[float]):
    """
    Pad extents by half a pixel in both directions, to make sure subsetting
    slices include all pixels crossed by the bounding box. Set extremes
    to maximum valid geospatial extents.
    """
    try:
        lon_pixel = abs(ds.lon.values[1] - ds.lon.values[0])
        lon_min = extents[0] - lon_pixel / 2
        lon_max = extents[2] + lon_pixel / 2
    except IndexError:
        # 1D dimension, leave extents as they were
        lon_min = extents[0]
        lon_max = extents[2]

    try:
        lat_pixel = abs(ds.lat.values[1] - ds.lat.values[0])
        lat_min = extents[1] - lat_pixel / 2
        lat_max = extents[3] + lat_pixel / 2
    except IndexError:
        lat_min = extents[1]
        lat_max = extents[3]

    lon_min = -180 if lon_min < -180 else lon_min
    lat_min = -90 if lat_min < -90 else lat_min
    lon_max = 180 if lon_max > 180 else lon_max
    lat_max = 90 if lat_max > 90 else lat_max

    return lon_min, lat_min, lon_max, lat_max


def reset_non_spatial(ds_source: xr.Dataset, ds_target: xr.Dataset):
    """
    Find non spatial data arrays in ds_source and set the corresponding
    data variables in ds_target to original ones.

    :param ds_source: Source dataset
    :param ds_target: Target dataset
    """
    non_spatial = list()
    for var_name in ds_source.data_vars.keys():
        if 'lat' not in ds_source[var_name].dims and \
           'lon' not in ds_source[var_name].dims:
            non_spatial.append(var_name)

    retset = ds_target
    for var in non_spatial:
        retset[var] = ds_source[var]

    return retset


def subset_spatial(ds: xr.Dataset,
                   bbox: Tuple[float, float, float, float],
                   crs: str=DEFAULT_CRS
                   ) -> xr.Dataset:
    """
    Do a spatial subset of the dataset

    :param ds: Dataset to subset
    :param bbox: Spatial region to subset
    :return: Subset dataset
    """

    if crs != DEFAULT_CRS:
        raise DataStoreError(f'CRS must be {DEFAULT_CRS}')

    # Validate whether lat and lon exists.
    if not hasattr(ds, 'lon') or not hasattr(ds, 'lat'):
        raise DataStoreError('Cannot apply regional subset. No (valid) geocoding found.')

    if hasattr(ds, 'lon') and len(ds.lon.shape) != 1 \
            or hasattr(ds, 'lat') and len(ds.lat.shape) != 1:
        raise DataStoreError('Geocoding not recognised. Variables "lat" and/or "lon" have more than one dimension.')

    extents = bbox
    explicit_coords = True
    lon_min, lat_min, lon_max, lat_max = extents
    # Validate input
    try:
        polygon = box(lon_min, lat_min, lon_max, lat_max)
    except BaseException as e:
        raise e

    # Validate the bounding box
    if (not (-90 <= lat_min <= 90)) or \
            (not (-90 <= lat_max <= 90)) or \
            (not (-180 <= lon_min <= 180)) or \
            (not (-180 <= lon_max <= 180)):
        raise DataStoreError('Provided polygon extends outside of geo-spatial bounds.'
                             ' Latitudes should be from -90 to 90 and longitudes from -180 to 180 degrees.')

    # Pad extents to include crossed pixels
    lon_min, lat_min, lon_max, lat_max = _pad_extents(ds, extents)

    crosses_antimeridian = (lon_min > lon_max) if explicit_coords else _crosses_antimeridian(polygon)
    lat_inverted = _lat_inverted(ds.lat)
    if lat_inverted:
        lat_index = slice(lat_max, lat_min)
    else:
        lat_index = slice(lat_min, lat_max)

    if crosses_antimeridian:
        # Shapely messes up longitudes if the polygon crosses the antimeridian
        if not explicit_coords:
            lon_min, lon_max = lon_max, lon_min

        # Can't perform a simple selection with slice, hence we have to
        # construct an appropriate longitude indexer for selection
        lon_left_of_idl = slice(lon_min, 180)
        lon_right_of_idl = slice(-180, lon_max)
        lon_index = xr.concat((ds.lon.sel(lon=lon_right_of_idl),
                               ds.lon.sel(lon=lon_left_of_idl)), dim='lon')

        indexers = {'lon': lon_index, 'lat': lat_index}
        retset = ds.sel(**indexers)

        # Preserve the original longitude dimension, masking elements that
        # do not belong to the polygon with NaN.
        # with monitor.observing('subset'):
        return reset_non_spatial(ds, retset.reindex_like(ds.lon))

    lon_slice = slice(lon_min, lon_max)
    indexers = {'lat': lat_index, 'lon': lon_slice}
    retset = ds.sel(**indexers)

    if len(retset.lat) == 0 or len(retset.lon) == 0:
        # Empty return dataset => region out of dataset bounds
        raise ValueError("Can not select a region outside dataset boundaries.")

    # The polygon doesn't cross the anti-meridian, it is a simple box -> Use a simple slice
    return reset_non_spatial(ds, retset)


def _crosses_antimeridian(region: Polygon) -> bool:
    """
    Determine if the given region crosses the Antimeridian line, by converting
    the given Polygon from -180;180 to 0;360 and checking if the antimeridian
    line crosses it.

    This only works with Polygons without holes

    :param region: Polygon to test
    """

    # Convert region to only have positive longitudes.
    # This way we can perform a simple antimeridian check

    old_exterior = region.exterior.coords
    new_exterior = []
    for point in old_exterior:
        lon, lat = point[0], point[1]
        if -180. <= lon < 0.:
            lon += 360.
        new_exterior.append((lon, lat))
    converted_region = Polygon(new_exterior)

    # There's a problem at this point. Any polygon crossed by the zero-th
    # meridian can in principle convert to an inverted polygon that is crossed
    # by the antimeridian.

    if not converted_region.is_valid:
        # The polygon 'became' invalid upon conversion => probably the original
        # polygon is what we want

        # noinspection PyBroadException
        try:
            # First try cleaning up geometry that is invalid
            converted_region = converted_region.buffer(0)
        except BaseException:
            pass
        if not converted_region.is_valid:
            return False

    test_line = LineString([(180, -90), (180, 90)])
    if test_line.crosses(converted_region):
        # The converted polygon seems to be valid and crossed by the
        # antimeridian. At this point there's no 'perfect' way how to tell if
        # we wanted the converted polygon or the original one.

        # A simple heuristic is to check for size. The smaller one is quite
        # likely the desired one
        if converted_region.area < region.area:
            return True
        else:
            return False
    else:
        return False


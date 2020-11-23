from unittest import TestCase

import numpy as np
import pandas as pd
import xarray as xr

from xcube_cci.normalize import normalize_cci_dataset
from xcube_cci.normalize import normalize_dims_description
from xcube_cci.normalize import normalize_variable_dims_description

class TestNormalize(TestCase):

    def test_normalize_zonal_lat_lon(self):
        resolution = 10
        lat_size = 3
        lat_coords = np.arange(0, 30, resolution)
        lon_coords = [i + 5. for i in np.arange(-180.0, 180.0, resolution)]
        lon_size = len(lon_coords)
        one_more_dim_size = 2
        one_more_dim_coords = np.random.random(2)

        var_values_1_1d = xr.DataArray(np.random.random(lat_size),
                                       coords=[('latitude_centers', lat_coords)],
                                       dims=['latitude_centers'],
                                       attrs=dict(chunk_sizes=[lat_size],
                                                  dimensions=['latitude_centers']))
        var_values_1_1d.encoding = {'chunks': (lat_size)}
        var_values_1_2d = xr.DataArray(np.array([var_values_1_1d.values for _ in lon_coords]).T,
                                       coords={'lat': lat_coords, 'lon': lon_coords},
                                       dims=['lat', 'lon'],
                                       attrs=dict(chunk_sizes=[lat_size, lon_size],
                                                  dimensions=['lat', 'lon']))
        var_values_1_2d.encoding = {'chunks':  (lat_size, lon_size)}
        var_values_2_2d = xr.DataArray(np.random.random(lat_size * one_more_dim_size).
                                       reshape(lat_size, one_more_dim_size),
                                       coords={'latitude_centers': lat_coords,
                                               'one_more_dim': one_more_dim_coords},
                                       dims=['latitude_centers', 'one_more_dim'],
                                       attrs=dict(chunk_sizes=[lat_size, one_more_dim_size],
                                                  dimensions=['latitude_centers', 'one_more_dim']))
        var_values_2_2d.encoding = {'chunks': (lat_size, one_more_dim_size)}
        var_values_2_3d = xr.DataArray(np.array([var_values_2_2d.values for _ in lon_coords]).T,
                                       coords={'one_more_dim': one_more_dim_coords,
                                               'lat': lat_coords,
                                               'lon': lon_coords,},
                                       dims=['one_more_dim', 'lat', 'lon',],
                                       attrs=dict(chunk_sizes=[one_more_dim_size,
                                                               lat_size,
                                                               lon_size],
                                                  dimensions=['one_more_dim', 'lat', 'lon']))
        var_values_2_3d.encoding = {'chunks':  (one_more_dim_size, lat_size, lon_size)}

        dataset = xr.Dataset({'first': var_values_1_1d, 'second': var_values_2_2d})
        expected = xr.Dataset({'first': var_values_1_2d, 'second': var_values_2_3d})
        expected = expected.assign_coords(
            lon_bnds=xr.DataArray([[i - (resolution / 2), i + (resolution / 2)] for i in expected.lon.values],
                                  dims=['lon', 'bnds']))
        expected = expected.assign_coords(
            lat_bnds=xr.DataArray([[i - (resolution / 2), i + (resolution / 2)] for i in expected.lat.values],
                                  dims=['lat', 'bnds']))
        actual = normalize_cci_dataset(dataset)

        xr.testing.assert_equal(actual, expected)
        self.assertEqual(actual.first.chunk_sizes, expected.first.chunk_sizes)
        self.assertEqual(actual.second.chunk_sizes, expected.second.chunk_sizes)

    def test_normalize_with_missing_time_dim(self):
        ds = xr.Dataset({'first': (['lat', 'lon'], np.zeros([90, 180])),
                         'second': (['lat', 'lon'], np.zeros([90, 180]))},
                        coords={'lat': np.linspace(-89.5, 89.5, 90),
                                'lon': np.linspace(-179.5, 179.5, 180)},
                        attrs={'time_coverage_start': '20120101',
                               'time_coverage_end': '20121231'})
        norm_ds = normalize_cci_dataset(ds)
        self.assertIsNot(norm_ds, ds)
        self.assertEqual(len(norm_ds.coords), 4)
        self.assertIn('lon', norm_ds.coords)
        self.assertIn('lat', norm_ds.coords)
        self.assertIn('time', norm_ds.coords)
        self.assertIn('time_bnds', norm_ds.coords)

        self.assertEqual(norm_ds.first.shape, (1, 90, 180))
        self.assertEqual(norm_ds.second.shape, (1, 90, 180))
        self.assertEqual(norm_ds.coords['time'][0], xr.DataArray(pd.to_datetime('2012-07-01T12:00:00')))
        self.assertEqual(norm_ds.coords['time_bnds'][0][0], xr.DataArray(pd.to_datetime('2012-01-01')))
        self.assertEqual(norm_ds.coords['time_bnds'][0][1], xr.DataArray(pd.to_datetime('2012-12-31')))

    def test_normalize_with_missing_time_dim_from_filename(self):
        ds = xr.Dataset({'first': (['lat', 'lon'], np.zeros([90, 180])),
                         'second': (['lat', 'lon'], np.zeros([90, 180]))},
                        coords={'lat': np.linspace(-89.5, 89.5, 90),
                                'lon': np.linspace(-179.5, 179.5, 180)},
                        )
        ds_encoding = dict(source='20150204_etfgz_20170309_dtsrgth')
        ds.encoding.update(ds_encoding)
        norm_ds = normalize_cci_dataset(ds)
        self.assertIsNot(norm_ds, ds)
        self.assertEqual(len(norm_ds.coords), 4)
        self.assertIn('lon', norm_ds.coords)
        self.assertIn('lat', norm_ds.coords)
        self.assertIn('time', norm_ds.coords)
        self.assertIn('time_bnds', norm_ds.coords)

        self.assertEqual(norm_ds.first.shape, (1, 90, 180))
        self.assertEqual(norm_ds.second.shape, (1, 90, 180))
        self.assertEqual(norm_ds.coords['time'][0], xr.DataArray(pd.to_datetime('2016-02-21T00:00:00')))
        self.assertEqual(norm_ds.coords['time_bnds'][0][0], xr.DataArray(pd.to_datetime('2015-02-04')))
        self.assertEqual(norm_ds.coords['time_bnds'][0][1], xr.DataArray(pd.to_datetime('2017-03-09')))

    def test_normalize_dims(self):
        dims_1 = dict(lat=1, lon=2, time=4)
        self.assertEqual(dims_1, normalize_dims_description(dims_1))

        dims_2 = dict(lat=1, lon=2, time=4, fgzrh=5, dfsraxt=6)
        self.assertEqual(dims_2, normalize_dims_description(dims_2))

        dims_3 = dict(latitude=1, lon=2, time=4)
        self.assertEqual(dims_1, normalize_dims_description(dims_3))

        dims_4 = dict(lat=1, longitude=2, time=4)
        self.assertEqual(dims_1, normalize_dims_description(dims_4))

        dims_5 = dict(latitude_centers=1, time=4)
        self.assertEqual(dims_1, normalize_dims_description(dims_5))

        dims_6 = dict(latitude=1, dhft=2, time=4)
        self.assertEqual(dict(lat=1, dhft=2, time=4), normalize_dims_description(dims_6))

    def test_normalize_variable_dims_description(self):
        dims_1 = ['time', 'lat', 'lon']
        self.assertEqual(dims_1, normalize_variable_dims_description(dims_1))

        dims_2 =['lat', 'lon']
        self.assertEqual(dims_1, normalize_variable_dims_description(dims_2))

        dims_3 = ['latitude', 'longitude']
        self.assertEqual(dims_1, normalize_variable_dims_description(dims_3))

        dims_4 = ['latitude_centers']
        self.assertEqual(dims_1, normalize_variable_dims_description(dims_4))

        dims_5 = ['lat', 'lon', 'draeftgyhesj']
        self.assertEqual(('time', 'draeftgyhesj', 'lat', 'lon'), normalize_variable_dims_description(dims_5))

        dims_6 = ['latitude_centers', 'draeftgyhesj']
        self.assertEqual(('time', 'draeftgyhesj', 'lat', 'lon'), normalize_variable_dims_description(dims_6))

        dims_7 = ['lat', 'gyfdvtz', 'time']
        self.assertIsNone(normalize_variable_dims_description(dims_7))

        dims_8 = ['gyfdvtz']
        self.assertIsNone(normalize_variable_dims_description(dims_8))

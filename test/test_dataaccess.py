import datetime as dt
import os
import unittest

from unittest import skip
from unittest import skipIf

from xcube_cci.dataaccess import _get_temporal_resolution_from_id
from xcube_cci.dataaccess import CciOdpCubeOpener
from xcube_cci.dataaccess import CciOdpDatasetOpener
from xcube_cci.dataaccess import CciOdpDataStore
from xcube.core.normalize import normalize_dataset
from xcube.core.store.descriptor import DatasetDescriptor
from xcube.core.store import DataStoreError
from xcube.core.verify import assert_cube


class DataAccessTest(unittest.TestCase):

    def test_get_temporal_resolution_from_id(self):
        self.assertEqual('1D',
                         _get_temporal_resolution_from_id('esacci.OZONE.day.L3.NP.sensor.platform.MERGED.fv0002.r1'))
        self.assertEqual('5D',
                         _get_temporal_resolution_from_id('esacci.OZONE.5-days.L3.NP.sensor.platform.MERGED.fv0002.r1'))
        self.assertEqual('1M',
                         _get_temporal_resolution_from_id('esacci.OZONE.mon.L3.NP.sensor.platform.MERGED.fv0002.r1'))
        self.assertEqual('1M',
                         _get_temporal_resolution_from_id('esacci.OZONE.month.L3.NP.sensor.platform.MERGED.fv0002.r1'))
        self.assertEqual('3M',
                         _get_temporal_resolution_from_id('esacci.OZONE.3-months.L3.NP.sensor.platform.MERGED.fv0002.r1'))
        self.assertEqual('1Y',
                         _get_temporal_resolution_from_id('esacci.OZONE.yr.L3.NP.sensor.platform.MERGED.fv0002.r1'))
        self.assertEqual('1Y',
                         _get_temporal_resolution_from_id('esacci.OZONE.year.L3.NP.sensor.platform.MERGED.fv0002.r1'))
        self.assertEqual('13Y',
                         _get_temporal_resolution_from_id('esacci.OZONE.13-yrs.L3.NP.sensor.platform.MERGED.fv0002.r1'))
        self.assertIsNone(
            _get_temporal_resolution_from_id('esacci.OZONE.climatology.L3.NP.sensor.platform.MERGED.fv0002.r1'))

class CciOdpDatasetOpenerTest(unittest.TestCase):

    def setUp(self) -> None:
        self.opener = CciOdpDatasetOpener()

    @skipIf(os.environ.get('XCUBE_DISABLE_WEB_TESTS', None) == '1', 'XCUBE_DISABLE_WEB_TESTS = 1')
    def test_dataset_names(self):
        self.assertTrue(len(self.opener.dataset_names) > 275)

    @skipIf(os.environ.get('XCUBE_DISABLE_WEB_TESTS', None) == '1', 'XCUBE_DISABLE_WEB_TESTS = 1')
    def test_describe_data(self):
        descriptor = self.opener.describe_data('esacci.OZONE.mon.L3.NP.multi-sensor.multi-platform.MERGED.fv0002.r1')
        self.assertIsNotNone(descriptor)
        self.assertIsInstance(descriptor, DatasetDescriptor)
        self.assertEqual('esacci.OZONE.mon.L3.NP.multi-sensor.multi-platform.MERGED.fv0002.r1', descriptor.data_id)
        self.assertEqual('dataset', str(descriptor.type_specifier))
        self.assertEqual(['lon', 'lat', 'layers', 'air_pressure', 'time'], list(descriptor.dims.keys()))
        self.assertEqual(360, descriptor.dims['lon'])
        self.assertEqual(180, descriptor.dims['lat'])
        self.assertEqual(16, descriptor.dims['layers'])
        self.assertEqual(17, descriptor.dims['air_pressure'])
        self.assertEqual(36, descriptor.dims['time'])
        self.assertEqual(9, len(descriptor.data_vars))
        self.assertEqual('surface_pressure', descriptor.data_vars[0].name)
        self.assertEqual(3, descriptor.data_vars[0].ndim)
        self.assertEqual(('time', 'lat', 'lon'), descriptor.data_vars[0].dims)
        self.assertEqual('float32', descriptor.data_vars[0].dtype)
        self.assertIsNone(descriptor.crs)
        self.assertEqual(1.0, descriptor.spatial_res)
        self.assertEqual(('1997-01-01', '2008-12-31'), descriptor.time_range)
        self.assertEqual('1M', descriptor.time_period)

        descriptor = self.opener.describe_data('esacci.AEROSOL.day.L3.AAI.multi-sensor.multi-platform.MSAAI.1-7.r1')
        self.assertIsNotNone(descriptor)
        self.assertIsInstance(descriptor, DatasetDescriptor)
        self.assertEqual('esacci.AEROSOL.day.L3.AAI.multi-sensor.multi-platform.MSAAI.1-7.r1', descriptor.data_id)
        self.assertEqual('dataset', str(descriptor.type_specifier))
        self.assertEqual(['longitude', 'latitude', 'time'], list(descriptor.dims.keys()))
        self.assertEqual(360, descriptor.dims['longitude'])
        self.assertEqual(180, descriptor.dims['latitude'])
        self.assertEqual(12644, descriptor.dims['time'])
        self.assertEqual(3, len(descriptor.data_vars))
        self.assertEqual('absorbing_aerosol_index', descriptor.data_vars[0].name)
        self.assertEqual(3, descriptor.data_vars[2].ndim)
        self.assertEqual(('latitude', 'longitude', 'time'), descriptor.data_vars[2].dims)
        self.assertEqual('float32', descriptor.data_vars[2].dtype)
        self.assertIsNone(descriptor.crs)
        self.assertEqual(1.0, descriptor.spatial_res)
        self.assertEqual(('1978-11-01', '2015-12-31'), descriptor.time_range)
        self.assertEqual('1D', descriptor.time_period)

    def test_get_open_data_params_schema_no_data(self):
        schema = self.opener.get_open_data_params_schema().to_dict()
        self.assertIsNotNone(schema)
        self.assertTrue('variable_names' in schema['properties'])
        self.assertTrue('time_range' in schema['properties'])
        self.assertTrue('bbox' in schema['properties'])
        self.assertTrue('spatial_res' in schema['properties'])
        self.assertTrue('time_period' in schema['properties'])
        self.assertTrue('crs' in schema['properties'])
        self.assertFalse(schema['additionalProperties'])

    @skipIf(os.environ.get('XCUBE_DISABLE_WEB_TESTS', None) == '1', 'XCUBE_DISABLE_WEB_TESTS = 1')
    def test_get_open_data_params_schema(self):
        schema = self.opener.get_open_data_params_schema(
            'esacci.OZONE.mon.L3.NP.multi-sensor.multi-platform.MERGED.fv0002.r1').to_dict()
        self.assertIsNotNone(schema)
        self.assertTrue('variable_names' in schema['properties'])
        self.assertTrue('time_range' in schema['properties'])
        self.assertTrue('bbox' in schema['properties'])
        self.assertTrue('spatial_res' in schema['properties'])
        self.assertTrue('time_period' in schema['properties'])
        self.assertTrue('crs' in schema['properties'])
        self.assertFalse(schema['additionalProperties'])

        schema = self.opener.get_open_data_params_schema(
            'esacci.AEROSOL.day.L3.AAI.multi-sensor.multi-platform.MSAAI.1-7.r1').to_dict()
        self.assertIsNotNone(schema)
        self.assertTrue('variable_names' in schema['properties'])
        self.assertTrue('time_range' in schema['properties'])
        self.assertTrue('bbox' in schema['properties'])
        self.assertTrue('spatial_res' in schema['properties'])
        self.assertTrue('time_period' in schema['properties'])
        self.assertTrue('crs' in schema['properties'])
        self.assertFalse(schema['additionalProperties'])

    @skipIf(os.environ.get('XCUBE_DISABLE_WEB_TESTS', None) == '1', 'XCUBE_DISABLE_WEB_TESTS = 1')
    def test_open_data(self):
        dataset = self.opener.open_data(
            'esacci.AEROSOL.day.L3C.AER_PRODUCTS.ATSR-2.Envisat.AATSR-ENVISAT-ENS_DAILY.v2-6.r1',
            variable_names=['AOD550', 'NMEAS'],
            time_range=['2002-07-02', '2002-07-05'],
            bbox=[-10.0, 40.0, 10.0, 60.0])
        self.assertIsNotNone(dataset)
        self.assertEqual({'AOD550', 'NMEAS'}, set(dataset.data_vars))
        self.assertEqual({'latitude', 'longitude', 'time'}, set(dataset.AOD550.dims))
        self.assertEqual({20, 20, 1}, set(dataset.AOD550.chunk_sizes))

    @skipIf(os.environ.get('XCUBE_DISABLE_WEB_TESTS', None) == '1', 'XCUBE_DISABLE_WEB_TESTS = 1')
    @skip('Disabled while time series are not supported')
    def test_open_data_with_time_series_and_latitude_centers(self):
        dataset = self.opener.open_data('esacci.OZONE.mon.L3.LP.SCIAMACHY.Envisat.SCIAMACHY_ENVISAT.v0001.r1',
                                        variable_names=['approximate_altitude', 'ozone_mixing_ratio',
                                                        'sample_standard_deviation'],
                                        time_range=['2009-05-02', '2009-08-31'],
                                        bbox=[-10.0, 40.0, 10.0, 60.0]
                                        )
        self.assertIsNotNone(dataset)
        self.assertEqual({'approximate_altitude', 'ozone_mixing_ratio', 'sample_standard_deviation'},
                         set(dataset.data_vars))
        self.assertEqual({'time', 'air_pressure', 'latitude_centers'},
                         dataset.ozone_mixing_ratio.dims)
        self.assertEqual({1, 32, 18}, dataset.ozone_mixing_ratio.chunk_sizes)


class CciOdpCubeOpenerTest(unittest.TestCase):

    def setUp(self) -> None:
        self.opener = CciOdpCubeOpener()

    @skipIf(os.environ.get('XCUBE_DISABLE_WEB_TESTS', None) == '1', 'XCUBE_DISABLE_WEB_TESTS = 1')
    def test_dataset_names(self):
        self.assertTrue(len(self.opener.dataset_names) < 200)
        self.assertTrue(len(self.opener.dataset_names) > 100)

    @skipIf(os.environ.get('XCUBE_DISABLE_WEB_TESTS', None) == '1', 'XCUBE_DISABLE_WEB_TESTS = 1')
    def test_describe_dataset(self):
        with self.assertRaises(DataStoreError) as dse:
            self.opener.describe_data('esacci.OZONE.mon.L3.NP.multi-sensor.multi-platform.MERGED.fv0002.r1')
        self.assertEqual('Cannot describe metadata of data resource '
                         '"esacci.OZONE.mon.L3.NP.multi-sensor.multi-platform.MERGED.fv0002.r1", '
                         'as it cannot be accessed by data accessor "dataset[cube]:zarr:cciodp".', f'{dse.exception}')
        descriptor = self.opener.describe_data('esacci.AEROSOL.day.L3.AAI.multi-sensor.multi-platform.MSAAI.1-7.r1')
        self.assertIsNotNone(descriptor)
        self.assertIsInstance(descriptor, DatasetDescriptor)
        self.assertEqual('esacci.AEROSOL.day.L3.AAI.multi-sensor.multi-platform.MSAAI.1-7.r1', descriptor.data_id)
        self.assertEqual('dataset[cube]', str(descriptor.type_specifier))
        self.assertEqual(['lat', 'lon', 'time'], list(descriptor.dims.keys()))
        self.assertEqual(360, descriptor.dims['lon'])
        self.assertEqual(180, descriptor.dims['lat'])
        self.assertEqual(12644, descriptor.dims['time'])
        self.assertEqual(3, len(descriptor.data_vars))
        self.assertEqual('absorbing_aerosol_index', descriptor.data_vars[0].name)
        self.assertEqual(3, descriptor.data_vars[0].ndim)
        self.assertEqual(('time', 'lat', 'lon'), descriptor.data_vars[0].dims)
        self.assertEqual('float32', descriptor.data_vars[0].dtype)
        self.assertIsNone(descriptor.crs)
        self.assertEqual(1.0, descriptor.spatial_res)
        self.assertEqual(('1978-11-01', '2015-12-31'), descriptor.time_range)
        self.assertEqual('1D', descriptor.time_period)

    def test_get_open_data_params_schema_no_data(self):
        schema = self.opener.get_open_data_params_schema().to_dict()
        self.assertIsNotNone(schema)
        self.assertTrue('variable_names' in schema['properties'])
        self.assertTrue('time_range' in schema['properties'])
        self.assertTrue('bbox' in schema['properties'])
        self.assertTrue('spatial_res' in schema['properties'])
        self.assertTrue('time_period' in schema['properties'])
        self.assertTrue('crs' in schema['properties'])
        self.assertFalse(schema['additionalProperties'])

    @skipIf(os.environ.get('XCUBE_DISABLE_WEB_TESTS', None) == '1', 'XCUBE_DISABLE_WEB_TESTS = 1')
    def test_get_open_data_params_schema(self):
        with self.assertRaises(DataStoreError) as dse:
            self.opener.get_open_data_params_schema(
                'esacci.OZONE.mon.L3.NP.multi-sensor.multi-platform.MERGED.fv0002.r1').to_dict()
        self.assertEqual('Cannot describe metadata of data resource '
                         '"esacci.OZONE.mon.L3.NP.multi-sensor.multi-platform.MERGED.fv0002.r1", '
                         'as it cannot be accessed by data accessor "dataset[cube]:zarr:cciodp".', f'{dse.exception}')

        schema = self.opener.get_open_data_params_schema(
            'esacci.AEROSOL.day.L3.AAI.multi-sensor.multi-platform.MSAAI.1-7.r1').to_dict()
        self.assertIsNotNone(schema)
        self.assertTrue('variable_names' in schema['properties'])
        self.assertTrue('time_range' in schema['properties'])
        self.assertTrue('bbox' in schema['properties'])
        self.assertTrue('spatial_res' in schema['properties'])
        self.assertTrue('time_period' in schema['properties'])
        self.assertTrue('crs' in schema['properties'])
        self.assertFalse(schema['additionalProperties'])

    @skipIf(os.environ.get('XCUBE_DISABLE_WEB_TESTS', None) == '1', 'XCUBE_DISABLE_WEB_TESTS = 1')
    def test_open_data(self):
        with self.assertRaises(DataStoreError) as dse:
            self.opener.open_data('esacci.AEROSOL.day.L3C.AER_PRODUCTS.AATSR.Envisat.ATSR2-ENVISAT-ENS_DAILY.v2-6.r1',
                                  variable_names=['AOD550', 'NMEAS'],
                                  time_range=['2009-07-02', '2009-07-05'],
                                  bbox=[-10.0, 40.0, 10.0, 60.0])
        self.assertEqual('Cannot describe metadata of data resource '
                         '"esacci.AEROSOL.day.L3C.AER_PRODUCTS.AATSR.Envisat.ATSR2-ENVISAT-ENS_DAILY.v2-6.r1", '
                         'as it cannot be accessed by data accessor "dataset[cube]:zarr:cciodp".', f'{dse.exception}')

    @skipIf(os.environ.get('XCUBE_DISABLE_WEB_TESTS', None) == '1', 'XCUBE_DISABLE_WEB_TESTS = 1')
    @skip('Disabled while time series are not supported')
    def test_open_data_with_time_series_and_latitude_centers(self):
        dataset = self.opener.open_data('esacci.OZONE.mon.L3.LP.SCIAMACHY.Envisat.SCIAMACHY_ENVISAT.v0001.r1',
                                        variable_names=['standard_error_of_the_mean', 'ozone_mixing_ratio',
                                                        'sample_standard_deviation'],
                                        time_range=['2009-05-02', '2009-08-31'],
                                        bbox=[-10.0, 40.0, 10.0, 60.0]
                                        )
        self.assertIsNotNone(dataset)
        self.assertEqual({'standard_error_of_the_mean', 'ozone_mixing_ratio',
                          'sample_standard_deviation'}, set(dataset.data_vars))
        self.assertEqual({'time', 'air_pressure', 'lat', 'lon'}, dataset.ozone_mixing_ratio.dims)
        self.assertEqual({1, 32, 18, 36}, set(dataset.ozone_mixing_ratio.chunk_sizes))


class CciOdpDataStoreTest(unittest.TestCase):

    def setUp(self) -> None:
        self.store = CciOdpDataStore()

    def test_get_data_store_params_schema(self):
        cci_store_params_schema = CciOdpDataStore.get_data_store_params_schema().to_dict()
        self.assertIsNotNone(cci_store_params_schema)
        self.assertTrue('opensearch_url' in cci_store_params_schema['properties'])
        self.assertTrue('opensearch_description_url' in cci_store_params_schema['properties'])

    def test_get_type_specifiers(self):
        # self.assertEqual(('dataset:zarr:cciodp', 'dataset[cube]:zarr:cciodp'), CciOdpDataStore.get_type_specifiers())
        self.assertEqual(('dataset', 'dataset[cube]'), CciOdpDataStore.get_type_specifiers())

    def test_get_type_specifiers_for_data(self):
        type_specifiers_for_data = self.store.get_type_specifiers_for_data(
            'esacci.AEROSOL.day.L3.AAI.multi-sensor.multi-platform.MSAAI.1-7.r1')
        self.assertEqual(('dataset', 'dataset[cube]'), type_specifiers_for_data)

        type_specifiers_for_data = self.store.get_type_specifiers_for_data(
            'esacci.OZONE.mon.L3.NP.multi-sensor.multi-platform.MERGED.fv0002.r1')
        self.assertEqual(('dataset', ), type_specifiers_for_data)

        with self.assertRaises(DataStoreError) as dse:
            self.store.get_type_specifiers_for_data('nonsense')
        self.assertEqual('Data resource "nonsense" does not exist in store', f'{dse.exception}')

    def test_get_search_params(self):
        search_schema = self.store.get_search_params_schema().to_dict()
        self.assertIsNotNone(search_schema)
        self.assertTrue('start_date' in search_schema['properties'])
        self.assertTrue('end_date' in search_schema['properties'])
        self.assertTrue('bbox' in search_schema['properties'])
        self.assertTrue('ecv' in search_schema['properties'])
        self.assertTrue('frequency' in search_schema['properties'])
        self.assertTrue('institute' in search_schema['properties'])
        self.assertTrue('processing_level' in search_schema['properties'])
        self.assertTrue('product_string' in search_schema['properties'])
        self.assertTrue('product_version' in search_schema['properties'])
        self.assertTrue('data_type' in search_schema['properties'])
        self.assertTrue('sensor' in search_schema['properties'])
        self.assertTrue('platform' in search_schema['properties'])

    @skipIf(os.environ.get('XCUBE_DISABLE_WEB_TESTS', None) == '1', 'XCUBE_DISABLE_WEB_TESTS = 1')
    def test_search(self):
        cube_search_result = list(self.store.search_data('dataset[cube]', ecv='FIRE', product_string='MODIS_TERRA'))
        self.assertIsNotNone(cube_search_result)
        self.assertEqual(1, len(cube_search_result))
        self.assertIsInstance(cube_search_result[0], DatasetDescriptor)
        self.assertEqual(5, len(cube_search_result[0].dims))
        self.assertEqual(6, len(cube_search_result[0].data_vars))
        self.assertEqual('esacci.FIRE.mon.L4.BA.MODIS.Terra.MODIS_TERRA.v5-1.grid', cube_search_result[0].data_id)
        self.assertEqual('1M', cube_search_result[0].time_period)
        self.assertEqual(0.25, cube_search_result[0].spatial_res)
        self.assertEqual('dataset[cube]', cube_search_result[0].type_specifier)
        self.assertEqual(('2001-01-01', '2019-12-31'), cube_search_result[0].time_range)

        dataset_search_result = list(self.store.search_data('dataset', ecv='FIRE', product_string='MODIS_TERRA'))
        self.assertIsNotNone(dataset_search_result)
        self.assertEqual(2, len(dataset_search_result))
        self.assertEqual('dataset', dataset_search_result[0].type_specifier)
        self.assertEqual('dataset', dataset_search_result[1].type_specifier)

        geodataframe_search_result = list(self.store.search_data('geodataframe'))
        self.assertIsNotNone(geodataframe_search_result)
        self.assertEqual(0, len(geodataframe_search_result))

    @skipIf(os.environ.get('XCUBE_DISABLE_WEB_TESTS', None) == '1', 'XCUBE_DISABLE_WEB_TESTS = 1')
    def test_has_data(self):
        self.assertTrue(self.store.has_data('esacci.FIRE.mon.L4.BA.MODIS.Terra.MODIS_TERRA.v5-1.grid'))
        self.assertFalse(self.store.has_data('esacci.WIND.mon.L4.BA.MODIS.Terra.MODIS_TERRA.v5-1.grid'))
        self.assertTrue(self.store.has_data('esacci.AEROSOL.day.L3.AAI.multi-sensor.multi-platform.MSAAI.1-7.r1'))
        self.assertTrue(self.store.has_data('esacci.OZONE.mon.L3.NP.multi-sensor.multi-platform.MERGED.fv0002.r1'))

        self.assertTrue(self.store.has_data('esacci.FIRE.mon.L4.BA.MODIS.Terra.MODIS_TERRA.v5-1.grid', 'dataset'))
        self.assertFalse(self.store.has_data('esacci.WIND.mon.L4.BA.MODIS.Terra.MODIS_TERRA.v5-1.grid', 'dataset'))
        self.assertTrue(self.store.has_data('esacci.AEROSOL.day.L3.AAI.multi-sensor.multi-platform.MSAAI.1-7.r1',
                         'dataset'))
        self.assertTrue(self.store.has_data('esacci.OZONE.mon.L3.NP.multi-sensor.multi-platform.MERGED.fv0002.r1',
                         'dataset'))

        self.assertTrue(self.store.has_data('esacci.FIRE.mon.L4.BA.MODIS.Terra.MODIS_TERRA.v5-1.grid', 'dataset[cube]'))
        self.assertFalse(self.store.has_data('esacci.WIND.mon.L4.BA.MODIS.Terra.MODIS_TERRA.v5-1.grid',
                         'dataset[cube]'))
        self.assertTrue(self.store.has_data('esacci.AEROSOL.day.L3.AAI.multi-sensor.multi-platform.MSAAI.1-7.r1',
                         'dataset[cube]'))
        self.assertFalse(self.store.has_data('esacci.OZONE.mon.L3.NP.multi-sensor.multi-platform.MERGED.fv0002.r1',
                         'dataset[cube]'))

    def test_describe_data(self):
        descriptor = self.store.describe_data('esacci.OZONE.mon.L3.NP.multi-sensor.multi-platform.MERGED.fv0002.r1')
        self.assertIsNotNone(descriptor)
        self.assertIsInstance(descriptor, DatasetDescriptor)
        self.assertEqual('esacci.OZONE.mon.L3.NP.multi-sensor.multi-platform.MERGED.fv0002.r1', descriptor.data_id)
        self.assertEqual('dataset', str(descriptor.type_specifier))
        self.assertEqual(['lon', 'lat', 'layers', 'air_pressure', 'time'], list(descriptor.dims.keys()))
        self.assertEqual(360, descriptor.dims['lon'])
        self.assertEqual(180, descriptor.dims['lat'])
        self.assertEqual(16, descriptor.dims['layers'])
        self.assertEqual(17, descriptor.dims['air_pressure'])
        self.assertEqual(36, descriptor.dims['time'])
        self.assertEqual(9, len(descriptor.data_vars))
        self.assertEqual('surface_pressure', descriptor.data_vars[0].name)
        self.assertEqual(3, descriptor.data_vars[0].ndim)
        self.assertEqual(('time', 'lat', 'lon'), descriptor.data_vars[0].dims)
        self.assertEqual('float32', descriptor.data_vars[0].dtype)
        self.assertIsNone(descriptor.crs)
        self.assertEqual(1.0, descriptor.spatial_res)
        self.assertEqual(('1997-01-01', '2008-12-31'), descriptor.time_range)
        self.assertEqual('1M', descriptor.time_period)

        descriptor = self.store.describe_data('esacci.AEROSOL.day.L3.AAI.multi-sensor.multi-platform.MSAAI.1-7.r1')
        self.assertIsNotNone(descriptor)
        self.assertIsInstance(descriptor, DatasetDescriptor)
        self.assertEqual('esacci.AEROSOL.day.L3.AAI.multi-sensor.multi-platform.MSAAI.1-7.r1', descriptor.data_id)
        self.assertEqual('dataset', str(descriptor.type_specifier))
        self.assertEqual(['longitude', 'latitude', 'time'], list(descriptor.dims.keys()))
        self.assertEqual(360, descriptor.dims['longitude'])
        self.assertEqual(180, descriptor.dims['latitude'])
        self.assertEqual(12644, descriptor.dims['time'])
        self.assertEqual(3, len(descriptor.data_vars))
        self.assertEqual('absorbing_aerosol_index', descriptor.data_vars[0].name)
        self.assertEqual(3, descriptor.data_vars[2].ndim)
        self.assertEqual(('latitude', 'longitude', 'time'), descriptor.data_vars[2].dims)
        self.assertEqual('float32', descriptor.data_vars[2].dtype)
        self.assertIsNone(descriptor.crs)
        self.assertEqual(1.0, descriptor.spatial_res)
        self.assertEqual(('1978-11-01', '2015-12-31'), descriptor.time_range)
        self.assertEqual('1D', descriptor.time_period)

        with self.assertRaises(DataStoreError) as dse:
            self.store.describe_data('esacci.OZONE.mon.L3.NP.multi-sensor.multi-platform.MERGED.fv0002.r1',
                                     type_specifier='dataset[cube]')
        self.assertEqual('Cannot describe metadata of data resource '
                         '"esacci.OZONE.mon.L3.NP.multi-sensor.multi-platform.MERGED.fv0002.r1", '
                         'as it cannot be accessed by data accessor "dataset[cube]:zarr:cciodp".', f'{dse.exception}')

        descriptor = self.store.describe_data('esacci.AEROSOL.day.L3.AAI.multi-sensor.multi-platform.MSAAI.1-7.r1',
                                              type_specifier='dataset[cube]')
        self.assertIsNotNone(descriptor)
        self.assertIsInstance(descriptor, DatasetDescriptor)
        self.assertEqual('esacci.AEROSOL.day.L3.AAI.multi-sensor.multi-platform.MSAAI.1-7.r1', descriptor.data_id)
        self.assertEqual('dataset[cube]', str(descriptor.type_specifier))
        self.assertEqual(['lat', 'lon', 'time'], list(descriptor.dims.keys()))
        self.assertEqual(360, descriptor.dims['lon'])
        self.assertEqual(180, descriptor.dims['lat'])
        self.assertEqual(12644, descriptor.dims['time'])
        self.assertEqual(3, len(descriptor.data_vars))
        self.assertEqual('absorbing_aerosol_index', descriptor.data_vars[0].name)
        self.assertEqual(3, descriptor.data_vars[0].ndim)
        self.assertEqual(('time', 'lat', 'lon'), descriptor.data_vars[0].dims)
        self.assertEqual('float32', descriptor.data_vars[0].dtype)
        self.assertIsNone(descriptor.crs)
        self.assertEqual(1.0, descriptor.spatial_res)
        self.assertEqual(('1978-11-01', '2015-12-31'), descriptor.time_range)
        self.assertEqual('1D', descriptor.time_period)

    @skipIf(os.environ.get('XCUBE_DISABLE_WEB_TESTS', None) == '1', 'XCUBE_DISABLE_WEB_TESTS = 1')
    def test_get_data_ids(self):
        dataset_ids_iter = self.store.get_data_ids()
        self.assertIsNotNone(dataset_ids_iter)
        dataset_ids = list(dataset_ids_iter)
        self.assertTrue(len(dataset_ids) > 200)
        self.assertIsNotNone(dataset_ids[0][1])

        dataset_ids_iter = self.store.get_data_ids(type_specifier='dataset[cube]', include_titles=False)
        self.assertIsNotNone(dataset_ids_iter)
        dataset_ids = list(dataset_ids_iter)
        self.assertTrue(len(dataset_ids) < 200)
        self.assertTrue(len(dataset_ids) > 100)
        self.assertIsNone(dataset_ids[0][1])

    def test_create_human_readable_title_from_id(self):
        self.assertEqual('OZONE CCI: Monthly multi-sensor L3 MERGED NP, vfv0002',
                         self.store._create_human_readable_title_from_data_id(
                             'esacci.OZONE.mon.L3.NP.multi-sensor.multi-platform.MERGED.fv0002.r1'))
        self.assertEqual('LC CCI: 13 year ASAR L4 Map WB, v4.0',
                         self.store._create_human_readable_title_from_data_id(
                             'esacci.LC.13-yrs.L4.WB.ASAR.Envisat.Map.4-0.r1'))

    def test_get_open_data_params_schema_no_data(self):
        schema = self.store.get_open_data_params_schema().to_dict()
        self.assertIsNotNone(schema)
        self.assertTrue('variable_names' in schema['properties'])
        self.assertTrue('time_range' in schema['properties'])
        self.assertTrue('bbox' in schema['properties'])
        self.assertTrue('spatial_res' in schema['properties'])
        self.assertTrue('time_period' in schema['properties'])
        self.assertTrue('crs' in schema['properties'])
        self.assertFalse(schema['additionalProperties'])

    @skipIf(os.environ.get('XCUBE_DISABLE_WEB_TESTS', None) == '1', 'XCUBE_DISABLE_WEB_TESTS = 1')
    def test_get_open_data_params_schema(self):
        schema = self.store.get_open_data_params_schema(
            'esacci.OZONE.mon.L3.NP.multi-sensor.multi-platform.MERGED.fv0002.r1').to_dict()
        self.assertIsNotNone(schema)
        self.assertTrue('variable_names' in schema['properties'])
        self.assertTrue('time_range' in schema['properties'])
        self.assertTrue('bbox' in schema['properties'])
        self.assertTrue('spatial_res' in schema['properties'])
        self.assertTrue('time_period' in schema['properties'])
        self.assertTrue('crs' in schema['properties'])
        self.assertFalse(schema['additionalProperties'])

        schema = self.store.get_open_data_params_schema(
            'esacci.AEROSOL.day.L3.AAI.multi-sensor.multi-platform.MSAAI.1-7.r1').to_dict()
        self.assertIsNotNone(schema)
        self.assertTrue('variable_names' in schema['properties'])
        self.assertTrue('time_range' in schema['properties'])
        self.assertTrue('bbox' in schema['properties'])
        self.assertTrue('spatial_res' in schema['properties'])
        self.assertTrue('time_period' in schema['properties'])
        self.assertTrue('crs' in schema['properties'])
        self.assertFalse(schema['additionalProperties'])

        with self.assertRaises(DataStoreError) as dse:
            self.store.get_open_data_params_schema(
                'esacci.OZONE.mon.L3.NP.multi-sensor.multi-platform.MERGED.fv0002.r1',
                'dataset[cube]:zarr:cciodp').to_dict()
        self.assertEqual('Cannot describe metadata of data resource '
                         '"esacci.OZONE.mon.L3.NP.multi-sensor.multi-platform.MERGED.fv0002.r1", '
                         'as it cannot be accessed by data accessor "dataset[cube]:zarr:cciodp".', f'{dse.exception}')

        schema = self.store.get_open_data_params_schema(
            'esacci.AEROSOL.day.L3.AAI.multi-sensor.multi-platform.MSAAI.1-7.r1', 'dataset[cube]:zarr:cciodp').to_dict()
        self.assertIsNotNone(schema)
        self.assertTrue('variable_names' in schema['properties'])
        self.assertTrue('time_range' in schema['properties'])
        self.assertTrue('bbox' in schema['properties'])
        self.assertTrue('spatial_res' in schema['properties'])
        self.assertTrue('time_period' in schema['properties'])
        self.assertTrue('crs' in schema['properties'])
        self.assertFalse(schema['additionalProperties'])

    def test_get_data_opener_ids(self):
        self.assertEqual(('dataset:zarr:cciodp', 'dataset[cube]:zarr:cciodp'),
                         self.store.get_data_opener_ids())
        self.assertEqual(('dataset:zarr:cciodp', ),
                         self.store.get_data_opener_ids(
                             'esacci.OZONE.mon.L3.NP.multi-sensor.multi-platform.MERGED.fv0002.r1'))
        self.assertEqual(('dataset:zarr:cciodp', ),
                         self.store.get_data_opener_ids(
                             'esacci.OZONE.mon.L3.NP.multi-sensor.multi-platform.MERGED.fv0002.r1', 'dataset'))

        with self.assertRaises(DataStoreError) as dse:
            self.store.get_data_opener_ids(
                'esacci.OZONE.mon.L3.NP.multi-sensor.multi-platform.MERGED.fv0002.r1', 'dataset[cube]')
        self.assertEqual('Data Resource "esacci.OZONE.mon.L3.NP.multi-sensor.multi-platform.MERGED.fv0002.r1" '
                         'is not available as specified type "dataset[cube]".', f'{dse.exception}')

        self.assertEqual(('dataset:zarr:cciodp', 'dataset[cube]:zarr:cciodp'),
                         self.store.get_data_opener_ids(
                             'esacci.AEROSOL.day.L3.AAI.multi-sensor.multi-platform.MSAAI.1-7.r1'))
        self.assertEqual(('dataset:zarr:cciodp', 'dataset[cube]:zarr:cciodp'),
                         self.store.get_data_opener_ids(
                             'esacci.AEROSOL.day.L3.AAI.multi-sensor.multi-platform.MSAAI.1-7.r1', 'dataset'))
        self.assertEqual(('dataset[cube]:zarr:cciodp', ),
                         self.store.get_data_opener_ids(
                             'esacci.AEROSOL.day.L3.AAI.multi-sensor.multi-platform.MSAAI.1-7.r1', 'dataset[cube]'))

        with self.assertRaises(DataStoreError) as dse:
            self.store.get_data_opener_ids('nonsense', 'dataset[cube]')
        self.assertEqual('Data Resource "nonsense" is not available.', f'{dse.exception}')

    @skipIf(os.environ.get('XCUBE_DISABLE_WEB_TESTS', None) == '1', 'XCUBE_DISABLE_WEB_TESTS = 1')
    def test_open_data(self):
        dataset = self.store.open_data(
            'esacci.AEROSOL.day.L3C.AER_PRODUCTS.ATSR-2.Envisat.AATSR-ENVISAT-ENS_DAILY.v2-6.r1',
            'dataset:zarr:cciodp',
            variable_names=['AOD550', 'NMEAS'],
            time_range=['2009-07-02', '2009-07-05'],
            bbox=[-10.0, 40.0, 10.0, 60.0])
        self.assertEqual({'AOD550', 'NMEAS'}, set(dataset.data_vars))
        self.assertEqual({'latitude', 'longitude', 'time'}, set(dataset.AOD550.dims))
        self.assertEqual({20, 20, 1}, set(dataset.AOD550.chunk_sizes))

        with self.assertRaises(DataStoreError) as dse:
            self.store.open_data('esacci.AEROSOL.day.L3C.AER_PRODUCTS.AATSR.Envisat.ATSR2-ENVISAT-ENS_DAILY.v2-6.r1',
                                 'dataset[cube]:zarr:cciodp',
                                  variable_names=['AOD550', 'NMEAS'],
                                  time_range=['2009-07-02', '2009-07-05'],
                                  bbox=[-10.0, 40.0, 10.0, 60.0])
        self.assertEqual('Cannot describe metadata of data resource '
                         '"esacci.AEROSOL.day.L3C.AER_PRODUCTS.AATSR.Envisat.ATSR2-ENVISAT-ENS_DAILY.v2-6.r1", '
                         'as it cannot be accessed by data accessor "dataset[cube]:zarr:cciodp".', f'{dse.exception}')


    @skipIf(os.environ.get('XCUBE_DISABLE_WEB_TESTS', None) == '1', 'XCUBE_DISABLE_WEB_TESTS = 1')
    @skip('Disabled while time series are not supported')
    def test_open_data_with_time_series_and_latitude_centers(self):
        dataset = self.store.open_data('esacci.OZONE.mon.L3.LP.SCIAMACHY.Envisat.SCIAMACHY_ENVISAT.v0001.r1',
                                       'dataset:zarr:cciodp',
                                        variable_names=['approximate_altitude', 'ozone_mixing_ratio',
                                                        'sample_standard_deviation'],
                                        time_range=['2009-05-02', '2009-08-31'],
                                        bbox=[-10.0, 40.0, 10.0, 60.0]
                                        )
        self.assertIsNotNone(dataset)
        self.assertEqual({'approximate_altitude', 'ozone_mixing_ratio', 'sample_standard_deviation'},
                         set(dataset.data_vars))
        self.assertEqual({'time', 'air_pressure', 'latitude_centers'},
                         set(dataset.ozone_mixing_ratio.dims))
        self.assertEqual({1, 32, 18}, set(dataset.ozone_mixing_ratio.chunk_sizes))
        dataset = self.store.open_data('esacci.OZONE.mon.L3.LP.SCIAMACHY.Envisat.SCIAMACHY_ENVISAT.v0001.r1',
                                       'dataset[cube]:zarr:cciodp',
                                        variable_names=['standard_error_of_the_mean', 'ozone_mixing_ratio',
                                                        'sample_standard_deviation'],
                                        time_range=['2009-05-02', '2009-08-31'],
                                        bbox=[-10.0, 40.0, 10.0, 60.0]
                                        )
        self.assertIsNotNone(dataset)
        self.assertEqual({'standard_error_of_the_mean', 'ozone_mixing_ratio', 'sample_standard_deviation'},
                         set(dataset.data_vars))
        self.assertEqual({'time', 'air_pressure', 'lat', 'lon'},
                         set(dataset.ozone_mixing_ratio.dims))
        self.assertEqual({1, 32, 18, 36}, set(dataset.ozone_mixing_ratio.chunk_sizes))


class CciDataNormalizationTest(unittest.TestCase):

    @skip('Execute to test whether all data sets can be normalized')
    def test_normalization(self):
        store = CciOdpDataStore()
        all_data = store.search_data()
        datasets_without_variables = []
        datasets_with_unsupported_frequencies = []
        datasets_that_could_not_be_opened = {}
        good_datasets = []
        for data in all_data:
            if 'satellite-orbit-frequency' in data.data_id or 'climatology' in data.data_id:
                print(f'Cannot read data due to unsupported frequency')
                datasets_with_unsupported_frequencies.append(data.data_id)
                continue
            if not data.data_vars or len(data.data_vars) < 1:
                print(f'Dataset {data.data_id} has not enough variables to open. Will skip.')
                datasets_without_variables.append(data.data_id)
                continue
            print(f'Attempting to open {data.data_id} ...')
            variable_names = []
            for i in range(min(3, len(data.data_vars))):
                variable_names.append(data.data_vars[i].name)
            start_time = dt.datetime.strptime(data.time_range[0], '%Y-%m-%dT%H:%M:%S').timestamp()
            end_time = dt.datetime.strptime(data.time_range[1], '%Y-%m-%dT%H:%M:%S').timestamp()
            center_time = start_time + (end_time - start_time)
            delta = dt.timedelta(days=30)
            range_start = (dt.datetime.fromtimestamp(center_time) - delta).strftime('%Y-%m-%d')
            range_end = (dt.datetime.fromtimestamp(center_time) + delta).strftime('%Y-%m-%d')
            dataset = store.open_data(data_id=data.data_id,
                                           variable_names=variable_names,
                                           time_range=[range_start, range_end]
                                           )
            print(f'Attempting to normalize {data.data_id} ...')
            cube = normalize_dataset(dataset)
            try:
                print(f'Asserting {data.data_id} ...')
                assert_cube(cube)
                good_datasets.append(data.data_id)
            except ValueError as e:
                print(e)
                datasets_that_could_not_be_opened[data.data_id] = e
                continue
        print(f'Datasets with unsupported frequencies (#{len(datasets_with_unsupported_frequencies)}): '
              f'{datasets_with_unsupported_frequencies}')
        print(f'Datasets without variables (#{len(datasets_without_variables)}): '
              f'{datasets_without_variables}')
        print(f'Datasets that could not be opened (#{len(datasets_that_could_not_be_opened)}): '
              f'{datasets_that_could_not_be_opened}')
        print(f'Datasets that were verified (#{len(good_datasets)}): {good_datasets}')

import os
import unittest

from unittest import skipIf

from xcube_cci.dataaccess import ZarrCciOdpDatasetAccessor

class DataAccessorTest(unittest.TestCase):

    @skipIf(os.environ.get('XCUBE_DISABLE_WEB_TESTS', None) == '1', 'XCUBE_DISABLE_WEB_TESTS = 1')
    def test_describe_dataset(self):
        accessor = ZarrCciOdpDatasetAccessor()
        descriptor = accessor.describe_dataset('esacci.OZONE.month.L3.NP.multi-sensor.multi-platform.MERGED.fv0002.r1')
        self.assertIsNotNone(descriptor)
        self.assertEqual('esacci.OZONE.month.L3.NP.multi-sensor.multi-platform.MERGED.fv0002.r1', descriptor.data_id)
        self.assertEqual(['lon', 'lat', 'layers', 'air_pressure', 'time'], list(descriptor.dims.keys()))
        self.assertEqual(360, descriptor.dims['lon'])
        self.assertEqual(180, descriptor.dims['lat'])
        self.assertEqual(16, descriptor.dims['layers'])
        self.assertEqual(17, descriptor.dims['air_pressure'])
        self.assertEqual(1, descriptor.dims['time'])
        self.assertEqual(9, len(descriptor.data_vars))
        self.assertEqual('surface_pressure', descriptor.data_vars[0].name)
        self.assertEqual(3, descriptor.data_vars[0].ndim)
        self.assertEqual(('time', 'lat', 'lon'), descriptor.data_vars[0].dims)
        self.assertEqual('float32', descriptor.data_vars[0].dtype)
        self.assertIsNone(descriptor.crs)
        self.assertEqual((1.0, 1.0), descriptor.spatial_res)
        self.assertEqual(('2000-02-01T00:00:00', '2014-12-31T23:59:59'), descriptor.time_range)
        self.assertEqual('1M', descriptor.time_period)

    @skipIf(os.environ.get('XCUBE_DISABLE_WEB_TESTS', None) == '1', 'XCUBE_DISABLE_WEB_TESTS = 1')
    def test_get_open_dataset_params_schema(self):
        accessor = ZarrCciOdpDatasetAccessor()
        schema = accessor.get_open_dataset_params_schema(
            'esacci.OZONE.month.L3.NP.multi-sensor.multi-platform.MERGED.fv0002.r1').to_dict()
        self.assertIsNotNone(schema)
        self.assertTrue('var_names' in schema['properties'])
        self.assertTrue('chunk_sizes' in schema['properties'])
        self.assertTrue('time_range' in schema['properties'])
        self.assertTrue('bbox' in schema['properties'])
        self.assertTrue('geometry_wkt' in schema['properties'])
        self.assertTrue('spatial_res' in schema['properties'])
        self.assertTrue('spatial_res_unit' in schema['properties'])
        self.assertTrue('crs' in schema['properties'])
        self.assertTrue('time_period' in schema['properties'])

    @skipIf(os.environ.get('XCUBE_DISABLE_WEB_TESTS', None) == '1', 'XCUBE_DISABLE_WEB_TESTS = 1')
    def test_open_dataset(self):
        accessor = ZarrCciOdpDatasetAccessor()
        dataset = accessor.open_dataset('esacci.OZONE.month.L3.NP.multi-sensor.multi-platform.MERGED.fv0002.r1',
                                        var_names=['surface_pressure', 'O3_du', 'O3e_du'],
                                        time_range=['2009-05-02', '2009-08-31'])
        self.assertIsNotNone(dataset)
        self.assertTrue('surface_pressure' in dataset.variables)
        self.assertTrue('O3_du' in dataset.variables)
        self.assertTrue('O3e_du' in dataset.variables)

    @skipIf(os.environ.get('XCUBE_DISABLE_WEB_TESTS', None) == '1', 'XCUBE_DISABLE_WEB_TESTS = 1')
    def test_iter_dataset_ids(self):
        accessor = ZarrCciOdpDatasetAccessor()
        dataset_ids_iter = accessor.iter_dataset_ids()
        self.assertIsNotNone(dataset_ids_iter)
        dataset_ids = list(dataset_ids_iter)
        self.assertEqual(175, len(dataset_ids))
        self.assertTrue('esacci.AEROSOL.day.L3C.AER_PRODUCTS.ATSR-2.ERS-2.ORAC.03-02.r1' in dataset_ids)
        self.assertTrue('esacci.OC.day.L3S.K_490.multi-sensor.multi-platform.MERGED.3-1.sinusoidal' in dataset_ids)

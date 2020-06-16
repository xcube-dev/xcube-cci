import os
import unittest

from unittest import skipIf

from xcube_cci.dataaccess_v4 import CciOdpDataStore

class DataAccessorTest(unittest.TestCase):

    def setUp(self) -> None:
        self.store = CciOdpDataStore()

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
    def test_get_open_data_params_schema(self):
        schema = self.store.get_open_data_params_schema(
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
    def test_search(self):
        search_result = list(self.store.search_data(ecv='FIRE'))
        self.assertIsNotNone(search_result)
        self.assertEqual(1, len(search_result))
        self.assertEqual(5, len(search_result[0].dims))
        self.assertEqual(6, len(search_result[0].data_vars))
        self.assertEqual('esacci.FIRE.mon.L4.BA.MODIS.Terra.MODIS_TERRA.v5-1.r1', search_result[0].data_id)
        self.assertEqual('31D', search_result[0].time_period)
        self.assertEqual((0.25, 0.25), search_result[0].spatial_res)
        self.assertEqual('dataset', search_result[0].type_id)
        self.assertEqual(('2000-02-01T00:00:00', '2014-12-31T23:59:59'), search_result[0].time_range)

    @skipIf(os.environ.get('XCUBE_DISABLE_WEB_TESTS', None) == '1', 'XCUBE_DISABLE_WEB_TESTS = 1')
    def test_has_data(self):
        self.assertTrue(self.store.has_data('esacci.FIRE.mon.L4.BA.MODIS.Terra.MODIS_TERRA.v5-1.r1'))
        self.assertFalse(self.store.has_data('esacci.WIND.mon.L4.BA.MODIS.Terra.MODIS_TERRA.v5-1.r1'))

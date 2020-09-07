import numpy
import os
import pandas as pd
import unittest

from unittest import skipIf

from xcube_cci.cciodp import CciOdp
from xcube_cci.chunkstore import CciChunkStore


class CciChunkStoreTest(unittest.TestCase):

    @skipIf(os.environ.get('XCUBE_DISABLE_WEB_TESTS', None) == '1', 'XCUBE_DISABLE_WEB_TESTS = 1')
    def setUp(self) -> None:
        cci_odp = CciOdp()
        dataset_id = 'esacci.OZONE.mon.L3.NP.multi-sensor.multi-platform.MERGED.fv0002.r1'
        time_range = (pd.to_datetime('2010-02-10', utc=True), pd.to_datetime('2010-05-20', utc=True))
        cube_params = dict(
            time_range=time_range,
            variable_names=['O3_vmr']
        )
        self._store = CciChunkStore(cci_odp, dataset_id, cube_params)

    @skipIf(os.environ.get('XCUBE_DISABLE_WEB_TESTS', None) == '1', 'XCUBE_DISABLE_WEB_TESTS = 1')
    def test_unconstrained_chunk_store(self):
        cci_odp = CciOdp()
        dataset_id = 'esacci.OZONE.mon.L3.NP.multi-sensor.multi-platform.MERGED.fv0002.r1'
        store = CciChunkStore(cci_odp, dataset_id, cube_params=None)
        self.assertIsNotNone(store)
        time_ranges = store._time_ranges
        self.assertEqual(144, len(time_ranges))
        self.assertEqual(pd.Timestamp('1997-01-01T00:00:00'), time_ranges[0][0])
        self.assertEqual(['surface_pressure', 'O3e_ndens', 'O3e_du', 'O3e_vmr',
                          'time', 'O3e_du_tot', 'O3_du_tot', 'O3_ndens', 'O3_du', 'O3_vmr'],
                        store._variable_names)

    @skipIf(os.environ.get('XCUBE_DISABLE_WEB_TESTS', None) == '1', 'XCUBE_DISABLE_WEB_TESTS = 1')
    def test_get_time_ranges(self):
        time_range = (pd.to_datetime('2010-02-10', utc=True), pd.to_datetime('2010-05-20', utc=True))
        cube_params = dict(time_range=time_range)
        time_ranges = self._store.get_time_ranges(
            'esacci.OZONE.mon.L3.NP.multi-sensor.multi-platform.MERGED.fv0002.r1', cube_params)
        self.assertEqual([('2010-02-01T00:00:00', '2010-03-01T00:00:00'),
                          ('2010-03-01T00:00:00', '2010-04-01T00:00:00'),
                          ('2010-04-01T00:00:00', '2010-05-01T00:00:00'),
                          ('2010-05-01T00:00:00', '2010-06-01T00:00:00')],
                         [(tr[0].isoformat(), tr[1].isoformat()) for tr in time_ranges])
        # get_time_range test for satellite-orbit-frequency data
        time_range = (pd.to_datetime('2002-07-24', utc=True), pd.to_datetime('2002-07-24', utc=True))
        cube_params = dict(time_range=time_range)
        time_ranges = self._store.get_time_ranges(
            'esacci.SST.satellite-orbit-frequency.L3U.SSTskin.AATSR.Envisat.AATSR.2-1.r1', cube_params)
        self.assertEqual([('2002-07-24T12:33:21', '2002-07-24T12:33:21'),
                          ('2002-07-24T14:13:57', '2002-07-24T14:13:57'),
                          ('2002-07-24T15:54:33', '2002-07-24T15:54:33'),
                          ('2002-07-24T17:35:09', '2002-07-24T17:35:09'),
                          ('2002-07-24T19:15:45', '2002-07-24T19:15:45'),
                          ('2002-07-24T20:56:21', '2002-07-24T20:56:21'),
                          ('2002-07-24T22:36:57', '2002-07-24T22:36:57')],
                         [(tr[0].isoformat(), tr[1].isoformat()) for tr in time_ranges])

    @skipIf(os.environ.get('XCUBE_DISABLE_WEB_TESTS', None) == '1', 'XCUBE_DISABLE_WEB_TESTS = 1')
    def test_get_dimension_indexes_for_chunk(self):
        dim_indexes = self._store._get_dimension_indexes_for_chunk('O3_vmr', (5, 0, 0, 0))
        self.assertIsNotNone(dim_indexes)
        self.assertEqual(slice(None, None, None), dim_indexes[0])
        self.assertEqual(slice(0, 9), dim_indexes[1])
        self.assertEqual(slice(0, 90), dim_indexes[2])
        self.assertEqual(slice(0, 180), dim_indexes[3])

    @skipIf(os.environ.get('XCUBE_DISABLE_WEB_TESTS', None) == '1', 'XCUBE_DISABLE_WEB_TESTS = 1')
    def test_get_encoding(self):
        encoding_dict = self._store.get_encoding('surface_pressure')
        self.assertTrue('fill_value' in encoding_dict)
        self.assertTrue('dtype' in encoding_dict)
        self.assertFalse('compressor' in encoding_dict)
        self.assertFalse('order' in encoding_dict)
        self.assertTrue(numpy.isnan(encoding_dict['fill_value']))
        self.assertEqual('float32', encoding_dict['dtype'])

    @skipIf(os.environ.get('XCUBE_DISABLE_WEB_TESTS', None) == '1', 'XCUBE_DISABLE_WEB_TESTS = 1')
    def test_get_attrs(self):
        attrs = self._store.get_attrs('surface_pressure')
        self.assertTrue('standard_name' in attrs)
        self.assertTrue('long_name' in attrs)
        self.assertTrue('units' in attrs)
        self.assertTrue('fill_value' in attrs)
        self.assertTrue('chunk_sizes' in attrs)
        self.assertTrue('data_type' in attrs)
        self.assertTrue('dimensions' in attrs)
        self.assertEqual('surface_air_pressure', attrs['standard_name'])
        self.assertEqual('Pressure at the bottom of the atmosphere.', attrs['long_name'])
        self.assertEqual('hPa', attrs['units'])
        self.assertTrue(numpy.isnan(attrs['fill_value']))
        self.assertEqual([1, 180, 360], attrs['chunk_sizes'])
        self.assertEqual('float32', attrs['data_type'])
        self.assertEqual(['time', 'lat', 'lon'], attrs['dimensions'])

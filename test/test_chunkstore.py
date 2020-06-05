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
        dataset_id = 'esacci.OZONE.month.L3.NP.multi-sensor.multi-platform.MERGED.fv0002.r1'
        open_params = {}
        time_range = (pd.to_datetime('2010-02-10', utc=True), pd.to_datetime('2010-05-20', utc=True))
        cube_params = dict(time_range=time_range)
        self._store = CciChunkStore(cci_odp, dataset_id, open_params, cube_params)

    @skipIf(os.environ.get('XCUBE_DISABLE_WEB_TESTS', None) == '1', 'XCUBE_DISABLE_WEB_TESTS = 1')
    def test_get_time_ranges(self):
        time_range = (pd.to_datetime('2010-02-10', utc=True), pd.to_datetime('2010-05-20', utc=True))
        cube_params = dict(time_range=time_range)
        time_ranges = self._store.get_time_ranges(
            'esacci.OZONE.month.L3.NP.multi-sensor.multi-platform.MERGED.fv0002.r1', cube_params)
        self.assertEqual([('2010-02-01T00:00:00', '2010-02-28T23:59:59'),
                          ('2010-03-01T00:00:00', '2010-03-31T23:59:59'),
                          ('2010-04-01T00:00:00', '2010-04-30T23:59:59'),
                          ('2010-05-01T00:00:00', '2010-05-31T23:59:59')],
                         [(tr[0].isoformat(), tr[1].isoformat()) for tr in time_ranges])

    @skipIf(os.environ.get('XCUBE_DISABLE_WEB_TESTS', None) == '1', 'XCUBE_DISABLE_WEB_TESTS = 1')
    def test_get_dimension_indexes_for_chunk(self):
        dim_indexes = self._store._get_dimension_indexes_for_chunk('O3_vmr', (5, 0, 0, 0))
        self.assertIsNotNone(dim_indexes)
        self.assertEqual(0, dim_indexes[0])
        self.assertEqual(slice(0, 8), dim_indexes[1])
        self.assertEqual(slice(0, 89), dim_indexes[2])
        self.assertEqual(slice(0, 179), dim_indexes[3])

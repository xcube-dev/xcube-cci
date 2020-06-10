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
        self.assertEqual('esacci.OZONE.month.L3.NP.multi-sensor.multi-platform.MERGED.fv0002.r1', descriptor.id)
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
        self.assertIsNone(descriptor.spatial_crs)
        self.assertEqual((1.0, 1.0), descriptor.spatial_resolution)
        self.assertEqual(('2000-02-01T00:00:00', '2014-12-31T23:59:59'), descriptor.temporal_coverage)
        self.assertEqual('1M', descriptor.temporal_resolution)

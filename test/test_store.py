import numpy
import unittest

from xcube_cci.cciodp import CciOdp
from xcube_cci.config import CubeConfig
from xcube_cci.store import CciStore

class CciStoreTest(unittest.TestCase):

    def setUp(self) -> None:
        cci_odp = CciOdp()
        config = CubeConfig(dataset_name='esacci.OZONE.month.L3.NP.multi-sensor.multi-platform.MERGED.fv0002.r1',
                            variable_names=['surface_pressure', 'O3_vmr', 'O3_du'],
                            geometry=(10, 10, 20, 20),
                            time_range=('2010-02-10', '2010-05-20')
                            )
        self._store = CciStore(cci_odp, config)

    def test_get_encoding(self):
        encoding_dict = self._store.get_encoding('surface_pressure')
        self.assertTrue('fill_value' in encoding_dict)
        self.assertTrue('dtype' in encoding_dict)
        self.assertFalse('compressor' in encoding_dict)
        self.assertFalse('order' in encoding_dict)
        self.assertTrue(numpy.isnan(encoding_dict['fill_value']))
        self.assertEqual('float32', encoding_dict['dtype'])


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

    def test_get_time_ranges(self):
        time_ranges = self._store.get_time_ranges()
        self.assertEqual([('2010-02-01T00:00:00', '2010-02-28T23:59:59'),
                          ('2010-03-01T00:00:00', '2010-03-31T23:59:59'),
                          ('2010-04-01T00:00:00', '2010-04-30T23:59:59'),
                          ('2010-05-01T00:00:00', '2010-05-31T23:59:59')],
                         [(tr[0].isoformat(), tr[1].isoformat()) for tr in time_ranges])

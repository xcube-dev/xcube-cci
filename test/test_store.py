import pandas
import unittest

from xcube_cci.cciodp import CciOdp
from xcube_cci.config import CubeConfig
from xcube_cci.store import CciStore

class CciStoreTest(unittest.TestCase):


    def test_get_encoding(self):
        cci_odp = CciOdp()
        config = CubeConfig(dataset_name='esacci.OZONE.month.L3.NP.multi-sensor.multi-platform.MERGED.fv0002.r1',
                            variable_names=['surface_pressure', 'O3_vmr', 'O3_du'],
                            geometry=(10, 10, 20, 20),
                            time_range=('2010-05-01', '2010-08-31'),
                            time_period=pandas.Timedelta('30D')
                            )
        store = CciStore(cci_odp, config)
        encoding_dict = store.get_encoding('surface_pressure')
        self.assertTrue('fill_value' in encoding_dict)
        self.assertTrue('dtype' in encoding_dict)
        self.assertFalse('compressor' in encoding_dict)
        self.assertFalse('order' in encoding_dict)
        self.assertEqual('NaN', encoding_dict['fill_value'])
        self.assertEqual('Float32', encoding_dict['dtype'])


    def test_get_attrs(self):
        cci_odp = CciOdp()
        config = CubeConfig(dataset_name='esacci.OZONE.month.L3.NP.multi-sensor.multi-platform.MERGED.fv0002.r1',
                            variable_names=['surface_pressure', 'O3_vmr', 'O3_du'],
                            geometry=(10,10,20,20),
                            time_range=('2010-05-01', '2010-08-31'),
                            time_period=pandas.Timedelta('30D')
                            )
        store = CciStore(cci_odp, config)
        attrs = store.get_attrs('surface_pressure')
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
        self.assertEqual('NaN', attrs['fill_value'])
        self.assertEqual([1, 180, 360], attrs['chunk_sizes'])
        self.assertEqual('Float32', attrs['data_type'])
        self.assertEqual(['time', 'lat', 'lon'], attrs['dimensions'])

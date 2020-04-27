import pandas
import unittest

from xcube_cci.config import CubeConfig
from xcube_cci.cube import open_cube

class OpenCubeTest(unittest.TestCase):


    def test_open_cube(self):
        config = CubeConfig(dataset_name='esacci.OZONE.month.L3.NP.multi-sensor.multi-platform.MERGED.fv0002.r1',
                            # fid='Nn9o6XAB5l1700gahU5G',
                            fid='4eb4e801424a47f7b77434291921f889',
                            variable_names=['surface_pressure', 'O3e_du_tot'],
                            geometry=(-5,42,30,58),
                            spatial_res=1.0,
                            # chunk_size=(10,10),
                            time_range=('1997-01-01', '1997-12-31'),
                            time_period=pandas.Timedelta('30D')
                            )
        cube = open_cube(config)
        self.assertIsNotNone(cube)
        # surface_pressure_data = cube.surface_pressure.sel(time='1997-05-01', method='nearest')
        # print(surface_pressure_data)
        cube.surface_pressure.sel(time='1997-05-01', method='nearest').plot.imshow(vmin=0, vmax=0.2, cmap='Greys_r',
                                                                                   figsize=(16, 10))

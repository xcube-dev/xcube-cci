import os
import unittest

from unittest import skipIf

from xcube_cci.config import CubeConfig
from xcube_cci.cube import open_cube

class OpenCubeTest(unittest.TestCase):

    @skipIf(os.environ.get('XCUBE_DISABLE_WEB_TESTS', None) == '1', 'XCUBE_DISABLE_WEB_TESTS = 1')
    def test_open_cube(self):
        config = CubeConfig(dataset_name='esacci.OZONE.mon.L3.NP.multi-sensor.multi-platform.MERGED.fv0002.r1',
                            variable_names=['surface_pressure', 'O3e_du_tot'],
                            geometry=(-5.0,42.0,30.0,58.0),
                            time_range=('1997-01-01', '1997-12-31')
                            )
        cube = open_cube(config)
        self.assertIsNotNone(cube)
        data = cube.surface_pressure.sel(time='1997-05-15 12:00:00', method='nearest')
        self.assertAlmostEqual(926.4256, data.values[0][0], 4)
        self.assertAlmostEqual(1000.27875, data.values[-1][-1], 5)

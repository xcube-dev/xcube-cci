import json
import os
import unittest

from xcube_cci.cciodp import _retrieve_attribute_info_from_das, CciOdp

class CciOdpTest(unittest.TestCase):

    def test_retrieve_attribute_info_from_das(self):
        das_file = os.path.join(os.path.dirname(__file__),
                                "resources/ESACCI-OZONE-L3-NP-MERGED-KNMI-199701-fv0002.nc.das")
        das = open(das_file)
        das_read = das.read()
        attribute_info = _retrieve_attribute_info_from_das(das_read)
        self.assertEqual(15, len(attribute_info))
        self.assertTrue('surface_pressure' in attribute_info)
        self.assertTrue('fill_value' in attribute_info['surface_pressure'])
        self.assertEqual('NaN', attribute_info['surface_pressure']['fill_value'])

    def test_get_data(self):
        cci_odp = CciOdp()
        request = dict(parentIdentifier='4eb4e801424a47f7b77434291921f889',
                       startDate='1997-05-01T00:00:00',
                       endDate='1997-05-01T00:00:00',
                       bbox=(-10.0, 40.0, 10.0, 60.0),
                       varNames=['surface_pressure', 'O3e_du_tot']
                       )
        data = cci_odp.get_data(request)
        self.assertIsNotNone(data)

    def test_dataset_names(self):
        cci_odp = CciOdp()
        dataset_names = cci_odp.dataset_names
        self.assertIsNotNone(dataset_names)

    def test_var_names(self):
        cci_odp = CciOdp()
        var_names = cci_odp.var_names('esacci.OZONE.month.L3.NP.multi-sensor.multi-platform.MERGED.fv0002.r1')
        self.assertIsNotNone(var_names)
        self.assertEqual(['surface_pressure', 'O3_du', 'O3e_du', 'O3_du_tot', 'O3e_du_tot', 'O3_vmr', 'O3e_vmr',
                          'O3_ndens', 'O3e_ndens'], var_names)

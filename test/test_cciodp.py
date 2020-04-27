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
        self.assertTrue('Float32 _FillValue' in attribute_info['surface_pressure'])
        self.assertEqual('NaN', attribute_info['surface_pressure']['Float32 _FillValue'])

    def test_get_data(self):
        cci_odp = CciOdp()
        request = dict(parentIdentifier='4eb4e801424a47f7b77434291921f889',
                       startDate='1997-05-01T00:00:00',
                       endDate='1997-05-01T00:00:00',
                       bbox=(-10, 40, 10, 60),
                       varNames=['surface_pressure', 'O3e_du_tot']
                       )
        cci_odp.get_data(request)

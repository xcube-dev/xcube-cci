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
        var_names = cci_odp.var_names('esacci.OC.8-days.L3S.OC_PRODUCTS.multi-sensor.multi-platform.MERGED.4-0.geographic')
        self.assertIsNotNone(var_names)
        self.assertEqual(['Rrs_412', 'Rrs_443', 'Rrs_490', 'Rrs_510', 'Rrs_555', 'Rrs_670', 'water_class1',
                          'water_class2', 'water_class3', 'water_class4', 'water_class5', 'water_class6',
                          'water_class7', 'water_class8', 'water_class9', 'water_class10', 'water_class11',
                          'water_class12', 'water_class13', 'water_class14', 'atot_412', 'atot_443', 'atot_490',
                          'atot_510', 'atot_555', 'atot_670', 'aph_412', 'aph_443', 'aph_490', 'aph_510', 'aph_555',
                          'aph_670', 'adg_412', 'adg_443', 'adg_490', 'adg_510', 'adg_555', 'adg_670', 'bbp_412',
                          'bbp_443', 'bbp_490', 'bbp_510', 'bbp_555', 'bbp_670', 'chlor_a', 'kd_490', 'Rrs_412_rmsd',
                          'Rrs_443_rmsd', 'Rrs_490_rmsd', 'Rrs_510_rmsd', 'Rrs_555_rmsd', 'Rrs_670_rmsd',
                          'Rrs_412_bias', 'Rrs_443_bias', 'Rrs_490_bias', 'Rrs_510_bias', 'Rrs_555_bias',
                          'Rrs_670_bias', 'chlor_a_log10_rmsd', 'chlor_a_log10_bias', 'aph_412_rmsd', 'aph_443_rmsd',
                          'aph_490_rmsd', 'aph_510_rmsd', 'aph_555_rmsd', 'aph_670_rmsd', 'aph_412_bias',
                          'aph_443_bias', 'aph_490_bias', 'aph_510_bias', 'aph_555_bias', 'aph_670_bias',
                          'adg_412_rmsd', 'adg_443_rmsd', 'adg_490_rmsd', 'adg_510_rmsd', 'adg_555_rmsd',
                          'adg_670_rmsd', 'adg_412_bias', 'adg_443_bias', 'adg_490_bias', 'adg_510_bias',
                          'adg_555_bias', 'adg_670_bias', 'kd_490_rmsd', 'kd_490_bias', 'SeaWiFS_nobs_sum',
                          'MODISA_nobs_sum', 'MERIS_nobs_sum', 'VIIRS_nobs_sum', 'total_nobs_sum'], var_names)

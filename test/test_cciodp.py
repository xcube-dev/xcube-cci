import numpy as np
import os
import unittest
from unittest import skipIf

from xcube_cci.cciodp import _retrieve_attribute_info_from_das, find_datetime_format, get_opendap_dataset, CciOdp

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

    @skipIf(os.environ.get('XCUBE_DISABLE_WEB_TESTS', None) == '1', 'XCUBE_DISABLE_WEB_TESTS = 1')
    def test_get_data(self):
        cci_odp = CciOdp()
        request = dict(parentIdentifier='4eb4e801424a47f7b77434291921f889',
                       startDate='1997-05-01T00:00:00',
                       endDate='1997-05-01T00:00:00',
                       varNames=['surface_pressure', 'O3e_du_tot']
                       )
        bbox = (-10.0, 40.0, 10.0, 60.0)
        data = cci_odp.get_data(request, bbox, {}, {})
        self.assertIsNotNone(data)
        data_array = np.frombuffer(data, dtype=np.float32)
        self.assertEqual(800, len(data_array))
        self.assertAlmostEqual(1003.18524, data_array[0], 5)
        self.assertAlmostEqual(960.9344, data_array[399], 4)
        self.assertAlmostEqual(0.63038087, data_array[400], 8)
        self.assertAlmostEqual(0.659591, data_array[799], 6)

    def test_dataset_names(self):
        cci_odp = CciOdp()
        dataset_names = cci_odp.dataset_names
        self.assertIsNotNone(dataset_names)
        list(dataset_names).sort()
        self.assertEqual(175, len(dataset_names))
        self.assertTrue('esacci.AEROSOL.day.L3C.AER_PRODUCTS.ATSR-2.ERS-2.ORAC.03-02.r1' in dataset_names)
        self.assertTrue('esacci.OC.day.L3S.K_490.multi-sensor.multi-platform.MERGED.3-1.sinusoidal' in dataset_names)

    @skipIf(os.environ.get('XCUBE_DISABLE_WEB_TESTS', None) == '1', 'XCUBE_DISABLE_WEB_TESTS = 1')
    def test_var_names(self):
        cci_odp = CciOdp()
        var_names = cci_odp.var_names('esacci.OC.8-days.L3S.K_490.multi-sensor.multi-platform.MERGED.4-0.geographic')
        self.assertIsNotNone(var_names)
        self.assertEqual(['MERIS_nobs_sum', 'MODISA_nobs_sum', 'SeaWiFS_nobs_sum', 'VIIRS_nobs_sum', 'kd_490',
                          'kd_490_bias', 'kd_490_rmsd', 'total_nobs_sum'], var_names)

    @skipIf(os.environ.get('XCUBE_DISABLE_WEB_TESTS', None) == '1', 'XCUBE_DISABLE_WEB_TESTS = 1')
    def test_get_dataset_info(self):
        cci_odp = CciOdp()
        dataset_info = cci_odp.get_dataset_info('esacci.CLOUD.month.L3C.CLD_PRODUCTS.MODIS.Terra.MODIS_TERRA.2-0.r1')
        self.assertIsNotNone(dataset_info)
        self.assertTrue('lat_res' in dataset_info)
        self.assertEqual(0.5, dataset_info['lat_res'])
        self.assertTrue('lon_res' in dataset_info)
        self.assertEqual(0.5, dataset_info['lon_res'])
        self.assertTrue('bbox' in dataset_info)
        self.assertEqual((-180.0, -90.0, 180.0, 90.0), dataset_info['bbox'])
        self.assertTrue('var_names' in dataset_info)
        self.assertEqual(['nobs', 'nobs_day', 'nobs_clear_day', 'nobs_cloudy_day', 'nobs_clear_night',
                          'nobs_cloudy_night', 'nobs_clear_twl', 'nobs_cloudy_twl', 'nobs_cloudy', 'nretr_cloudy',
                          'nretr_cloudy_liq', 'nretr_cloudy_ice', 'nretr_cloudy_day', 'nretr_cloudy_day_liq',
                          'nretr_cloudy_day_ice', 'nretr_cloudy_low', 'nretr_cloudy_mid', 'nretr_cloudy_high', 'cfc',
                          'cfc_std', 'cfc_prop_unc', 'cfc_corr_unc', 'cfc_unc', 'cfc_low', 'cfc_mid', 'cfc_high',
                          'cfc_day', 'cfc_night', 'cfc_twl', 'ctt', 'ctt_std', 'ctt_prop_unc', 'ctt_corr_unc',
                          'ctt_unc', 'ctt_corrected', 'ctt_corrected_std', 'ctt_corrected_prop_unc',
                          'ctt_corrected_corr_unc', 'ctt_corrected_unc', 'stemp', 'stemp_std', 'stemp_prop_unc',
                          'stemp_corr_unc', 'stemp_unc', 'cth', 'cth_std', 'cth_prop_unc', 'cth_corr_unc', 'cth_unc',
                          'cth_corrected', 'cth_corrected_std', 'cth_corrected_prop_unc', 'cth_corrected_corr_unc',
                          'cth_corrected_unc', 'ctp', 'ctp_std', 'ctp_prop_unc', 'ctp_corr_unc', 'ctp_unc', 'ctp_log',
                          'ctp_corrected', 'ctp_corrected_std', 'ctp_corrected_prop_unc', 'ctp_corrected_corr_unc',
                          'ctp_corrected_unc', 'cph', 'cph_std', 'cph_day', 'cph_day_std', 'cer', 'cer_std',
                          'cer_prop_unc', 'cer_corr_unc', 'cer_unc', 'cot', 'cot_log', 'cot_std', 'cot_prop_unc',
                          'cot_corr_unc', 'cot_unc', 'cee', 'cee_std', 'cee_prop_unc', 'cee_corr_unc', 'cee_unc',
                          'cla_vis006', 'cla_vis006_std', 'cla_vis006_prop_unc', 'cla_vis006_corr_unc',
                          'cla_vis006_unc', 'cla_vis006_liq', 'cla_vis006_liq_std', 'cla_vis006_liq_unc',
                          'cla_vis006_ice', 'cla_vis006_ice_std', 'cla_vis006_ice_unc', 'cla_vis008', 'cla_vis008_std',
                          'cla_vis008_prop_unc', 'cla_vis008_corr_unc', 'cla_vis008_unc', 'cla_vis008_liq',
                          'cla_vis008_liq_std', 'cla_vis008_liq_unc', 'cla_vis008_ice', 'cla_vis008_ice_std',
                          'cla_vis008_ice_unc', 'lwp', 'lwp_std', 'lwp_prop_unc', 'lwp_corr_unc', 'lwp_unc',
                          'lwp_allsky', 'iwp', 'iwp_std', 'iwp_prop_unc', 'iwp_corr_unc', 'iwp_unc', 'iwp_allsky',
                          'cer_liq', 'cer_liq_std', 'cer_liq_prop_unc', 'cer_liq_corr_unc', 'cer_liq_unc', 'cer_ice',
                          'cer_ice_std', 'cer_ice_prop_unc', 'cer_ice_corr_unc', 'cer_ice_unc', 'cot_liq',
                          'cot_liq_std', 'cot_liq_prop_unc', 'cot_liq_corr_unc', 'cot_liq_unc', 'cot_ice',
                          'cot_ice_std', 'cot_ice_prop_unc', 'cot_ice_corr_unc', 'cot_ice_unc', 'hist2d_cot_ctp',
                          'hist1d_cot', 'hist1d_ctp', 'hist1d_ctt', 'hist1d_cer', 'hist1d_cwp', 'hist1d_cla_vis006',
                          'hist1d_cla_vis008'], dataset_info['var_names'])
        self.assertTrue('temporal_coverage_start' in dataset_info)
        self.assertEqual('2000-02-01T00:00:00', dataset_info['temporal_coverage_start'])
        self.assertTrue('temporal_coverage_end' in dataset_info)
        self.assertEqual('2014-12-31T23:59:59', dataset_info['temporal_coverage_end'])


    @skipIf(os.environ.get('XCUBE_DISABLE_WEB_TESTS', None) == '1', 'XCUBE_DISABLE_WEB_TESTS = 1')
    def test_description(self):
        cci_odp = CciOdp()
        description = cci_odp.description
        self.assertIsNotNone(description)
        self.assertEqual('cciodp', description['id'])
        self.assertEqual('ESA CCI Open Data Portal', description['name'])
        import json
        with open('cci_datasets.json', 'w') as fp:
            json.dump(description, fp, indent=4)

    def test_shorten_dataset_name(self):
        cci_odp = CciOdp()
        self.assertEqual('gdrvtzsw', cci_odp._shorten_dataset_name('gdrvtzsw'))
        self.assertEqual('Ozone CCI: Level3 Nadir Ozone Profile Merged Data Product version 2',
                         cci_odp._shorten_dataset_name('ESA Ozone Climate Change Initiative (Ozone CCI): '
                                                       'Level3 Nadir Ozone Profile Merged Data Product version 2'))
        self.assertEqual('Ozone CCI: L3 Nadir Ozone Profile Merged Data Product v2',
                         cci_odp._shorten_dataset_name('ESA Ozone Climate Change Initiative (Ozone CCI): '
                                                       'Level 3 Nadir Ozone Profile Merged Data Product, version 2'))

    @skipIf(os.environ.get('XCUBE_DISABLE_WEB_TESTS', None) == '1', 'XCUBE_DISABLE_WEB_TESTS = 1')
    def test_get_opendap_dataset(self):
        opendap_url = 'http://cci-odp-data2.ceda.ac.uk/thredds/dodsC/esacci/aerosol/data/AATSR_SU/L3/v4.21/DAILY/2002/' \
                      '07/20020724-ESACCI-L3C_AEROSOL-AER_PRODUCTS-AATSR_ENVISAT-SU_DAILY-v4.21.nc'
        dataset = get_opendap_dataset(opendap_url)
        self.assertIsNotNone(dataset)
        self.assertEqual(53, len(list(dataset.keys())))
        self.assertTrue('AOD550_mean' in dataset.keys())
        self.assertEqual('atmosphere_optical_thickness_due_to_ambient_aerosol',
                         dataset['AOD550_mean'].attributes['standard_name'])
        self.assertEqual(('latitude', 'longitude'), dataset['AOD550_mean'].dimensions)
        self.assertEqual(64800, dataset['AOD550_mean'].size)

    def test_get_res(self):
        cci_odp = CciOdp()
        nc_attrs = dict(geospatial_lat_resolution=24.2,
                        geospatial_lon_resolution=30.1)
        self.assertEquals(24.2, cci_odp._get_res(nc_attrs, 'lat'))
        self.assertEquals(30.1, cci_odp._get_res(nc_attrs, 'lon'))

        nc_attrs = dict(resolution=5.0)
        self.assertEquals(5.0, cci_odp._get_res(nc_attrs, 'lat'))
        self.assertEquals(5.0, cci_odp._get_res(nc_attrs, 'lon'))

        nc_attrs = dict(resolution='12x34 degree')
        self.assertEquals(12.0, cci_odp._get_res(nc_attrs, 'lat'))
        self.assertEquals(34.0, cci_odp._get_res(nc_attrs, 'lon'))

    def test_find_datetime_format(self):
        time_format, start, end, timedelta = find_datetime_format('fetgzrs2015ydhfbgv')
        self.assertEqual('%Y', time_format)
        self.assertEqual(7, start)
        self.assertEqual(11, end)
        self.assertEqual(1, timedelta.years)
        self.assertEqual(0, timedelta.months)
        self.assertEqual(0, timedelta.days)
        self.assertEqual(0, timedelta.hours)
        self.assertEqual(0, timedelta.minutes)
        self.assertEqual(-1, timedelta.seconds)

        time_format, start, end, timedelta = find_datetime_format('fetz23gxgs20150213ydh391fbgv')
        self.assertEqual('%Y%m%d', time_format)
        self.assertEqual(10, start)
        self.assertEqual(18, end)
        self.assertEqual(0, timedelta.years)
        self.assertEqual(0, timedelta.months)
        self.assertEqual(1, timedelta.days)
        self.assertEqual(0, timedelta.hours)
        self.assertEqual(0, timedelta.minutes)
        self.assertEqual(-1, timedelta.seconds)

        time_format, start, end, timedelta = find_datetime_format('f23gxgs19961130191846y391fbgv')
        self.assertEqual('%Y%m%d%H%M%S', time_format)
        self.assertEqual(7, start)
        self.assertEqual(21, end)
        self.assertEqual(0, timedelta.years)
        self.assertEqual(0, timedelta.months)
        self.assertEqual(0, timedelta.days)
        self.assertEqual(0, timedelta.hours)
        self.assertEqual(0, timedelta.minutes)
        self.assertEqual(0, timedelta.seconds)


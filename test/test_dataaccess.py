import datetime as dt
import os
import unittest

from unittest import skip
from unittest import skipIf

from xcube_cci.cciodp import CciOdp
from xcube_cci.dataaccess import CciOdpDataOpener
from xcube_cci.dataaccess import CciOdpDataStore
from xcube.core.normalize import normalize_dataset
from xcube.core.store.descriptor import DatasetDescriptor
from xcube.core.verify import assert_cube

class CciOdpDataOpenerTest(unittest.TestCase):

    def setUp(self) -> None:
        self.opener = CciOdpDataOpener(cci_odp=CciOdp())

    @skipIf(os.environ.get('XCUBE_DISABLE_WEB_TESTS', None) == '1', 'XCUBE_DISABLE_WEB_TESTS = 1')
    def test_describe_datasets(self):
        descriptors = self.opener.describe_datasets(
            ['esacci.OZONE.mon.L3.NP.multi-sensor.multi-platform.MERGED.fv0002.r1',
             'esacci.FIRE.mon.L4.BA.MODIS.Terra.MODIS_TERRA.v5-1.grid'
            ]
        )
        self.assertIsNotNone(descriptors)
        self.assertEqual(2, len(descriptors))
        descriptor = descriptors[0]
        self.assertIsInstance(descriptor, DatasetDescriptor)
        self.assertEqual('esacci.OZONE.mon.L3.NP.multi-sensor.multi-platform.MERGED.fv0002.r1', descriptor.data_id)
        self.assertIsNone(descriptor.dims)
        self.assertEqual(9, len(descriptor.data_vars))
        self.assertEqual('surface_pressure', descriptor.data_vars[0].name)
        self.assertEqual(0, descriptor.data_vars[0].ndim)
        self.assertEqual((), descriptor.data_vars[0].dims)
        self.assertEqual('unknown', descriptor.data_vars[0].dtype)
        self.assertIsNone(descriptor.crs)
        self.assertIsNone(descriptor.spatial_res)
        self.assertEqual(('1997-01-01T00:00:00', '2008-12-31T00:00:00'), descriptor.time_range)
        self.assertIsNone(descriptor.time_period)
        self.assertEqual('esacci.FIRE.mon.L4.BA.MODIS.Terra.MODIS_TERRA.v5-1.grid', descriptors[1].data_id)

    @skipIf(os.environ.get('XCUBE_DISABLE_WEB_TESTS', None) == '1', 'XCUBE_DISABLE_WEB_TESTS = 1')
    def test_describe_dataset(self):
        descriptor = self.opener.describe_data('esacci.OZONE.mon.L3.NP.multi-sensor.multi-platform.MERGED.fv0002.r1')
        self.assertIsNotNone(descriptor)
        self.assertIsInstance(descriptor, DatasetDescriptor)
        self.assertEqual('esacci.OZONE.mon.L3.NP.multi-sensor.multi-platform.MERGED.fv0002.r1', descriptor.data_id)
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
        self.assertIsNone(descriptor.crs)
        self.assertEqual(1.0, descriptor.spatial_res)
        self.assertEqual(('1997-01-01T00:00:00', '2008-12-31T00:00:00'), descriptor.time_range)
        self.assertEqual('1M', descriptor.time_period)

    @skipIf(os.environ.get('XCUBE_DISABLE_WEB_TESTS', None) == '1', 'XCUBE_DISABLE_WEB_TESTS = 1')
    def test_get_open_data_params_schema(self):
        schema = self.opener.get_open_data_params_schema(
            'esacci.OZONE.mon.L3.NP.multi-sensor.multi-platform.MERGED.fv0002.r1').to_dict()
        self.assertIsNotNone(schema)
        self.assertTrue('variable_names' in schema['properties'])
        self.assertTrue('time_range' in schema['properties'])

    @skipIf(os.environ.get('XCUBE_DISABLE_WEB_TESTS', None) == '1', 'XCUBE_DISABLE_WEB_TESTS = 1')
    def test_open_dataset(self):
        dataset = self.opener.open_data('esacci.OZONE.mon.L3.NP.multi-sensor.multi-platform.MERGED.fv0002.r1',
                                        variable_names=['surface_pressure', 'O3_du', 'O3e_du'],
                                        time_range=['2009-05-02', '2009-08-31'],
                                        bbox=[-10.0, 40.0, 10.0, 60.0]
                                        )
        self.assertIsNotNone(dataset)
        self.assertTrue('surface_pressure' in dataset.variables)
        self.assertTrue('O3_du' in dataset.variables)
        self.assertTrue('O3e_du' in dataset.variables)


class CciOdpDataStoreTest(unittest.TestCase):

    def setUp(self) -> None:
        self.store = CciOdpDataStore()

    @skip('Test takes long')
    def test_description(self):
        description = self.store.description
        self.assertIsNotNone(description)
        self.assertEqual('cciodp', description['store_id'])
        self.assertEqual('ESA CCI Open Data Portal', description['description'])
        import json
        with open('cci_store_datasets.json', 'w') as fp:
            json.dump(description, fp, indent=4)

    def test_get_data_store_params_schema(self):
        cci_store_params_schema = CciOdpDataStore.get_data_store_params_schema().to_dict()
        self.assertIsNotNone(cci_store_params_schema)
        self.assertTrue('opensearch_url' in cci_store_params_schema['properties'])
        self.assertTrue('opensearch_description_url' in cci_store_params_schema['properties'])

    def test_get_search_params(self):
        search_schema = self.store.get_search_params_schema().to_dict()
        self.assertIsNotNone(search_schema)
        self.assertTrue('start_date' in search_schema['properties'])
        self.assertTrue('end_date' in search_schema['properties'])
        self.assertTrue('bbox' in search_schema['properties'])
        self.assertTrue('ecv' in search_schema['properties'])
        self.assertTrue('frequency' in search_schema['properties'])
        self.assertTrue('institute' in search_schema['properties'])
        self.assertTrue('processing_level' in search_schema['properties'])
        self.assertTrue('product_string' in search_schema['properties'])
        self.assertTrue('product_version' in search_schema['properties'])
        self.assertTrue('data_type' in search_schema['properties'])
        self.assertTrue('sensor' in search_schema['properties'])
        self.assertTrue('platform' in search_schema['properties'])

    @skipIf(os.environ.get('XCUBE_DISABLE_WEB_TESTS', None) == '1', 'XCUBE_DISABLE_WEB_TESTS = 1')
    def test_search(self):
        search_result = list(self.store.search_data(ecv='FIRE', processing_level='L4', product_string='MODIS_TERRA'))
        self.assertIsNotNone(search_result)
        self.assertEqual(1, len(search_result))
        self.assertIsInstance(search_result[0], DatasetDescriptor)
        self.assertEqual(5, len(search_result[0].dims))
        self.assertEqual(6, len(search_result[0].data_vars))
        self.assertEqual('esacci.FIRE.mon.L4.BA.MODIS.Terra.MODIS_TERRA.v5-1.grid', search_result[0].data_id)
        self.assertEqual('31D', search_result[0].time_period)
        self.assertEqual(0.25, search_result[0].spatial_res)
        self.assertEqual('dataset', search_result[0].type_id)
        self.assertEqual(('2001-01-01T00:00:00', '2019-12-31T23:59:59'), search_result[0].time_range)

    @skipIf(os.environ.get('XCUBE_DISABLE_WEB_TESTS', None) == '1', 'XCUBE_DISABLE_WEB_TESTS = 1')
    def test_has_data(self):
        self.assertTrue(self.store.has_data('esacci.FIRE.mon.L4.BA.MODIS.Terra.MODIS_TERRA.v5-1.grid'))
        self.assertFalse(self.store.has_data('esacci.WIND.mon.L4.BA.MODIS.Terra.MODIS_TERRA.v5-1.grid'))

    @skipIf(os.environ.get('XCUBE_DISABLE_WEB_TESTS', None) == '1', 'XCUBE_DISABLE_WEB_TESTS = 1')
    def test_get_data_ids(self):
        dataset_ids_iter = self.store.get_data_ids()
        self.assertIsNotNone(dataset_ids_iter)
        dataset_ids = list(dataset_ids_iter)
        self.assertEqual(266, len(dataset_ids))

    def test_create_human_readable_title_from_id(self):
        self.assertEqual('OZONE CCI: Monthly multi-sensor L3 MERGED NP, vfv0002',
                         self.store._create_human_readable_title_from_id(
                             'esacci.OZONE.mon.L3.NP.multi-sensor.multi-platform.MERGED.fv0002.r1'))
        self.assertEqual('LC CCI: 13 year ASAR L4 Map WB, v4.0',
                         self.store._create_human_readable_title_from_id(
                             'esacci.LC.13-yrs.L4.WB.ASAR.Envisat.Map.4-0.r1'))


class CciDataNormalizationTest(unittest.TestCase):

    @skip('Execute to test whether all data sets can be normalized')
    def test_normalization(self):
        store = CciOdpDataStore()
        all_data = store.search_data()
        datasets_without_variables = []
        datasets_with_unsupported_frequencies = []
        datasets_that_could_not_be_opened = {}
        good_datasets = []
        for data in all_data:
            if 'satellite-orbit-frequency' in data.data_id or 'climatology' in data.data_id:
                print(f'Cannot read data due to unsupported frequency')
                datasets_with_unsupported_frequencies.append(data.data_id)
                continue
            if not data.data_vars or len(data.data_vars) < 1:
                print(f'Dataset {data.data_id} has not enough variables to open. Will skip.')
                datasets_without_variables.append(data.data_id)
                continue
            print(f'Attempting to open {data.data_id} ...')
            variable_names = []
            for i in range(min(3, len(data.data_vars))):
                variable_names.append(data.data_vars[i].name)
            start_time = dt.datetime.strptime(data.time_range[0], '%Y-%m-%dT%H:%M:%S').timestamp()
            end_time = dt.datetime.strptime(data.time_range[1], '%Y-%m-%dT%H:%M:%S').timestamp()
            center_time = start_time + (end_time - start_time)
            delta = dt.timedelta(days=30)
            range_start = (dt.datetime.fromtimestamp(center_time) - delta).strftime('%Y-%m-%d')
            range_end = (dt.datetime.fromtimestamp(center_time) + delta).strftime('%Y-%m-%d')
            dataset = store.open_data(data_id=data.data_id,
                                           variable_names=variable_names,
                                           time_range=[range_start, range_end]
                                           )
            print(f'Attempting to normalize {data.data_id} ...')
            cube = normalize_dataset(dataset)
            try:
                print(f'Asserting {data.data_id} ...')
                assert_cube(cube)
                good_datasets.append(data.data_id)
            except ValueError as e:
                print(e)
                datasets_that_could_not_be_opened[data.data_id] = e
                continue
        print(f'Datasets with unsupported frequencies (#{len(datasets_with_unsupported_frequencies)}): '
              f'{datasets_with_unsupported_frequencies}')
        print(f'Datasets without variables (#{len(datasets_without_variables)}): '
              f'{datasets_without_variables}')
        print(f'Datasets that could not be opened (#{len(datasets_that_could_not_be_opened)}): '
              f'{datasets_that_could_not_be_opened}')
        print(f'Datasets that were verified (#{len(good_datasets)}): {good_datasets}')

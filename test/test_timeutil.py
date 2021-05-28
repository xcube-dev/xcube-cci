from unittest import TestCase
from xcube_cci.timeutil import get_timestrings_from_string


class TimeUtilTest(TestCase):

    def test_get_timestrings_from_string(self):
        timestamp_1, timestamp_2 = get_timestrings_from_string(
            '20020401-20020406-ESACCI-L3C_AEROSOL-AEX-GOMOS_ENVISAT-AERGOM_5days-fv2.19.nc')
        self.assertEqual('2002-04-01T00:00:00', timestamp_1)
        self.assertEqual('2002-04-06T00:00:00', timestamp_2)

        timestamp_1, timestamp_2 = get_timestrings_from_string(
            '20020401-ESACCI-L3C_AEROSOL-AEX-GOMOS_ENVISAT-AERGOM_5days-fv2.19.nc')
        self.assertEqual('2002-04-01T00:00:00', timestamp_1)
        self.assertIsNone(timestamp_2)

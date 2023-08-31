from unittest import TestCase
from xcube_cci.timeutil import get_timestrings_from_string


class TimeUtilTest(TestCase):

    def test_get_timestrings_from_string(self):
        timestring_1, timestring_2 = get_timestrings_from_string(
            '20020401-20020406-ESACCI-L3C_AEROSOL-AEX_ENVISAT-AERGOM_5days-fv2.19.nc'
        )
        self.assertEqual('2002-04-01T00:00:00', timestring_1)
        self.assertEqual('2002-04-06T00:00:00', timestring_2)

        timestring_1, timestring_2 = get_timestrings_from_string(
            '20020401-ESACCI-L3C_AEROSOL-AEX-GOMOS_ENVISAT-AERGOM_5days-fv2.19.nc'
        )
        self.assertEqual('2002-04-01T00:00:00', timestring_1)
        self.assertIsNone(timestring_2)

    def test_get_timestrings_from_string_months(self):
        timestring_1, timestring_2 = get_timestrings_from_string(
            'ESACCI-AEROSOL-L3-AAI-MSAAI-39Y1m_APRIL-fv1.7.nc'
        )
        self.assertEqual(4, timestring_1)
        self.assertIsNone(timestring_2)

        timestring_1, timestring_2 = get_timestrings_from_string(
            'ESACCI-AEROSOL-L3-AAI-MSAAI-39Y1m_OCTOBER-fv1.7.nc'
        )
        self.assertEqual(10, timestring_1)
        self.assertIsNone(timestring_2)


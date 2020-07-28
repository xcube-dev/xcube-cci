# The MIT License (MIT)
# Copyright (c) 2020 by the xcube development team and contributors
#
# Permission is hereby granted, free of charge, to any person obtaining a copy of
# this software and associated documentation files (the "Software"), to deal in
# the Software without restriction, including without limitation the rights to
# use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
# of the Software, and to permit persons to whom the Software is furnished to do
# so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

DEFAULT_CRS = 'http://www.opengis.net/def/crs/EPSG/0/4326'
DEFAULT_TILE_SIZE = 1000
CCI_MAX_IMAGE_SIZE = 2500
DATA_ARRAY_NAME = 'var_data'
OPENSEARCH_CEDA_URL = "http://opensearch-test.ceda.ac.uk/opensearch/request"
CCI_ODD_URL = 'http://opensearch-test.ceda.ac.uk/opensearch/description.xml?parentIdentifier=cci'

DEFAULT_RETRY_BACKOFF_MAX = 40  # milliseconds
DEFAULT_RETRY_BACKOFF_BASE = 1.001
DEFAULT_NUM_RETRIES = 200

DATA_OPENER_ID = 'dataset:zarr:cciodp'
WGS84_CRS = 'http://www.opengis.net/def/crs/EPSG/0/4326'
DEFAULT_CRS = WGS84_CRS

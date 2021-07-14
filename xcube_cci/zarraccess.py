# The MIT License (MIT)
# Copyright (c) 2021 by the xcube development team and contributors
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
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

from typing import Any, Tuple

from xcube.core.store import DataStoreError
from xcube.core.store.stores.s3 import S3DataStore
from xcube.util.jsonschema import JsonObjectSchema

CCI_ZARR_STORE_BUCKET_NAME = 'esacci'
CCI_ZARR_STORE_ENDPOINT = 'https://cci-ke-o.s3-ext.jc.rl.ac.uk:8443/'


class CciZarrDataStore(S3DataStore):

    def __init__(self):
        zarr_store_params = dict(bucket_name=CCI_ZARR_STORE_BUCKET_NAME,
                                 endpoint_url=CCI_ZARR_STORE_ENDPOINT,
                                 anon=True)
        super().__init__(**zarr_store_params)

    @classmethod
    def get_data_store_params_schema(cls) -> JsonObjectSchema:
        return JsonObjectSchema()

    def get_data_writer_ids(self, type_specifier: str = None) -> \
            Tuple[str, ...]:
        return ()

    def get_write_data_params_schema(self, writer_id: str = None) -> \
            JsonObjectSchema:
        return JsonObjectSchema()

    def write_data(self,
                   data: Any,
                   data_id: str = None,
                   writer_id: str = None,
                   replace: bool = False,
                   **write_params) -> str:
        raise DataStoreError('The CciZarrDataStore is read-only.')

    def delete_data(self, data_id: str):
        raise DataStoreError('The CciZarrDataStore is read-only.')

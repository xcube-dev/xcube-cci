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

import xarray as xr
from typing import Iterator, Mapping, Any
import zarr

from xcube.core.store.dataset import DatasetDescriptor
from xcube.core.store.store import CubeOpener, CubeStore
from xcube.util.jsonschema import JsonObjectSchema

from .cciodp import CciOdp
from .chunkstore import CciChunkStore

class CciCubeStore(CubeStore, CubeOpener):

    @classmethod
    def get_cube_store_params_schema(cls) -> JsonObjectSchema:
        """
        Get descriptions of parameters that must or can be used to instantiate a new cube store object.
        Parameters are named and described by the properties of the returned JSON object schema.
        The default implementation returns JSON object schema that can have any properties.
        """
        return JsonObjectSchema(additional_properties=True)

    def iter_cubes(self) -> Iterator[DatasetDescriptor]:
        """
        Iterate descriptors of all cubes in this store.
        :return: A cube descriptor iterator.
        """
        pass

    def get_open_cube_params_schema(self, cube_id: str) -> JsonObjectSchema:
        """
        Get descriptions of parameters that must or can be used to open a cube from the store.
        Parameters are named and described by the properties of the returned JSON object schema.
        The default implementation returns JSON object schema that can have any properties.
        """
        return JsonObjectSchema()

    def open_cube(self,
                  cube_id: str,
                  open_params: Mapping[str, Any] = None,
                  cube_params: Mapping[str, Any] = None) -> xr.Dataset:
        """
        Open a cube from this cube store.

        :param cube_id: The cube identifier.
        :param open_params: Open parameters.
        :param cube_params: Cube generation parameters.
        :return: The cube.
        """
        # cube_path = self.get_cube_path(cube_id)
        max_cache_size: int = 2 ** 30
        cci_odp = CciOdp()
        chunk_store = CciChunkStore(cci_odp, cube_id, open_params, cube_params)
        if max_cache_size:
            chunk_store = zarr.LRUStoreCache(chunk_store, max_cache_size)
        raw_ds = xr.open_zarr(chunk_store)
        return cci_normalize(raw_ds, cube_id, cube_params, cci_odp)
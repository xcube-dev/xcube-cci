# The MIT License (MIT)
# Copyright (c) 2020 by the ESA CCI Toolbox development team and contributors
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

import json
import logging
import os
import os.path
import shutil
import time
import traceback
from typing import List, Optional

import click
import xarray as xr
import zarr.storage

from xcube.core.normalize import cubify_dataset
from xcube_cci.cciodp import CciOdp
from xcube_cci.chunkstore import CciChunkStore

DEFAULT_OUTPUT = 'open-report'


@click.command()
@click.option('--output', 'output_dir',
              metavar='OUTPUT_DIR',
              default=DEFAULT_OUTPUT,
              help=f'Output directory. Defaults to "{DEFAULT_OUTPUT}".')
@click.option('--force',
              is_flag=True,
              help=f'Force deletion of an existing OUTPUT_DIR.')
@click.option('--cache', 'cache_size',
              metavar='CACHE_SIZE',
              help=f'Cache size, e.g. "100M" or "2G". If given, an in-memory LRU cache will be used.')
@click.option('--observe',
              is_flag=True,
              help=f'Dump data requests. '
                   f'Note, turning this flag on will slightly increase measured durations.')
@click.argument('dataset_id', nargs=-1, required=False)
def gen_report(output_dir: str,
               force: bool,
               cache_size: Optional[str],
               observe: bool,
               dataset_id: List[str]):
    """
    Opens CCI ODP datasets and generates a report in OUTPUT_DIR from opening a dataset.

    If DATASET_ID are omitted, all ODP datasets are opened. Otherwise, only the given
    datasets will be opened. A DATASET_ID may be just a part of the actual dataset
    identifier to be selected, e.g. "CLOUD" or "5-day".

    For each selected dataset, the following reports are generated after opening it:

    \b
    * ${OUTPUT_DIR}/SUCCESS@${DATASET_ID}.json - on success only;
    * ${OUTPUT_DIR}/ERROR@${DATASET_ID}.json - on error only;
    * ${OUTPUT_DIR}/ERROR@${DATASET_ID}.txt - on error only, contains exception traceback.

    If the "--observe" flag is used, an additional report is generated comprising each
    individual data request made:

    * ${OUTPUT_DIR}/OBSERVED@${DATASET_ID}.txt - in any case.

    The program will exit with an error if OUTPUT_DIR exists. To force deletion of
    an existing OUTPUT_DIR, use the "--force" flag.

    """
    logging.basicConfig(format='%(asctime)s %(levelname)s: %(message)s',
                        level=logging.DEBUG,
                        datefmt='%Y-%m-%d %H:%M:%S')
    if os.path.isdir(output_dir):
        if force:
            shutil.rmtree(output_dir)
        else:
            raise click.ClickException(f'Output directory "{output_dir}" already exists.')
    if not os.path.isdir(output_dir):
        os.makedirs(output_dir)

    cache_size = parse_cache_size(cache_size)

    odp = CciOdp()

    selected_ds_ids = get_selected_dataset_ids(odp, dataset_id)

    logging.info(f'Running tests for {len(selected_ds_ids)} datasets...')

    id_to_ts = dict()

    for i, ds_id in enumerate(selected_ds_ids):
        data_type = ''
        observer, obs_fp = new_observer(output_dir, ds_id) if observe else (None, None)

        logging.info(f'Attempting to open #{i+1} of {len(selected_ds_ids)}: {ds_id}')

        t0 = time.perf_counter()

        try:
            store = CciChunkStore(odp, ds_id, observer=observer, trace_store_calls=True)
        except Exception as e:
            report_error(output_dir, ds_id, t0, 'CciChunkStore()', e, data_type)
            continue

        if cache_size > 0:
            store = zarr.storage.LRUStoreCache(store, max_size=cache_size)

        try:
            logging.info('Attempting to open zarr ...')
            ds = xr.open_zarr(store)
            data_type = 'dataset'
            report_success(output_dir, ds_id, t0, ds, data_type)
        except Exception as e:
            report_error(output_dir, ds_id, t0, 'xr.open_zarr()', e, data_type)

        id_to_ts[ds_id] = data_type

        if obs_fp is not None:
            obs_fp.close()

    report_type_specifiers(output_dir, ts_dict=id_to_ts)

    logging.info(f'Tests completed.')


def get_selected_dataset_ids(odp, ds_id_parts):
    all_ds_ids = odp.dataset_names
    if ds_id_parts:
        ds_id_parts_lc = [ds_id_part.lower() for ds_id_part in ds_id_parts]
        selected_ds_ids = set()
        for ds_id in all_ds_ids:
            ds_id_lc = ds_id.lower()
            if any(map(lambda ds_id_part: ds_id_part in ds_id_lc, ds_id_parts_lc)):
                selected_ds_ids.add(ds_id)
    else:
        selected_ds_ids = all_ds_ids
    selected_ds_ids = sorted(selected_ds_ids)
    return selected_ds_ids


def new_observer(output_dir, ds_id):
    path = os.path.join(output_dir, f'OBSERVED@{ds_id}.txt')
    fp = open(path, 'a')

    def observer(var_name: str = None, chunk_index=None, time_range=None, duration=None, exception=None):
        tr = f'{time_range[0]}/{time_range[1]}'
        data = f'{var_name};{chunk_index};{tr};{format_millis(duration)}'
        status = f'{type(exception).__name__}({exception})' if exception else 'OK'
        fp.write(f'{status};{data}\n')
        fp.flush()

    return observer, fp


def report_success(output_dir: str, ds_id: str, t0: float, ds: xr.Dataset, type_specifier: str):
    obj = ds_to_dict(ds_id, time.perf_counter() - t0, ds, type_specifier)
    path = os.path.join(output_dir, f'SUCCESS@{ds_id}.json')
    try:
        with open(path, 'w') as fp:
            json.dump(obj, fp, indent=2)
        logging.info(f'{ds_id} took {format_millis(obj["duration"])} seconds')
    except Exception as e:
        if os.path.isfile(path):
            os.remove(path)
        report_error(output_dir, ds_id, time.perf_counter() - t0, 'to_json()', e)


def report_error(output_dir: str, ds_id: str, t0: float, stage: str, e: Exception, type_specifier: str):
    obj = error_to_dict(ds_id, stage, time.perf_counter() - t0, e, type_specifier)
    json_path = os.path.join(output_dir, f'ERROR@{ds_id}.json')
    text_path = os.path.join(output_dir, f'ERROR@{ds_id}.txt')
    with open(json_path, 'w') as fp:
        json.dump(obj, fp, indent=2)
    with open(text_path, 'w') as fp:
        traceback.print_exc(file=fp)
    logging.error(f'{stage}: {ds_id} took {format_millis(obj["duration"])}: {e}')


def report_type_specifiers(output_dir: str, ts_dict: dict):
    json_path = os.path.join(output_dir, f'type_specifiers.json')
    with open(json_path, 'w') as fp:
        json.dump(ts_dict, fp, indent=2)


def ds_to_dict(ds_id: str, duration: float, ds: xr.Dataset, type_specifier: str) -> dict:
    return dict(ds_id=ds_id,
                status='ok',
                type_specifier=type_specifier,
                duration=duration,
                dataset=dict(sizes=dict(ds.sizes),
                             coord_vars=vars_to_list(ds.coords),
                             data_vars=vars_to_list(ds.data_vars),
                             attrs=dict(ds.attrs)))


def error_to_dict(ds_id: str, stage: str, duration: float, e: Exception, type_specifier: str) -> dict:
    return dict(ds_id=ds_id,
                typeSpecifier=type_specifier,
                status='error',
                duration=duration,
                error=dict(type=str(type(e)),
                           stage=stage,
                           message=f'{e}'))


def vars_to_list(vars) -> list:
    return [var_to_dict(k, v) for k, v in vars.items()]


def var_to_dict(name: str, var: xr.DataArray) -> dict:
    return dict(name=name,
                dtype=str(var.dtype),
                dims=list(var.dims),
                attrs=dict(var.attrs))


def parse_cache_size(cache_size: Optional[str]) -> int:
    if not cache_size:
        return 0
    cache_size_lc = cache_size.lower()

    def _parse_cache_size():
        for suffix, factor in (
                ('kib', 1024), ('mib', 1024 ** 2), ('gib', 1024 ** 3), ('tib', 1024 ** 4),
                ('b', 1), ('k', 1000), ('m', 1000 ** 2), ('g', 1000 ** 3), ('t', 1000 ** 4)):
            if cache_size_lc.endswith(suffix):
                return int(cache_size[0:-len(suffix)]) * factor
        return int(cache_size)

    try:
        size = _parse_cache_size()
        if size < 0:
            raise ValueError()
        return size
    except ValueError:
        raise click.ClickException(f'Invalid cache size: "{cache_size}"')


def format_millis(time: float) -> str:
    return f'{round(time * 1000)} ms'


if __name__ == '__main__':
    gen_report()

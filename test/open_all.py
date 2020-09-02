import json
import logging
import os
import os.path
import shutil
import sys
import time
import traceback
from typing import List

import xarray as xr

from xcube_cci.cciodp import CciOdp
from xcube_cci.chunkstore import CciChunkStore


def main(argv: List[str]):
    logging.basicConfig(format='%(asctime)s %(levelname)s: %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')
    output_dir = argv[1] if len(argv) == 2 else 'odp-report'
    if len(argv) > 2:
        raise ValueError('Unexpected arguments: ', argv[2:])
    if os.path.isdir(output_dir):
        shutil.rmtree(output_dir)
    if not os.path.isdir(output_dir):
        os.makedirs(output_dir)
    gen_report(output_dir)


def gen_report(output_dir: str):
    odp = CciOdp()

    dataset_names = odp.dataset_names

    logging.info(f'Running tests with {len(dataset_names)} datasets...')

    for ds_id in dataset_names:
        t0 = time.perf_counter()

        try:
            md = odp.get_dataset_metadata(ds_id)
            time_range = md['temporal_coverage_start'], md['temporal_coverage_end']
            variable_names = odp.var_names(ds_id)
            store = CciChunkStore(odp, ds_id, dict(variable_names=variable_names, time_range=time_range))
            # store = CciChunkStore(odp, ds_id)
        except Exception as e:
            report_error(output_dir, ds_id, t0, 'CciChunkStore()', e)
            continue

        try:
            ds = xr.open_zarr(store)
            report_success(output_dir, ds_id, t0, ds)
        except Exception as e:
            report_error(output_dir, ds_id, t0, 'xr.open_zarr()', e)

    logging.info(f'Tests completed.')


def report_success(output_dir: str, ds_id: str, t0: float, ds: xr.Dataset):
    obj = ds_to_dict(ds_id, time.perf_counter() - t0, ds)
    path = os.path.join(output_dir, f'success_{ds_id}.json')
    try:
        with open(path, 'w') as fp:
            json.dump(obj, fp, indent=2)
        logging.info(f'{ds_id} took {format_millis(obj["duration"])} seconds')
    except Exception as e:
        if os.path.isfile(path):
            os.remove(path)
        report_error(output_dir, ds_id, time.perf_counter() - t0, 'to_json()', e)


def report_error(output_dir: str, ds_id: str, t0: float, stage: str, e: Exception):
    obj = error_to_dict(ds_id, stage, time.perf_counter() - t0, e)
    json_path = os.path.join(output_dir, f'error_{ds_id}.json')
    text_path = os.path.join(output_dir, f'error_{ds_id}.txt')
    with open(json_path, 'w') as fp:
        json.dump(obj, fp, indent=2)
    with open(text_path, 'w') as fp:
        traceback.print_exc(file=fp)
    logging.error(f'{stage}: {ds_id} took {format_millis(obj["duration"])}: {e}')


def ds_to_dict(ds_id: str, duration: float, ds: xr.Dataset) -> dict:
    return dict(ds_id=ds_id,
                status='ok',
                duration=duration,
                dataset=dict(dims=list(ds.dims),
                             coord_vars=vars_to_list(ds.coords),
                             data_vars=vars_to_list(ds.data_vars),
                             attrs=dict(ds.attrs)))


def error_to_dict(ds_id: str, stage: str, duration: float, e: Exception) -> dict:
    return dict(ds_id=ds_id,
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
                dims=dict(var.dims),
                attrs=dict(var.attrs))


def format_millis(time: float) -> str:
    return f'{round(time * 1000)} ms'


if __name__ == '__main__':
    main(sys.argv)

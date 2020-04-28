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

from typing import Any, Dict, Optional, List, Tuple

import click

from xcube_cci.version import version

DEFAULT_GEN_OUTPUT_PATH = 'out.zarr'


@click.command(name="gen",
               # Required for parsing geometry bbox with negative coordinates
               context_settings={"ignore_unknown_options": True})
@click.argument('request', default=None, type=str, required=False)
# @click.option('--dataset', '-d', 'dataset_name',
#               help='Dataset name. The name of a valid ESA CCI ODP dataset.')
# @click.option('--band', '-b', 'band_names',
#               help='Band name. The name of a band in given dataset. '
#                    'Can be repeated for multiple bands. Defaults to all bands of a dataset.',
#               multiple=True)
# @click.option('--tile_size', '--tile-size',
#               help='Tile size given as number of grid cells using format "<size>" or "<width>,<height>".')
# @click.option('--geom', '-g', 'geometry',
#               help='Geometry WKT, GeoJSON object, or bounding box using format "<lon1>,<lat1>,<lon2>,<lat2>". '
#                    'Coordinates must be in decimal degree.')
# @click.option('--time', '-t', 'time_range',
#               help='Time or time range using format "date" or "<first-date>,<last-date>".')
# @click.option('--period', '-p', 'time_period',
#               help='Time (aggregation) period. Format is "<period>" or "<num><period>" '
#                    'where <num> is a positive integer and <period> is one of "H", "D", "W", "Y". '
#                    'Defaults to a period suitable for the dataset.')
# @click.option('--output', '-o', 'output_path',
#               help=f'Output ZARR directory. Defaults to "{DEFAULT_GEN_OUTPUT_PATH}".')
# @click.option('--4d', 'four_d',
#               is_flag=True,
#               help='Write a single data 4D array "band_data" to the output. '
#                    'Will slightly increase execution speed. '
#                    'By default, bands are written to separate 3D arrays, e.g. "B01", "B02".')
# @click.option('--verbose', '-v',
#               is_flag=True,
#               help='Print information about each single ESA CCI ODP request to stdout.')
def gen(request: Optional[str] = None,
        dataset_name: Optional[str] = None,
        var_names: Optional[Tuple] = None,
        tile_size: Optional[str] = None,
        geometry: Optional[str] = None,
        time_range: Optional[str] = None,
        time_period: Optional[str] = None,
        output_path: Optional[str] = None,
        four_d: bool = False,
        verbose: bool = False
        ):
    """
    Generate a data cube from the ESA CCI Open Data Portal.

    By default, the command will create a Zarr dataset with 3D arrays
    for each variable e.g. "surface_pressure" with dimensions "time", "lat", "lon".

    REQUEST may have JSON or YAML format.
    You can also pipe a JSON request into this command.
    """
    import json
    import os.path
    import sys
    import xarray as xr
    from xcube.core.dsio import write_dataset
    from xcube.util.perf import measure_time
    from xcube_cci.config import CubeConfig
    from xcube_cci.observers import Observers
    from xcube_cci.cciodp import CciOdp
    from xcube_cci.store import CciStore

    if request:
        request_dict = _load_request(request)
    elif not sys.stdin.isatty():
        request_dict = json.load(sys.stdin)
    else:
        request_dict = {}

    cube_config_dict = request_dict.get('cube_config', {})
    _overwrite_config_params(cube_config_dict,
                             dataset_name=dataset_name,
                             var_names=var_names if var_names else None,  # because of multiple=True
                             tile_size=tile_size,
                             geometry=geometry,
                             time_range=time_range,
                             time_period=time_period,
                             four_d=four_d)

    input_config_dict = request_dict.get('input_config', {})
    # TODO: validate input_config_dict

    output_config_dict = request_dict.get('output_config', {})
    _overwrite_config_params(output_config_dict,
                             path=output_path)
    # TODO: validate output_config_dict

    cube_config = CubeConfig.from_dict(cube_config_dict,
                                       exception_type=click.ClickException)

    if 'path' in output_config_dict:
        output_path = output_config_dict.pop('path')
    else:
        output_path = DEFAULT_GEN_OUTPUT_PATH
    if not _is_bucket_url(output_path) and os.path.exists(output_path):
        raise click.ClickException(f'Output {output_path} already exists. Move it away first.')

    cci_odp = CciOdp()

    print(f'Writing cube to {output_path}...')

    with measure_time() as cm:
        store = CciStore(cci_odp, cube_config)
        request_collector = Observers.request_collector()
        store.add_observer(request_collector)
        if verbose:
            store.add_observer(Observers.request_dumper())
        cube = xr.open_zarr(store)
        if _is_bucket_url(output_path):
            client_kwargs = {k: output_config_dict.pop(k)
                             for k in ('provider_access_key_id', 'provider_secret_access_key')
                             if k in output_config_dict}
            write_dataset(cube, output_path, format_name='zarr', client_kwargs=client_kwargs, **output_config_dict)
        else:
            write_dataset(cube, output_path, **output_config_dict)

    print(f"Cube written to {output_path}, took {'%.2f' % cm.duration} seconds.")

    if verbose:
        request_collector.stats.dump()


@click.command(name="req")
@click.option('--output', '-o', 'output_path', metavar='OUTPUT',
              help="A configuration JSON or YAML file to which the request template will be written. "
                   f"Defaults to stdout.")
@click.option('--s3', 'is_s3_config', is_flag=True,
              help="Whether to create an example request that would write to a user defined AWS S3 bucket.")
def req(output_path: str,
        is_s3_config: bool):
    """
    Write a request template file.
    The generated file will use default or example values for all request parameters.
    It should be edited and can then be passed as argument to the "xcube cci gen" command:

    \b
        $ xcube sg req -o request.json
        $ vi request.json
        $ xcube sh gen request.json
    """
    import json
    import os.path
    import sys
    import yaml

    input_config = dict(
    )
    cube_config = dict(
        dataset_name='esacci.OZONE.month.L3.NP.multi-sensor.multi-platform.MERGED.fv0002.r1',
        variable_names=['surface_pressure', 'O3e_du_tot'],
        tile_size=[20, 20],
        geometry=[7.0, 53.00, 9.0, 55.0],
        time_range=['1997-01-01', '1997-12-01'],
        time_period='8D',
    )
    if is_s3_config:
        output_config = dict(
            path='https://s3.amazonaws.com/MY_BUCKET/MY_CUBE_NAME.zarr',
            provider_access_key_id='MY_AWS_S3_ACCESS_KEY_ID',
            provider_secret_access_key='MY_AWS_S3_SECRET_ACCESS_KEY',
        )
    else:
        output_config = dict(
            path=DEFAULT_GEN_OUTPUT_PATH
        )
    config_dict = dict(input_config=input_config,
                       cube_config=cube_config,
                       output_config=output_config)
    if output_path:
        if os.path.exists(output_path):
            raise click.ClickException(f'Output {output_path} already exists. Move it away first.')
        with open(output_path, 'w') as fp:
            if output_path.endswith('.json'):
                json.dump(config_dict, fp, indent=2)
            else:
                yaml.dump(config_dict, fp, indent=2)
    else:
        json.dump(config_dict, sys.stdout, indent=2)


@click.command(name="info")
@click.argument('datasets', nargs=-1)
def info(datasets: List[str] = None):
    """
    Print CCI Opendata Portal metadata info.
    The list of available dataset names are returned.
    """
    from xcube_cci.cciodp import CciOdp

    cci_odp = CciOdp()
    import json
    if not datasets:
        response = dict(datasets=cci_odp.dataset_names)
    else:
        response = dict()
        for dataset_name in datasets:
            var_names = cci_odp.var_names(dataset_name)
            vars = dict()
            for var_name in var_names:
                # todo read variable metadata
                vars[var_name] = {}
            response[dataset_name] = vars
    print(json.dumps(response, indent=2))


# noinspection PyShadowingBuiltins,PyUnusedLocal
@click.group(name="cci")
@click.version_option(version)
def cli():
    """
    CCI Open Data Portal tools for xcube.
    """


cli.add_command(gen)
cli.add_command(req)
cli.add_command(info)


def _load_request(request_file: Optional[str]) -> Dict[str, Any]:
    import json
    import os.path
    import sys
    import yaml

    if request_file and not os.path.exists(request_file):
        raise click.ClickException(f'Configuration file {request_file} not found.')

    try:
        if not request_file:
            if not sys.stdin.isatty():
                return json.load(sys.stdin)
            else:
                return {}
        with open(request_file, 'r') as fp:
            if request_file.endswith('.json'):
                return json.load(fp)
            else:
                return yaml.safe_load(fp)
    except BaseException as e:
        raise click.ClickException(f'Error loading configuration file {request_file}: {e}')


def _overwrite_config_params(config: Dict[str, Any], **config_updates):
    config.update({k: v for k, v in config_updates.items() if v is not None})


def _is_bucket_url(path: str):
    url_parts = path.split('://')
    return len(url_parts) >= 2 \
           and len(url_parts[0]) > 0 \
           and len(url_parts[1]) > 0 \
           and url_parts[0].isidentifier()

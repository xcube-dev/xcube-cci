[![Build Status](https://travis-ci.com/dcs4cop/xcube-cci.svg?branch=master)](https://travis-ci.com/dcs4cop/xcube-cci)

# xcube-cci

An [xcube plugin](https://xcube.readthedocs.io/en/latest/plugins.html) that allows 
generating data cubes from the ESA CCI Open Data Portal.

### Installing the xcube-cci plugin

#### Installation into a new environment with conda

xcube-cci and all necessary dependencies (including xcube itself) are available
on [conda-forge](https://conda-forge.org/), and can be installed using the
[conda package manager](https://docs.conda.io/projects/conda/en/latest/).
The conda package manager itself can be obtained in the [miniconda
distribution](https://docs.conda.io/en/latest/miniconda.html). 
Once conda is installed, xcube-cci can be installed like this:

```
$ conda create --name xcube-cci-environment --channel conda-forge xcube-cci
$ conda activate xcube-cci-environment
```

The name of the environment may be freely chosen.

#### Installation into an existing environment with conda

xcube-cci can also be installed into an existing conda environment.
With an existing conda environment activated, execute this command:

```
$ conda install --channel conda-forge xcube-cci
```

xcube and any other necessary dependencies will be installed or updated if they are not 
already installed in a compatible version.

#### Installation into an existing environment from the repository

If you want to install xcube-cci directly from the git repository (for example
in order to use an unreleased version or to modify the code), you can do so as follows:

```
$ git clone https://github.com/dcs4cop/xcube-cci.git
$ cd xcube-ccs
$ conda env create
$ conda activate xcube-cci
$ python setup.py develop
```

## Testing

You can run the unit tests for xcube-cci by executing

```
$ pytest
```

in the `xcube-cci` repository. Note that, in order to successfully run the
tests using the current repository version of xcube-cci, you may also need to
install the repository version of xcube rather than its latest conda-forge
release.

To create a test coverage report, you can use

```
coverage run --include='xcube_cci/**' --module pytest
coverage html
```

This will write a coverage report to `htmlcov/index.html`.

## Use

Jupyter notebooks demonstrating the use of the xcube-cds plugin can be found
in the `examples/notebooks/` subdirectory of the repository.

## Releasing

To release `xcube-cci`, please follow the steps outlined in the 
[xcube Developer Guide](https://github.com/dcs4cop/xcube/blob/master/docs/source/devguide.md#release-process).

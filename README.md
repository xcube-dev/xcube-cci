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

## Preparing a release

This section is intended for developers preparing a new release of xcube-cci.

### Pre-release tasks

 - Make sure that all unit tests pass and that test coverage is 100% (or
   as near to 100% as practicable).
 - Remove any pre-release (‘dev’, ‘rc’ etc.) suffix from the version number in
   `xcube_cci/version.py`.
 - Make sure that the readme and changelog are up to date. Remove any
   pre-release suffix from the current (first) section title of the changelog.

### Making a GitHub release

Create a release tag on GitHub.

 - Tag version name should be the version number prefixed by ‘v’.
 - Release title should be version name without the 'v' prefix.
 - Description should be a list of changes in this version (pasted in
   from most recent section of changelog).
   
Creating the release will automatically produce a source code archive as an
associated asset, which is needed to create the conda package.

### Updating the conda package

These instructions are based on the documentation at
https://conda-forge.org/docs/maintainer/updating_pkgs.html .

Conda-forge packages are produced from a github feedstock repository belonging
to the conda-forge organization. 
The feedstock for xcube-cci is at https://github.com/conda-forge/xcube-cci-feedstock.  
The package is updated by forking this repository, creating a new branch for the 
changes, and creating a pull request to merge this branch into conda-forge's feedstock
repository. 
dcs4cop's fork is at https://github.com/dcs4cop/xcube-cci-feedstock . 
In detail, the steps are:

1. Update the [dcs4cop fork](https://github.com/dcs4cop/xcube-cci-feedstock)
   of the feedstock repository, if it's not already up to date with
   conda-forge's upstream repository.

2. Rerender the feedstock using `conda-smithy`. This updates common conda-forge
   feedstock files. It's probably easiest to install `conda-smithy` in a fresh
   environment for this.
   
   ```
   conda install -c conda-forge conda-smithy
   conda smithy rerender -c auto
   ```
   
   It's also possible to have the rendering done by a bot as part of the pull
   request, but this doesn't seem to work very reliably in practice.

3. Clone the repository locally and create a new branch. The name of the branch
   is not strictly prescribed, but it's sensible to choose an informative
   name like `update_0_5_3`.

4. Update `recipe/meta.yaml` for the new version. Mainly this will involve the 
   following steps:
   
   1. Update the value of the `version` variable (or, if the version number
      has not changed, increment the build number).
   
   2. If the version number *has* changed, ensure that the build number is
      set to 0.
   
   3. Update the sha256 hash of the source archive prepared by GitHub.
   
   4. If the dependencies have changed, update the list of dependencies in the
      `-run` subsection to match those in the `environment.yml` file.

   5. Commit the changes, push them to GitHub, and create a pull request at
      https://github.com/dcs4cop/xcube-cci-feedstock .

   6. Once conda-forge's automated checks have passed, merge the pull request.

Once the pull request has been merged, the updated package should usually 
become available from conda-forge within a couple of hours.


### Post-release tasks

 - Update the version number in `xcube_cci/version.py` to a "dev0" derivative 
   of the next planned release number. 
   For example, if version 0.5.1 has just been released and the next version is 
   planned to be 0.5.2, the version number should be set to 0.5.2.dev0.
   Always increase the patch number.

 - Add a new first section to the changelog with the new version number.

# xcube-cci

An [xcube plugin]() that allows generating data cubes from the ESA CCI Open Data Portal.

## Setup

First install [`xcube`](https://github.com/dcs4cop/xcube), 
then the `xcube_cci` plugin.

### Install xcube

Once xcube 0.5 becomes available on [conda-forge](https://conda-forge.org/), 
you can install it with
    
    $ conda create --name xcube xcube>=0.5
    $ conda activate xcube

If you prefer to build `xcube` from source, use:

    $ git clone https://github.com/dcs4cop/xcube.git
    $ cd xcube
    $ conda env create
    $ conda activate xcube
    $ python setup.py develop
        
### Install xcube_cci

While `xcube_cci` is not yet available from conda-forge, install it from sources. 
We'll need to update the `xcube` environment first, then install `xcube_cci`:

    $ conda activate xcube
    
    $ git clone https://github.com/dcs4cop/xcube-cci.git
    $ cd xcube-cci
    $ python setup.py develop

Once `xcube_cci` is available from conda-forge:

    $ conda activate xcube
    $ conda install -c conda-forge xcube_cci

### Test:

You can run the unit tests for `xcube_cci` by executing

```
$ pytest
```

in the `xcube-cci` repository.
    
## Tools

Check available xcube CLI extensions added by `xcube_cci` plugin:

    $ xcube cci --help
    $ xcube cci gen --help
    $ xcube cci info --help

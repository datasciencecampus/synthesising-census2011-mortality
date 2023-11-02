# `cenmort`: synthesising the census-mortality dataset

This repository contains the source code required to create a differentially
private, synthetic version of the census-mortality linked dataset using a
generalised version of the winning method from the 2018 NIST competition,
`MST`. Details of the method are available in https://doi.org/10.29012/jpc.778.

The published implementation of `MST` uses a Python library called `mbi`, which
relies on members of the usual Python scientific stack: NumPy and Pandas.
However, to synthesise a dataset at the scale we consider here (~54M rows), we
require tools designed to work at scale. This implementation achieves this
scalability by adapting aspects of the `MST` code base to use PySpark.

Details on how this directory is structured can be found in `STRUCTURE.md`.


## Installation

To access the source code in this repository, you will need to clone it and
install the package locally:

```bash
$ git clone http://gitlab-01-01/syntdatamort/syntdatamort.git
$ cd syntdatamort
$ python -m pip install .
```

These commands will install the `cenmort` package and most of its dependencies,
which are listed in `requirements.txt`. Note that you may need to use `python3`
if your default Python command points at Python 2. You can check by running
`python --version`. If you intend to do any development on `cenmort`, be sure
to install it as editable: `python -m pip install -e .`.

As well as this, you will need to install the `mbi` package, which is available
[on GitHub](https://github.com/ryan112358/private-pgm). If you are working in
DAP, you will have to import this repository manually into your local project
space before installing it as you did with the `cenmort` package above.

The final dependency required to run the code in this repository is PySpark.
The source code for `cenmort` was developed using `pyspark@2.4.8` but `2.4.x`
should work without issue. If you are working in DAP, PySpark is already
installed and cannot be updated.


## Setting up your environment

To use this repository, you must set up a number of environment variables. The
purpose of these is to avoid hard-coding secrets into the source code. In
particular, you must define the following:

- `CENMORT_HOME`: the location of the `syntdatamort` repository. Used for
  storing log files locally.
- `WORKSPACE`: the location of your working space, where data and objects will
  be stored. For use in DAP, point this at the context's workspace.
- `PYSPARK_PYTHON` and `PYSPARK_DRIVER_PYTHON`: a Python (3.6+) executable that
  is visible to all nodes on your cluster. The version of Python must include
  installations of `pyarrow==0.14.1`, `numpy==1.19.5` and `pandas==1.1.5`.
  Distributing these dependencies allows for the use of vectorised UDFs in
  PySpark. For use in DAP, point this at the `miscMods_v4.04` Python 3 command.
- `EPSILON`: a number defining the primary privacy-loss parameter. If not set,
  we use `1` by default.
- `DELTA`: a number defining the secondary privacy-loss parameter. If not set,
  we use `1e-9` by default.
- `ROWS` or `CHUNK`: the number of rows to synthesise in each chunk. By
  default, chunks are made up of 10,000 records. This parameter is limited by
  the memory available on the driver node and worker nodes, so please adjust
  accordingly. Note that smaller chunks suffer from lower utility.
- `ESTIMATOR_SEED`: random seed to use for fitting the estimator model. By
  default, no seed is used and results are not reproducible.
- `TOTAL`: the total number of rows in the final synthetic dataset. If you do
  not set this variable (or specify it on the command line) then the synthetic
  dataset will be the same size as the original dataset. 

Further customisation (e.g., of the Spark sessions) can be controlled by
editing the requisite job script(s) directly.


## Synthesised columns

To improve the quality of the final synthetic dataset, and to ease writing this
implementation, we do not synthesise every column from the original dataset.

A list of included columns is available in `GUIDE.md`.

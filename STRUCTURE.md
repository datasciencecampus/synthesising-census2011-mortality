# Structure

This directory is separated into three main components: `jobs`, `logs` and
`src`. There are also a number of top-level files used for configuration.

The `jobs` directory contains the job scripts used to synthesise the data. Each
script corresponds to a step in synthesising the data: preprocessing,
estimating, synthesising and postprocessing.

We utilise the `logging` package to maintain a record of any job run; these
records are stored in the `logs` directory. Each record is stored and named
according to the job in question and the timestamp of its execution.

To make the job scripts more readable, some of the core functionality of the
code base is modularised out into the `cenmort` package, which is stored in
`src`. Within `src/cenmort`, there is a separate module for command-line
argument parsers and a utility module for common functions. There are also
sub-packages for the mechanism-specific code and functions for jobs. The `MST` module contains a 
PySpark-compatible implementation of MST. Inspired by the NIST competition 
winning code. The `cdp2adp.py` module has been lifted from 
(https://github.com/IBM/discrete-gaussian-differential-privacy).


The full directory tree (as of v0.4.0) is as follows:

```
syntdatamort/
├── CHECKLIST.md
├── GUIDE.md
├── jobs/
│   ├── 01-preprocess.py
│   ├── 02-estimate.py
│   ├── 03-synthesise.py
|   ├── 04-postprocess.py
│   └── 05-analyse.py
├── LICENSE
├── logs/
│   └── README.md
├── pyproject.toml
├── README.md
├── STRUCTURE.md
├── requirements.txt
├── setup.py
└── src/
    └── cenmort/
        ├── jobs/
        │   ├── __init__.py
        │   ├── estimate.py
        │   ├── postprocess.py
        │   ├── preprocess.py
        │   └── synthesise.py
        ├── mechanisms/
        │   ├── __init__.py
        │   ├── cdp2adp.py
        │   └── mst.py
        ├── utility/
        │   ├── __init__.py
        │   └── generic.py
        ├── __init__.py
        ├── parser.py
        └── tools.py
```
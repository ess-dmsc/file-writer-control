# File-writer-control
This is a library for controlling the [ESS HDF5/NeXus file-writer application](https://github.com/ess-dmsc/kafka-to-nexus).
The file-writer is controlled by sending commands to it via an Apache Kafka broker.
This library implements the encoding/decoding of commands as well as an abstraction of this interface in order to simplify
the commands and control.

## Getting started

You can install the library with pip by running
```bash
pip install file-writer-control
```

The _examples_ directory contains different examples of how to use the library - we recommend you start there.

## Installing dependencies

This library uses the [_kafka-python_](https://kafka-python.readthedocs.io/en/master/index.html) library for the
communication with the Kafka broker and the [_python-streaming-data-types_](https://github.com/ess-dmsc/python-streaming-data-types)
for serialising and de-serialising messages to and from the filewriter. These dependencies can be installed by executing
the following command in the root of the repository:

```bash
pip install -r requirements.txt
```

Alternatively, to install the dependencies such that they are only available to the current user, execute the following command:

```bash
pip install --user -r requirements.txt
```

**Note:** This library was developed using Python 3.8 but it is likely that it will work with Python 3.6 and up.

## Running the unit tests
For running the unit tests, execute the following command in the root of this repository:

```bash
python -m pytest -s .
```

## Installing the development version locally

First, uninstall any existing versions of this library:

```bash
pip uninstall file-writer-control
```

Then, from the *file-writer-control* root directory, run the following command:

```bash
pip install --user -e ./
```

## Uploading package to *pypi*

```bash
rm -rf build dist
```

```bash
python setup.py sdist bdist_wheel
```

```bash
twine check dist/*
```

```bash
twine upload dist/*
```

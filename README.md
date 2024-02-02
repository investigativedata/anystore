[![anystore on pypi](https://img.shields.io/pypi/v/anystore)](https://pypi.org/project/anystore/)
[![Python test and package](https://github.com/investigativedata/anystore/actions/workflows/python.yml/badge.svg)](https://github.com/investigativedata/anystore/actions/workflows/python.yml)
[![pre-commit](https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit)](https://github.com/pre-commit/pre-commit)
[![Coverage Status](https://coveralls.io/repos/github/investigativedata/anystore/badge.svg?branch=main)](https://coveralls.io/github/investigativedata/anystore?branch=main)
[![GPL-3.0 License](https://img.shields.io/pypi/l/anystore)](./LICENSE)

# anystore

Store anything anywhere. A wrapper around wrappers to avoid boilerplate code (because we are lazy).

`anystore` helps you to transfer data from and to a various range of sources (local filesystem, http, s3, redis, sql, ...) with a unified high-level interface. It's main use case is to store data pipeline outcomes in a distributed cache, so that different programs or coworkers can access intermediate results based on different settings (e.g. testing: use local cache store, production: cache to s3 bucket)

### Why?

[In our several data engineering projects](https://investigativedata.io/#projects) we always wrote boilerplate code that handles the featureset of `anystore` but not in a reusable way.

This library shall be a thin and stable foundation for data wrangling related python programs.

## Overview

`anystore` is built on top of [`fsspec`](https://filesystem-spec.readthedocs.io/en/latest/index.html) and provides an easy wrapper for reading and writing content from and to arbitrary locations:

### command line

```bash
anystore -i ./local/foo.txt -o s3://mybucket/other.txt

echo "hello" | anystore -o sftp://user:password@host:/tmp/world.txt

anystore -i https://investigativedata.io > index.html
```

### python

```python
from anystore.io import smart_read, smart_write

data = smart_read("s3://mybucket/data.txt")
smart_write(".local/data", data)
```

## Simple key/value store

`anystore` can use a configurable store:

### command line

```bash
anystore --store .cache put foo "bar"

anystore --store .cache get foo
# "bar"
```

### python

```python
from anystore import Store

# pass through `fsspec` configuration for specific storage backend:
store = Store(uri="s3://mybucket/data", backend_config={"client_kwargs":{
    "aws_access_key_id": "my-key",
    "aws_secret_access_key": "***",
    "endpoint_url": "https://s3.local"
}})

store.get("/2023/1.txt")
store.put("/2023/2.txt", my_data)
```

## Decorate your functions

When working on scripts, one sometimes wants just a simple cache setup. Maybe it should be persistent, maybe even somewhere in the cloud so that another coworker can take over. Maybe we want a different storage during testing our scripts... everything easily handled by `anystore`:

```python
from anystore import anycache

# use decorator
@anycache(uri="s3://mybucket/cache")
def download_file(url):
    # a very time consuming task
    return result

# 1. time: slow
res = download_file("https://example.com/foo.txt")

# 2. time: fast, as now cached
res = download_file("https://example.com/foo.txt")
```

## Install

    pip install anystore


## development

This package is using [poetry](https://python-poetry.org/) for packaging and dependencies management, so first [install it](https://python-poetry.org/docs/#installation).

Clone this repository to a local destination.

Within the root directory, run

    poetry install --with dev

This installs a few development dependencies, including [pre-commit](https://pre-commit.com/) which needs to be registered:

    poetry run pre-commit install

Before creating a commit, this checks for correct code formatting (isort, black) and some other useful stuff (see: `.pre-commit-config.yaml`)

### test

    make test

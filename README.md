[![anystore on pypi](https://img.shields.io/pypi/v/anystore)](https://pypi.org/project/anystore/)
[![Python test and package](https://github.com/investigativedata/anystore/actions/workflows/python.yml/badge.svg)](https://github.com/investigativedata/anystore/actions/workflows/python.yml)
[![pre-commit](https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit)](https://github.com/pre-commit/pre-commit)
[![Coverage Status](https://coveralls.io/repos/github/investigativedata/anystore/badge.svg?branch=main)](https://coveralls.io/github/investigativedata/anystore?branch=main)
[![GPL-3.0 License](https://img.shields.io/pypi/l/anystore)](./LICENSE)

# anystore

Store anything anywhere. A wrapper around wrappers to avoid boilerplate code (because we are lazy).

`anystore` helps you to transfer data from and to a various range of sources (local filesystem, http, s3, redis, sql, ...) with a unified interface. It's main use case is to store data pipeline outcomes in a distributed cache, so that different programs or coworkers can access intermediate results.

## Install

    pip install anystore

## Usage

```python
from anystore import Store

store = Store()

assert store.get("foo/bar.txt", raise_on_nonexist=False) is None

store.set("foo/bar.txt", "Hello world")
assert store.get("foo/bar.txt") == "Hello world"
```

It comes with a handy decorator:


```python
from anystore import anycache

# use decorator
@anycache(storage="s3://mybucket/cache")
def download_file(url):
    # a very time consuming task
    return result

# 1. time: slow
res = download_file("https://example.com/foo.txt")

# 2. time: fast, as now cached
res = download_file("https://example.com/foo.txt")
```


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

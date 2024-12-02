## Install

`anystore` requires python 3.11 or later.

    pip install anystore

Verify it is working:

    anystore --help

## command line

Without further [configuration](./configuration.md), `anystore` uses a file backend in the current working directory with the path `./.anystore`.

Store a simple key value pair via the command line:

```bash
echo "world" | anystore put "hello"
```

Retrieve the value for the key:

```bash
anystore get "hello"
# world
```

List all the keys in the store:

```bash
anystore keys
```

See [configuration](./configuration.md) and [store usage](./usage.md) for more documentation about different store backends.

### generic io

`anystore` is built on top of [`fsspec`](https://filesystem-spec.readthedocs.io/en/latest/index.html) and provides an easy wrapper for reading and writing content from and to arbitrary locations using the `io` command:

```bash
anystore io -i ./local/foo.txt -o s3://mybucket/other.txt

echo "hello" | anystore io -o sftp://user:password@host:/tmp/world.txt

anystore io -i https://investigativedata.io > index.html
```

See `anystore io --help` for details.

## python

`anystore` is designed as a base layer for python programs dealing with data streaming from and to arbitrary backends using a configurable `Store`:

```python
from anystore import get_store

# pass through `fsspec` configuration for specific storage backend:
store = get_store(uri="s3://mybucket/data", backend_config={"client_kwargs":{
    "aws_access_key_id": "my-key",
    "aws_secret_access_key": "***",
    "endpoint_url": "https://s3.local"
}})

content = store.get("2023/1.txt")

store.put("2023/2.txt", "other content")

txt_blobs = store.iterate_keys(glob="**/*.txt")
```

### generic io

Simple helper functions for reading and writing to arbitrary backends:

```python
from anystore import smart_read, smart_write

data = smart_read("s3://mybucket/data.txt")
smart_write(".local/data", data)
```

[Reference](./reference/io.md)

### streaming

When dealing with more data (e.g., using `anystore` [as a file storage](./storage.md)), keys can be _opened_ similar to `open()`:

```python
from anystore import get_store

source = get_store("./local_data")
target = get_store("s3://mybucket")

for path in source.iterate_keys():
    # streaming copy:
    with source.open(path) as i:
        with target.open(path, "wb") as o:
            i.write(o.read())
```

Stream a key's content line by line:

```python
import orjson
from anystore import smart_stream

while data := smart_stream("s3://mybucket/data.json"):
    yield orjson.loads(data)
```

[Head over to overview](./overview.md)

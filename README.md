# MyScale Database Client

[![PyPI version](https://badge.fury.io/py/myscaledb-client.svg)](https://badge.fury.io/py/myscaledb-client)
[![Documentation Status](https://readthedocs.org/projects/myscaledb-client/badge/?version=latest)](https://myscaledb-client.readthedocs.io/en/latest/?badge=latest)

`myscaledb-client` is an async/sync http(s) MyScale client for python 3.6+ supporting
type conversion in both directions, streaming, lazy decoding on select queries,
and a fully typed interface.

## Table of Contents

- [MyScale Database Client](#myscale-database-client)
  - [Table of Contents](#table-of-contents)
  - [Installation](#installation)
  - [Quick Start](#quick-start)
    - [Connecting to MyScale](#connecting-to-myscale)
    - [Querying the database](#querying-the-database)
  - [Documentation](#documentation)

## Installation

We recommend you to install it with command:

```bash
pip install myscaledb-client
```

## Quick Start

### Connecting to MyScale

The quickest way to get up and running with `myscaledb-client` is to simply connect
and check MyScale is alive.

```python
# This is a demo using Client.
# Client works in sync mode and is simple to use.

from myscaledb import Client

def main():
    client = Client(url='http://localhost:8123')
    alive = client.is_alive()
    print(f"Is MyScale alive? -> {alive}")

if __name__ == '__main__':
    main()
```

### Querying the database

Create a table with 4 dimensional vectors:

```python
client.execute(
    """CREATE TABLE default.test
    (
        id UInt64,
        name String,
        vector FixedArray(Float32, 4)
    )
    ENGINE = MergeTree ORDER BY id"""
)
```

View all tables in current database:

```python
res = client.fetch(query="show tables")
print([row[0] for row in res])
```

## Documentation

To check out the [API docs](https://myscaledb-client.readthedocs.io/en/latest/introduction.html), visit the [readthedocs site](https://myscaledb-client.readthedocs.io/en/latest/introduction.html).

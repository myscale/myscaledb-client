# MyScale Database Client

[![PyPI version](https://badge.fury.io/py/myscaledb-client.svg)](https://badge.fury.io/py/myscaledb-client)
[![Documentation Status](https://readthedocs.org/projects/myscaledb-client/badge/?version=latest)](https://myscaledb-client.readthedocs.io/en/latest/?badge=latest)

`myscaledb-client` is an async/sync http(s) MyScale client for python 3.6+ supporting
type conversion in both directions, streaming, lazy decoding on select queries,
and a fully typed interface.

## Table of Contents

- [Installation](#installation)
- [Quick Start](#quick-start)
- [Documentation](#documentation)
- [Type Conversion](#type-conversion)
- [Connection Pool Settings](#connection-pool-settings)
- [Notes on Speed](#notes-on-speed)

## Installation

You can use it with either
[aiohttp](https://github.com/aio-libs/aiohttp) or
[httpx](https://github.com/encode/httpx) http connectors.

To use with `aiohttp` install it with command:

```bash
> pip install aiochclient[aiohttp]
```

Or `aiochclient[aiohttp-speedups]` to install with extra speedups.

To use with `httpx` install it with command:

```bash
> pip install aiochclient[httpx]
```

Or `aiochclient[httpx-speedups]` to install with extra speedups.

Installing with `[*-speedups]` adds the following:

- [cChardet](https://pypi.python.org/pypi/cchardet) for `aiohttp` speedup
- [aiodns](https://pypi.python.org/pypi/aiodns) for `aiohttp` speedup
- [ciso8601](https://github.com/closeio/ciso8601) for ultra-fast datetime
  parsing while decoding data from ClickHouse for `aiohttp` and `httpx`.

Additionally the installation process attempts to use Cython for a speed boost
(roughly 30% faster).

## Quick Start

### Connecting to ClickHouse

`aiochclient` needs `aiohttp.ClientSession` or `httpx.AsyncClient` to connect to ClickHouse:

```python
from myscaledb import AsyncClient
from aiohttp import ClientSession


async def main():
  async with ClientSession() as s:
    client = AsyncClient(s)
    assert await client.is_alive()  # returns True if connection is Ok

```

### Querying the database

```python
await client.execute(
    "CREATE TABLE t (a UInt8, b Tuple(Date, Nullable(Float32))) ENGINE = Memory"
)
```

For INSERT queries you can pass values as `*args`. Values should be
iterables:

```python
await client.execute(
    "INSERT INTO t VALUES",
    (1, (dt.date(2018, 9, 7), None)),
    (2, (dt.date(2018, 9, 8), 3.14)),
)
```

For fetching all rows at once use the
[`fetch`](https://aiochclient.readthedocs.io/en/latest/api.html#aiochclient.ChClient.fetch)
method:
```python
all_rows = await client.fetch("SELECT * FROM t")
```

For fetching first row from result use the
[`fetchrow`](https://aiochclient.readthedocs.io/en/latest/api.html#aiochclient.ChClient.fetchrow)
method:
```python
row = await client.fetchrow("SELECT * FROM t WHERE a=1")

assert row[0] == 1
assert row["b"] == (dt.date(2018, 9, 7), None)
```

You can also use
[`fetchval`](https://aiochclient.readthedocs.io/en/latest/api.html#aiochclient.ChClient.fetchval)
method, which returns first value of the first row from query result:
```python
val = await client.fetchval("SELECT b FROM t WHERE a=2")

assert val == (dt.date(2018, 9, 8), 3.14)
```

With async iteration on the query results stream you can fetch multiple
rows without loading them all into memory at once:

```python
async for row in client.iterate(
    "SELECT number, number*2 FROM system.numbers LIMIT 10000"
):
    assert row[0] * 2 == row[1]
```

Use `fetch`/`fetchrow`/`fetchval`/`iterate` for SELECT queries and `execute` or
any of last for INSERT and all another queries.

### Working with query results

All fetch queries return rows as lightweight, memory efficient objects. _Before
v`1.0.0` rows were only returned as tuples._ All rows have a full mapping interface, where you can
get fields by names or indexes:

```python
row = await client.fetchrow("SELECT a, b FROM t WHERE a=1")

assert row["a"] == 1
assert row[0] == 1
assert row[:] == (1, (dt.date(2018, 9, 8), 3.14))
assert list(row.keys()) == ["a", "b"]
assert list(row.values()) == [1, (dt.date(2018, 9, 8), 3.14)]
```

## Documentation

To check out the [api docs](https://myscaledb-client.readthedocs.io/en/latest/reference.html),
visit the [readthedocs site.](https://myscaledb-client.readthedocs.io/en/latest/).

## Type Conversion

`myscaledb-client` automatically converts types from ClickHouse to python types and
vice-versa.

| ClickHouse type | Python type |
|:----------------|:------------|
| `UInt8` | `int` |
| `UInt16` | `int` |
| `UInt32` | `int` |
| `UInt64` | `int` |
| `Int8` | `int` |
| `Int16` | `int` |
| `Int32` | `int` |
| `Int64` | `int` |
| `Float32` | `float` |
| `Float64` | `float` |
| `String` | `str` |
| `FixedString` | `str` |
| `Enum8` | `str` |
| `Enum16` | `str` |
| `Date` | `datetime.date` |
| `DateTime` | `datetime.datetime` |
| `DateTime64` | `datetime.datetime` |
| `Decimal` | `decimal.Decimal` |
| `Decimal32` | `decimal.Decimal` |
| `Decimal64` | `decimal.Decimal` |
| `Decimal128` | `decimal.Decimal` |
| `IPv4` | `ipaddress.IPv4Address` |
| `IPv6` | `ipaddress.IPv6Address` |
| `UUID` | `uuid.UUID` |
| `Nothing` | `None` |
| `Tuple(T1, T2, ...)` | `Tuple[T1, T2, ...]` |
| `Array(T)` | `List[T]` |
| `Nullable(T)` | `None` or `T` |
| `LowCardinality(T)` | `T` |

## Connection Pool Settings

`myscaledb-client` uses the
[aiohttp.TCPConnector](https://docs.aiohttp.org/en/stable/client_advanced.html#limiting-connection-pool-size)
to determine pool size.  By default, the pool limit is 100 open connections.

## Notes on Speed

It's highly recommended using `uvloop` and installing `myscaledb-client` with
speedups for the sake of speed. Some recent benchmarks on our
machines without parallelization:

- 180k-220k rows/sec on SELECT
- 50k-80k rows/sec on INSERT

_Note: these benchmarks are system dependent_

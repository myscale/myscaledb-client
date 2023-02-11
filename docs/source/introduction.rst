.. _install:

Introduction
============

``myscaledb-client`` is an async/sync http(s) MyScale client for python 3.6+ supporting
type conversion in both directions, streaming, lazy decoding on select queries,
and a fully typed interface.

MyScale is a vector database built on the top of ClickHouse. We forked and
modified `aiochclient`_ to support vector related queries, and also add a
synchronous client. Since MyScale is compatible with ClickHouse,
``myscaledb-client`` can be also used as a ClickHouse client.

.. _aiochclient: https://github.com/maximdanilchenko/aiochclient/

Use ``myscaledb-client`` for a simple interface into your MyScale
deployment.

Requirements
------------

``myscaledb-client`` works on Linux, OSX, and Windows.

It requires Python >= 3.6 due to the use of types.

Installation
------------

You can install ``myscaledb-client`` with ``pip`` or your favourite package manager.
We recommend you to install it with command:

::

    $ pip install myscaledb-client


Add the ``-U`` switch to update to the latest version if ``myscaledb-client`` is
already installed.

To use with `aiohttp` install it with command:

::

    $ pip install 'myscaledb-client[aiohttp]'

Or ``myscaledb-client[aiohttp-speedups]`` to install with extra speedups.
By default, ``aiohttp`` is included if not specified.

To use with ``httpx`` install it with command:

::

    $ pip install 'myscaledb-client[httpx]'


Or ``myscaledb-client[httpx-speedups]`` to install with extra speedups.

Installing with ``[*-speedups]`` adds the following:

* `cChardet`_ for ``aiohttp`` speedup
* `aiodns`_ for ``aiohttp`` speedup
* `ciso8601`_ for ultra-fast datetime parsing while
  decoding data from MyScale for ``aiohttp`` and ``httpx``.

.. _cChardet: https://pypi.python.org/pypi/cchardet
.. _aiodns: https://pypi.python.org/pypi/aiodns
.. _ciso8601: https://github.com/closeio/ciso8601



Quick Start
-----------

The quickest way to get up and running with ``myscaledb-client`` is to simply connect
and check MyScale is alive. Here's how you would do that:

::

    # This is a demo using AsyncClient.
    # AsyncClient can give you a higher degree of concurrency, it requires an understanding of asynchronous programming and provides higher performance.

    import asyncio
    from myscaledb import AsyncClient
    from aiohttp import ClientSession

    async def main():
        async with ClientSession() as s:
            async with AsyncClient(s) as client:
                alive = await client.is_alive()
                print(f"Is MyScale alive? -> {alive}")
                res = await client.fetch(query="select now()")
                for record in res:
                    print(f"{record[0]}")

    if __name__ == '__main__':
        asyncio.run(main())

::

    # This is a demo using Client.
    # Client works in sync mode and is simple to use.

    from myscaledb import Client

    def main():
        client = Client()
        alive = client.is_alive()
        print(f"Is MyScale alive? -> {alive}")
        res = client.fetch(query="select now()")
        for record in res:
            print(f"{record[0]}")

    if __name__ == '__main__':
        main()

This automatically queries a instance of MyScale on ``localhost:8123`` with the
default user. You may want to set up a different connection to test. To do that,
change the following line::

    client = Client()

To something like::

    client = Client(url='http://localhost:8123')

Type Conversion
---------------

``myscaledb-client`` automatically converts types from MyScale to python types and
vice-versa.

==================   =================
MyScale Type         Python Type
==================   =================
UInt8                 int
UInt16                int
UInt32                int
UInt64                int
Int8                  int
Int16                 int
Int32                 int
Int64                 int
Float32               float
Float64               float
String                str
FixedString           str
Enum8                 str
Enum16                str
Date                  datetime.date
DateTime              datetime.date
DateTime64            datetime.date
Decimal               decimal.Decimal
Decimal32             decimal.Decimal
Decimal64             decimal.Decimal
Decimal128            decimal.Decimal
IPv4                  ipaddress.IPv4Address
IPv6                  ipaddress.IPv6Address
UUID                  uuid.UUID
Nothing               None
Tuple(T1, T2, ...)    Tuple[T1, T2, ...]
Array(T)              List[T]
Nullable(T)           None or T
LowCardinality(T)     T
Map(T1, T2)           Dict[T1, T2]
==================   =================


Connection Pool Settings
------------------------

``myscaledb-client`` uses the `aiohttp.TCPConnector`_ to determine pool size.  By default, the pool limit is 100 open connections.

.. _aiohttp.TCPConnector: https://docs.aiohttp.org/en/stable/client_advanced.html#limiting-connection-pool-size

You can find more sample code to operate MyScale in the :ref:`reference`.
Continue reading to learn more about ``myscaledb-client``.

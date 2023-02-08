.. _install:

Introduction
============

`myscaledb-client` is an async/sync http(s) MyScale client for python 3.6+ supporting
type conversion in both directions, streaming, lazy decoding on select queries,
and a fully typed interface.

MyScale is a vector database built on the top of ClickHouse. We forked and
modified `aiochclient`_ to support vector related queries, and also add a
synchronous client. Since MyScale is compatible with ClickHouse,
`myscaledb-client` can be also used as a ClickHouse client.

.. _aiochclient: https://github.com/maximdanilchenko/aiochclient/

Use `myscaledb-client` for a simple interface into your MyScale/ClickHouse
deployment.

Requirements
------------

`myscaledb-client` works on Linux, OSX, and Windows.

It requires Python >= 3.6 due to the use of types.

Installation
------------

You can install `myscaledb-client` with `pip` or your favourite package manager, We recommend you to install it with command:

::

    $ pip install myscaledb-client


Add the ``-U`` switch to update to the latest version if `myscaledb-client` is already installed.

To use with `aiohttp` install it with command:

::

    $ pip install 'myscaledb-client[aiohttp]'


Or `myscaledb-client[aiohttp-speedups]` to install with extra speedups.

To use with `httpx` install it with command:

::

    $ pip install 'myscaledb-client[httpx]'


Or `myscaledb-client[httpx-speedups]` to install with extra speedups.

Installing with `[*-speedups]` adds the following:

- `cChardet`_ for `aiohttp` speedup
- `aiodns`_ for `aiohttp` speedup
- `ciso8601`_ for ultra-fast datetime parsing while decoding data from ClickHouse for `aiohttp` and `httpx`.

.. _cChardet: https://pypi.python.org/pypi/cchardet
.. _aiodns: https://pypi.python.org/pypi/aiodns
.. _ciso8601: https://github.com/closeio/ciso8601



Quick Start
-----------

The quickest way to get up and running with `myscaledb-client` is to simply connect
and check ClickHouse is alive. Here's how you would do that:

::

    # This is a demo using AsyncClient.
    # AsyncClient can give you a higher degree of concurrency, it requires an understanding of asynchronous programming.

    import asyncio
    from myscaledb import AsyncClient
    from aiohttp import ClientSession

    async def main():
        async with ClientSession() as s:
            async with AsyncClient(s) as client:
                alive = await client.is_alive()
                print(f"Is ClickHouse alive? -> {alive}")

    if __name__ == '__main__':
        asyncio.run(main())

::

    # This is a demo using Client.
    # Client works in sync mode, in line with most people's programming habits.

    from myscaledb import Client

    def main():
        client = Client()
        alive = client.is_alive()
        print(f"Is ClickHouse alive? -> {alive}")

    if __name__ == '__main__':
        main()

This automatically queries a instance of ClickHouse on `localhost:8123` with the
default user. You may want to set up a different connection to test. To do that,
change the following line::

    client = Client()

To something like::

    client = Client(url='http://localhost:8123')

You can find more sample code to operate ClickHouse in the :ref:`reference`.
Continue reading to learn more about `myscaledb-client`.

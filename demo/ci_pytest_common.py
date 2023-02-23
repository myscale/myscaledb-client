import datetime as dt
from decimal import Decimal
from ipaddress import IPv4Address, IPv6Address
from uuid import uuid4

import pytest

from config import clickhouse_ip
from myscaledb import Client


@pytest.fixture
def uuid():
    return uuid4()


@pytest.fixture
def rows(uuid):
    return [
        [
            1,
            1000,
            10000,
            12_345_678_910,
            -4,
            -453,
            21322,
            -32123,
            23.432,
            -56754.564_542,
            "hello man",
            "hello fixed man".ljust(32, " "),
            dt.date(2018, 9, 21),
            dt.datetime(2018, 9, 21, 10, 32, 23),
            "hello",
            "world",
            [1, 2, 3, 4],
            (4, "hello"),
            0,
            ["hello", "world"],
            ["hello", "world"],
            ["hello", None],
            "'.\b.\f.\r.\n.\t.\\.",
            uuid,
            [uuid, uuid, uuid],
            ["hello", "world", "hello"],
            [dt.date(2018, 9, 21), dt.date(2018, 9, 22)],
            [
                dt.datetime(2018, 9, 21, 10, 32, 23),
                dt.datetime(2018, 9, 21, 10, 32, 24),
            ],
            "hello man",
            "hello man",
            777,
            dt.date(1994, 9, 7),
            dt.datetime(2018, 9, 21, 10, 32, 23),
            Decimal('1234.5678'),
            Decimal('1234.56'),
            Decimal('1234.56'),
            Decimal('123.56'),
            [[1, 2, 3], [1, 2], [6, 7]],
            IPv4Address('116.253.40.133'),
            IPv6Address('2001:44c8:129:2632:33:0:252:2'),
            dt.datetime(2018, 9, 21, 10, 32, 23),
            True,
            {'yolo': {12: [0.1, 0.2, 0.3, 0.4], 18: [1.1, 1.2, 1.3, 1.4]},
             "charis": {14: [2.1, 2.2, 2.3, 2.4], 43: [3.1, 3.2, 3.3, 3.4]}}
        ],
        [
            2,
            1000,
            10000,
            12_345_678_910,
            -4,
            -453,
            21322,
            -32123,
            23.432,
            -56754.564_542,
            "hello man",
            "hello fixed man".ljust(32, " "),
            None,
            None,
            "hello",
            "world",
            [1, 2, 3, 4],
            (4, "hello"),
            None,
            [],
            [],
            [],
            "'\b\f\r\n\t\\",
            None,
            [],
            [],
            [],
            [],
            "hello man",
            None,
            777,
            dt.date(1994, 9, 7),
            dt.datetime(2018, 9, 21, 10, 32, 23),
            Decimal('1234.5678'),
            Decimal('1234.56'),
            Decimal('1234.56'),
            Decimal('123.56'),
            [],
            None,
            None,
            # python time stamp
            # 1546300800000,
            dt.datetime(2019, 1, 1, 3, 0),
            False,
            {"yolo": {12: [0.1, 0.2, 0.3, 0.4], 18: [1.1, 1.2, 1.3, 1.4]},
             'charis': {14: [2.1, 2.2, 2.3, 2.4], 43: [3.1, 3.2, 3.3, 3.4]}}
        ],
    ]


@pytest.fixture
def client():
    client = Client(url=f'http://{clickhouse_ip}:8123',
                    user="default",
                    password="",
                    allow_suspicious_low_cardinality_types=1)
    return client


@pytest.fixture
def all_types_db(client, rows):
    client.execute("DROP TABLE IF EXISTS all_types")
    client.execute("DROP TABLE IF EXISTS test_cache")
    client.execute("DROP TABLE IF EXISTS test_cache_mv")
    client.execute(
        """
    CREATE TABLE all_types (uint8 UInt8,
                            uint16 UInt16,
                            uint32 UInt32,
                            uint64 UInt64,
                            int8 Int8,
                            int16 Int16,
                            int32 Int32,
                            int64 Int64,
                            float32 Float32,
                            float64 Float64,
                            string String,
                            fixed_string FixedString(32),
                            date Nullable(Date),
                            datetime Nullable(DateTime),
                            enum8 Enum8('hello' = 1, 'world' = 2),
                            enum16 Enum16('hello' = 1000, 'world' = 2000),
                            array_uint8 Array(UInt8),
                            tuple Tuple(UInt8, String),
                            nullable Nullable(Int8),
                            array_string Array(String),
                            array_low_cardinality_string Array(LowCardinality(String)),
                            array_nullable_string Array(Nullable(String)),
                            escape_string String,
                            uuid Nullable(UUID),
                            array_uuid Array(UUID),
                            array_enum Array(Enum8('hello' = 1, 'world' = 2)),
                            array_date Array(Date),
                            array_datetime Array(DateTime),
                            low_cardinality_str LowCardinality(String),
                            low_cardinality_nullable_str LowCardinality(Nullable(String)),
                            low_cardinality_int LowCardinality(Int32),
                            low_cardinality_date LowCardinality(Date),
                            low_cardinality_datetime LowCardinality(DateTime),
                            decimal32 Decimal32(4),
                            decimal64 Decimal64(2),
                            decimal128 Decimal128(6),
                            decimal Decimal(6, 3),
                            array_array_int Array(Array(Int32)),
                            ipv4 Nullable(IPv4),
                            ipv6 Nullable(IPv6),
                            datetime64 DateTime64(3, 'Europe/Moscow'),
                            bool Bool,
                            map Map(String, Map(Int32, Array(Float32)))
                            ) ENGINE = Memory
    """
    )
    client.execute(
        """
        CREATE TABLE test_cache (
          key           String,
          int32Cache    AggregateFunction(avg, Int32),
          float32Cache  SimpleAggregateFunction(sum, Float64))
        ENGINE = AggregatingMergeTree()
        ORDER BY key
        """
    )
    client.execute(
        """
        CREATE MATERIALIZED VIEW test_cache_mv TO test_cache AS
          SELECT avgState(int32), sum(float32) FROM all_types
        """
    )
    # print(*rows)
    client.execute("INSERT INTO all_types VALUES", *rows)


@pytest.fixture
def init_myscaledb(all_types_db, rows, client, request):
    request.cls.client = client
    cls_rows = rows
    request.cls.rows = [tuple(r) for r in cls_rows]

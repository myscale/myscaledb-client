from ci_pytest_common import *
from myscaledb import ClientError


@pytest.mark.client
@pytest.mark.usefixtures("init_myscaledb")
class TestClient:
    def test_is_alive(self):
        assert self.client.is_alive() is True

    def test_bad_query(self):
        with pytest.raises(ClientError):
            self.client.execute("SELE")

    def test_bad_select(self):
        with pytest.raises(ClientError):
            self.client.execute("SELECT * FROM all_types WHERE", 1, 2, 3, 4)


@pytest.mark.types
@pytest.mark.usefixtures("init_myscaledb")
class TestTypes:
    def select_field(self, field):
        return self.client.fetchval(f"SELECT {field} FROM all_types WHERE uint8=1")

    def select_record(self, field):
        return self.client.fetchrow(f"SELECT {field} FROM all_types WHERE uint8=1")

    def select_field_bytes(self, field):
        return self.client.fetchval(f"SELECT {field} FROM all_types WHERE uint8=1", decode=False)

    def select_record_bytes(self, field):
        return self.client.fetchrow(f"SELECT {field} FROM all_types WHERE uint8=1", decode=False)

    def test_uint8(self):
        result = 1
        assert self.select_field("uint8") == result
        record = self.select_record("uint8")
        assert record[0] == result
        assert record["uint8"] == result

        result = b"1"
        assert self.select_field_bytes("uint8") == result
        record = self.select_record_bytes("uint8")
        assert record[0] == result
        assert record["uint8"] == result

    def test_uint16(self):
        result = 1000
        assert self.select_field("uint16") == result
        record = self.select_record("uint16")
        assert record[0] == result
        assert record["uint16"] == result

        result = b"1000"
        assert self.select_field_bytes("uint16") == result
        record = self.select_record_bytes("uint16")
        assert record[0] == result
        assert record["uint16"] == result

    def test_uint32(self):
        result = 10000
        assert self.select_field("uint32") == result
        record = self.select_record("uint32")
        assert record[0] == result
        assert record["uint32"] == result

        result = b"10000"
        assert self.select_field_bytes("uint32") == result
        record = self.select_record_bytes("uint32")
        assert record[0] == result
        assert record["uint32"] == result

    def test_uint64(self):
        result = 12_345_678_910
        assert self.select_field("uint64") == result
        record = self.select_record("uint64")
        assert record[0] == result
        assert record["uint64"] == result

        result = b"12345678910"
        assert self.select_field_bytes("uint64") == result
        record = self.select_record_bytes("uint64")
        assert record[0] == result
        assert record["uint64"] == result

    def test_int8(self):
        result = -4
        assert self.select_field("int8") == result
        record = self.select_record("int8")
        assert record[0] == result
        assert record["int8"] == result

        result = b"-4"
        assert self.select_field_bytes("int8") == result
        record = self.select_record_bytes("int8")
        assert record[0] == result
        assert record["int8"] == result

    def test_int16(self):
        result = -453
        assert self.select_field("int16") == result
        record = self.select_record("int16")
        assert record[0] == result
        assert record["int16"] == result

        result = b"-453"
        assert self.select_field_bytes("int16") == result
        record = self.select_record_bytes("int16")
        assert record[0] == result
        assert record["int16"] == result

    def test_int32(self):
        result = 21322
        assert self.select_field("int32") == result
        record = self.select_record("int32")
        assert record[0] == result
        assert record["int32"] == result

        result = b"21322"
        assert self.select_field_bytes("int32") == result
        record = self.select_record_bytes("int32")
        assert record[0] == result
        assert record["int32"] == result

    def test_int64(self):
        result = -32123
        assert self.select_field("int64") == result
        record = self.select_record("int64")
        assert record[0] == result
        assert record["int64"] == result

        result = b"-32123"
        assert self.select_field_bytes("int64") == result
        record = self.select_record_bytes("int64")
        assert record[0] == result
        assert record["int64"] == result

    def test_float32(self):
        result = 23.432
        assert self.select_field("float32") == result
        record = self.select_record("float32")
        assert record[0] == result
        assert record["float32"] == result

        result = b"23.432"
        assert self.select_field_bytes("float32") == result
        record = self.select_record_bytes("float32")
        assert record[0] == result
        assert record["float32"] == result

    def test_float64(self):
        result = -56754.564_542
        assert self.select_field("float64") == result
        record = self.select_record("float64")
        assert record[0] == result
        assert record["float64"] == result

        result = b"-56754.564542"
        assert self.select_field_bytes("float64") == result
        record = self.select_record_bytes("float64")
        assert record[0] == result
        assert record["float64"] == result

    def test_string(self):
        result = "hello man"
        assert self.select_field("string") == result
        record = self.select_record("string")
        assert record[0] == result
        assert record["string"] == result

        result = b"hello man"
        assert self.select_field_bytes("string") == result
        record = self.select_record_bytes("string")
        assert record[0] == result
        assert record["string"] == result

    def test_fixed_string(self):
        result = "hello fixed man".ljust(32, " ")
        assert self.select_field("fixed_string") == result
        record = self.select_record("fixed_string")
        assert record[0] == result
        assert record["fixed_string"] == result

        result = b"hello fixed man".ljust(32, b" ")
        assert self.select_field_bytes("fixed_string") == result
        record = self.select_record_bytes("fixed_string")
        assert record[0] == result
        assert record["fixed_string"] == result

    def test_date(self):
        result = dt.date(2018, 9, 21)
        assert self.select_field("date") == result
        record = self.select_record("date")
        assert record[0] == result
        assert record["date"] == result

        result = b"2018-09-21"
        assert self.select_field_bytes("date") == result
        record = self.select_record_bytes("date")
        assert record[0] == result
        assert record["date"] == result

    def test_datetime(self):
        result = dt.datetime(2018, 9, 21, 10, 32, 23)
        assert self.select_field("datetime") == result
        record = self.select_record("datetime")
        assert record[0] == result
        assert record["datetime"] == result

        result = b"2018-09-21 10:32:23"
        assert self.select_field_bytes("datetime") == result
        record = self.select_record_bytes("datetime")
        assert record[0] == result
        assert record["datetime"] == result

    def test_enum8(self):
        result = "hello"
        assert self.select_field("enum8") == result
        record = self.select_record("enum8")
        assert record[0] == result
        assert record["enum8"] == result

        result = b"hello"
        assert self.select_field_bytes("enum8") == result
        record = self.select_record_bytes("enum8")
        assert record[0] == result
        assert record["enum8"] == result

    def test_enum16(self):
        result = "world"
        assert self.select_field("enum16") == result
        record = self.select_record("enum16")
        assert record[0] == result
        assert record["enum16"] == result

        result = b"world"
        assert self.select_field_bytes("enum16") == result
        record = self.select_record_bytes("enum16")
        assert record[0] == result
        assert record["enum16"] == result

    def test_array_uint8(self):
        result = [1, 2, 3, 4]
        assert self.select_field("array_uint8") == result
        record = self.select_record("array_uint8")
        assert record[0] == result
        assert record["array_uint8"] == result

        result = b"[1,2,3,4]"
        assert self.select_field_bytes("array_uint8") == result
        record = self.select_record_bytes("array_uint8")
        assert record[0] == result
        assert record["array_uint8"] == result

    def test_tuple(self):
        result = (4, "hello")
        assert self.select_field("tuple") == result
        record = self.select_record("tuple")
        assert record[0] == result
        assert record["tuple"] == result

        result = b"(4,'hello')"
        assert self.select_field_bytes("tuple") == result
        record = self.select_record_bytes("tuple")
        assert record[0] == result
        assert record["tuple"] == result

    def test_nullable(self):
        result = 0
        assert self.select_field("nullable") == result
        record = self.select_record("nullable")
        assert record[0] == result
        assert record["nullable"] == result

        result = b"0"
        assert self.select_field_bytes("nullable") == result
        record = self.select_record_bytes("nullable")
        assert record[0] == result
        assert record["nullable"] == result

    def test_array_string(self):
        result = ["hello", "world"]
        assert self.select_field("array_string") == result
        record = self.select_record("array_string")
        assert record[0] == result
        assert record["array_string"] == result

        result = b"['hello','world']"
        assert self.select_field_bytes("array_string") == result
        record = self.select_record_bytes("array_string")
        assert record[0] == result
        assert record["array_string"] == result

    def test_array_low_cardinality_string(self):
        result = ["hello", "world"]
        assert self.select_field("array_low_cardinality_string") == result
        record = self.select_record("array_low_cardinality_string")
        assert record[0] == result
        assert record["array_low_cardinality_string"] == result

        result = b"['hello','world']"
        assert self.select_field_bytes("array_low_cardinality_string") == result
        record = self.select_record_bytes("array_low_cardinality_string")
        assert record[0] == result
        assert record["array_low_cardinality_string"] == result

    def test_array_nullable_string(self):
        result = ["hello", None]
        assert self.select_field("array_nullable_string") == result
        record = self.select_record("array_nullable_string")
        assert record[0] == result
        assert record["array_nullable_string"] == result

        result = b"['hello',NULL]"
        assert self.select_field_bytes("array_nullable_string") == result
        record = self.select_record_bytes("array_nullable_string")
        assert record[0] == result
        assert record["array_nullable_string"] == result

    def test_escape_string(self):
        result = "'.\b.\f.\r.\n.\t.\\."
        assert self.select_field("escape_string") == result
        record = self.select_record("escape_string")
        assert record[0] == result
        assert record["escape_string"] == result

        result = b"\\'.\\b.\\f.\\r.\\n.\\t.\\\\."
        assert self.select_field_bytes("escape_string") == result
        record = self.select_record_bytes("escape_string")
        assert record[0] == result
        assert record["escape_string"] == result

    def test_uuid(self, uuid):
        result = uuid
        assert self.select_field("uuid") == result
        record = self.select_record("uuid")
        assert record[0] == result
        assert record["uuid"] == result

        result = str(uuid).encode()
        assert self.select_field_bytes("uuid") == result
        record = self.select_record_bytes("uuid")
        assert record[0] == result
        assert record["uuid"] == result

    def test_array_uuid(self, uuid):
        result = [uuid, uuid, uuid]
        assert self.select_field("array_uuid") == result
        record = self.select_record("array_uuid")
        assert record[0] == result
        assert record["array_uuid"] == result

        result = str([str(uuid), str(uuid), str(uuid)]).replace(" ", "").encode()
        assert self.select_field_bytes("array_uuid") == result
        record = self.select_record_bytes("array_uuid")
        assert record[0] == result
        assert record["array_uuid"] == result

    def test_array_enum(self):
        result = ["hello", "world", "hello"]
        assert self.select_field("array_enum ") == result
        record = self.select_record("array_enum ")
        assert record[0] == result
        assert record["array_enum"] == result

        result = b"['hello','world','hello']"
        assert self.select_field_bytes("array_enum ") == result
        record = self.select_record_bytes("array_enum ")
        assert record[0] == result
        assert record["array_enum"] == result

    def test_array_date(self):
        assert self.select_field("array_date ") == [
            dt.date(2018, 9, 21),
            dt.date(2018, 9, 22),
        ]
        assert self.select_field_bytes("array_date ") == (
            b"['2018-09-21','2018-09-22']"
        )

    def test_array_datetime(self):
        assert self.select_field("array_datetime ") == [
            dt.datetime(2018, 9, 21, 10, 32, 23),
            dt.datetime(2018, 9, 21, 10, 32, 24),
        ]
        assert self.select_field_bytes("array_datetime ") == (
            b"['2018-09-21 10:32:23','2018-09-21 10:32:24']"
        )

    def test_low_cardinality_str(self):
        result = "hello man"
        assert self.select_field("low_cardinality_str") == result
        record = self.select_record("low_cardinality_str")
        assert record[0] == result
        assert record["low_cardinality_str"] == result

        result = b"hello man"
        assert self.select_field_bytes("low_cardinality_str") == result
        record = self.select_record_bytes("low_cardinality_str")
        assert record[0] == result
        assert record["low_cardinality_str"] == result

    def test_low_cardinality_nullable_str(self):
        result = "hello man"
        assert self.select_field("low_cardinality_nullable_str") == result
        record = self.select_record("low_cardinality_nullable_str")
        assert record[0] == result
        assert record["low_cardinality_nullable_str"] == result

        result = b"hello man"
        assert self.select_field_bytes("low_cardinality_nullable_str") == result
        record = self.select_record_bytes("low_cardinality_nullable_str")
        assert record[0] == result
        assert record["low_cardinality_nullable_str"] == result

    def test_low_cardinality_int(self):
        result = 777
        assert self.select_field("low_cardinality_int") == result
        record = self.select_record("low_cardinality_int")
        assert record[0] == result
        assert record["low_cardinality_int"] == result

        result = b"777"
        assert self.select_field_bytes("low_cardinality_int") == result
        record = self.select_record_bytes("low_cardinality_int")
        assert record[0] == result
        assert record["low_cardinality_int"] == result

    def test_low_cardinality_date(self):
        result = dt.date(1994, 9, 7)
        assert self.select_field("low_cardinality_date") == result
        record = self.select_record("low_cardinality_date")
        assert record[0] == result
        assert record["low_cardinality_date"] == result

        result = b"1994-09-07"
        assert self.select_field_bytes("low_cardinality_date") == result
        record = self.select_record_bytes("low_cardinality_date")
        assert record[0] == result
        assert record["low_cardinality_date"] == result

    def test_low_cardinality_datetime(self):
        assert self.select_field("low_cardinality_datetime") == dt.datetime(
            2018, 9, 21, 10, 32, 23
        )

        assert (
                self.select_field_bytes("low_cardinality_datetime")
                == b"2018-09-21 10:32:23"
        )

    def test_decimal(self):
        assert self.select_field("decimal") == Decimal("123.56")

        assert self.select_field_bytes("decimal") == b"123.56"

    def test_decimal32(self):
        assert self.select_field("decimal32") == Decimal("1234.5678")

        assert self.select_field_bytes("decimal32") == b"1234.5678"

    def test_decimal64(self):
        assert self.select_field("decimal64") == Decimal("1234.56")

        assert self.select_field_bytes("decimal64") == b"1234.56"

    def test_decimal128(self):
        assert self.select_field("decimal128") == Decimal("1234.56")

        assert self.select_field_bytes("decimal128") == b"1234.56"

    def test_array_of_arrays(self):
        assert self.select_field("array_array_int") == [[1, 2, 3], [1, 2], [6, 7]]

        assert (
                self.select_field_bytes("array_array_int") == b"[[1,2,3],[1,2],[6,7]]"
        )

    def test_ipv4(self):
        assert self.select_field("ipv4") == IPv4Address("116.253.40.133")

        assert self.select_field_bytes("ipv4") == b"116.253.40.133"

    def test_ipv6(self):
        assert self.select_field("ipv6") == IPv6Address(
            '2001:44c8:129:2632:33:0:252:2'
        )

        assert self.select_field_bytes("ipv6") == b"2001:44c8:129:2632:33:0:252:2"

    def test_datetime64(self):
        result = dt.datetime(2018, 9, 21, 10, 32, 23)
        assert self.select_field("datetime") == result
        record = self.select_record("datetime")
        assert record[0] == result
        assert record["datetime"] == result

        result = b"2018-09-21 10:32:23"
        assert self.select_field_bytes("datetime") == result
        record = self.select_record_bytes("datetime")
        assert record[0] == result
        assert record["datetime"] == result

    def test_named_tuples(self):
        """Named tuples are used for example in geohash functions

        https://clickhouse.com/docs/en/sql-reference/data-types/tuple/#addressing-tuple-elements
        """

        result = self.client.fetchval(
            f"SELECT (1.0, 2.0)::Tuple(x Float64, y Float64)"
        )
        print(result)
        assert round(result[0]) == 1
        assert round(result[1]) == 2

    def test_multi_map(self):
        result = {'yolo': {12: [0.1, 0.2, 0.3, 0.4], 18: [1.1, 1.2, 1.3, 1.4]},
                  'charis': {14: [2.1, 2.2, 2.3, 2.4], 43: [3.1, 3.2, 3.3, 3.4]}}
        assert self.select_field("map") == result
        record = self.select_record("map")
        assert record[0] == result
        assert record["map"] == result
        result = b"{'yolo':{12:[0.1,0.2,0.3,0.4],18:[1.1,1.2,1.3,1.4]}," \
                 b"'charis':{14:[2.1,2.2,2.3,2.4],43:[3.1,3.2,3.3,3.4]}}"
        assert self.select_field_bytes("map") == result
        record = self.select_record_bytes("map")
        assert record[0] == result
        assert record["map"] == result

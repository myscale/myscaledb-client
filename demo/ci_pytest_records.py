from ci_pytest_common import *


@pytest.mark.record
@pytest.mark.usefixtures("init_myscaledb")
class TestRecord:
    def test_common_objects(self):
        records = self.client.fetch("SELECT * FROM all_types")
        assert id(records[0]._converters) == id(records[1]._converters)
        assert id(records[0]._names) == id(records[1]._names)

    def test_lazy_decoding(self):
        record = self.client.fetchrow("SELECT * FROM all_types WHERE uint8=2")
        assert type(record._row) == bytes
        # after print, record will be decoded
        print(record)
        assert type(record._row) == tuple
        assert type(record._row[0]) == int

    def test_mapping(self):
        record = self.client.fetchrow("SELECT * FROM all_types WHERE uint8=2")
        assert list(record.values())[0] == 2
        assert list(record.keys())[0] == "uint8"
        assert list(record.items())[0] == ("uint8", 2)
        assert record.get("uint8") == 2
        assert record.get(0) == 2

    def test_bool(self):
        records = self.client.fetch(
            "SELECT uniq(array_string) FROM all_types GROUP BY array_string WITH TOTALS"
        )
        assert bool(records[-2]) is False

    def test_len(self):
        record = self.client.fetchrow("SELECT * FROM all_types WHERE uint8=2")
        assert len(record) == len(self.rows[1])

    def test_index_error(self):
        record = self.client.fetchrow("SELECT * FROM all_types WHERE uint8=2")
        with pytest.raises(IndexError):
            record[43]
        records = self.client.fetch(
            "SELECT uniq(array_string) FROM all_types GROUP BY array_string WITH TOTALS"
        )
        with pytest.raises(IndexError):
            records[-2][0]

    @pytest.mark.skip
    def test_empty_string(self):
        self.client.execute("INSERT INTO all_types (uint8, string) VALUES", (6, ''))
        result = self.client.fetch("SELECT string FROM all_types WHERE uint8=6")
        assert len(result) == 1
        record = result[0]
        assert record['string'] == ''

    def test_key_error(self):
        record = self.client.fetchrow("SELECT * FROM all_types WHERE uint8=2")
        with pytest.raises(KeyError):
            record["no_such_key"]
        records = self.client.fetch(
            "SELECT uniq(array_string) FROM all_types GROUP BY array_string WITH TOTALS"
        )
        with pytest.raises(KeyError):
            records[-2]["a"]

    def test_explain_results(self):
        record = self.client.fetch("EXPLAIN SELECT * FROM all_types WHERE uint8=2")
        assert len(record) != 0

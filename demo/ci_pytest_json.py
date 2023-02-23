from ci_pytest_common import *


@pytest.mark.usefixtures("init_myscaledb")
class TestJson:
    def test_json_insert_select(self):
        sql = "INSERT INTO all_types FORMAT JSONEachRow"
        records = [
            {"decimal32": 32},
        ]
        self.client.execute(sql, *records)

        sql = "INSERT INTO all_types"
        records = [
            {"fixed_string": "simple string", "low_cardinality_str": "meow test"},
        ]
        self.client.execute(sql, *records, json=True)

        result = self.client.fetch(
            "SELECT * FROM all_types WHERE decimal32 = 32 FORMAT JSONEachRow"
        )
        assert len(result) == 1
        result = self.client.fetch(
            "SELECT fixed_string, low_cardinality_str FROM all_types "
            "WHERE low_cardinality_str = 'meow test'",
            json=True,
        )
        assert result == [
            {
                "fixed_string": "simple string\x00\x00\x00\x00\x00\x00\x00\x00"
                                "\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00",
                "low_cardinality_str": "meow test",
            }
        ]

from ci_pytest_common import *


@pytest.mark.fetching
@pytest.mark.usefixtures("init_myscaledb")
class TestFetching:
    def test_fetchrow_full(self):
        assert (self.client.fetchrow("SELECT * FROM all_types WHERE uint8=1"))[:] == self.rows[0]

    def test_fetchrow_full_with_params(self):
        assert (self.client.fetchrow("SELECT * FROM all_types WHERE uint8={u8}", params={"u8": 1}))[:] == self.rows[0]

    def test_fetchrow_with_empties(self):
        assert (self.client.fetchrow("SELECT * FROM all_types WHERE uint8=2"))[:] == self.rows[1]

    def test_fetchrow_none_result(self):
        assert (self.client.fetchrow("SELECT * FROM all_types WHERE uint8=42")) is None

    def test_fetchrow_none_result_with_params(self):
        assert (self.client.fetchrow("SELECT * FROM all_types WHERE uint8={u8}", params={'u8': 42})) is None

    def test_fetchone_full(self):
        assert (self.client.fetchval("SELECT * FROM all_types WHERE uint8=1")) == self.rows[0][0]

    def test_fetchone_with_empties(self):
        assert (self.client.fetchrow("SELECT * FROM all_types WHERE uint8=2"))[:] == self.rows[1]

    def test_fetchone_none_result(self):
        assert (self.client.fetchval("SELECT * FROM all_types WHERE uint8=42")) is None

    def test_fetchval_none_result(self):
        assert (self.client.fetchval("SELECT uint8 FROM all_types WHERE uint8=42")) is None

    def test_fetchval_none_result_with_params(self):
        assert (self.client.fetchval("SELECT uint8 FROM all_types WHERE uint8={u8}", params={"u8": 42})) is None

    def test_fetch(self):
        rows = self.client.fetch("SELECT * FROM all_types")
        assert [row[:] for row in rows] == self.rows

    def test_iterate(self):
        assert [row[:] for row in self.client.iterate("SELECT * FROM all_types")] == self.rows

    def test_select_with_execute(self):
        assert (self.client.execute("SELECT * FROM all_types WHERE uint8=1")) is None

    def test_describe_with_fetch(self):
        described_columns = self.client.fetch("DESCRIBE TABLE all_types", json=True)
        assert described_columns is not None
        assert 'type' in described_columns[0]
        assert 'name' in described_columns[0]

    def test_show_tables_with_fetch(self):
        tables = self.client.fetch("SHOW TABLES")
        assert "all_types" in [row[0] for row in self.client.fetch("show tables")]
        assert "test_cache" in [row[0] for row in self.client.fetch("show tables")]
        assert "test_cache_mv" in [row[0] for row in self.client.fetch("show tables")]
        assert tables[0]._row.decode() == 'all_types'

    def test_aggr_merge_tree(self):
        avg_value = self.client.execute("SELECT avg(int32) FROM all_types")
        avg_cache = self.client.execute("SELECT avgMerge(int32Cache) FROM test_cache")
        assert avg_value == avg_cache

    def test_exists_table(self):
        exists = self.client.fetchrow("EXISTS TABLE all_types")
        assert exists == {'result': 1}

    def test_no_params(self):
        """It should be possible to have the aliases we want if we don't use any params"""
        res = self.client.fetchrow('SELECT 1 AS "{not_a_param}" FROM all_types')
        assert res["{not_a_param}"] == 1


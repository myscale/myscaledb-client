import time

from config import clickhouse_ip
from utils import read_from_csv
from myscaledb import Client

client = Client(url=f'http://{clickhouse_ip}:8123', user="default", password="")


def create_table():
    drop_query = "drop table if exists default.test_vector"
    create_query = "create table if not exists default.test_vector(`id` Int32, `vector` Array(Float32), " \
                   "CONSTRAINT check_length CHECK length(vector) = 960)" \
                   " engine=MergeTree ORDER BY id"
    print(drop_query)
    client.execute(drop_query)
    print(create_query)
    client.execute(create_query)


def upload_csv_data(file_path: str):
    print(f"upload csv data from: {file_path}")
    client.execute("insert into default.test_vector FORMAT CSV", file_path)


def create_wait_hnsw():
    print(f"waiting index build finish")
    client.execute("alter table default.test_vector add vector index hnsw_index vector type HNSWFLAT('metric_type=L2')")
    while True:
        status = client.fetchval("select status from system.vector_indices where table='test_vector'")
        if status == "Built":
            break
        else:
            print(status, end=" ", flush=True)
            time.sleep(3)
    print("")
    print("index build finish")


def vector_search(search_file_path: str):
    print("vector search begin")
    for id_vector in read_from_csv(query_out_path=search_file_path):
        res = client.fetch(f"select id, distance('topK=100')(vector,{id_vector.vector}) as dis from test_vector")
        print('.', end='', flush=True)
        assert len([row for row in res]) == 100
    print('')
    print("vector search finish")


if __name__ == "__main__":
    create_table()
    upload_csv_data("resources/gist_960_1k.csv")
    create_wait_hnsw()
    vector_search("resources/gist_960_1k.csv")

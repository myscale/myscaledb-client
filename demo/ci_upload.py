from config import clickhouse_ip
from myscaledb import Client

client = Client(url=f'http://{clickhouse_ip}:8123', user="default", password="")

# create  table
print("\n<======= Create table ========>")
drop_query = "drop table if exists default.test_id_vector_text_map_s_m"
create_query = "create table if not exists default.test_id_vector_text_map_s_m(`id` Int32, `vector` Array(Float32), " \
               "`text` String ,`map` Map(String,Map(String,String)), CONSTRAINT check_length CHECK length(vector) = 4)" \
               " engine=MergeTree ORDER BY id"
drop_query2 = "drop table if exists default.test_id_map_s_i_a"
create_query2 = "create table if not exists default.test_id_map_s_i_a(`id` Int32, `map` Map(String,Map(Int32,Array(Float32))))" \
                " engine=MergeTree ORDER BY id"
print(drop_query)
client.execute(drop_query)
print(create_query)
client.execute(create_query)
print(drop_query2)
client.execute(drop_query2)
print(create_query2)
client.execute(create_query2)
assert "test_id_vector_text_map_s_m" in [row[0] for row in client.fetch("show tables")]
assert "test_id_map_s_i_a" in [row[0] for row in client.fetch("show tables")]

print("\n<======= Upload from csv file ========>")
print("======> upload file id_vector_text_map(string,map(string,string)).csv")
# upload data from csv
client.execute("insert into default.test_id_vector_text_map_s_m FORMAT CSV", "resources/id_vector_text_map(string,map("
                                                                             "string,string)).csv")
for row in client.fetch("select * from default.test_id_vector_text_map_s_m order by id asc"):
    print(row)
assert len([row for row in client.fetch("select * from default.test_id_vector_text_map_s_m")]) == 3

print("======> upload file id_map(string,map(int,array)).csv")
client.execute("insert into default.test_id_map_s_i_a FORMAT CSV", "resources/id_map(string,map(int,array)).csv")
for row in client.fetch("select * from default.test_id_map_s_i_a order by id asc"):
    print(row)
assert len([row for row in client.fetch("select * from default.test_id_map_s_i_a")]) == 1

# upload data from python variable
print("\n<======= Upload from python variable ========>")
print("======> upload map(string,map(string,string))")
res = []
res.append((3, [1.4, 2.4, 3.4, 4.4], "python 🚀 yolo and charis's date of birth",
            {"yolo": {'Lunar': '2011-3-19', 'Solar': '2011-1-17'},
             'charis': {"Lunar": '2012-4-27', 'Solar': '2012-3-26'}}))
res.append([4, [1.5, 2.5, 3.5, 4.5], "python 🚀 jessica and morty's date of birth",
            {"jessica": {'Lunar': '2003-3-19', "Solar": '2003-1-17'},
             'morty': {"Lunar": '2002-4-27', 'Solar': '2002-3-26'}}])
client.execute("insert into default.test_id_vector_text_map_s_m values ", res)
for row in client.fetch("select * from default.test_id_vector_text_map_s_m order by id asc"):
    print(row)
assert len([row for row in client.fetch("select * from default.test_id_vector_text_map_s_m")]) == 5
print("======> upload map(string,map(int,array))")
res2 = []
res2.append((1,
             {'case1': {1: [1.2, 3.2, 1.1], 2: [2.2, 4.2, 3.1], 3: [2.5, 3.4, 5.1]},
              'case2': {1: [1.2, 3.2, 1.1], 2: [2.2, 4.2, 3.1], 3: [2.5, 3.4, 5.1]}}))
res2.append([2,
             {"case1": {1: [1.2, 3.2, 1.1], 2: [2.2, 4.2, 3.1], 3: [2.5, 3.4, 5.1]},
              "case2": {1: [1.2, 3.2, 1.1], 2: [2.2, 4.2, 3.1], 3: [2.5, 3.4, 5.1]}}
             ])
client.execute("insert into default.test_id_map_s_i_a values ", res2)
for row in client.fetch("select * from default.test_id_map_s_i_a order by id asc"):
    print(row)
assert len([row for row in client.fetch("select * from default.test_id_map_s_i_a")]) == 3

# upload data from python tuples
print("\n<======= Upload from python tuples ========>")
client.execute("insert into default.test_id_vector_text_map_s_m values ",
               (5, [1.6, 2.6, 3.6, 4.6], "python tuple 🌹 yolo and charis's date of birth",
                {'yolo': {'Lunar': '2011-3-19', 'Solar': '2011-1-17'},
                 'charis': {'Lunar': '2012-4-27', 'Solar': '2012-3-26'}}),
               [6, [1.7, 2.7, 3.7, 4.7], "python tuple 🌹 yolo and charis's date of birth",
                {'yolo': {'Lunar': '2011-3-19', 'Solar': '2011-1-17'},
                 'charis': {'Lunar': '2012-4-27', 'Solar': '2012-3-26'}}])
for row in client.fetch("select * from default.test_id_vector_text_map_s_m order by id asc"):
    print(row)

# res=client.fetch("select * from default.test_id_vector_text_map_s_m order by id asc",json=True)
# print(res)
assert len([row for row in client.fetch("select * from default.test_id_vector_text_map_s_m")]) == 7

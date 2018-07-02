import json
import string
from bson.objectid import ObjectId
import bson.json_util
import ast
import pandas as pd
import simplejson as sj

def cleanup_id(_id_dict_str):
	return bson.json_util.dumps(ObjectId(_id_dict_str))

data = '{"$oid" : "5b360148e2c380447c429500"}'
data2 = '{\"$oid\" : \"5b360148e2c380447c429510\"}'
data3 = "{'$oid' : '5b360148e2c380447c42c520'}"
data4 = "{\'$oid\' : \'5b360148e2c380447c429530\'}"
data5 = "{\"$oid\" : \"5b360148e2c380447c429540\"}"

print(data)
print(data2)
print(data3)
print(data4)
print(data5)

print(bson.json_util.loads(data))
print(bson.json_util.loads(data2))
print(bson.json_util.loads(sj.dumps(data3)))
print(bson.json_util.loads(data4))
print(bson.json_util.loads(data5))


print(cleanup_id(data))
print(cleanup_id(data2))
print(cleanup_id(data3))
print(cleanup_id(data4))
print(cleanup_id(data5))
# data_json = json.loads(data)
# bad_data_json = json.loads(bad_data)

# _id_str = data_json['$oid']
# bad_id_str = bad_data_json['$oid']

# def _id_str_check(_id):
# 	for char in _id:
# 		if char not in string.hexdigits:
# 			return False
# 	return len(_id) == 24

# print(_id_str_check(_id_str))
# print(ObjectId(_id_str))
# print(json.loads(data))
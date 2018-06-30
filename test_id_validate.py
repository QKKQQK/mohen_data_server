import json
import string

data = {"$oid" : "5b360148e2c380447c429c50"}

bad_data = '{"$oid" : "5b360148e2c380447c429c5"}'

data_json = json.loads(data)
bad_data_json = json.loads(bad_data)

_id_str = data_json['$oid']
bad_id_str = bad_data_json['$oid']

def _id_str_check(_id):
	for char in _id:
		if char not in string.hexdigits:
			return False
	return len(_id) == 24

print(_id_check(_id_str))
print(len(bad_id_str))
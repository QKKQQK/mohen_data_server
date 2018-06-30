from bson.objectid import ObjectId
import random
import time as t
import bson.json_util as bson
from datetime import datetime, timedelta, tzinfo
import sys
import getopt
import json

def print_usage():
	print("Usage: random_data.py -n <number_of_records_with_same_id>")

def random_id_from_int(n):
	to_hex = format(random.randint(0, n), 'x')
	to_hex = to_hex.zfill(24)
	return str(to_hex)

def rand_record():
	utc_timestamp = t.time() + t.timezone
	exttype = random.randint(1, 600)
	rand_extlist = {}
	if random.randint(0, 9) > 4:
		rand_extlist = {'test_path' : [{'$oid' : random_id_from_int(10000)} \
				for _ in range(random.randint(0,10))] }

	data = {
	  # 上传唯一id
	  "_id" : bson.dumps(ObjectId()),
	  # parent id
	  "pid" : {'$oid' : random_id_from_int(1000)},
	  # 事件名称
	  "name" : random.sample(html_tags, 1)[0],
	  # 可用作删除
	  "flag" : 1,
	  # 分类
	  "exttype" : exttype,
	  "type" : exttype // 10,
	  # 拓展类别，如用户性别
	  "tag" : [{'$oid' : random_id_from_int(1000)} for _ in range(random.randint(0,5))],
	  # 知识点树路径
	  "klist" : [{'$oid' : random_id_from_int(1000)} for _ in range(random.randint(0,5))],
	  # 关系树路径
	  "rlist" : [{'$oid' : random_id_from_int(1000)} for _ in range(random.randint(0,7))],
	  # 拓展路径
	  "extlist" : rand_extlist,
	  # 届
	  "ugroup" : random.randint(2010, 2018),
	  # 用户ID
	  "uid" : bson.dumps(ObjectId()),
	  # 文件ID
	  "fid" : bson.dumps(ObjectId()),
	  # 设备ID
	  "eid" : bson.dumps(ObjectId()),
	  # 外部ID
	  "openid" : {'$oid' : random_id_from_int(100)},
	  # 次数
	  "v1" : random.uniform(10, 90),
	  # 时长(秒)
	  "v2" : random.uniform(50, 500),
	  # 拓展数值
	  "v3" : { 'test_v3' : random.uniform(200, 222222)},
	  # 字符串数值
	  "cfg" : random.sample(html_tags, 1)[0],
	  # UTC 日期时间
	  "utc_date" : {'$date' : utc_timestamp}
	}
	return data

html_tags = ["a","abbr","acronym","address","applet","area","article","aside",
			 "audio","b","base","basefont","bdi","bdo","big","blockquote","body",
			 "br","button","canvas","caption","center","cite","code","col",
			 "colgroup","command","datalist","dd","del","details","dir","div",
			 "dfn","dialog","dl","dt","em","embed","fieldset","figcaption",
			 "figure","font","footer","form","frame","frameset","h6","head",
			 "header","hr","html","i","iframe","img","input","ins","isindex",
			 "kbd","keygen","label","legend","li","link","map","mark","menu",
			 "meta","meter","nav","noframes","noscript","object","ol","optgroup",
			 "option","output","p","param","pre","progress","q","rp","rt","ruby",
			 "s","samp","script","section","select","small","source","span",
			 "strike","strong","style","sub","details","sup","table","tbody",
			 "td","textarea","tfoot","th","thead","time","title","tr","track",
			 "tt","u","ul","var","video","wbr","xmp"]

def main():
	n_same_id_records = 0
	n_unique_id_records = 0
	try:
		opts, args = getopt.getopt(sys.argv[1:], "n:s:")
	except getopt.GetoptError:
		print_usage()
		sys.exit(1)
	if len(sys.argv) < 3:
		print_usage()
		sys.exit(1)
	for opt, arg in opts:
		if opt == '-s':
			try:
				n_same_id_records = int(arg)
				if n_same_id_records < 0:
					print("相同_id的数据数量需要大于等于零")
					print_usage()
					sys.exit(2)
			except ValueError:
				print_usage()
				sys.exit(2)
		elif opt == '-n':
			try:
				n_unique_id_records = int(arg)
				if n_unique_id_records <= 0:
					print("不同_id的数据数量需要大于零")
					print_usage()
					sys.exit(2)
			except ValueError:
				print_usage()
				sys.exit(2)
		else:
			print_usage()
			sys.exit(3)

	data = []
	for _ in range(n_unique_id_records):
		data.append(rand_record())
	for i in range(n_same_id_records):
		record = data[i]
		record['cfg'] = '此条数据为相同_id覆盖数据'
		data.append(record)
	req_body = {'data' : data}
	with open('data.json', 'w') as outfile:
		outfile.write(json.dumps(req_body, indent=4))

if __name__ == "__main__":
    main()
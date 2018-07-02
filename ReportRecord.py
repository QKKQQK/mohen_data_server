import json 
import sys
from bson.objectid import ObjectId
import bson.json_util
import ast

empty_id = {'$oid' : '000000000000000000000000'}

class ReportRecord:
	def __init__(self, data):
		self._id = data['_id']
		self.pid = data['pid'] if 'pid' in data else empty_id
		self.name = data['name'] if 'name' in data else ""
		self.flag = data['flag'] if 'flag' in data else 1
		self.exttype = data['exttype']
		self.type = data['type']
		self.tag = data['tag'] if 'tag' in data else []
		self.klist = data['klist'] if 'klist' in data else []
		self.rlist = data['rlist'] if 'rlist' in data else []
		self.extlist = data['extlist'] if 'extlist' in data else {}
		self.ugroup = data['ugroup'] if 'ugroup' in data else 0
		self.uid = data['uid'] if 'uid' in data else empty_id
		self.fid = data['fid'] if 'fid' in data else empty_id
		self.eid = data['eid']
		self.openid = data['openid']
		self.v1 = data['v1']
		self.v2 = data['v2'] if 'v2' in data else 0
		self.v3 = data['v3'] if 'v3' in data else {}
		self.cfg = data['cfg'] if 'cfg' in data else ""
		self.utc_date = data['utc_date']

	def get_id_str(self):
		return self._id

	def toJSON(self):
		return json.dumps(self, default=lambda o: o.__dict__, indent=4)
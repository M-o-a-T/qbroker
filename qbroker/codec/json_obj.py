#!/usr/bin/python
# -*- coding: utf-8 -*-

from __future__ import print_function,absolute_import
import sys
from time import mktime
from json.encoder import JSONEncoder
from json.decoder import JSONDecoder
from qbroker.util import attrdict, TZ,UTC, format_dt
import datetime as dt
from collections.abc import Mapping

CODEC = "application/json+obj"
DEFAULT = True

DEBUG=False

type2cls = {}
name2cls = {}
def _reg(cls):
	type2cls[cls.cls.__name__] = cls
	name2cls[cls.clsname] = cls
	return cls

@_reg
class _datetime(object):
	cls = dt.datetime
	clsname = "datetime"

	@staticmethod
	def encode(obj):
		## the string is purely for human consumption and therefore does not have a time zone
		return {"t":mktime(obj.timetuple()),"s":format_dt(obj)}

	@staticmethod
	def decode(t=None,s=None,a=None,k=None,**_):
		if t:
			return dt.datetime.utcfromtimestamp(t).replace(tzinfo=UTC).astimezone(TZ)
		else: ## historic
			assert a
			return dt.datetime(*a).replace(tzinfo=TZ)

@_reg
class _date(object):
	cls = dt.date
	clsname = "date"

	@staticmethod
	def encode(obj):
		return {"d":obj.toordinal(), "s":obj.strftime("%Y-%m-%d")}

	@staticmethod
	def decode(d=None,s=None,a=None,**_):
		if d:
			return dt.date.fromordinal(d)
		## historic
		return dt.date(*a)

@_reg
class _time(object):
	cls = dt.time
	clsname = "time"

	@staticmethod
	def encode(obj):
		ou = obj.replace(tzinfo=UTC)
		secs = ou.hour*3600+ou.minute*60+ou.second
		return {"t":secs,"s":"%02d:%02d:%02d" % (ou.hour,ou.minute,ou.second)}

	@staticmethod
	def decode(t=None,s=None,a=None,k=None,**_):
		if t:
			return dt.datetime.utcfromtimestamp(t).time()
		return dt.time(*a)

class Encoder(JSONEncoder):
	def __init__(self,tablespace,main=()):
		self.objcache = {}
		self.main = main
		self.tablespace=tablespace
		super(Encoder,self).__init__(skipkeys=False, ensure_ascii=False,
			check_circular=False, allow_nan=False, sort_keys=False,
			indent=(2 if DEBUG else None),
			separators=((', ', ': ') if DEBUG else (',', ':')))

	def default(self, data):
		obj = type2cls.get(data.__class__.__name__,None)
		if obj is not None:
			data = obj.encode(data)
			data["_o"] = obj.clsname
			return data

		if self.tablespace is not None and isinstance(data,self.tablespace.table_data):
			d = {"_table":data.name}
			return d

		if hasattr(data,"_read"):
			ci = (data._t.name,data._id)
			cr = self.objcache.get(ci,None)
			if cr is not None:
				d = {"_cr":cr}
			else:
				cr = len(self.objcache)+1
				self.objcache[ci] = cr

				d = {"_index": data._id, "_table": data._t.name, "_cr": cr}
				send = False
				if data in self.main:
					send = True
				else:
					mode = getattr(data,'_mode',F_STANDARD)
					if mode in (F_SQL,F_STORE):
						send = True
				if isinstance(data._d,Exception):
					d["_error"] = str(data._d)
				elif send:
					for k,v in data:
						d[k] = v
			return d
		return super(Encoder,self).default(data)

def encode(data,tablespace=None):
	main = set()
	if hasattr(data,"_read"):
		if data._d is None:
			data._read()
		main.add(data)
	else:
		try:
			d = data["data"]
			if d._d is None:
				d._read()
			if isinstance(d,(list,tuple)):
				for dd in d:
					main.add(dd)
			else:
				main.add(d)
		except (TypeError,KeyError,AttributeError):
			pass
	return Encoder(tablespace,main).encode(data)

class Decoder(JSONDecoder):
	def __init__(self, proxy,tablespace):
		self.objcache = {}
		self.proxy = proxy
		self.tablespace = tablespace
		super(Decoder,self).__init__(object_hook=self.hook)

	def hook(self,data):
		if not isinstance(data,Mapping):
			return data

		ev = data.pop('_o',None)
		if ev is not None:
			return name2cls[ev].decode(**data)

		cr = data.pop('_cr',None)
		if cr is None:
			if '_table' in data and len(data) == 1:
				data = self.tablespace[data['_table']]
			else:
				data = attrdict(data)
			return data
		d = self.objcache.get(cr,None)
		if d is None:
			d = self.proxy()
			self.objcache[cr]=d

		try:
			t = data.pop('_table')
			idx = data.pop('_index')
			idx = tuple(tuple(x) for x in idx) # convert from nested list
		except KeyError:
			pass # recursive or whatever
		else:
			d = d._set(t,idx,**data)
			self.objcache[cr] = d
		return d
	
def decode(data, tablespace=None, proxy=None, p1=None):
	if isinstance(data,bytes):
		data = data.decode('utf-8')

	d = Decoder(proxy,tablespace)
	if p1:
		d.objcache[1] = p1
	return d.decode(data)


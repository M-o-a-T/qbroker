#!/usr/bin/python
# -*- coding: utf-8 -*-

from __future__ import print_function,absolute_import
import sys
from time import mktime
from json.encoder import JSONEncoder
from json.decoder import JSONDecoder
from qbroker.util import attrdict
from collections.abc import Mapping
from .registry import type2cls,name2cls

CODEC = "application/json+repr"
DEFAULT = False

DEBUG=False

class Encoder(JSONEncoder):
	def __init__(self,main=()):
		self.objcache = {}
		self.main = main
		super(Encoder,self).__init__(skipkeys=False, ensure_ascii=False,
			check_circular=False, allow_nan=False, sort_keys=False,
			indent=(2 if DEBUG else None),
			separators=((', ', ': ') if DEBUG else (',', ':')))

	def default(self, data):
		obj = type2cls.get(type(data),None)
		if obj is not None:
			data = obj.encode(data)
			data["_o"] = obj.clsname
			return data

		try:
			return super(Encoder,self).default(data)
		except Exception:
			return {'_or':repr(data)}

def encode(data):
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
	res = Encoder(main).encode(data)
	if isinstance(res,str):
		res = res.encode('utf-8')
	return res

class Decoder(JSONDecoder):
	def __init__(self, proxy):
		self.objcache = {}
		self.proxy = proxy
		super(Decoder,self).__init__(object_hook=self.hook)

	def hook(self,data):
		if not isinstance(data,Mapping):
			return data

		ev = data.pop('_o',None)
		if ev is not None:
			return name2cls[ev].decode(**data)

		rep = data.pop('_or',None)
		if rep is not None:
			return rep

		cr = data.pop('_cr',None)
		if cr is None:
			data = attrdict(data)
			return data
		d = self.objcache.get(cr,None)
		if d is None:
			d = self.proxy()
			self.objcache[cr]=d

		return d
	
def decode(data, proxy=None, p1=None):
	if isinstance(data,bytes):
		data = data.decode('utf-8')

	d = Decoder(proxy)
	if p1:
		d.objcache[1] = p1
	return d.decode(data)


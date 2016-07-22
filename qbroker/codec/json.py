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

CODEC = "application/json"

DEBUG=False

class Encoder(JSONEncoder):
	def __init__(self):
		super(Encoder,self).__init__(skipkeys=False, ensure_ascii=False,
			check_circular=True, allow_nan=False, sort_keys=False,
			indent=(2 if DEBUG else None),
			separators=((', ', ': ') if DEBUG else (',', ':')))
enc = Encoder()

def encode(data,tablespace=None):
	res = enc.encode(data)
	if isinstance(res,str):
		res = res.encode('utf-8')
	return res

class Decoder(JSONDecoder):
	pass
dec = Decoder()

def decode(data, tablespace=None, proxy=None, p1=None):
	if isinstance(data,bytes):
		data = data.decode('utf-8')
	return dec.decode(data)


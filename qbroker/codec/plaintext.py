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

CODEC = "text/plain"

DEBUG=False

def encode(data):
	if not data:
		data = b""
	elif isinstance(data,str):
		data = data.encode('utf-8')
	elif not isinstance(data,bytes): # pre-encoded. Oh well.
		raise RuntimeError("Need Unicode, not %s"%str(type(data)))
	# else:
    #     data.decode('utf-8') # assertion only
	return data

def decode(data):
	data = data.decode('utf-8')
	return data


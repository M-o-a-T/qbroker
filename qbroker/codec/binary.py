#!/usr/bin/python
# -*- coding: utf-8 -*-

from __future__ import print_function,absolute_import

CODEC = "application/binary"

def encode(data):
	if not data:
		data = b''
	elif isinstance(data,str):
		data = data.encode('utf-8')
	elif not isinstance(data,bytes):
		raise RuntimeError("Need bytes, not %s"%str(type(data)))
	return data

def decode(data):
	assert isinstance(data,bytes), type(data)
	return data


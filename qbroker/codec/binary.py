#!/usr/bin/python
# -*- coding: utf-8 -*-

from __future__ import print_function,absolute_import

CODEC = "application/binary"

def encode(data):
	return data

def decode(data):
	if isinstance(data,str):
		data = data.encode('utf-8')
	return data


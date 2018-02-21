# -*- coding: utf-8 -*-
from __future__ import absolute_import, print_function, division, unicode_literals
##
## This file is part of QBroker, an easy to use RPC and broadcast
## client+server using AMQP.
##
## QBroker is Copyright © 2016 by Matthias Urlichs <matthias@urlichs.de>,
## it is licensed under the GPLv3. See the file `README.rst` for details,
## including optimistic statements by the author.
##
## This paragraph is auto-generated and may self-destruct at any time,
## courtesy of "make update". The original is in ‘utils/_boilerplate.py’.
## Thus, please do not remove the next line, or insert any blank lines.
##BP

import inspect

from . import CC_MSG,CC_DICT,CC_DATA
from .util import attrdict, import_string, uuidstr

async def coro_wrapper(proc, *a, **kw):
	"""\
		This code is responsible for turning whatever callable you pass in
		into a "yield from"-style coroutine.
		"""
	proc = proc(*a, **kw)
	if inspect.isawaitable(proc):
		proc = await proc
	return proc

class RPCservice(object):
	"""\
		This object wraps one specific RPC service.
		"""
	queue = None
	is_alert = None

	def __new__(cls, fn, name=None, **kw):
		if isinstance(fn, RPCservice):
			return fn
		return object.__new__(cls)

	def __init__(self, fn, name=None, call_conv=CC_MSG, durable=None, ttl=None):
		if isinstance(fn, RPCservice):
			return
		if name is None:
			name = fn.__module__+'.'+fn.__name__
		self.fn = fn
		self.name = name
		self.call_conv = call_conv
		self.durable = durable
		self.uuid = uuidstr()
		self.ttl = ttl
	
	async def run(self, *a,**k):
		res = await coro_wrapper(self.fn, *a, **k)
		return res

	def __str__(self):
		if self.is_alert:
			n = "ALERT"
		elif self.is_alert is not None:
			n = "RPC"
		else:
			n = '?'
		return "‹%s %s %s %s›" % (n,self.name,self.call_conv,self.fn)
		

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

import asyncio
import inspect

from . import CC_MSG,CC_DICT,CC_DATA
from ..util import attrdict, import_string, uuidstr

if not hasattr(inspect,'iscoroutinefunction'):
	inspect.iscoroutinefunction = inspect.isgeneratorfunction
if not hasattr(inspect,'iscoroutine'):
	inspect.iscoroutine = inspect.isgenerator
if not hasattr(inspect,'isawaitable'):
	def isawaitable(object):
		return False
	inspect.isawaitable = isawaitable

@asyncio.coroutine
def coro_wrapper(proc, *a,**k):
	"""\
		This code is responsible for turning whatever callable you pass in
		into a "yield from"-style coroutine.
		"""
	proc = proc(*a,**k)
	if hasattr(proc,'__await__'):
		return (yield from proc.__await__())
	if inspect.iscoroutine(proc) or inspect.isgenerator(proc):
		proc = (yield from proc)
	return proc

class RPCservice(object):
	"""\
		This object handles one specific RPC service
		"""
	queue = None
	is_alert = None
	call_conv = None
	durable = False
	ttl = None

	def __init__(self, fn,name=None, call_conv=CC_MSG, durable=None, ttl=None):
		if name is None:
			name = fn.__module__+'.'+fn.__name__
		self.fn = fn
		self.name = name
		self.call_conv = call_conv
		self.durable = durable
		self.uuid = uuidstr()
		if ttl is not None:
			self.ttl = ttl
	
	@asyncio.coroutine
	def run(self, *a,**k):
		res = (yield from coro_wrapper(self.fn,*a,**k))
		return res

	def __str__(self):
		if self.is_alert:
			n = "ALERT"
		elif self.is_alert is not None:
			n = "RPC"
		else:
			n = '?'
		return "‹%s %s %s %s›" % (n,self.name,self.call_conv,self.fn)
		

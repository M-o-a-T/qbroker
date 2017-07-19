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
import weakref
from ..util.weak import OptWeakValueDictionary

import logging
logger = logging.getLogger(__name__)

class Debugger(object):
	"""QBroker insert for debugging."""

	def __init__(self, conn):
		self.conn = weakref.ref(conn)
		self.env = OptWeakValueDictionary()

	@asyncio.coroutine
	def run(self, cmd=None, **args):
		"""Connect. This may fail."""

		if cmd is None:
			return dict((x[4:],x.__doc__) for x in dir(self) if x.startswith("run_"))
		conn = self.conn()
		return (yield from getattr(self,'run_'+cmd)(conn=conn, **args))

	@asyncio.coroutine
	def run_eval(self,conn, code=None, **args):
		res = eval(code,dict(self.env),args)
		if inspect.iscoroutine(res):
			res = yield from res
		return res
	
	@asyncio.coroutine
	def run_ping(self,conn):
		return "pong"

class _NOTGIVEN:
	pass


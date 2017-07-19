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

import logging
logger = logging.getLogger(__name__)

class Debugger(object):
	"""QBroker insert for debugging."""

	def __init__(self):
		self.env = {}

	@asyncio.coroutine
	def run(self, cmd=None, **args):
		"""Connect. This may fail."""

		if cmd is None:
			if args:
				raise RuntimeError("Need 'cmd' parameter")
			return dict((x[4:],getattr(self,x).__doc__) for x in dir(self) if x.startswith("run_"))
		return (yield from getattr(self,'run_'+cmd)(**args))

	@asyncio.coroutine
	def run_eval(self, code=None, mode="eval", **args):
		"""Evaluate @code (string). @mode may be 'exec', 'single' or 'eval' (default).
		    All other arguments are used as local variables.
			Non-local values are persistent.
		    """
		ed = dict(self.env)
		code = compile(code,"(debug)",mode)
		loc = args.copy()
		res = eval(code, self.env,loc)
		if inspect.iscoroutine(res):
			res = yield from res
		for k,v in loc.items():
			if k not in args:
				self.env[k] = v
		return res
	
	@asyncio.coroutine
	def run_ping(self):
		"""Return 'pong'"""
		return "pong"


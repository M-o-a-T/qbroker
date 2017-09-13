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
	def run(self,msg):
		"""Evaluate a debugger command."""
		msg.codec = 'application/json+repr'
		args = msg.data if msg.data != '' else {}
		cmd = args.pop('cmd',None)

		if cmd is None:
			if args:
				raise RuntimeError("Need 'cmd' parameter")
			return dict((x[4:],getattr(self,x).__doc__) for x in dir(self) if x.startswith("run_"))
		return (yield from getattr(self,'run_'+cmd)(**args))

	@asyncio.coroutine
	def run_env(self, **args):
		"""Dump the debugger's environment"""
		return dict((k,v) for k,v in self.env.items() if k != '__builtins__')

	@asyncio.coroutine
	def run_eval(self, code=None, mode="eval", **args):
		"""Evaluate @code (string). @mode may be 'exec', 'single' or 'eval'/'vars' (default).
		    All other arguments are used as local variables.
			Non-local values are persistent.

			'vars' is like 'eval' but applies vars() to the result.
		    """
		do_vars = False
		if mode == "vars":
			mode = "eval"
			do_vars = True

		ed = dict(self.env)
		code = compile(code,"(debug)",mode)
		loc = args.copy()
		res = eval(code, self.env,loc)
		if inspect.iscoroutine(res):
			res = yield from res
		for k,v in loc.items():
			if k not in args:
				self.env[k] = v
		if do_vars:
			try:
				r = vars(res)
			except TypeError:
				r = {}
				for k in dir(res):
					v = getattr(res,k)
					if not callable(v):
						r[k]=v
			r['__obj__'] = str(res)
			res = r
		return res
	
	@asyncio.coroutine
	def run_ping(self):
		"""Return 'pong'"""
		return "pong"


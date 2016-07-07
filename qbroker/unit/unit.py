# -*- coding: utf-8 -*-
from __future__ import absolute_import, print_function, division, unicode_literals
##
## This file is part of QBroker, a distributed data access manager.
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
from ..util import uuidstr, combine_dict
from ..util.sync import SyncFuncs
from .msg import RequestMsg,PollMsg,AlertMsg
from .rpc import CC_MSG
from . import DEFAULT_CONFIG
from collections.abc import Mapping

import logging
logger = logging.getLogger(__name__)

class _NOTGIVEN:
	pass

class Unit(object, metaclass=SyncFuncs):
	"""The basic QBroker messenger."""
	config = None # configuration data
	conn = None # AMQP receiver
	uuid = None # my UUID
	restarting = None

	def __init__(self, app, *, loop=None, **cfg):
		self._loop = loop or asyncio.get_event_loop()
		self.app = app

		self.config = combine_dict(cfg, DEFAULT_CONFIG)
		self.restarting = asyncio.Event(loop=loop)

		self.rpc_endpoints = {}
		self.alert_endpoints = {}

	@asyncio.coroutine
	def start(self, *args, restart=False):
		"""Connect. This may fail."""

		if self.uuid is None:
			self.uuid = uuidstr()

		self.register_alert("qbroker.ping",self._alert_ping)
		self.register_rpc("qbroker.ping."+self.uuid, self._reply_ping)

		yield from self._create_conn()
		yield from self.alert('qbroker.restart' if restart else 'qbroker.start', uuid=self.uuid, app=self.app, args=args)
		self.restarting.set()
	
	@asyncio.coroutine
	def restart(self, t_min=10,t_inc=20,t_max=100):
		"""Reconnect. This will not fail."""
		while self.restarting is not None:
			yield from self.restarting.wait()

		try:
			self._kill()
		except Exception:
			pass
		
		while True:
			try:
				yield from self.start(restart=True)
			except Exception as exc:
				logger.exception("Reconnect of %s failed", self)
				yield from asyncio.sleep(t_min, loop=self._loop)
				t_min = min(t_min+t_inc, t_max)
			else:
				return
	
	@asyncio.coroutine
	def stop(self, rc=0):
		yield from self.alert('qbroker.stop', uuid=self.uuid, exitcode=rc)

		c,self.conn = self.conn,None
		if c:
			try:
				yield from c.close()
			except Exception: # pragma: no cover
				logger.exception("closing connection")
		self._kill()
	
	## client

	@asyncio.coroutine
	def rpc(self,name, _data=None, **data):
		"""Send a RPC request.
		Returns the response. 
		"""
		if _data is not None:
			assert not data
			data = _data
		msg = RequestMsg(name, self, data)
		res = (yield from self.conn.call(msg))
		res.raise_if_error()
		return res.data

	@asyncio.coroutine
	def alert(self,name, _data=None, *, timeout=None,callback=None,call_conv=CC_MSG, **data):
		"""Send a broadcast alert.
		If @callback is not None, call on each response until the time runs out
		"""
		if _data is not None:
			assert not data
			data = _data
		if callback:
			msg = PollMsg(name, self, data=data, callback=callback,call_conv=call_conv)
		else:
			msg = AlertMsg(name, self, data=data)
		res = (yield from self.conn.call(msg, timeout=timeout))
		return res
		
	## server

	def register_rpc(self, *a, _async=False, _alert=False, call_conv=CC_MSG):
		"""\
			Register an RPC listener.
				
				conn.register_rpc(RPCservice(fn,name))
				conn.register_rpc(name,fn)
				conn.register_rpc(fn)
				@conn_register(name)
				def fn(…): pass
				@conn_register
				def fn(…): pass

			If connected, you must use the _async version.
			"""
		name = None
		@asyncio.coroutine
		def reg_async(fn,epl):
			if _alert:
				yield from self.conn.register_alert(fn)
			else:
				yield from self.conn.register_rpc(fn)
			epl[name] = fn
			return fn
		def reg(fn):
			nonlocal name
			from .rpc import RPCservice
			if not isinstance(fn,RPCservice):
				if name is None:
					name = fn.__name__
					name = name.replace('._','.')
					name = name.replace('_','.')
				fn = RPCservice(name=name,fn=fn, call_conv=call_conv)
			elif name is None:
				name = fn.name
			assert fn.is_alert is None
			if _alert:
				epl = self.alert_endpoints
			else:
				epl = self.rpc_endpoints
			assert name not in epl, name
			fn.is_alert = _alert
			if _async and self.conn is not None:
				return reg_async(fn,epl)
			else:
				assert self.conn is None,"Use register_*_async() when connected"
			epl[name] = fn
			return fn.fn
		assert len(a) <= 2
		if len(a) == 0:
			return reg
		elif len(a) == 2:
			name = a[0]
			return reg(a[1])
		else:
			a = a[0]
			if isinstance(a,str):
				name = a
				return reg
			else:
				return reg(a)
	
	def register_rpc_async(self, *a, **kw):
		"""Register an RPC listener while connected.
				
				yield from conn.register_rpc(RPCservice(fn,name))
				yield from conn.register_rpc(name,fn)
				yield from conn.register_rpc(fn)
			"""
		return self.register_rpc(*a, _async=True, **kw)

	def register_alert(self, *a, _async=False, **kw):
		"""Register an alert listener.
		   See register_rpc for calling conventions."""
		return self.register_rpc(*a, _async=_async, _alert=True, **kw)

	def register_alert_async(self, *a, **kw):
		"""Register an alert listener.
		   See register_rpc_async for calling conventions."""
		return self.register_rpc(*a, _async=True, _alert=True, **kw)

	def unregister_rpc(self, fn, _async=False,_alert=False):
		if not isinstance(fn,str):
			if hasattr(fn,'name'):
				fn = fn.name
			else:
				fn = fn.__name__
				fn = fn.replace('._','.')
				fn = fn.replace('_','.')
		if _alert:
			epl = self.alert_endpoints
		else:
			epl = self.rpc_endpoints
		fn = epl.pop(fn)
		if fn.is_alert != _alert:
			raise RuntimeError("register/unregister alert: %s/%s" % (fn.is_alert,_alert))
		if _async:
			if _alert:
				return self.conn.unregister_alert(fn)
			else:
				return self.conn.unregister_rpc(fn)

	def unregister_alert(self, fn):
		return self.unregister_rpc(fn, _alert=True)

	def unregister_rpc_async(self, fn):
		return self.unregister_rpc(fn, _async=True)

	def unregister_alert_async(self, fn):
		return self.unregister_rpc(fn, _async=True, _alert=True)

	def _alert_ping(self,msg):
		return dict(
			app=self.app,
			uuid=self.uuid,
			)

	def _reply_ping(self,msg):
		return dict(
			app=self.app,
			uuid=self.uuid,
			rpc=list(self.rpc_endpoints.keys()),
			alert=list(self.alert_endpoints.keys()),
			)
		
	## cleanup, less interesting (hopefully)

	def __del__(self):
		self._kill(deleting=True)

	def _kill(self, deleting=False):
		self._kill_conn(deleting=deleting)

	def _kill_conn(self, deleting=False):
		c,self.conn = self.conn,None
		if c: # pragma: no cover
			try:
				c._kill()
			except Exception:
				if not deleting:
					logger.exception("closing connection")

	@asyncio.coroutine
	def _create_conn(self):
		from .conn import Connection
		conn = Connection(self)
		yield from conn.connect()
		for d in self.rpc_endpoints.values():
			yield from conn.register_rpc(d)
		for d in self.alert_endpoints.values():
			yield from conn.register_alert(d)
		self.conn = conn


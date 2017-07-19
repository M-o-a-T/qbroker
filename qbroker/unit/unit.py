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
from traceback import print_exc
from ..util import uuidstr, combine_dict
from ..util.sync import SyncFuncs, sync_maker,gevent_maker
from .msg import RequestMsg,PollMsg,AlertMsg
from .rpc import CC_MSG,CC_DATA,CC_DICT
from . import DEFAULT_CONFIG
from collections.abc import Mapping
from aioamqp.exceptions import ChannelClosed

import logging
logger = logging.getLogger(__name__)

class Unit(object, metaclass=SyncFuncs):
	"""The basic QBroker messenger."""
	config = None # configuration data
	conn = None # AMQP receiver
	uuid = None # my UUID
	restarting = None
	args = ()
	debug = None

	def __init__(self, app, *, loop=None, hidden=False, **cfg):
		"""\
			Connect to an AMQP server. See qbroker.unit.DEFAULT_CONFIG for
			all recognized parameters.

			>>> u = Unit("my_nice_server", 
			...      amqp=dict(
			...         server=dict(
			...            login="foo",
			...            password="bar",
			...            virtualhost="/test")))

			You need to call .start() to actually initiate a connection.
			"""

		self.hidden = hidden

		self._loop = loop or asyncio.get_event_loop()
		self.app = app

		self.config = combine_dict(cfg, DEFAULT_CONFIG)
		self.restarting = asyncio.Event(loop=loop)

		self.rpc_endpoints = {}
		self.alert_endpoints = {}

		if self.config['amqp']['handlers']['debug']:
			from .debug import Debugger
			self.debug = Debugger()

	@asyncio.coroutine
	def start(self, *args, restart=False, _setup=None):
		"""Connect. This may fail."""

		if self.uuid is None:
			self.uuid = uuidstr()
		if args:
			self.args = args

		if not restart and not self.hidden:
			self.register_alert("qbroker.ping",self._alert_ping, call_conv=CC_DATA)
			self.register_rpc("qbroker.ping", self._reply_ping)
			self.register_alert("qbroker.app."+self.app, self._alert_ping, call_conv=CC_DATA)
			self.register_rpc("qbroker.app."+self.app, self._reply_ping)
			self.register_rpc("qbroker.debug."+self.app, self._reply_debug, call_conv=CC_DICT)
			# uuid: done in conn setup

		yield from self._create_conn(_setup=_setup)
		if not self.hidden:
			yield from self.alert('qbroker.restart' if restart else 'qbroker.start', uuid=self.uuid, app=self.app, args=args)
		if not restart:
			self.restarting.set()
			self.restarting = None
	
	@asyncio.coroutine
	def restart(self, t_min=10,t_inc=20,t_max=100):
		"""Reconnect. This will not fail."""
		if self.restarting is not None:
			yield from self.restarting.wait()
			return
		self.restarting = asyncio.Event(loop=self._loop)

		try:
			self.close()
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
				self.restarting.set()
				self.restarting = None
				return
	
	def restart_cb(self,f):
		def done(ff):
			if ff.cancelled():
				return
			try:
				ff.result()
			except Exception as exc:
				print_exc()

		if f.cancelled():
			return
		f = asyncio.ensure_future(self.restart(), loop=self._loop)
		f.add_done_callback(done)

	@asyncio.coroutine
	def stop(self, rc=0):
		if self.conn is None:
			return
		if not self.hidden:
			try:
				yield from self.alert('qbroker.stop', uuid=self.uuid, exitcode=rc)
			except ChannelClosed:
				pass

		yield from self.conn.close()
		self.close()

	## client

	@asyncio.coroutine
	def rpc(self,name, _data=None, *, _dest=None,_uuid=None, **data):
		"""Send a RPC request.
		Returns the response. 
		"""
		if _data is not None:
			assert not data, data
			data = _data
		msg = RequestMsg(name, self, data)
		assert _uuid is None or _dest is None
		if _dest is not None:
			_dest = 'qbroker.app.'+_dest
		elif _uuid is not None:
			_dest = 'qbroker.uuid.'+_uuid

		while self.conn is None:
			yield from self.restart()
		res = (yield from self.conn.call(msg,dest=_dest))
		res.raise_if_error()
		return res.data

	@asyncio.coroutine
	def alert(self,name, _data=None, *, _dest=None,_uuid=None, timeout=None,callback=None,call_conv=CC_MSG, **data):
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
		assert _uuid is None or _dest is None
		if _dest is not None:
			_dest = 'qbroker.uuid.'+_dest
		elif _uuid is not None:
			_dest = 'qbroker.uuid.'+_uuid
		while self.conn is None:
			yield from self.restart()
		res = (yield from self.conn.call(msg, dest=_dest, timeout=timeout))
		return res
		
	## server

	def register_rpc(self, *a, _async=False, _alert=False, call_conv=CC_MSG, durable=None,ttl=None):
		"""\
			Register an RPC listener.
				
				conn.register_rpc(RPCservice(fn,name))
				conn.register_rpc(name,fn)
				conn.register_rpc(fn)
				@conn_register(name)
				def fn(…): pass
				@conn_register
				def fn(…): pass

			Set @durable to True for delivery to a persistent queue, or to
			a unique queue name. Note that this queue will never be deleted
			by qbroker.

			@ttl is in seconds.

			If connected, you need to use the _async version.
			(Or _sync or _gevent, if applicable)
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
				fn = RPCservice(name=name,fn=fn, call_conv=call_conv, durable=durable,ttl=ttl)
			else:
				if name is None:
					name = fn.name
				if durable is not None:
					assert durable is fn.durable
				if ttl is not None:
					assert ttl == fn.ttl
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
			assert isinstance(name,str), a
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
	register_rpc_sync = sync_maker("register_rpc_async")
	register_rpc_gevent = gevent_maker("register_rpc_async")

	def register_alert(self, *a, _async=False, **kw):
		"""Register an alert listener.
		   See register_rpc for calling conventions."""
		return self.register_rpc(*a, _async=_async, _alert=True, **kw)

	def register_alert_async(self, *a, **kw):
		"""Register an alert listener.
		   See register_rpc_async for calling conventions."""
		return self.register_rpc(*a, _async=True, _alert=True, **kw)
	register_alert_sync = sync_maker("register_alert_async")
	register_alert_gevent = gevent_maker("register_alert_async")

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
	unregister_rpc_sync = sync_maker("unregister_rpc_async")
	unregister_rpc_gevent = gevent_maker("unregister_rpc_async")

	def unregister_alert_async(self, fn):
		return self.unregister_rpc(fn, _async=True, _alert=True)
	unregister_alert_sync = sync_maker("unregister_alert_async")
	unregister_alert_gevent = gevent_maker("unregister_alert_async")

	def _alert_ping(self,msg):
		if isinstance(msg,Mapping):
			if msg.get('app',self.app) != self.app:
				return
			if msg.get('uuid',self.uuid) != self.uuid:
				return
		return dict(
			app=self.app,
			uuid=self.uuid,
			args=self.args,
			)

	def _reply_ping(self,msg):
		return dict(
			app=self.app,
			args=self.args,
			uuid=self.uuid,
			rpc=list(self.rpc_endpoints.keys()),
			alert=list(self.alert_endpoints.keys()),
			)
		
	@asyncio.coroutine
	def _reply_debug(self,**data):
		return (yield from self.debug.run(**data))

	def debug_env(self, **data):
		if self.debug is None:
			return
		self.debug.env.update(data)
		
	## cleanup, less interesting (hopefully)

	def __del__(self):
		self._kill_conn(deleting=True)

	def close(self):
		self._kill_conn()

	def _kill_conn(self, deleting=False):
		c,self.conn = self.conn,None
		if c: # pragma: no cover
			try:
				c._kill()
			except Exception:
				if not deleting:
					logger.exception("closing connection")

	@asyncio.coroutine
	def _create_conn(self, _setup=None):
		from .conn import Connection
		conn = Connection(self)
		yield from conn.connect(_setup=_setup)
		for d in self.rpc_endpoints.values():
			yield from conn.register_rpc(d)
		for d in self.alert_endpoints.values():
			yield from conn.register_alert(d)
		self.conn = conn
		conn.amqp.when_disconnected.add_done_callback(self.restart_cb)


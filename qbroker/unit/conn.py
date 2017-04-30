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

import weakref
import asyncio
import aioamqp
from collections.abc import Mapping

from .msg import _RequestMsg,PollMsg,RequestMsg,BaseMsg
from .rpc import CC_DICT,CC_DATA
from ..codec import get_codec
from ..util import import_string

import logging
logger = logging.getLogger(__name__)

class DeadLettered(RuntimeError):
	def __init__(self, exchange,routing_key):
		self.exchange = exchange
		self.routing_key = routing_key

	def __str__(self):
		return "Dead: queue=%s route=%s" % (self.exchange, self.routing_key)

	def __reduce__(self):
		return (self.__class__,(self.exchange,self.routing_key),{})

class _ch(object):
	"""Helper object"""
	channel = None
	exchange = None
	queue = None

class Connection(object):
	amqp = None # connection
	alert_bc = False

	def __init__(self,unit,codec=None):
		if codec is None:
			codec = unit.config['amqp'].get('codec', 'DEFAULT')
		if isinstance(codec,str):
			codec = get_codec(codec)
		self._loop = unit._loop
		self.rpcs = {}
		self.alerts = {}
		self.replies = {}
		self.unit = weakref.ref(unit)
		cfg = unit.config['amqp']['server']
		if 'connect_timeout' in cfg:
			cfg['connect_timeout'] = float(cfg['connect_timeout'])
		if 'ssl' in cfg and isinstance(cfg['ssl'],str):
			cfg['ssl'] = cfg['ssl'].lower() == 'true'
		if 'port' in cfg:
			cfg['port'] = int(cfg['port'])
		self.cfg = cfg
		self.codec = codec

	@asyncio.coroutine
	def connect(self, _setup=None):
		logger.debug("Connecting %s",self)
		try:
			self.amqp_transport,self.amqp = (yield from aioamqp.connect(loop=self._loop, protocol_factory=NotifyingAmqpProtocol, **self.cfg))
			#self.amqp_transport,self.amqp = (yield from aioamqp.connect(loop=self._loop, **self.cfg))
		except Exception as e:
			logger.exception("Not connected to AMPQ: host=%s vhost=%s user=%s", self.cfg['host'],self.cfg['virtualhost'],self.cfg['login'])
			raise
		if _setup is not None:
			yield from _setup(self.amqp)
		self.amqp._init_futures(self._loop)
		yield from self.setup_channels()
		logger.debug("Connected %s",self)

	@asyncio.coroutine
	def _setup_one(self,name,typ,callback=None, q=None, route_key=None, exclusive=None, alt=None):
		"""\
			Register a channel. Internal helper.
			"""
		unit = self.unit()
		cfg = unit.config['amqp']
		ch = _ch()
		setattr(self,name,ch)
		logger.debug("setup RPC for %s",name)
		ch.channel = (yield from self.amqp.channel())
		ch.exchange = cfg['exchanges'][name]
		logger.debug("Chan %s: exchange %s", ch.channel,cfg['exchanges'][name])
		if exclusive is None:
			exclusive = (q is not None)
		d = {}
		if alt is not None:
			d["alternate-exchange"] = cfg['exchanges'][alt]
		try:
			yield from ch.channel.exchange_declare(cfg['exchanges'][name], typ, auto_delete=False, durable=True, passive=False, arguments=d)
		except aioamqp.exceptions.ChannelClosed as exc:
			if exc.code != 406: # PRECONDITION_FAILED
				raise
			ch.channel = (yield from self.amqp.channel())
			yield from ch.channel.exchange_declare(cfg['exchanges'][name], typ, passive=True)
			logger.warning("passive: %s",repr(exc))

		if q is not None:
			assert callback is not None
			d = {}
			ttl = cfg['ttl'].get(name,0)
			if ttl:
				d["x-dead-letter-exchange"] = cfg['queues']['dead']
				d["x-message-ttl"] = int(1000*cfg['ttl'][name])
			ch.queue = (yield from ch.channel.queue_declare(cfg['queues'][name]+q, auto_delete=True, passive=False, exclusive=exclusive, arguments=d))
			yield from ch.channel.basic_qos(prefetch_count=1,prefetch_size=0,connection_global=False)
			logger.debug("Chan %s: read %s", ch.channel,cfg['queues'][name]+q)
			yield from ch.channel.basic_consume(queue_name=cfg['queues'][name]+q, callback=callback)
			if route_key is not None:
				logger.debug("Chan %s: bind %s %s %s", ch.channel,cfg['exchanges'][name], route_key, ch.queue['queue'])
				yield from ch.channel.queue_bind(ch.queue['queue'], cfg['exchanges'][name], routing_key=route_key)
				pass
		else:
			assert callback is None

		logger.debug("setup RPC for %s done",name)

	@asyncio.coroutine
	def setup_channels(self):
		"""Configure global channels"""
		u = self.unit()
		# See doc/qbroker.rst
		yield from self._setup_one("alert",'topic', self._on_alert, u.uuid)
		yield from self._setup_one("rpc",'topic', self._on_rpc, u.uuid, 'qbroker.uuid.'+u.uuid, alt="dead")
		yield from self._setup_one("reply",'direct', self._on_reply, u.uuid, u.uuid)
		if u.config['amqp']['handlers']['dead']:
			yield from self._setup_one("dead",'topic', self._on_dead_rpc, "rpc", exclusive=False, route_key='#')

	@asyncio.coroutine
	def _on_dead_rpc(self, channel,body,envelope,properties):
		try:
			codec = get_codec(properties.content_type)
			msg = codec.decode(body)
			msg = BaseMsg.load(msg,envelope,properties)
			reply = msg.make_response()
			reply_to = getattr(msg, 'reply_to',None)
			exc = envelope.exchange_name
			if exc.startswith("dead"):
				exc = properties.headers['x-death'][0]['exchange']
			exc = DeadLettered(exc, envelope.routing_key)
			reply.set_error(exc, envelope.routing_key, "reply")
			reply,props = reply.dump(self)
			reply = self.codec.encode(reply)
			yield from self.reply.channel.publish(reply, self.reply.exchange, reply_to, properties=props)
		finally:
			yield from channel.basic_client_ack(envelope.delivery_tag)
		
	@asyncio.coroutine
	def _on_alert(self, channel,body,envelope,properties):
		logger.debug("read alert message %s",envelope.delivery_tag)
		try:
			codec = get_codec(properties.content_type)
			msg = codec.decode(body)
			msg = BaseMsg.load(msg,envelope,properties)
			try:
				rpc = self.alerts[msg.routing_key]
			except KeyError:
				n = msg.routing_key
				while True:
					i = n.rfind('.')
					if i < 1:
						rpc = self.alerts.get('#',None)
						if rpc is not None:
							break
						raise
					n = n[:i]
					rpc = self.alerts.get(n+'.#',None)
					if rpc is not None:
						break
			if rpc.call_conv == CC_DICT:
				a=(); k=msg.data
				if not isinstance(k,Mapping):
					assert k == ''
					k = {}
			elif rpc.call_conv == CC_DATA:
				a=(msg.data,); k={}
			else:
				a=(msg,); k={}

			reply_to = getattr(msg, 'reply_to',None)
			if reply_to:
				try:
					data = (yield from rpc.run(*a,**k))
				except Exception as exc:
					reply = msg.make_response()
					logger.exception("error on alert %s: %s", envelope.delivery_tag, body)
					reply.set_error(exc, rpc.name,"reply")
				else:
					if data is None:
						return
					reply = msg.make_response()
					reply.data = data
				reply,props = reply.dump(self)
				reply = self.codec.encode(reply)
				yield from self.reply.channel.publish(reply, self.reply.exchange, reply_to, properties=props)
			else:
				try:
					yield from rpc.run(*a,**k)
				except Exception as exc:
					logger.exception("error on alert %s: %s", envelope.delivery_tag, body)

		except Exception as exc:
			logger.exception("problem with rpc %s: %s", envelope.delivery_tag, body)
			yield from self.alert.channel.basic_reject(envelope.delivery_tag)
		else:
			logger.debug("ack rpc %s",envelope.delivery_tag)
			yield from channel.basic_client_ack(envelope.delivery_tag)

	@asyncio.coroutine
	def _on_rpc(self, channel,body,envelope,properties):
		logger.debug("read rpc message %s",envelope.delivery_tag)
		try:
			codec = get_codec(properties.content_type)
			msg = codec.decode(body)
			msg = BaseMsg.load(msg,envelope,properties)
			rpc = self.rpcs[msg.routing_key]
			reply = msg.make_response()
			try:
				if rpc.call_conv == CC_DICT:
					a=(); k=msg.data
				elif rpc.call_conv == CC_DATA:
					a=(msg.data,); k={}
				else:
					a=(msg,); k={}
				reply.data = (yield from rpc.run(*a,**k))
			except Exception as exc:
				logger.exception("error on rpc %s: %s", envelope.delivery_tag, body)
				reply.set_error(exc, rpc.name,"reply")
			reply,props = reply.dump(self)
			reply = self.codec.encode(reply)
			yield from channel.publish(reply, self.reply.exchange, msg.reply_to, properties=props)
		except Exception as exc:
			logger.exception("problem with rpc %s: %s", envelope.delivery_tag, body)
			yield from channel.basic_reject(envelope.delivery_tag)
		else:
			logger.debug("ack rpc %s",envelope.delivery_tag)
			yield from channel.basic_client_ack(envelope.delivery_tag)

	@asyncio.coroutine
	def _on_reply(self, channel,body,envelope,properties):
		logger.debug("read reply message %s",envelope.delivery_tag)
		try:
			codec = get_codec(properties.content_type)
			msg = codec.decode(body)
			msg = BaseMsg.load(msg,envelope,properties)
			f,req = self.replies[msg.correlation_id]
			try:
				yield from req.recv_reply(f,msg)
			except Exception as exc: # pragma: no cover
				if not f.done():
					f.set_exception(exc)
		except Exception as exc:
			yield from self.reply.channel.basic_reject(envelope.delivery_tag)
			logger.exception("problem with message %s: %s", envelope.delivery_tag, body)
		else:
			logger.debug("ack message %s",envelope.delivery_tag)
			yield from channel.basic_client_ack(envelope.delivery_tag)

	@asyncio.coroutine
	def call(self,msg, timeout=None, dest=None):
		if dest is None:
			dest = msg.routing_key
		cfg = self.unit().config['amqp']
		if timeout is None:
			tn = getattr(msg,'_timer',None)
			if tn is not None:
				timeout = self.unit().config['amqp']['timeout'].get(tn,None)
				if timeout is not None:
					timeout = float(timeout)
		assert isinstance(msg,_RequestMsg)
		data,props = msg.dump(self)
		data = self.codec.encode(data)
		if timeout is not None:
			f = asyncio.Future(loop=self._loop)
			id = msg.message_id
			self.replies[id] = (f,msg)
		logger.debug("Send %s to %s: %s", dest, cfg['exchanges'][msg._exchange], data)
		while True:
			try:
				yield from getattr(self,msg._exchange).channel.publish(data, cfg['exchanges'][msg._exchange], dest, properties=props)
			except aioamqp.exceptions.ChannelClosed:
				logger.warn("CLOSED sending %s to %s: %s", dest, cfg['exchanges'][msg._exchange], data)
				yield from self.unit().restart()
			else:
				break
		if timeout is None:
			return
		try:
			yield from asyncio.wait_for(f,timeout, loop=self._loop)
		except asyncio.TimeoutError:
			if isinstance(msg,PollMsg):
				return msg.replies
			raise # pragma: no cover
		finally:
			del self.replies[id]
		return f.result()
		
	@asyncio.coroutine
	def register_rpc(self,rpc):
		ch = self.rpc
		cfg = self.unit().config['amqp']
		assert rpc.name not in self.rpcs
		rpc.channel = (yield from self.amqp.channel())
		d = {}
		if cfg['ttl']['rpc'] or rpc.ttl:
			d["x-dead-letter-exchange"] = cfg['queues']['dead']
			d["x-message-ttl"] = int(1000*(rpc.ttl if rpc.ttl else cfg['ttl']['rpc']))
		self.rpcs[rpc.name] = rpc
		try:
			rpc.queue = (yield from rpc.channel.queue_declare(cfg['queues']['rpc']+rpc.name, auto_delete=not rpc.durable, durable=rpc.durable, passive=False, arguments=d))
			logger.debug("Chan %s: bind %s %s %s", ch.channel,cfg['exchanges']['rpc'], rpc.name, rpc.queue['queue'])
			yield from rpc.channel.queue_bind(rpc.queue['queue'], cfg['exchanges']['rpc'], routing_key=rpc.name)

			yield from rpc.channel.basic_qos(prefetch_count=1,prefetch_size=0,connection_global=False)
			logger.debug("Chan %s: read %s", rpc.channel,rpc.queue['queue'])
			yield from rpc.channel.basic_consume(queue_name=rpc.queue['queue'], callback=self._on_rpc, consumer_tag=rpc.uuid)
		except BaseException:
			del self.rpcs[rpc.name]
			raise

	@asyncio.coroutine
	def unregister_rpc(self,rpc):
		ch = self.rpc
		cfg = self.unit().config['amqp']
		if isinstance(rpc,str):
			rpc = self.rpcs.pop(rpc)
		else:
			del self.rpcs[rpc.name]
		assert rpc.queue is not None
		logger.debug("Chan %s: unbind %s %s %s", ch.channel,cfg['exchanges']['rpc'], rpc.name, rpc.queue['queue'])
		yield from rpc.channel.queue_unbind(rpc.queue['queue'], cfg['exchanges']['rpc'], routing_key=rpc.name)
		logger.debug("Chan %s: noread %s", rpc.channel,rpc.queue['queue'])
		yield from rpc.channel.basic_cancel(consumer_tag=rpc.uuid)

	@asyncio.coroutine
	def register_alert(self,rpc):
		assert rpc.name not in self.alerts
		dn = n = rpc.name
		if rpc.name.endswith('.#'):
			n = n[:-2]
			dn = n+'._all_'
		if len(n) > 1 and '#' in n:
			raise RuntimeError("I won't find that")

		self.alerts[rpc.name] = rpc
		if rpc.name.endswith('.#'):
			self.alert_bc = True
		ch = None

		try:
			if rpc.durable:
				if isinstance(rpc.durable,str):
					dn = rpc.durable
				else:
					dn = self.unit().config['amqp']['queues']['msg']+dn
				ch = (yield from self.amqp.channel())
				d = {}
				if rpc.ttl is not None:
					d["x-message-ttl"] = int(1000*rpc.ttl)
				q = (yield from ch.queue_declare(dn, auto_delete=False, passive=False, exclusive=False, durable=True, arguments=d))
				yield from ch.basic_consume(queue_name=q['queue'], callback=self._on_alert)
			else:
				ch = self.alert.channel
				q = self.alert.queue
			yield from ch.queue_bind(q['queue'], self.alert.exchange, routing_key=rpc.name)
		except BaseException:
			del self.alerts[rpc.name]
			if ch is not None:
				yield from ch.close()
			raise

		rpc.ch = ch
		rpc.q = q

	@asyncio.coroutine
	def unregister_alert(self,rpc):
		if isinstance(rpc,str):
			rpc = self.alerts.pop(rpc)
		else:
			del self.alerts[rpc.name]
		ch = self.alert
		if rpc.durable:
			yield from rpc.ch.close()
		else:
			yield from rpc.ch.queue_unbind(rpc.q['queue'], ch.exchange, routing_key=rpc.name)

	@asyncio.coroutine
	def close(self):
		a,self.amqp = self.amqp,None
		if a is not None:
			logger.debug("Disconnecting %s",self)
			try:
				yield from a.close(timeout=1)
			except Exception: # pragma: no cover
				logger.exception("closing the connection")
			self.amqp_transport = None
			logger.debug("Disconnected %s",self)

	def _kill(self):
		self.amqp = None
		a,self.amqp_transport = self.amqp_transport,None
		if a is not None:
			logger.debug("Killing %s",self)
			try:
				a.close()
			except Exception: # pragma: no cover
				logger.exception("killing the connection")

import aioamqp.protocol
class NotifyingAmqpProtocol(aioamqp.protocol.AmqpProtocol):
	"""Adds a future that triggers when the protocol gets disconnected"""
	when_disconnected = None

	def _init_futures(self,loop):
		self.when_disconnected = asyncio.Future(loop=loop)

	def connection_made(self, exc):
		if self.when_disconnected is not None and self.when_disconnected.done():
			self.when_disconnected = asyncio.Future(loop=self.when_disconnected._loop)
		super().connection_made(exc)

	def connection_lost(self, exc):
		super().connection_lost(exc)
		if self.when_disconnected is not None:
			if exc is None:
				self.when_disconnected.set_result(None)
			else:
				self.when_disconnected.set_exception(exc)


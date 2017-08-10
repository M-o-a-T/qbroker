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
from time import time

from . import CC_MSG,CC_DICT,CC_DATA
from qbroker.util import uuidstr, _NOTGIVEN
#from aioamqp.properties import Properties
from qbroker.util import attrdict; Properties = attrdict
from qbroker.codec.registry import BaseCodec, register_obj
obj_codec = BaseCodec()
obj_codec.code_lists = 2

_types = {}
_fmap = {
	}
def fmap(s):
	r = _fmap.get(s,_NOTGIVEN)
	if r is _NOTGIVEN:
		_fmap[s] = r = s.replace('-','_')
	return r

class FieldCollect(type):
	"""\
		A metaclass which coalesces "fields" class attributes from the base
		classes into one coherent set
		"""
	def __new__(meta, name, bases, dct):
		# Grab all known field names
		s = set()
		for b in bases:
			s.update(getattr(b,'fields',()))
		b = dct.get('fields',"")
		if b:
			if isinstance(b,str):
				b = b.split(" ")
			assert not s.intersection(b), (s,b)
			s.update(b)
		dct['fields'] = s

		res = super(FieldCollect, meta).__new__(meta, name, bases, dct)
		t = dct.get('type',None)
		if t is not None:
			_types[t] = res
		return res

class _MsgPart(object, metaclass=FieldCollect):
	"""\
		A message part.
		"""
	def dump(self):
		"""Convert myself to a dict"""
		obj = {}
		for f in self.fields:
			try:
				obj[f] = getattr(self, fmap(f))
			except AttributeError:
				pass
		return obj
	
	def _load(self, props):
		"""Load myself from a proplist"""
		if props.headers is None:
			return
		for f in self.fields:
			v = props.headers.get(f,_NOTGIVEN)
			if v is not _NOTGIVEN:
				setattr(self, fmap(f), v)

	def __eq__(self, other):
		for f in "type version data error".split(): # self.fields:
			a = getattr(self, f, _NOTGIVEN)
			b = getattr(other, f, _NOTGIVEN)
			if a == b:
				continue
			return False # pragma: no cover
		return True

class MsgError(RuntimeError,_MsgPart):
	"""Proxy for a remote error"""
	fields = "status id part message cls"
	exc = None

	def __init__(self, data=None):
		if data is not None:
			for f,v in data.items():
				setattr(self,fmap(f),v)

	def dump(self):
		"""Convert myself to a dict"""
		if isinstance(self.err,bytes):
			return self.err
		return super().dump()

	@property
	def failed(self):
		"""Is this message really an error?"""
		if self.status in ('ok','warn'): # pragma: no cover ## XXX TODO
			return False
		if self.status in ('error','fail'):
			return True
		raise RuntimeError("Unknown error status: "+str(self.status)) # pragma: no cover
	
	def _load(self, props):
		"""Load myself from a proplist"""
		super()._load(props)

		v = props.headers.get('err',_NOTGIVEN)
		if v is not _NOTGIVEN:
			exc = obj_codec.decode(v)
			self.exc = exc
			self.cls = exc.__class__.__name__
			if getattr(self,'message',None) is None:
				self.message = str(exc)

	@classmethod
	def build(cls, exc, eid,part, fail=False):
		obj = cls()
		obj.status = "fail" if fail else "error"
		obj.eid = eid
		obj.part = part
		obj.cls = exc.__class__.__name__
		obj.message = str(exc)
		obj.exc = exc
		return obj

	def __repr__(self):
		if self.exc is not None:
			return repr(self.exc)
		return "%s(%s)" % (self.cls, repr(self.message))
	def __str__(self):
		if self.exc is not None:
			return str(self.exc)
		return self.message
	def __hash__(self):
		return id(self)

@register_obj
class _MsgError(object):
	cls = MsgError
	clsname = "m_err"

	map = {'s':'status', 'i':'id', 'p':'part', 'm':'message', 'c':'cls', 'e': 'exc'}

	@staticmethod
	def encode(obj):
		res = {}
		for a,b in _MsgError.map.items():
			v = getattr(obj,b,None)
			if v is not None:
				res[a] = v
		return res

	@staticmethod
	def decode(**kv):
		res = MsgError()
		for a,b in _MsgError.map.items():
			v = kv.get(a,None)
			if v is not None:
				setattr(res,b,v)
		return res

class BaseMsg(_MsgPart):
	version = 1
	debug = False
	# type: needs to be overridden
	fields = "type version debug message-id"
	_timer = None

	data = None
	error = None

	def __init__(self, data=None):
		if not hasattr(self,'message_id'):
			self.message_id = uuidstr()

	def __repr__(self): # pragma: no cover
		return "%s._load(%s)" % (self.__class__.__name__, repr(self.__dict__))

	def dump(self,conn, codec=None):
		props = Properties()
		if codec is None:
			codec = conn.codec
		obj = super().dump()
		for f in 'type message-id reply-to correlation-id'.split(' '):
			m = obj.pop(f,None)
			if m is not None:
				setattr(props,fmap(f), m)
		props.timestamp = int(time())
		props.user_id = conn.cfg['login']
		props.content_type = codec.CODEC
		props.app_id = conn.unit().uuid
		# props.delivery_mode = 2
		if self.error is not None:
			obj['error'] = obj_codec.encode(self.error)
		if obj:
			props.headers = obj

		data = self.data
		if data is None:
			data = ""
		return data,props

	def set_error(self, *a, **k):
		self.error = MsgError.build(*a,**k)

	@staticmethod
	def load(data,env,props):
		t = props.type
		if t is None:
			t = 'alert' # message from non-qbroker
		res = _types[t]._load(data,props)

		for f in 'type message-id reply-to user-id timestamp content-type app-id correlation-id'.split(' '):
			ff = fmap(f)
			m = getattr(props,ff,_NOTGIVEN)
			if m is not _NOTGIVEN:
				setattr(res,ff,m)
		rk = getattr(res,'routing_key',None)
		if not rk:
			res.routing_key = env.routing_key
		return res

	@classmethod
	def _load(cls, msg,props):
		obj = cls()
		super(BaseMsg,obj)._load(props)
		obj.data = msg
		if props.headers is not None and 'error' in props.headers:
			obj.error = obj_codec.decode(props.headers['error'])
		return obj

	@property
	def failed(self):
		return self.error is not None and self.error.failed

	def raise_if_error(self):
		if self.error and self.error.failed:
			if self.error.exc is not None: # error passed the codec
				raise self.error.exc
			raise self.error

class _RequestMsg(BaseMsg):
	"""A request packet. The remaining fields are data elements."""
	fields = "routing-key reply-to"

	def __init__(self, routing_key=None, data=None):
		super().__init__()
		self.routing_key = routing_key
		self.data = data

	def make_response(self, **data):
		return ResponseMsg(self, **data)

#	def make_error_response(self, exc, eid,part, fail=False):
#		res = ResponseMsg(self)
#		res.error = MsgError.build(exc, eid,part)
#		return error

class RequestMsg(_RequestMsg):
	type = "request"
	_exchange = "rpc" # lookup key for the exchange name
	_timer = "rpc" # lookup key for the timeout

	def __init__(self, routing_key=None, _unit=None, data=None):
		super().__init__(routing_key=routing_key, data=data)
		if _unit is not None:
			self.reply_to = _unit.uuid

	@asyncio.coroutine
	def recv_reply(self, f,reply):
		"""Client side: Incoming reply. @f is the future to trigger when complete."""
		f.set_result(reply)

class AlertMsg(_RequestMsg):
	"""An alert which is not replied to"""
	type = "alert"
	_exchange = "alert" # lookup key for the exchange name

	def __init__(self, routing_key=None, _unit=None, data=None):
		super().__init__(routing_key=routing_key, data=data)
		# do not set reply_to

class PollMsg(AlertMsg):
	"""An alert which requests replies"""
	_timer = "poll" # lookup key for the timeout

	def __init__(self, routing_key=None, _unit=None, callback=None,call_conv=CC_MSG, data=None):
		super().__init__(routing_key=routing_key, _unit=_unit, data=data)
		if _unit is not None:
			self.reply_to = _unit.uuid
		self.callback = callback
		self.call_conv = call_conv
		self.replies = 0

	@asyncio.coroutine
	def recv_reply(self, f,msg):
		"""Incoming reply. @f is the future to trigger when complete."""
		try:
			if self.call_conv == CC_MSG:
				a=(msg,); k={}
			elif msg.failed:
				return # ignore error replies
			elif self.call_conv == CC_DICT:
				a=(); k=msg.data
			elif self.call_conv == CC_DATA:
				a=(msg.data,); k={}
			else: # pragma: no cover
				raise RuntimeError("Unknown encoding: %s"%self.call_conv) 
			r = self.callback(*a,**k)
			if asyncio.iscoroutine(r):
				yield from r
		except StopIteration:
			f.set_result(self.replies+1)
		except Exception as exc:
			f.set_exception(exc)
		else:
			self.replies += 1

class ResponseMsg(BaseMsg):
	type = "reply"
	fields = "correlation-id"

	def __init__(self,request=None, data=None):
		super().__init__(data=data)
		if request is not None:
			self.correlation_id = request.message_id


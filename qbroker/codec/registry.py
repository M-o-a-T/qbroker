#!/usr/bin/python
# -*- coding: utf-8 -*-

from __future__ import print_function,absolute_import

from qbroker.util import TZ,UTC, format_dt, import_string
import base64
import datetime as dt

import sys
from time import mktime
from pprint import pformat
from collections import namedtuple

from qbroker.util import TZ,UTC, format_dt, attrdict

import logging
logger = logging.getLogger(__name__)

"""\
This module implements a registry for (de)coding arbitrary objects.
It intentionally does not use native Python mechanisms.

"""
__all__ = "type2cls name2cls register_obj".split()

class _NOTGIVEN:
	pass

class TypeDict(dict):
	def get(self,cls,d=_NOTGIVEN):
		if hasattr(cls,"__mro__"):
			for c in cls.__mro__:
				r = super().get(c.__module__+'.'+c.__name__,_NOTGIVEN)
				if r is not _NOTGIVEN:
					return r
		else:
			d = super().get(cls,d)
		if d is not _NOTGIVEN:
			return d
		raise KeyError(cls)

type2cls = TypeDict()
name2cls = {}

def register_obj(cls):
	type2cls[cls.cls.__module__+'.'+cls.cls.__name__] = cls
	name2cls[cls.clsname] = cls
	return cls

@register_obj
class _binary(object):
	"""A codec for bytes.
		Try to convert to an utf-8 string; if not possible, use base64.a85."""
	cls = bytes
	clsname = "bin"

	@staticmethod
	def encode(obj):
		## the string is purely for human consumption and therefore does not have a time zone
		try:
			obj = obj.decode('utf-8',errors='strict')
		except Exception:
			return {"b":base64.a85encode(obj).decode('utf-8'), "s":obj.decode('utf-8',errors='ignore')}
		else:
			return {"s":obj}

	@staticmethod
	def decode(b=None,s=None):
		if b is None:
			return s.encode('utf-8')
		else:
			return base64.a85decode(b)

@register_obj
class _datetime(object):
	cls = dt.datetime
	clsname = "datetime"

	@staticmethod
	def encode(obj):
		## the string is purely for human consumption and therefore does not have a time zone
		return {"t":mktime(obj.timetuple()),"s":format_dt(obj)}

	@staticmethod
	def decode(t=None,s=None,a=None,k=None,**_):
		if t:
			return dt.datetime.utcfromtimestamp(t).replace(tzinfo=UTC).astimezone(TZ)
		else: ## historic
			assert a
			return dt.datetime(*a).replace(tzinfo=TZ)

@register_obj
class _date(object):
	cls = dt.date
	clsname = "date"

	@staticmethod
	def encode(obj):
		return {"d":obj.toordinal(), "s":obj.strftime("%Y-%m-%d")}

	@staticmethod
	def decode(d=None,s=None,a=None,**_):
		if d:
			return dt.date.fromordinal(d)
		## historic
		return dt.date(*a)

@register_obj
class _time(object):
	cls = dt.time
	clsname = "time"

	@staticmethod
	def encode(obj):
		ou = obj.replace(tzinfo=UTC)
		secs = ou.hour*3600+ou.minute*60+ou.second
		return {"t":secs,"s":"%02d:%02d:%02d" % (ou.hour,ou.minute,ou.second)}

	@staticmethod
	def decode(t=None,s=None,a=None,k=None,**_):
		if t:
			return dt.datetime.utcfromtimestamp(t).time()
		return dt.time(*a)

@register_obj
class _exc(object):
	cls = BaseException
	clsname = "exc"

	@staticmethod
	def encode(obj):
		n = obj.__class__.__module__ == "builtins", obj.__class__.__module__
		x = obj.__reduce__()
		def _enc(cls,a=None,k=None):
			res = {"e" if isinstance(obj,Exception) else 'b': cls.__module__+'.'+cls.__name__}
			if a:
				# res['a'] = a
				for n,x in enumerate(a):
					res['a'+str(n)]=x
			if k:
				res['k'] = k
			res['r'] = repr(obj)
			res['s'] = str(obj)
			return res
		return _enc(*x)

	@staticmethod
	def decode(b=None,e=None,a=None,k=None, r=None,s=None, **kv):
		if a is None:
			a = ()
		if k is None:
			k = {}
		if b is not None:
			assert e is None
			e = b
		exc = import_string(e)
		if not isinstance(exc,type) or not issubclass(exc,BaseException):
			raise RuntimeError("Tried to decode a non-exception",e)
		if not a and 'a0' in kv:
			n=0
			a=[]
			while True:
				try:
					a.append(kv.pop('a'+str(n)))
				except KeyError:
					break
				n += 1
		exc = exc(*a,**k)
		if s is not None:
			exc.__str = s
		if r is not None:
			exc.__repr = r
		return exc



class _notGiven: pass
class ComplexObjectError(Exception): pass

DecodeRef = namedtuple('DecodeRef',('oid','parent','offset', 'cache'))
# This is used to store references to to-be-amended objects.
# The idea is that if a newly-decoded object encounters this, it can
# replace the offending reference by looking up the result in the cache.
# 
# Currently, this type only provides a hint about the problem's origin if
# it is ever encountered outside the decoding process. The problem does not
# occur in actual code because most objects are just transmitted as
# references, to be evaluated later (when an attribute referring to the
# object is accessed).

class ServerError(Exception):
	name = "ServerError"
	_traceback = None
	_repr = None

	def __repr__(self):
		return self._repr

	def __str__(self):
		r = self.__repr__()
		if self._traceback is None: return r
		return r+"\n"+"".join(self._traceback)

scalar_types = {type(None),float,bytes}
from six import string_types,integer_types
for s in string_types+integer_types: scalar_types.add(s)
scalar_types = tuple(scalar_types) # isinstance() doesn't like sets

class BaseCodec(object):
	"""\
		Serialize any object structure to something dict/list-based and
		non-self-referential, suitable for generic JSON/BSON/XML/whatever-ization.

		The result is an easy-to-understand linear dict/list structure that
		converts to any generic serialization format and which is not specific to Python.

		This code is compatible with the json+obj codec if there are no
		objects with multiple references.
		"""

	def _encode(self, data, objcache,objref, p=None,off=None):
		# @objcache: dict: id(obj) => (seqnum,encoded,selfref,data)
		#            `encoded` will be set to the encoded object so that
		#            the seqnum can be removed later if it turns out not to
		#            be needed.
		#            `selfref` is None (incomplete), False (recursive),
		#            or an (id,parent,position) tuple used to fix-up a prior
		#            reference to this object when decoding.
		#            `data` is the original data. We need to keep it around
		#            because if there is no other reference to it, it'll be
		#            freed, causing the id to be re-used. Not good.
		# 
		# @objref: dict: seqnum => oid: objects which are actually required for proper
		#          encoding.

		# @p,@off: p[off] == data. Required for patching cyclic references.
		
		# If this is a Werkzeug localproxy, dereference it
		ac = getattr(data,'_get_current_object',None)
		if ac is not None:
			data = ac()

		# Scalars (integers, strings) do not refer to other objects and
		# thus are never encoded.
		if isinstance(data,scalar_types):
			return data

		# Have I seen this object before?
		did = id(data)
		oid = objcache.get(did,None)
		if oid is not None:
			# Yes.
			if oid[1] is None: # it's incomplete: mark as recursive structure.
				oid[2] = False

			# Point to it.
			oid = oid[0]
			objref[oid] = did
			return {'_or':oid}

		# No, this is a new object: Generate a new ID for it.
		oid = 1+len(objcache)
		objcache[did] = [oid,None,None,data]
		# we need to keep the data around, see above
		
		if isinstance(data,(list,tuple)):
			res = []
			i = 0
			n = len(objref)
			for x in data:
				res.append(self._encode(x,objcache,objref, p=res,off=i))
				i += 1

			# If a list is at top level or contains references, store as
			# a dict because we need to add ref fields
			if len(objref) != n or p is None:
				res = { '_o':'LIST','_d':res }

		else:
			odata = data

			if type(data) is not dict:
				obj = type2cls.get(type(data),None)
				if obj is None:
					raise NotImplementedError("I don't know how to encode %s: %r"%(repr(data.__class__),data,))
				data = obj.encode(data)
				if isinstance(data,tuple):
					obj,data = data
				else:
					obj = obj.clsname
			else:
				obj = None

			res = type(data)()
			for k,v in data.items():
				# Transparent encoding: _ofoo => _o_foo, undone in the decoder
				# so that our _o and _oi values don't clash with whatever
				nk = '_o_'+k[2:] if k.startswith('_o') else k

				res[nk] = self._encode(v,objcache,objref, p=res,off=k)

			if obj is not None:
				res['_o'] = obj

		did = objcache[did]
		did[1] = res
		if did[2] is None:
			# order non-recursive objects by completion time.
			# Need to mangle the offset
			d = objcache['done']
			did[2] = (d,p,('_o_'+off[2:] if isinstance(off,string_types) and off.startswith('_o') else off))
			objcache['done'] = d+1
		return res

	def encode(self, data, **kw):
		"""\
			Encode this data structure. Recursive structures or
			multiply-used objects are handled mostly-correctly.
			"""
		# No, not yet / did not work: slower path
		objcache = {"done":1}
		objref = {}
		res = self._encode(data, objcache,objref)
		del objcache['done']
		cache = []

		if objref:
			# At least one reference was required.
			def _sorter(k):
				c,d,e,x = objcache[k]
				if type(e) is not tuple: return 9999999999
				return e[0]

			for d in sorted(objref.values(), key=_sorter):
				oid,v,f,x = objcache[d]
				# add object IDs to those objects which need it
				v['_oi']=oid
				if isinstance(f,tuple):
					# Referenced. Add to cache and replace with fix-up.
					f[1][f[2]] = {'_or':oid}
					cache.append(v)
		res.update(kw)
		if cache:
			res['_oc'] = cache
		return res
	
	def encode_error(self, err, tb=None):
		"""\
			Special method for encoding an error, with optional traceback.

			Note that this will not pass through the normal encoder, so the
			data should be strings (or, in case of the traceback, a list of
			strings).
			"""
		res = {}

		if isinstance(err,string_types):
			err = Exception(err)
		# don't use the normal 
		res['error'],cache = BaseCodec.encode(self,err)
		if cache:
			res['cache'] = cache

		if tb is not None:
			if hasattr(tb,'tb_frame'):
				tb = format_tb(tb)
			res['tb'] = tb
		return res

	def _decode(self,data, objcache,objtodo, p=None,off=None):
		# Decode the data recursively.
		#
		# @objcache: dict seqnum=>result
		# 
		# @objtodo: Fixup data, list of (seqnum,parent,index). See below.
		#
		# @p, @off: parent object and index which refer to this object.
		#
		# During decoding, information to recover an object may not be
		# available, i.e. we encounter an object reference while decoding
		# the data it refers to.
		# The @objtodo array records where the actual result is supposed to
		# be stored, as soon as we have it.
		# 
		# This process does not work with recursive object references
		# within other objects. That'd require a more expensive/intrusive
		# decoding framework. TODO: Detect this case.

		if isinstance(data, scalar_types):
			return data

		# "Unmolested" lists are passed through.
		if isinstance(data,(list,tuple)):
			return type(data)(self._decode(v,objcache,objtodo) for v in data)

		if type(data) is dict:
			objref = data.pop('_or',None)
			if objref is not None:
				res = objcache.get(objref,None)
				if res is None:
					# need to fix the problem later
					res = DecodeRef(objref,p,off, objcache)
					objtodo.append(res)
				return res

			oid = data.pop('_oi',None)
			obj = data.pop('_o',None)
			if obj == 'LIST':
				res = []
				if oid is not None:
					objcache[oid] = res
				k = 0
				for v in data['_d']:
					res.append(self._decode(v,objcache,objtodo, res,k))
					k += 1
				return res
			
			res = {}
			for k,v in data.items():
				if k.startswith("_o"):
					assert k[2] == '_',nk # unknown meta key?
					nk = '_o'+k[3:]
				else:
					nk = k
				res[nk] = self._decode(v,objcache,objtodo, res,k)

			if obj is not None:
				try:
					res = name2cls[obj].decode(**res)
				except Exception:
					logger.error("Decoding: %s:\n%s\n%s",obj,pformat(data), pformat(res))
					logger.error("Decoding:: %r",name2cls[obj])
					raise
			if oid is not None:
				objcache[oid] = res
			return res

		raise NotImplementedError("Don't know how to decode %r"%data)
	
	def _cleanup(self, objcache,objtodo):
		# resolve the "todo" stuff
		for d,p,k,_ in objtodo:
			p[k] = objcache[d]
		
	def decode(self, data):
		"""\
			Decode the data:
			Reverse everything the encoder does as cleanly as possible.
			"""

		objcache = {}
		objtodo = []

		for obj in data.pop('_oc',()):
			self._decode(obj, objcache,objtodo)
			# side effect: populate objcache

		res = self._decode(data, objcache,objtodo)
		self._cleanup(objcache,objtodo)
		if isinstance(res,DecodeRef):
			res = objcache[res.oid]
		return res


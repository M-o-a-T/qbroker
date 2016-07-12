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

# Utility code

from base64 import b64encode
from collections.abc import Mapping
from importlib import import_module

def uuidstr(u=None):
	if u is None:
		import uuid
		u=uuid.uuid1()
	return b64encode(u.bytes, altchars=b'-_').decode('ascii').rstrip('=')

def import_string(name):
	"""Import a module, or resolve an attribute of a module."""
	name = str(name)
	try:
		return import_module(name)
	except ImportError:
		if '.' not in name:
			raise
		module, obj = name.rsplit('.', 1)
		try:
			return getattr(import_string(module),obj)
		except AttributeError:
			raise AttributeError(name)

def combine_dict(*d):
	res = {}
	keys = {}
	if len(d) <= 1:
		return d
	for kv in d:
		for k,v in kv.items():
			if k not in keys:
				keys[k] = []
			keys[k].append(v)
	for k,v in keys.items():
		if len(v) == 1:
			res[k] = v[0]
		elif not isinstance(v[0],Mapping):
			for vv in v[1:]:
				assert not isinstance(vv,Mapping)
			res[k] = v[0]
		else:
			res[k] = combine_dict(*v)
	return res

class _missing: pass

class attrdict(dict):
	"""A dictionary which can be accessed via attributes, for convenience"""
	def __init__(self,*a,**k):
		super(attrdict,self).__init__(*a,**k)
		self._done = set()

	def __getattr__(self,a):
		if a.startswith('_'):
			return super(attrdict,self).__getattr__(a)
		try:
			return self[a]
		except KeyError:
			raise AttributeError(a)
	def __setattr__(self,a,b):
		if a.startswith("_"):
			super(attrdict,self).__setattr__(a,b)
		else:
			self[a]=b
	def __delattr__(self,a):
		del self[a]

# Python doesn't support setting an attribute on MethodType,
# and the thing is not subclass-able either,
# so I need to implement my own.
# Fortunately, this is reasonably easy.
from types import MethodType 
class _ClassMethodType(object):
	def __init__(self,_func,_self,_cls=None, **attrs):
		self.im_func = self.__func__ = _func
		self.im_self = self.__self__ = _self
		self.im_class = _cls
		self.__name__ = _func.__name__
		for k,v in attrs.items():
			setattr(self,k,v)
	def __call__(self,*a,**k):
		return self.__func__(self.im_self,*a,**k)

class _StaticMethodType(_ClassMethodType):
	def __call__(self,*a,**k):
		return self.__func__(*a,**k)

def exported_classmethod(_fn=None,**attrs):
	"""\
		A classmethod thing which supports attributed methods.

		Usage:
			class test(object):
				@exported_classmethod(bar=42)
				def foo(cls, …):
					pass
			assert test.foo.bar == 42
		"""
	if _fn is None:
		def xfn(_fn):
			return exported_classmethod(_fn,**attrs)
		return xfn
	res = _ClassMethodAttr(_fn)
	attrs = _dab_(attrs)
	for k,v in attrs.items():
		setattr(res,k,v)
	res._attrs = attrs
	return res

def exported_staticmethod(_fn=None,**attrs):
	"""\
		A classmethod thing which supports attributed methods.

		Usage:
			class test(object):
				@exported_staticmethod(bar=42)
				def foo(…):
					pass
			assert test.foo.bar == 42
		"""
	if _fn is None:
		def xfn(_fn):
			return exported_staticmethod(_fn,**attrs)
		return xfn
	res = _StaticMethodAttr(_fn)
	attrs = _dab_(attrs)
	for k,v in attrs.items():
		setattr(res,k,v)
	res._attrs = attrs
	return res

def exported_property(fget=None,fset=None,fdel=None,doc=None,**attrs):
	"""\
		A classmethod thing which supports attributed methods.

		Usage:
			class test(object):
				@exported_staticmethod(bar=42)
				def foo(…):
					pass
			assert test.foo.bar == 42
		"""
	if fget is None and attrs is not None:
		def xfn(fget=None,fset=None,fdel=None,doc=None):
			return exported_property(fget=fget,fset=fset,fdel=fdel,doc=doc,**attrs)
		return xfn
	attrs = dict(_dab_(attrs))
	res = _PropertyAttr(fget=fget,fset=fset,fdel=fdel,doc=doc,**attrs)
	return res

class _PropertyAttr(property):
	"""Yet another rewrite in Python."""
	# The problem here is that
	#	@exported_property
	#	def x(self): …
	#	@x.setter
	#	def x(self,val): …
	# will call exported_property() TWICE. (Or thrice if you also want a deleter.)
	# Therefore we stash the attributes in the function.
	# TODO: check if one of these has attributes which are not "ours"?

	def __init__(self, fget=None,fset=None,fdel=None,doc=None,**attrs):
		property.__init__(self,fget=fget,fset=fset,fdel=fdel,doc=doc)
		f = fget
		if not attrs:
			if not f or not f.__dict__:
				f = fset
				if not f or not f.__dict__:
					f = fdel
			if f:
				attrs = f.__dict__
		for f in (fget,fset,fdel):
			if f is None:
				continue
			for k,v in attrs.items():
				setattr(f,k,v)
		for k,v in attrs.items():
			setattr(self,k,v)
		self.__doc__=doc

class _ClassMethodAttr(classmethod):
	def __get__(self,i,t=None):
		if i is not None:
			# Ignore calls on objects
			return classmethod.__get__(self,i,t)

		# Otherwise we need to build this thing ourselves.
		# TODO: use our data directly, rather than calling up.
		res = classmethod.__get__(self,i,t)
		return _ClassMethodType(res.__func__,res.__self__, **self._attrs)

class _StaticMethodAttr(classmethod):
	def __get__(self,i,t=None):
		if i is not None:
			# Ignore calls on objects
			return classmethod.__get__(self,i,t)

		# same as classmethod (almost)
		res = classmethod.__get__(self,i,t)
		return _StaticMethodType(res.__func__,res.__self__, **self._attrs)


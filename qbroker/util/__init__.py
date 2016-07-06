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

# Utility code

import asyncio
from base64 import b64encode
from collections.abc import Mapping
from functools import wraps,partial
from importlib import import_module
import inspect
from pprint import pformat
import threading


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

class AioRunner:
	"""A singleton which supplies a thread for running asyncio tasks.

	Call AioRunner.init(setup,teardown) to set things up; these must
	be argument-less coroutines which are executed in the new task.

	Call AioRunner.start() to actually create an asyncio event loop.

	Call AioRunner.stop() to halt things.

	Exceptions in setup/teardown will be propagated to start/stop.
	"""
	_obj = None
	thread = None
	lock = threading.Lock()
	setup = None
	teardown = None
	_loop = None

	def init(self, setup=None,teardown=None):
		with self.lock:
			assert self.thread is None
			self.setup = setup
			self.teardown = teardown

	def start(self):
		if self.thread is not None:
			return
		with self.lock:
			assert self.setup is not None
			assert self._loop is None

			if self.thread is not None:
				return

			from concurrent import futures
			self.done = futures.Future()
			self.ready = futures.Future()
			self.thread = threading.Thread(target=self._runner)
			self.thread.start()
			try:
				return self.ready.result()
			except Exception as ex:
				self.thread.join()
				self.thread = None
				raise
			finally:
				del self.ready

	def stop(self):
		if self._loop is None:
			return
		self._loop.call_soon_threadsafe(self.end.set_result,None)
		try:
			self.done.result()
		finally:
			self._loop.close()
			self.thread.join()
			self.thread = None
			self._loop = None
			self.end = None
			self.done = None

	@property
	def loop(self):
		if self._loop is None:
			self.start()
		return self._loop

	def _runner(self):
		self._loop = asyncio.new_event_loop()
		asyncio.set_event_loop(self._loop)
		self.end = asyncio.Future(loop=self._loop)

		@asyncio.coroutine
		def _worker():
			try:
				yield from self.setup()
			except Exception as ex:
				self.ready.set_exception(ex)
				return
			else:
				self.ready.set_result(None)
			yield from self.end
			if self.teardown is not None:
				yield from self.teardown()

		try:
			self._loop.run_until_complete(_worker())
		except Exception as ex:
			self.done.set_exception(ex)
		finally:
			self.done.set_result(None)
			
	def run_async(self,proc,*args, _async=False,_timeout=None, **kwargs):
		"""Run an asyncio-using procedure.
		
		@_async: set if you want to run it in the background and get the
		         future returned instead.
		@_timeout: set if you want the procedure cancelled if it takes
		           longer than that many seconds.

		All other parameters will be passed to @proc.
		
		"""
		from concurrent import futures

		def runner(fx):
			try:
				f = asyncio.ensure_future(proc(*args,**kwargs))
			except BaseException as exc:
				fx.set_exception(exc)
			else:
				def done(ff):
					assert f is ff, (f,ff)
					try:
						fx.set_result(f.result())
					except Exception as ex:
						print_exc()
						fx.set_exception(exc)
				f.add_done_callback(done)

		fx = futures.Future()
		AioRunner.loop.call_soon_threadsafe(runner,fx)
		if _timeout is not None:
			fx = asyncio.wait_for(fx,_timeout,loop=AioRunner.loop)
		if _async:
			return fx
		else:
			return fx.result()

AioRunner = AioRunner()


class SyncFuncs(type):
	""" A metaclass which adds synchronous version of coroutines.

	This metaclass finds all coroutine functions defined on a class
	and adds a synchronous version with a '_sync' suffix appended to the
	original function name.

	The sync version will behave as if it were called via
	`AioRunner.run_async`, including its _async and _timeout arguments.

	"""
	def __new__(cls, clsname, bases, dct, **kwargs):
		new_dct = {}
		for name,val in dct.items():
			# Make a sync version of all coroutine functions
			if asyncio.iscoroutinefunction(val):
				meth = cls.sync_maker(name)
				syncname = '{}_sync'.format(name)
				meth.__name__ = syncname
				meth.__qualname__ = '{}.{}'.format(clsname, syncname)
				new_dct[syncname] = meth
		dct.update(new_dct)
		return super().__new__(cls, clsname, bases, dct)

	@staticmethod
	def sync_maker(func):
		def sync_func(self, *args, **kwargs):
			meth = getattr(self, func)
			return AioRunner.run_async(meth, *args,**kwargs)
		return sync_func


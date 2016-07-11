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
import qbroker

loop = None
AioRunner = None

def setup(sync=False,gevent=False):
	global AioRunner
	if sync and not AioRunner:
		import threading

		class AioRunner:
			"""A singleton which supplies a thread for running asyncio tasks.

			Call AioRunner.init(setup,teardown) to set things up; these must
			be argument-less coroutines which are executed in the new task.
			If the loop is already running, @setup is scheduled immediately.

			Call AioRunner.start() to actually create an asyncio event loop.

			Call AioRunner.stop() to halt things. All registered teardown functions
			will be called in reverse order.
			Calls to .start() and .stop() must be balanced.

			Exceptions in setup will be propagated.
			Exceptions in teardown are logged but otherwise ignored.
			"""

			def __init__(self):
				self.lock = threading.Lock()
				self._cleanup()

			def start(self, setup=None,teardown=None):
				with self.lock:
					if self._loop is not None:
						self.run_async(setup)
						if teardown is not None:
							self.teardown.append(teardown)
						self.started += 1
						return

					self.setup = setup

					from concurrent import futures
					self.done = futures.Future()
					self.ready = futures.Future()
					self.thread = threading.Thread(target=self._runner)
					self.thread.start()
					try:
						res = self.ready.result()
					except Exception as ex:
						self.thread.join()
						self._cleanup()
						raise
					else:
						if teardown is not None:
							self.teardown.append(teardown)
						return res
					finally:
						del self.ready

			def stop(self):
				with self.lock:
					assert self._loop is not None, "not started"
					if self.started > 0:
						self.started -= 1
						return
					self._loop.call_soon_threadsafe(self.end.set_result,None)
					try:
						self.done.result()
					finally:
						if self._loop is not None:
							self._loop.close()
						if self.thread is not None:
							self.thread.join()
						self._cleanup()

			def _cleanup(self):
				self._loop = None
				self.thread = None
				self.end = None
				self.done = None
				self.started = 0
				self.setup = None
				self.teardown =[]

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
						if self.setup is not None:
							yield from self.setup()
					except Exception as ex:
						self.ready.set_exception(ex)
					else:
						self.ready.set_result(None)
						yield from self.end
					for fn in self.teardown[::-1]:
						try:
							yield from fn()
						except Exception as ex:
							logger.exception("Error during %s", fn)

				try:
					self._loop.run_until_complete(_worker())
				except Exception as ex:
					self.done.set_exception(ex)
				else:
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
						f = asyncio.ensure_future(proc(*args,**kwargs), loop=self.loop)
					except BaseException as exc:
						fx.set_exception(exc)
					else:
						if _timeout is not None:
							f = asyncio.ensure_future(asyncio.wait_for(f,_timeout,loop=self.loop), loop=self.loop)
						def done(ff):
							assert f is ff, (f,ff)
							try:
								fx.set_result(f.result())
							except Exception as exc:
								fx.set_exception(exc)
						f.add_done_callback(done)

				fx = futures.Future()
				AioRunner.loop.call_soon_threadsafe(runner,fx)
				if _async:
					return fx
				else:
					return fx.result()

		AioRunner = AioRunner()

		def unit_sync(*a,**k):
			return AioRunner.run_async(qbroker.unit, *args,**kwargs)
		qbroker.unit_sync = unit_sync
	
	global loop
	if gevent and not loop:
		global aiogevent

		import aiogevent
		asyncio.set_event_loop_policy(aiogevent.EventLoopPolicy())
		loop = asyncio.get_event_loop()

		def unit_gevent(*a,**k):
			return aiogevent.yield_future(asyncio.ensure_future(qbroker.unit(*args,**kwargs), loop=loop))
		qbroker.unit_gevent = unit_gevent


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
				meth = sync_maker(name)
				syncname = '{}_sync'.format(name)
				meth.__name__ = syncname
				meth.__qualname__ = '{}.{}'.format(clsname, syncname)
				new_dct[syncname] = meth

				meth = gevent_maker(name)
				syncname = '{}_gevent'.format(name)
				meth.__name__ = syncname
				meth.__qualname__ = '{}.{}'.format(clsname, syncname)
				new_dct[syncname] = meth
		dct.update(new_dct)

		return super().__new__(cls, clsname, bases, dct)

def sync_maker(func):
	def sync_func(self, *args, **kwargs):
		meth = getattr(self, func)
		return AioRunner.run_async(meth, *args,**kwargs)
	return sync_func

def gevent_maker(func):
	def gevent_func(self, *args, **kwargs):
		meth = getattr(self, func)
		return aiogevent.yield_future(asyncio.ensure_future(meth(*args,**kwargs), loop=loop))
	return gevent_func


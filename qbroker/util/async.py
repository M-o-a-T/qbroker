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

import asyncio
import inspect
import signal

import logging
logger = logging.getLogger(__name__)

iscoroutinefunction = getattr(inspect,'iscoroutinefunction', lambda _:False)

class Main:
	"""Implement a bare-bones mainloop for asyncio."""

	def __init__(self):
		self.loop = asyncio.new_event_loop()
		asyncio.set_event_loop(self.loop)
		self._sig = asyncio.Event()
		self._cleanup = []

		self.loop.add_signal_handler(signal.SIGINT,self._tilt)
		self.loop.add_signal_handler(signal.SIGTERM,self._tilt)

	@asyncio.coroutine
	def at_start(self):
		"""Called after successful startup. Overrideable."""
		yield None

	@asyncio.coroutine
	def _at_stop(self):
		"""Process cleanup code. Don't override."""
		for fn,a,k in self._cleanup[::-1]:
			try:
				if inspect.isgeneratorfunction(fn) or iscoroutinefunction(fn):
					yield from fn(*a,**k)
				else:
					fn(*a,**k)
			except Exception:
				logger.exception("Cleanup: %s %s %s",fn,repr(a),repr(k))

	def add_cleanup(self,fn,*a,**k):
		"""Register some clean-up code. Processed in reverse order."""
		self._cleanup.append((fn,a,k))
	
	def run(self):
		self.loop.run_until_complete(self._run())

	@asyncio.coroutine
	def _run(self):
		yield from self.at_start()
		yield from self._sig.wait()
		yield from self._at_stop()

	def _tilt(self):
		self.loop.remove_signal_handler(signal.SIGINT)
		self.loop.remove_signal_handler(signal.SIGTERM)
		self.loop.call_soon_threadsafe(self._tilt2)
	def _tilt2(self):
		self._sig.set()
	
	def stop(self):
		"""Stop the loop."""
		self._tilt()


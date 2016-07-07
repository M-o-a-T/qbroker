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
import signal

class Main:
	def __init__(self):
		self.loop = asyncio.new_event_loop()
		asyncio.set_event_loop(self.loop)
		self._sig = asyncio.Event()

		self.loop.add_signal_handler(signal.SIGINT,self._tilt)
		self.loop.add_signal_handler(signal.SIGTERM,self._tilt)

	@asyncio.coroutine
	def start(self):
		yield None

	@asyncio.coroutine
	def stop(self):
		yield None

	def run(self):
		self.loop.run_until_complete(self._run())
	@asyncio.coroutine
	def _run(self):
		yield from self.start()
		yield from self._sig.wait()
		yield from self.stop()

	def _tilt(self):
		self.loop.remove_signal_handler(signal.SIGINT)
		self.loop.remove_signal_handler(signal.SIGTERM)
		self.loop.call_soon_threadsafe(self._tilt2)
	def _tilt2(self):
		self._sig.set()


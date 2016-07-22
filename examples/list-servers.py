#!/usr/bin/env python3
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
from qbroker.unit import Unit, CC_DATA
from qbroker.util.tests import load_cfg
from pprint import pprint
from traceback import print_exc

import logging
import sys
logging.basicConfig(stream=sys.stderr, level=logging.INFO)

import os
cfg = os.environ.get("QBROKER","test.cfg")
u=Unit("test.client.list_servers", **load_cfg(cfg)['config'])

def cb(data):
	pprint(data)
	f = asyncio.ensure_future(u.rpc('qbroker.ping', _dest=data['uuid']))
	def d(f):
		try:
			pprint(f.result())
		except Exception:
			print_exc()
	f.add_done_callback(d)

@asyncio.coroutine
def example(app=None):
	yield from u.start()
	yield from asyncio.sleep(1)
	d = {}
	if app is not None:
		d['app'] = app
	try:
		yield from u.alert("qbroker.ping",callback=cb,call_conv=CC_DATA, timeout=2, _data=d)
	finally:
		yield from u.stop()

def main():
	import sys
	loop = asyncio.get_event_loop()
	loop.run_until_complete(example(*sys.argv[1:]))
main()


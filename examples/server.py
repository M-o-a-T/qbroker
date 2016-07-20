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

import logging
import sys
logging.basicConfig(stream=sys.stderr, level=logging.INFO)

import os
cfg = os.environ.get("QBROKER","test.cfg")
u=Unit("test.server", **load_cfg(cfg)['config'])

@u.register_rpc("example.hello", call_conv=CC_DATA)
@asyncio.coroutine
def hello(name="Joe"):
	yield from asyncio.sleep(1)
	return "Hello %s!" % name
	
@asyncio.coroutine
def example():
	yield from u.start()
	try:
		yield from asyncio.sleep(200)
	finally:
		yield from u.stop()

def main():
	loop = asyncio.get_event_loop()
	loop.run_until_complete(example())
main()


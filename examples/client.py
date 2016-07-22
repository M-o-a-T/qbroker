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
from qbroker.unit import Unit
from qbroker.util.tests import load_cfg
from traceback import print_exc
import logging
import sys
import json
logging.basicConfig(stream=sys.stderr, level=logging.INFO)

import os
cfg = os.environ.get("QBROKER","test.cfg")
u=Unit("test.client", **load_cfg(cfg)['config'])

@asyncio.coroutine
def example(type="example.hello",content=""):
	rc = 0
	yield from u.start(*sys.argv)
	yield from asyncio.sleep(0.2) # allow monitor to attach
	i = type.find('::')
	if i > 0:
		dest = type[i+2:]
		type = type[:i]
	else:
		dest = None
	if content is None:
		content = ''
	elif content == '-':
		content = sys.stdin.read()
	try:
		content = json.loads(content)
	except ValueError:
		print("Warning: content is not JSON, sending as string", file=sys.stderr)
	try:
		res = (yield from u.rpc(type, _data=content, _dest=dest))
		print(res)
	except Exception:
		print_exc()
		rc = 2
	finally:
		yield from u.stop(rc)

def main(type=None, content=None):
	loop = asyncio.get_event_loop()
	loop.run_until_complete(example(type,content))

if __name__ == '__main__':
	main(*sys.argv[1:])


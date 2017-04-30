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
import os
from qbroker.unit import make_unit, DEFAULT_CONFIG
from qbroker.util import combine_dict

TIMEOUT=0.5

from qbroker.util.tests import load_cfg
CFG="test.cfg"
for x in range(10):
	if os.path.exists(CFG):
		break
	CFG=os.path.join(os.pardir,CFG)
cfg = load_cfg(CFG)['config']
cfg = combine_dict(cfg, DEFAULT_CONFIG)


@asyncio.coroutine
def unit(name,*a,**k):
	@asyncio.coroutine
	def setup(c):
		ch = yield from c.channel()
		cf = cfg['amqp']['exchanges']
		yield from ch.exchange_delete(cf['alert'])
		yield from ch.exchange_delete(cf['rpc'])
		yield from ch.exchange_delete(cf['dead'])
		yield from ch.exchange_delete(cf['reply'])
		yield from ch.close()
	u = yield from make_unit(name,*a,_setup=(setup if name in ("test.ping.A",) else None),**k)
	return u


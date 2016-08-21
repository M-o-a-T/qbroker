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
from qbroker.unit import make_unit

TIMEOUT=0.5

@asyncio.coroutine
def unit(name,*a,**k):
	@asyncio.coroutine
	def setup(c):
		ch = yield from c.channel()
		yield from ch.exchange_delete('alert')
		yield from ch.exchange_delete('rpc')
		yield from ch.exchange_delete('dead')
		yield from ch.exchange_delete('reply')
		yield from ch.close()
	u = yield from make_unit(name,*a,_setup=(setup if name in ("test.ping.A",) else None),**k)
	return u


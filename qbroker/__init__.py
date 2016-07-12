#!/usr/bin/env python
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

__VERSION__ = (0,8,2)

# Python 3.5 deprecates .async in favor of .ensure_future
import asyncio
if not hasattr(asyncio,'ensure_future'):
	asyncio.ensure_future = asyncio.async

from .unit import Unit, make_unit

def setup(*a,**k):
	import qbroker.util.sync as sync
	return sync.setup(*a,**k)

# unit_sync() and/or unit_gevent() will be added by qbroker.util.sync.setup()

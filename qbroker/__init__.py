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

import trio

import logging
logger = logging.getLogger(__name__)

from async_generator import asynccontextmanager

@asynccontextmanager
async def open_broker(*args, **kwargs):
	"""\
		Context manager to create a restarting AMQP connection.
		"""
	from .broker import Broker
	async with trio.open_nursery() as nursery:
		async with Broker(*args, nursery=nursery, **kwargs) as b:
			yield b

# Calling conventions for RPC-registered procedures
CC_MSG="_msg" # pass the whole message (default)
CC_DATA="_data" # pass the data element
CC_DICT="_dict" # assume data is a dict and apply it


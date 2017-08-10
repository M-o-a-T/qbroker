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

##
## Configuration: look up, in order:
## yaml_cfg.config
## etcd.specific.APP.config
## etcd.config
## 

import asyncio
from ..util import attrdict

import logging
logger = logging.getLogger(__name__)

# Calling conventions for RPC-registered procedures
CC_MSG="_msg" # pass the whole message (default)
CC_DATA="_data" # pass the data element
CC_DICT="_dict" # assume data is a dict and apply it

DEFAULT_CONFIG=dict(
	amqp=dict(
		server=dict(
			host='localhost',
			login='guest',
			password='guest',
			virtualhost='/qbroker',
			ssl=False,
			connect_timeout=10,
		),
		exchanges=dict(	  # all are persistent
			alert='alert', # topic: broadcast messages, may elicit multiple replies
			rpc='rpc',	 # topic: RPC requests, will trigger exactly one reply
			reply='reply', # direct: all replies go here
			dead='dead',   # fanout: dead messages (TTL expires, reject, RPC/alert unrouteable, …)
		),
		queues=dict(
			alert='alert_',# plus the unit UUID. Nonpersistent.
			msg='msg.',# plus the routing key. Persistent alerts.
			rpc='rpc_',	# plus the command name. Persistent.
			reply='reply_',# plus the unit UUID
			dead='dead',   # no add-on. Persistent. No TTL here!
		),
		handlers=dict(
			dead=False, # add a handler for dead messages
			debug=False, # add code to debug the connection
		),
		codec='DEFAULT',
		ttl=dict(
			rpc=10,
		),
		timeout=dict(
			rpc=15,
			poll=30,
		),
		retries=dict(),
	))

@asyncio.coroutine
def make_unit(*a, _setup=None, **kw):
	"""\
		Create and start a QBroker unit.

		See qbroker.unit.Unit for parameters.
		"""
	u = Unit(*a,**kw)
	yield from u.start(_setup=_setup)
	return u

from .unit import Unit


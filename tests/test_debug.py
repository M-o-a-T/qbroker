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

import pytest
import os
import asyncio
from qbroker.unit import Unit, CC_DICT,CC_DATA,CC_MSG
from testsupport import unit, TIMEOUT, cfg
from qbroker.unit.msg import MsgError,AlertMsg
from qbroker.unit.conn import DeadLettered
import unittest
from unittest.mock import Mock

def test_basic(loop):
	u = Unit("test.zero", loop=loop, **cfg)
	loop.run_until_complete(u.start())
	loop.run_until_complete(u.stop())

@pytest.yield_fixture
def unit1(loop):
	yield from _unit("one",loop)
@pytest.yield_fixture
def unit2(loop):
	yield from _unit("two",loop)
def _unit(name,loop):
	u = loop.run_until_complete(unit("test."+name, loop=loop, **cfg))
	yield u
	x = u.stop()
	loop.run_until_complete(x)

@pytest.mark.run_loop
@asyncio.coroutine
def test_debug(unit1, unit2, loop):
	unit1.debug_env(foo="bar")
	res = (yield from unit2.rpc("qbroker.debug.test.one"))
	assert set(res) == set(('eval','ping'))
	assert 'pong' in res['ping']
	with pytest.raises(RuntimeError):
		res = (yield from unit2.rpc("qbroker.debug.test.one", foo="bar"))
	res = (yield from unit2.rpc("qbroker.debug.test.one", cmd="ping"))
	assert res == "pong"
	res = (yield from unit2.rpc("qbroker.debug.test.one", cmd="eval",code="foo"))
	assert res == "bar"
	res = (yield from unit2.rpc("qbroker.debug.test.one", cmd="eval",code="baz",baz="quux"))
	assert res == "quux"
	res = (yield from unit2.rpc("qbroker.debug.test.one", cmd="eval",code="what='ever'",mode="single",what="duh"))
	assert res in (None,"")
	with pytest.raises(NameError):
		res = (yield from unit2.rpc("qbroker.debug.test.one", cmd="eval",code="what"))
	res = (yield from unit2.rpc("qbroker.debug.test.one", cmd="eval",code="what='ever'",mode="single"))
	assert res in (None,"")
	res = (yield from unit2.rpc("qbroker.debug.test.one", cmd="eval",code="what"))
	assert res == "ever"


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
import pytest
import os
from qbroker import CC_DICT,CC_DATA,CC_MSG
from qbroker import open_broker
from qbroker.msg import MsgError,AlertMsg
from qbroker.conn import DeadLettered
from .testsupport import TIMEOUT, cfg, unit
import unittest
from unittest.mock import Mock

@pytest.mark.trio
async def test_basic():
    async with open_broker("test.zero", **cfg) as b:
        await trio.sleep(TIMEOUT/2)

@pytest.mark.trio
async def test_debug():
    async with unit(1) as unit1:
        async with unit(2) as unit2:
            unit1.debug_env(foo="bar")
            res = await unit2.rpc("qbroker.debug.app.test.debug.debug.1")
            assert set(res) == set(('eval','ping','env')), res
            assert 'pong' in res['ping'], res
            with pytest.raises(RuntimeError):
                res = await unit2.rpc("qbroker.debug.app.test.debug.debug.1", dict(foo="bar"))
            with pytest.raises(SyntaxError):
                await unit2.rpc("qbroker.debug.app.test.debug.debug.1",dict(cmd="eval",code="blubb("))
            res = await unit2.rpc("qbroker.debug.app.test.debug.debug.1", dict(cmd="ping"))
            assert res == "pong", res
            res = await unit2.rpc("qbroker.debug.app.test.debug.debug.1", dict(cmd="eval",code="foo"))
            assert res == "bar", res
            res = await unit2.rpc("qbroker.debug.app.test.debug.debug.1", dict(cmd="eval",code="baz",baz="quux"))
            assert res == "quux", res
            res = await unit2.rpc("qbroker.debug.app.test.debug.debug.1", dict(cmd="eval",code="what='ever'",mode="single",what="duh"))
            assert res in (None,"")
            with pytest.raises(NameError):
                res = await unit2.rpc("qbroker.debug.app.test.debug.debug.1", dict(cmd="eval",code="what"))
            res = await unit2.rpc("qbroker.debug.app.test.debug.debug.1", dict(cmd="eval",code="what='ever'",mode="single"))
            assert res in (None,""), res
            res = await unit2.rpc("qbroker.debug.app.test.debug.debug.1", dict(cmd="eval",code="what"))
            assert res == "ever", res
            await unit2.rpc("qbroker.debug.app.test.debug.debug.1",dict(cmd="eval",mode="exec",code="""\
def hello():
    return "yo"
"""))
            res = await unit2.rpc("qbroker.debug.app.test.debug.debug.1", dict(cmd="eval",code="hello()"))
            assert res == "yo", res

@pytest.mark.trio
async def test_reconnect():
    return
    async with unit(1) as unit1:
        async with unit(2) as unit2:
            unit1.debug_env(conn=unit1,xconn=unit2,retry=0)
            await unit2.rpc("qbroker.debug.app.test.debug.reconnect.1",cmd="eval",mode="exec",code="""\
def test_conn(c):
    global retry
    retry += 1
    if retry in (2,4):
        import os
        os.close(c.conn.amqp._stream_reader._transport._sock_fd)
    return retry
    """)

            res = await unit2.rpc("qbroker.debug.app.test.debug.reconnect.1", cmd="eval",code="test_conn(conn)")
            assert res == 1
            res = await unit2.rpc("qbroker.debug.app.test.debug.reconnect.1", cmd="eval",code="test_conn(conn)", _timeout=2,_retries=1)
            assert res == 3
            res = await unit2.rpc("qbroker.debug.app.test.debug.reconnect.1", cmd="eval",code="test_conn(xconn)", _timeout=2,_retries=1)
            assert res == 5


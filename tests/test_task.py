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
import trio
from qbroker import CC_TASK
from .testsupport import TIMEOUT, cfg, unit
from qbroker.msg import MsgError,AlertMsg
from qbroker.conn import DeadLettered
import unittest
from unittest.mock import Mock
import contextlib
import socket

async def plain_job(msg):
    await msg.reply("Foo "+msg.data['x'])
    await trio.sleep(TIMEOUT/2)
    await msg.reply("Bar "+msg.data['y'])
    await trio.sleep(TIMEOUT/2)
    await msg.reply("Baz "+msg.data['z'])

async def good_job(msg):
    await msg.reply("Foobar "+msg.data)
    await msg.ack()

async def bad_job(msg):
    await msg.reject()

async def return_job(msg):
    await trio.sleep(TIMEOUT/2)
    return 2*msg.data

@pytest.mark.trio
async def test_task_basic():
    async with unit(1) as unit1:
        async with unit(2) as unit2:
            for u in (unit1,unit2):
                await u.register(plain_job,"task.plain", call_conv=CC_TASK)
                await u.register(good_job,"task.good", call_conv=CC_TASK)
                await u.register(bad_job,"task.bad", call_conv=CC_TASK)
                await u.register(return_job,"task.return", call_conv=CC_TASK)

            res = []
            async for r in unit2.rpc_multi("task.plain", dict(x='one', y='two', z='three'), max_replies=3,max_delay=TIMEOUT*3/2):
                res.append(r)
            assert res == ["Foo one","Bar two","Baz three"]

            res = await unit2.rpc("task.good", "barbaz")
            assert res == "Foobar barbaz"

            with pytest.raises(DeadLettered):
                res = await unit2.rpc("task.bad")

            res = await unit2.rpc("task.return", 21)
            assert res == 42


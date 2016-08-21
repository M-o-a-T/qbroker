# -*- coding: UTF-8 -*-

import qbroker.util.sync as sync
sync.setup(sync=True)
AioRunner = sync.AioRunner

import asyncio
import os
import pytest
import sys
import unittest

from functools import partial
from qbroker.unit import Unit
from testsupport import unit,TIMEOUT,cfg
from qbroker.unit.msg import MsgError
from traceback import print_exc


#MY_DIR = os.path.abspath(os.path.dirname(__file__))
#sys.path.append(MY_DIR)

class TestPing(unittest.TestCase):
    q = None
    plonged = None

    def setUp(self):
        AioRunner.start(self.setUp_async, self.tearDown_async)

    def tearDown(self):
        AioRunner.stop()

    @asyncio.coroutine
    def setUp_async(self):
        self.unit = yield from unit("test.ping.A", loop=AioRunner.loop, **cfg)
        yield from self.unit.register_rpc_async(self.pling)

    @asyncio.coroutine
    def tearDown_async(self):
        if self.q is not None:
            yield from self.q.put(None)
            self.q = None
        yield from self.unit.stop()

    @asyncio.coroutine
    def pling(self, data):
        return "plong"

    @asyncio.coroutine
    def ping_b(self):
        u = yield from unit("test.ping.B", loop=AioRunner.loop, **cfg)
        try:
            plong = yield from asyncio.wait_for(u.rpc("pling"),1,loop=AioRunner.loop)
            assert plong == "plong"
        finally:
            yield from u.stop()
        self.plonged = True

    def test_ping_sync(self):
        u = Unit("test.ping.SYNC", loop=AioRunner.loop, **cfg)
        u.start_sync()
        try:
            plong = u.rpc_sync("pling", _timeout=TIMEOUT*2)
            assert plong == "plong"
        finally:
            u.stop_sync()

    @asyncio.coroutine
    def ping_bad(self):
        u = yield from unit("test.ping.BAD", loop=AioRunner.loop, **cfg)
        try:
            plong = yield from asyncio.wait_for(u.rpc("plinnnnng"),1,loop=AioRunner.loop)
        except asyncio.TimeoutError:
            pass
        except MsgError:
            pass
        else:
            assert False, "did not raise an error"
        finally:
            yield from u.stop()
        self.plonged = False

    def test_ping(self):
        AioRunner.run_async(self.ping_b)
        assert self.plonged is True
        self.plonged = None

    def test_ping_bad(self):
        AioRunner.run_async(self.ping_bad)
        assert self.plonged is False
        self.plonged = None
        

if __name__ == "__main__":
    unittest.main()

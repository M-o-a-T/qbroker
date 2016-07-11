# -*- coding: UTF-8 -*-

import asyncio
import os
import sys
import unittest
import gevent
import aiogevent

if not hasattr(asyncio,'ensure_future'):
    asyncio.ensure_future = asyncio.async

# This is not really necessary for QBroker per se.
# These tests are only intended to ensure that asyncio and gevent play well together.
# Thus, a separate gevent-aware version of QBroker will not be necessary.

class TestPing(unittest.TestCase):
    q = None
    plonged = None

    def setUp(self):
        asyncio.set_event_loop_policy(aiogevent.EventLoopPolicy())
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)

    def tearDown(self):
        self.loop.close()

    @asyncio.coroutine
    def hello_world(self):
        yield from asyncio.sleep(1, loop=self.loop)
        return 53

    def test_loop(self):
        self.loop.run_until_complete(self.hello_world())

    def runner(self):
        gevent.sleep(1)
        self.f.set_result(42)

    def test_spawn(self):
        self.f = asyncio.Future(loop=self.loop)
        j = gevent.spawn(self.runner)
        self.loop.run_until_complete(self.f)
        r = self.f.result()
        assert r == 42, r
        j.join()

    def helloer(self):
        j = asyncio.ensure_future(self.hello_world())
        r = aiogevent.yield_future(j)
        return r+2

    def loop_thread(self, f):
        self.loop.run_until_complete(f)

    def test_yield(self):
        f = asyncio.Future(loop=self.loop)
        j1 = gevent.spawn(self.helloer)
        j2 = gevent.spawn(self.loop_thread,f)
        r = j1.get()
        j1.join()
        f.set_result(None)
        j2.join()
        assert r == 55, r

    def test_wrap(self):
        j = gevent.spawn(self.helloer)
        f = aiogevent.wrap_greenlet(j)
        self.loop.run_until_complete(f)
        r = f.result()
        j.join()
        assert r == 55, r



# -*- coding: UTF-8 -*-


import pytest
import qbroker
import asyncio
import unittest
try:
    qbroker.setup(gevent=True)
except ImportError:
    skip = pytest.mark.skip
    async_gevent = lambda x:x
else:
    skip = lambda x:x

    from qbroker.util import sync

    import aiogevent
    import gevent
    import os
    import pytest
    import sys

    from qbroker.unit import Unit
    from qbroker.util.sync import async_gevent
    from testsupport import unit,TIMEOUT,cfg


#MY_DIR = os.path.abspath(os.path.dirname(__file__))
#sys.path.append(MY_DIR)

@skip
class TestPing(unittest.TestCase):
    q = None
    plonged = None

    def setUp(self):
        qbroker.loop.run_until_complete(self.setUp_async())

    def tearDown(self):
        qbroker.loop.run_until_complete(self.tearDown_async())

    @asyncio.coroutine
    def setUp_async(self):
        self.unit = yield from unit("test.ping.A", loop=qbroker.loop, **cfg)
        yield from self.unit.register_rpc_async(self.pling)

    @asyncio.coroutine
    def tearDown_async(self):
        if self.q is not None:
            yield from self.q.put(None)
            self.q = None
        yield from self.unit.stop()

    @async_gevent
    def pling(self, data):
        return "plong"

    def test_ping_gevent(self):
        j = gevent.spawn(self._test_ping_gevent)
        f = aiogevent.wrap_greenlet(j, loop=qbroker.loop)
        qbroker.loop.run_until_complete(f)
        j.join()

    def _test_ping_gevent(self):
        u = Unit("test.ping.GEVENT", loop=qbroker.loop, **cfg)
        u.start_gevent()
        try:
            plong = u.rpc_gevent("pling", _timeout=TIMEOUT*2)
            assert plong == "plong"
        finally:
            u.stop_gevent()


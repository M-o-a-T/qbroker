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
from qbroker import CC_DICT,CC_DATA,CC_MSG, open_broker
from .testsupport import TIMEOUT, cfg, unit
from qbroker.msg import MsgError,AlertMsg
from qbroker.conn import DeadLettered
import unittest
from unittest.mock import Mock
import contextlib
import socket

@pytest.mark.trio
async def test_conn_not():
    with contextlib.closing(socket.socket()) as sock:
        sock.bind(('127.0.0.1', 0))
        unused_tcp_port = sock.getsockname()[1]

    cfg['amqp']['server']['port'] = unused_tcp_port
    with pytest.raises(OSError):
        async with open_broker("test.no_port", **cfg):
            assert False,"duh?"
    del cfg['amqp']['server']['port']

@pytest.mark.trio
async def test_rpc_basic():
    async with unit(1) as unit1:
        async with unit(2) as unit2:
            call_me = Mock(side_effect=lambda x: "foo "+x)
            call_msg = Mock(side_effect=lambda m: "foo "+m.data['x'])
            r1 = await unit1.register_rpc("my.call",call_me, call_conv=CC_DATA)
            await unit1.register_rpc("my.call.x",call_me, call_conv=CC_DICT)
            await unit1.register_rpc("my.call.m",call_msg, call_conv=CC_MSG)
            res = await unit2.rpc("my.call", "one")
            assert res == "foo one"
            res = await unit1.rpc("my.call", "two")
            assert res == "foo two"
            with pytest.raises(TypeError):
                res = await unit1.rpc("my.call", x="two")
            res = await unit1.rpc("my.call.x", x="three")
            assert res == "foo three"
            with pytest.raises(TypeError):
                res = await unit1.rpc("my.call", y="duh")
            res = await unit1.rpc("my.call.m", x="four")
            assert res == "foo four"
            await unit1.unregister_rpc(r1)
            with trio.move_on_after(TIMEOUT*5/2):
                try:
                    await unit1.rpc("my.call", "No!")
                except DeadLettered as exc:
                    pass

@pytest.mark.trio
async def test_rpc_decorated():
    async with unit(1) as unit1:
        async with unit(2) as unit2:
            @unit1.on_rpc("my.call", call_conv=CC_DATA)
            @unit1.on_rpc("my.call.x", call_conv=CC_DICT)
            def call_me(x):
                return "foo "+x
            @unit1.on_rpc("my.call.m", call_conv=CC_MSG)
            def call_msg(m):
                return "foo "+m.data['x']
            await unit1._wait_queue()
            res = await unit2.rpc("my.call", "one")
            assert res == "foo one"
            res = await unit1.rpc("my.call", "two")
            assert res == "foo two"
            with pytest.raises(TypeError):
                res = await unit1.rpc("my.call", x="two")
            res = await unit1.rpc("my.call.x", x="three")
            assert res == "foo three"
            with pytest.raises(TypeError):
                res = await unit1.rpc("my.call", y="duh")
            res = await unit1.rpc("my.call.m", x="four")
            assert res == "foo four"
            await unit1.unregister_rpc(r1)
            with trio.move_on_after(TIMEOUT*5/2):
                try:
                    await unit1.rpc("my.call", "No!")
                except DeadLettered as exc:
                    pass

from qbroker.codec.registry import register_obj
class _TestError(Exception):
    pass

@pytest.mark.trio
async def test_rpc_error():
    async with unit(1) as unit1:
        async with unit(2) as unit2:
            def call_me():
                raise _TestError("foo")
            r1 = await unit1.register_rpc("err.call",call_me, call_conv=CC_DICT)
            try:
                res = await unit2.rpc("err.call")
            except _TestError as exc:
                pass
            except MsgError:
                assert False, "MsgError raised instead of _TestError"
            else:
                assert False, "No error raised"

@pytest.mark.trio
async def test_rpc_direct():
    async with unit(1) as unit1:
        async with unit(2) as unit2:
            call_me = Mock(side_effect=lambda x: "foo "+str(x))
            r1 = await unit1.register_rpc("my.call",call_me, call_conv=CC_DATA)
            res = await unit2.rpc("my.call", "one",uuid=unit1.uuid)
            assert res == "foo one"
            with trio.move_on_after(TIMEOUT*5/2):
                try:
                    t = await unit2.rpc("my.call", "No!", uuid=unit2.uuid)
                except DeadLettered as exc:
                    #assert exc.cls == "DeadLettered"
                    assert str(exc).startswith("Dead: queue=rpc route=qbroker.uuid."), str(exc)
                else:
                    assert False,"did not error"

@pytest.mark.trio
async def test_rpc_unencoded():
    async with unit(1) as unit1:
        async with unit(2) as unit2:
            ncalls = 0
            def call_me():
                nonlocal ncalls 
                ncalls += 1
                return object()
            await unit1.register_rpc("my.call",call_me, call_conv=CC_DICT)
            with trio.move_on_after(TIMEOUT):
                try:
                    r = await unit2.rpc("my.call")
                except DeadLettered as exc:
                    # message was rejected. Thus deadlettered.
                    #assert exc.cls == "DeadLettered"
                    assert str(exc) == "Dead: queue=rpc route=my.call", str(exc)
                except Exception as exc:
                    assert False,exc
                else:
                    assert False,r
            assert ncalls == 1, ncalls

def something_named(foo):
    return "bar "+foo

@pytest.mark.trio
async def test_rpc_named():
    async with unit(1) as unit1:
        async with unit(2) as unit2:
            await unit1.register_rpc(something_named, call_conv=CC_DATA)
            res = await unit2.rpc("something.named", "one")
            assert res == "bar one"
            res = await unit1.rpc("something.named", "two")
            assert res == "bar two"

@pytest.mark.trio
async def test_rpc_explicit():
    async with unit(1) as unit1:
        async with unit(2) as unit2:
            from qbroker.rpc import RPCservice
            s = RPCservice(something_named,call_conv=CC_DATA)
            await unit1.register_rpc(s)
            # something_named.__module__ depends on what the test was called with
            res = await unit2.rpc(something_named.__module__+".something_named", "one")
            assert res == "bar one"
            res = await unit1.rpc(something_named.__module__+".something_named", "two")
            assert res == "bar two"

@pytest.mark.trio
async def test_alert_callback():
    async with unit(1) as unit1:
        async with unit(2) as unit2:
            alert_me = Mock(side_effect=lambda y: "bar "+y)
            r1 = await unit1.register_alert("my.alert",alert_me, call_conv=CC_DICT)
            await unit2.register_alert("my.alert",alert_me, call_conv=CC_DICT)
            n = 0
            async for x in unit2.alert("my.alert",y="dud",callback=cb,call_conv=CC_DATA, timeout=TIMEOUT):
                n += 1
                assert x == "bar dud", x
            assert n == 2
            n = 0
            async for x in unit1.alert("my.alert",{'y':"dud"},callback=cb,call_conv=CC_DATA, timeout=TIMEOUT):
                n += 1
                assert x == "bar dud", x
            assert n == 2
            await unit1.unregister_alert(r1)
            async for x in unit1.alert("my.alert",{'y':"dud"},callback=cb,call_conv=CC_DATA, timeout=TIMEOUT):
                n += 1
                assert x == "bar dud", x
            assert n == 3

@pytest.mark.trio
async def test_alert_uncodeable():
    async with unit(1) as unit1:
        async with unit(2) as unit2:
            alert_me = Mock(side_effect=lambda : object())
            await unit1.register_alert("my.alert",alert_me, call_conv=CC_DICT)
            def cb(msg):
                assert False,"Called?"
            async for x in await unit2.alert("my.alert",callback=cb, timeout=TIMEOUT):
                assert False, x

@pytest.mark.trio
async def test_alert_binary():
    async with unit(1) as unit1:
        async with unit(2) as unit2:
            done = [False]
            def alert_me(msg):
                assert msg == "Hellö".encode("utf-8")
                done[0] = True
            await unit1.register_alert("my.alert",alert_me, call_conv=CC_DATA)
            await unit2.alert("my.alert", "Hellö", codec="application/binary")
            await trio.sleep(TIMEOUT/2)
            assert done[0]

@pytest.mark.trio
async def test_alert_oneway():
    async with unit(1) as unit1:
        async with unit(2) as unit2:
            alert_me1 = Mock()
            alert_me2 = Mock()
            alert_me3 = Mock()
            alert_me4 = Mock()
            await unit1.register_alert("my.alert1",alert_me1, call_conv=CC_DICT)
            await unit1.register_alert("my.alert2",alert_me2, call_conv=CC_DATA)
            await unit1.register_alert("my.alert3",alert_me3) # default is CC_MSG
            await unit1.register_alert("my.#",alert_me4) # default is CC_MSG
            await unit2.alert("my.alert1",{'y':"dud"})
            await unit2.alert("my.alert2",{'y':"dud"})
            await unit2.alert("my.alert3",{'y':"dud"})
            await unit2.alert("my.alert4.whatever",{'z':"dud"})
            await trio.sleep(TIMEOUT/2)
            alert_me1.assert_called_with(y='dud')
            alert_me2.assert_called_with(dict(y='dud'))
            alert_me3.assert_called_with(AlertMsg(data=dict(y='dud')))
            alert_me4.assert_called_with(AlertMsg(data=dict(z='dud')))

@pytest.mark.trio
async def test_alert_no_data():
    async with unit(1) as unit1:
        async with unit(2) as unit2:
            alert_me1 = Mock(side_effect=lambda x: "")
            alert_me2 = Mock(side_effect=lambda : {})
            await unit1.register_alert("my.alert1",alert_me1, call_conv=CC_DATA)
            await unit2.register_alert("my.alert2",alert_me2, call_conv=CC_DICT)
            async def recv2(*a,**k):
                return {}
            n = 0
            async for d in await unit2.alert("my.alert1", "", callback=recv1, call_conv=CC_DATA, timeout=TIMEOUT):
                n += 1
                assert d == ""
            alert_me1.assert_called_with("")
            assert n == 1
            n = 0
            async for d in await unit2.alert("my.alert2", callback=recv2, call_conv=CC_DICT, timeout=TIMEOUT):
                n += 1
                await trio.sleep(0.01)
                assert not d

            alert_me2.assert_called_with()
            assert n == 1

@pytest.mark.trio
async def test_alert_durable():
    async with unit(1) as unit1:
        async with unit(2) as unit2:
            ncalls = 0
            def alert_me():
                nonlocal ncalls
                ncalls += 1
            r1 = await unit1.register_alert("my.dur.alert",alert_me, call_conv=CC_DICT, durable=True, ttl=1)
            r2 = await unit2.register_alert("my.dur.alert",alert_me, call_conv=CC_DICT, durable=True, ttl=1)

            await unit2.alert("my.dur.alert")
            await trio.sleep(TIMEOUT*3/2)
            assert ncalls == 1

            # Now check if this thing really is durable
            await unit1.unregister_alert(r1)
            await unit2.unregister_alert(r2)
            await trio.sleep(TIMEOUT/2)
            await unit2.alert("my.dur.alert")
            await trio.sleep(TIMEOUT*3/2)
            assert ncalls == 1
            r1 = await unit1.register_alert("my.dur.alert",alert_me, call_conv=CC_DICT, durable=True, ttl=1)
            await trio.sleep(TIMEOUT*3/2)
            assert ncalls == 2

@pytest.mark.trio
async def test_alert_nondurable():
    async with unit(1) as unit1:
        async with unit(2) as unit2:
            ncalls = 0
            def alert_me():
                nonlocal ncalls
                ncalls += 1
            r1 = await unit1.register_alert("my.alert",alert_me, call_conv=CC_DICT)
            r2 = await unit2.register_alert("my.alert",alert_me, call_conv=CC_DICT)

            await unit2.alert("my.alert")
            await trio.sleep(TIMEOUT*3/2)
            assert ncalls == 2

            # now verify that messages do get lost
            await unit1.unregister_alert(r1)
            await unit2.unregister_alert(r2)
            await unit2.alert("my.alert")
            await trio.sleep(TIMEOUT*3/2)
            assert ncalls == 2
            r1 = await unit1.register_alert("my.alert",alert_me, call_conv=CC_DICT)
            await trio.sleep(TIMEOUT*3/2)
            assert ncalls == 2

@pytest.mark.trio
async def test_alert_stop():
    async with unit(1) as unit1:
        async with unit(2) as unit2:
            ncall = 0
            nhit = 0
            async def sleep1():
                nonlocal nhit
                nhit += 1
                await trio.sleep(TIMEOUT/2)
                return False
            async def sleep2():
                nonlocal nhit
                nhit += 1
                await trio.sleep(TIMEOUT)
                return False
            await unit1.register_alert("my.sleep",sleep1, call_conv=CC_DICT)
            await unit2.register_alert("my.sleep",sleep2, call_conv=CC_DICT)
            async for msg in await unit2.alert("my.sleep", callback=recv, timeout=TIMEOUT*5/2):
                ncall += 1
                break
            assert nhit == 2, nhit
            assert ncall == 1, ncall

@pytest.mark.trio
async def test_reg():
    async with unit(1) as unit1:
        async with unit(2) as unit2:
            rx = 0
            async for d in await unit2.alert("qbroker.ping", callback=recv, timeout=TIMEOUT, call_conv=CC_DICT):
                if d['uuid'] == unit1.uuid:
                    assert d['app'] == unit1.app
                    rx += 1
                elif d['uuid'] == unit2.uuid:
                    assert d['app'] == unit2.app
                    rx += 1
                # There may be others.
            assert res >= 2
            assert rx == 2

            res = await unit2.rpc("qbroker.ping", uuid=unit1.uuid)
            assert res['app'] == unit1.app
            assert "qbroker.ping" in res['rpc'], res['rpc']

@pytest.mark.trio
async def test_alert_error():
    async with unit(1) as unit1:
        async with unit(2) as unit2:
            def err(x):
                raise RuntimeError("dad")
            error_me1 = Mock(side_effect=err)
            await unit1.register_alert("my.error1",error_me1, call_conv=CC_DATA)
            called = false
            async for d in unit2.alert("my.error1", min_replies=1,max_replies=2,timeout=TIMEOUT):
                assert not called
                assert d.error.cls == "RuntimeError"
                assert d.error.message == "dad"
                called = true
            error_me1.assert_called_with("")

            async for d in unit2.alert("my.error1", call_conv=CC_DATA, min_replies=1,max_replies=2,timeout=TIMEOUT):
                assert False

            with pytest.raises(RuntimeError):
                await unit2.poll_one("my.error1")

@pytest.mark.trio
async def test_reg_error():
    async with unit(1) as unit1:
        with pytest.raises(AssertionError):
            await unit1.register_rpc("my.call",Mock())
        with pytest.raises(AssertionError):
            await unit1.register_alert("my.alert",Mock())

@pytest.mark.trio
async def test_rpc_bad_params():
    async with unit(1) as unit1:
        async with unit(2) as unit2:
            call_me = Mock(side_effect=lambda x: "foo "+x)
            await unit1.register_rpc("my.call",call_me, call_conv=CC_DATA)
            try:
                res = await unit2.rpc("my.call", x="two")
            except TypeError as exc:
                assert type(exc) == TypeError
                assert "convert" in str(exc) or "must be " in str(exc)
            else:
                assert False,"exception not called"
    
@pytest.mark.trio
async def test_rpc_unroutable():
    async with unit(1) as unit1:
        async with unit(2) as unit2:
            call_me = Mock(side_effect=lambda x: "foo "+str(x))
            await unit1.register_rpc("my.call",call_me, call_conv=CC_DATA)
            try:
                res = await unit2.rpc("my.non_routed.call")
            except DeadLettered as exc:
                #assert exc.cls == "DeadLettered"
                assert str(exc) == "Dead: queue=rpc route=my.non_routed.call", str(exc)
            else:
                assert False,"exception not called"
            assert call_me.call_count == 0
    
@pytest.mark.trio
async def test_reg_sync():
    u = open_broker("test.three", **cfg)
    @u.rpc("foo.bar")
    def foo_bar_0(msg):
        return "quux from "+msg.data['baz']
    @u.rpc
    def foo_bar_1(msg):
        return "quux from "+msg.data['baz']
    @u.rpc(call_conv=CC_DICT)
    def foo_bar_2(baz):
        return "quux from "+baz
    assert "foo.bar.2" in u.rpc_endpoints
    async with u:
        x = await u.rpc("foo.bar",baz="nixx")
        y = await u.rpc("foo.bar.1",baz="nixy")
        z = await u.rpc("foo.bar.2",baz="nixz")
        assert x == "quux from nixx"
        assert y == "quux from nixy"
        assert z == "quux from nixz"
    await u.unregister_rpc("foo.bar.2")
    assert "foo.bar.2" not in u.rpc_endpoints


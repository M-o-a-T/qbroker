# -*- coding: utf-8 -*-
#
# This file is part of QBroker, an easy to use RPC and broadcast
# client+server using AMQP.
#
# QBroker is Copyright © 2016-2018 by Matthias Urlichs <matthias@urlichs.de>,
# it is licensed under the GPLv3. See the file `README.rst` for details,
# including optimistic statements by the author.
#
# This paragraph is auto-generated and may self-destruct at any time,
# courtesy of "make update". The original is in ‘utils/_boilerplate.py’.
# Thus, please do not remove the next line, or insert any blank lines.
#BP

import pytest
import os
import trio
from qbroker import CC_DICT, CC_DATA, CC_MSG, CC_TASK, open_broker
from .testsupport import TIMEOUT, cfg, unit
from qbroker.msg import MsgError, AlertMsg
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
            assert False, "duh?"
    del cfg['amqp']['server']['port']


@pytest.mark.trio
async def test_rpc_basic():
    async with unit(1) as unit1:
        async with unit(2) as unit2:
            call_me = Mock(side_effect=lambda x: "foo " + x)
            call_msg = Mock(side_effect=lambda m: "foo " + m.data['x'])
            with pytest.raises(RuntimeError):
                await unit1.register(call_me, "my.#.call")
            r1 = await unit1.register(call_me, "my.call", call_conv=CC_DATA)
            await unit1.register(call_me, "my.call.x", call_conv=CC_DICT)
            await unit1.register(call_msg, "my.call.m", call_conv=CC_MSG)
            res = await unit2.rpc("my.call", "one", result_conv=CC_MSG)
            assert res.data == "foo one"
            res = await unit1.rpc("my.call", "two", result_conv=CC_DATA)
            assert res == "foo two"
            with pytest.raises(RuntimeError):
                res = await unit1.rpc("my.call", "nix", result_conv=CC_DICT)
            with pytest.raises(RuntimeError):
                res = await unit1.rpc("my.call", "nix", result_conv=CC_TASK)
            with pytest.raises(TypeError):
                res = await unit1.rpc("my.call", dict(x="two"), debug=True)
            res = await unit1.rpc("my.call.x", dict(x="three"))
            assert res == "foo three"
            with pytest.raises(TypeError):
                res = await unit1.rpc("my.call", dict(y="duh"), debug=True)
            res = await unit1.rpc("my.call.m", dict(x="four"))
            assert res == "foo four"
            await unit1.unregister(r1)
            with trio.move_on_after(TIMEOUT * 5 / 2):
                try:
                    await unit1.rpc("my.call", "No!")
                except DeadLettered as exc:
                    pass


@pytest.mark.trio
async def test_rpc_decorated():
    async with unit(1) as unit1:
        async with unit(2) as unit2:

            @unit1.on_rpc("my.call.x", call_conv=CC_DICT)
            def call_me(x):
                return "foo " + x

            r1 = unit1.on_rpc(call_me, "my.call", call_conv=CC_DATA)

            @unit1.on_rpc("my.call.m", call_conv=CC_MSG)
            def call_msg(m):
                return "foo " + m.data['x']

            await unit1.wait_queue()
            res = await unit2.rpc("my.call", "one")
            assert res == "foo one"
            res = await unit1.rpc("my.call", "two")
            assert res == "foo two"
            with pytest.raises(TypeError):
                res = await unit1.rpc("my.call", dict(x="two"), debug=True)
            res = await unit1.rpc("my.call.x", dict(x="three"))
            assert res == "foo three"
            with pytest.raises(TypeError):
                res = await unit1.rpc("my.call", dict(y="duh"), debug=True)
            res = await unit1.rpc("my.call.m", dict(x="four"))
            assert res == "foo four"
            await unit1.unregister(r1)
            with trio.move_on_after(TIMEOUT * 5 / 2):
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

            r1 = await unit1.register(call_me, "err.call", call_conv=CC_DICT)
            try:
                res = await unit2.rpc("err.call", debug=True)
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
            #call_me = Mock(side_effect=lambda x: "foo "+str(x))
            def call_me(x):
                return "foo " + x

            r1 = await unit1.register(call_me, "my.call", call_conv=CC_DATA)

            with pytest.raises(RuntimeError):
                await unit2.rpc("my.call", "nix", uuid=unit1.uuid, dest="whoever")

            res = await unit2.rpc("my.call", "one", uuid=unit1.uuid)
            assert res == "foo one", res
            with trio.move_on_after(TIMEOUT * 5 / 2):
                try:
                    t = await unit2.rpc("my.call", "No!", uuid=unit2.uuid)
                except DeadLettered as exc:
                    #assert exc.cls == "DeadLettered"
                    assert str(exc).startswith("Dead: queue=rpc route=qbroker.uuid."), str(exc)
                else:
                    assert False, "did not error"


@pytest.mark.trio
async def test_rpc_unencoded():
    async with unit(1) as unit1:
        async with unit(2) as unit2:
            ncalls = 0

            def call_me():
                nonlocal ncalls
                ncalls += 1
                return object()

            await unit1.register(call_me, "my.call", call_conv=CC_DICT)
            with trio.move_on_after(TIMEOUT):
                try:
                    r = await unit2.rpc("my.call")
                except DeadLettered as exc:
                    # message was rejected. Thus deadlettered.
                    #assert exc.cls == "DeadLettered"
                    assert str(exc) == "Dead: queue=rpc route=my.call", str(exc)
                except TypeError as exc:
                    # alternate resolution: forward the error
                    assert "Object of type 'object' is not JSON serializable" in str(exc), exc
                except Exception as exc:
                    assert False, exc
                else:
                    assert False, r
            assert ncalls == 1, ncalls


def something_named(foo):
    return "bar " + foo


@pytest.mark.trio
async def test_rpc_named():
    async with unit(1) as unit1:
        async with unit(2) as unit2:
            await unit1.register(something_named, call_conv=CC_DATA)
            res = await unit2.rpc("something.named", "one")
            assert res == "bar one"
            res = await unit1.rpc("something.named", "two")
            assert res == "bar two"


#@pytest.mark.trio
#async def test_rpc_explicit():
#    async with unit(1) as unit1:
#        async with unit(2) as unit2:
#            from qbroker.rpc import RPCservice
#            s = RPCservice(something_named,call_conv=CC_DATA)
#            await unit1.register(s)
#            # something_named.__module__ depends on what the test was called with
#            res = await unit2.rpc(something_named.__module__+".something_named", "one")
#            assert res == "bar one"
#            res = await unit1.rpc(something_named.__module__+".something_named", "two")
#            assert res == "bar two"


@pytest.mark.trio
async def test_alert_callback():
    async with unit(1) as unit1:
        async with unit(2) as unit2:
            alert_me = Mock(side_effect=lambda y: "bar " + y)
            r1 = await unit1.register(alert_me, "my.alert", call_conv=CC_DICT, multiple=True)
            await unit2.register(alert_me, "my.alert", call_conv=CC_DICT, multiple=True)
            n = 0
            async for x in unit2.poll("my.alert", {'y': "dud1"}, result_conv=CC_MSG,
                                      max_delay=TIMEOUT):
                n += 1
                assert x.data == "bar dud1", x
            assert n == 2
            n = 0
            async for x in unit1.poll("my.alert", {'y': "dud2"}, result_conv=CC_DATA,
                                      max_delay=TIMEOUT):
                n += 1
                assert x == "bar dud2", x
            assert n == 2
            await unit1.unregister(r1)
            async for x in unit1.poll("my.alert", {'y': "dud3"}, result_conv=CC_DATA,
                                      max_delay=TIMEOUT):
                n += 1
                assert x == "bar dud3", x
            assert n == 3


@pytest.mark.trio
async def test_alert_uncodeable():
    async with unit(1) as unit1:
        async with unit(2) as unit2:
            alert_me = Mock(side_effect=lambda: object())
            await unit1.register(alert_me, "my.alert", call_conv=CC_DICT, multiple=True)
            for debug in (0, 1):
                try:
                    async for x in unit2.poll("my.alert", max_delay=TIMEOUT, debug=debug):
                        assert False, x
                except TypeError as exc:
                    assert debug
                    assert "Object of type 'object' is not JSON serializable" in str(exc)
                except DeadLettered as exc:
                    assert not debug
                    pass
                else:
                    assert False


@pytest.mark.trio
async def test_alert_binary():
    async with unit(1) as unit1:
        async with unit(2) as unit2:
            done = [False]

            def alert_me(msg):
                assert msg == "Hellö".encode("utf-8")
                done[0] = True

            await unit1.register(alert_me, "my.alert", call_conv=CC_DATA, multiple=True)
            await unit2.alert("my.alert", "Hellö", codec="application/binary")
            await trio.sleep(TIMEOUT / 2)
            assert done[0]


@pytest.mark.trio
async def test_alert_oneway():
    async with unit(1) as unit1:
        async with unit(2) as unit2:
            alert_me1 = Mock()
            alert_me2 = Mock()
            alert_me3 = Mock()
            alert_me4 = Mock()

            def _alert_me3(msg):
                alert_me3(msg.data)

            def _alert_me4(msg):
                alert_me4(msg.data)

            await unit1.register(alert_me1, "my.alert1", call_conv=CC_DICT, multiple=True)
            await unit1.register(alert_me2, "my.alert2", call_conv=CC_DATA, multiple=True)
            await unit1.register(_alert_me3, "my.alert3", multiple=True)  # default is CC_MSG
            await unit1.register(_alert_me4, "my.#", multiple=True)  # default is CC_MSG
            await unit2.alert("my.alert1", {'y': "dud"})
            await unit2.alert("my.alert2", {'y': "dud"})
            await unit2.alert("my.alert3", {'y': "dud"})
            await unit2.alert("my.alert4.whatever", {'z': "dud"})
            await trio.sleep(TIMEOUT / 2)
            alert_me1.assert_called_with(y='dud')
            alert_me2.assert_called_with(dict(y='dud'))
            alert_me3.assert_called_with(dict(y='dud'))
            alert_me4.assert_called_with(dict(z='dud'))


@pytest.mark.trio
async def test_alert_no_data():
    async with unit(1) as unit1:
        async with unit(2) as unit2:
            alert_me1 = Mock(side_effect=lambda x: "")
            alert_me2 = Mock(side_effect=lambda: {})
            await unit1.register(alert_me1, "my.alert1", call_conv=CC_DATA, multiple=True)
            await unit2.register(alert_me2, "my.alert2", call_conv=CC_DICT, multiple=True)

            async def recv2(*a, **k):
                return {}

            n = 0
            async for d in unit2.poll("my.alert1", "", result_conv=CC_DATA, max_delay=TIMEOUT):
                n += 1
                assert d == ""
            alert_me1.assert_called_with("")
            assert n == 1
            n = 0
            async for d in unit2.poll("my.alert2", result_conv=CC_DATA, max_delay=TIMEOUT):
                n += 1
                await trio.sleep(0.01)
                assert not d

            alert_me2.assert_called_with()
            assert n == 1


@pytest.mark.trio
async def test_alert_durable1():
    async def check_durable(ev, task_status=trio.TASK_STATUS_IGNORED):
        def alert_me(x):
            return 2 * x

        async with unit(1) as unit1:
            await unit1.register(
                alert_me, "my.dur.alert1", call_conv=CC_DATA, durable=True, ttl=1, multiple=True
            )
        task_status.started()
        await trio.sleep(TIMEOUT / 2)
        async with unit(1) as unit1:
            await unit1.register(
                alert_me, "my.dur.alert1", call_conv=CC_DATA, durable=True, ttl=1, multiple=True
            )
            await ev.wait()

    ev = trio.Event()
    async with unit(2) as unit2:
        await unit2.nursery.start(check_durable, ev)
        with trio.fail_after(TIMEOUT):
            res = await unit2.poll_first("my.dur.alert1", 123)
        assert res == 246
        ev.set()


@pytest.mark.trio
async def test_alert_durable2():
    async with unit(1) as unit1:
        async with unit(2) as unit2:
            ncalls = 0

            def alert_me():
                nonlocal ncalls
                ncalls += 1

            r1 = await unit1.register(
                alert_me,
                "my.dur.alert2",
                call_conv=CC_DICT,
                durable="my_very_durable_test",
                ttl=1,
                multiple=True
            )
            r2 = await unit2.register(
                alert_me,
                "my.dur.alert2",
                call_conv=CC_DICT,
                durable="my_very_durable_test",
                ttl=1,
                multiple=True
            )

            await unit2.alert("my.dur.alert2")
            await trio.sleep(TIMEOUT * 3 / 2)
            assert ncalls == 1

            # Now check if this thing really is durable
            await unit1.unregister(r1)
            await unit2.unregister(r2.tag)
            await trio.sleep(TIMEOUT / 2)
            await unit2.alert("my.dur.alert2")
            await trio.sleep(TIMEOUT * 3 / 2)
            assert ncalls == 1
            r1 = await unit1.register(
                alert_me,
                "my.dur.alert2",
                call_conv=CC_DICT,
                durable="my_very_durable_test",
                ttl=1,
                multiple=True
            )
            await trio.sleep(TIMEOUT * 3 / 2)
            assert ncalls == 2


@pytest.mark.trio
async def test_alert_nondurable():
    async with unit(1) as unit1:
        async with unit(2) as unit2:
            ncalls = 0

            def alert_me():
                nonlocal ncalls
                ncalls += 1

            r1 = await unit1.register(alert_me, "my.alert", call_conv=CC_DICT, multiple=True)
            r2 = await unit2.register(alert_me, "my.alert", call_conv=CC_DICT, multiple=True)

            await unit2.alert("my.alert")
            await trio.sleep(TIMEOUT * 3 / 2)
            assert ncalls == 2

            # now verify that messages do get lost
            await unit1.unregister(r1)
            await unit2.unregister(r2)
            await unit2.alert("my.alert")
            await trio.sleep(TIMEOUT * 3 / 2)
            assert ncalls == 2
            r1 = await unit1.register(alert_me, "my.alert", call_conv=CC_DICT, multiple=True)
            await trio.sleep(TIMEOUT * 3 / 2)
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
                await trio.sleep(TIMEOUT / 2)
                return False

            async def sleep2():
                nonlocal nhit
                nhit += 1
                await trio.sleep(TIMEOUT)
                return False

            await unit1.register(sleep1, "my.sleep", call_conv=CC_DICT, multiple=True)
            await unit2.register(sleep2, "my.sleep", call_conv=CC_DICT, multiple=True)
            async for msg in unit2.poll("my.sleep", max_delay=TIMEOUT * 5 / 2):
                ncall += 1
                break
            assert nhit == 2, nhit
            assert ncall == 1, ncall


@pytest.mark.trio
async def test_reg():
    async with unit(1) as unit1:
        async with unit(2) as unit2:
            rx = 0
            async for d in unit2.poll("qbroker.ping", max_delay=TIMEOUT, result_conv=CC_DATA):
                if d['uuid'] == unit1.uuid:
                    assert d['app'] == unit1.app
                    rx += 1
                elif d['uuid'] == unit2.uuid:
                    assert d['app'] == unit2.app
                    rx += 1
                # There may be others.
            assert rx == 2

            with pytest.raises(trio.TooSlowError):
                async for d in unit2.poll("qbroker.ping", min_replies=99, max_delay=TIMEOUT / 2,
                                          result_conv=CC_DATA):
                    pass

            res = await unit2.rpc("qbroker.ping", dest=unit1.app)
            assert res['app'] == unit1.app
            assert "rpc.qbroker.ping" in res['endpoints'], res['endpoints']


@pytest.mark.trio
async def test_alert_error():
    async with unit(1) as unit1:
        async with unit(2) as unit2:

            def err(x):
                raise RuntimeError("dad")

            error_me1 = Mock(side_effect=err)
            await unit1.register(error_me1, "my.error1", call_conv=CC_DATA, multiple=True)
            called = False
            async for d in unit2.poll("my.error1", min_replies=1, max_replies=2, max_delay=TIMEOUT,
                                      result_conv=CC_MSG, debug=True):
                assert not called
                assert d.error.cls == "RuntimeError"
                assert d.error.message == "dad"
                called = True
            error_me1.assert_called_with(None)

            with pytest.raises(RuntimeError):
                async for d in unit2.poll("my.error1", result_conv=CC_DATA, min_replies=1,
                                          max_replies=2, max_delay=TIMEOUT):
                    assert False

            with pytest.raises(RuntimeError):
                await unit2.poll_first("my.error1")


@pytest.mark.trio
async def test_rpc_bad_params():
    async with unit(1) as unit1:
        async with unit(2) as unit2:
            call_me = Mock(side_effect=lambda x: "foo " + x)
            await unit1.register(call_me, "my.call", call_conv=CC_DATA)
            try:
                res = await unit2.rpc("my.call", dict(x="two"), debug=True)
            except TypeError as exc:
                assert type(exc) == TypeError
                assert "convert" in str(exc) or "must be " in str(exc)
            else:
                assert False, "exception not called"


@pytest.mark.trio
async def test_rpc_unroutable():
    async with unit(1) as unit1:
        async with unit(2) as unit2:
            call_me = Mock(side_effect=lambda x: "foo " + str(x))
            await unit1.register(call_me, "my.call", call_conv=CC_DATA)
            try:
                res = await unit2.rpc("my.non_routed.call")
            except DeadLettered as exc:
                #assert exc.cls == "DeadLettered"
                assert str(exc) == "Dead: queue=rpc route=my.non_routed.call", str(exc)
            else:
                assert False, "exception not called"
            assert call_me.call_count == 0


@pytest.mark.trio
async def test_enter_twice():
    u_ = unit(1)
    with pytest.raises(RuntimeError):
        with u_ as us:
            pass
    async with u_ as u:
        with pytest.raises(RuntimeError):
            async with u_ as uu:
                pass
        with pytest.raises(RuntimeError):
            async with u as uuu:
                pass


@pytest.mark.trio
async def test_reg_sync():
    async with unit(1) as u:

        @u.on_rpc("foo.bar", call_conv=CC_DATA)
        def foo_bar_0(data):
            return "quux from " + data['baz']

        @u.on_rpc
        def foo_bar_1(msg):
            return "quux from " + msg.data['baz']

        @u.on_rpc(call_conv=CC_DICT)
        def foo_bar_2(baz):
            return "quux from " + baz

        await u.wait_queue()
        assert "rpc.foo.bar.2" in u._endpoints
        x = await u.rpc("foo.bar", dict(baz="nixx"))
        y = await u.rpc("foo.bar.1", dict(baz="nixy"))
        z = await u.rpc("foo.bar.2", dict(baz="nixz"))
        assert x == "quux from nixx"
        assert y == "quux from nixy"
        assert z == "quux from nixz"
        assert "rpc.foo.bar.2" in u._endpoints
        await u.unregister("rpc.foo.bar.2")
        assert "rpc.foo.bar.2" not in u._endpoints

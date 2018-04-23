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

import trio
from . import CC_MSG, CC_DATA
from .conn import Connection
from .rpc import RPCservice
from .config import DEFAULT_CONFIG
from .util import uuidstr, combine_dict
from collections.abc import Mapping
from async_generator import aclosing
from functools import partial

import logging
logger = logging.getLogger(__name__)


class Broker:
    """The basic QBroker messenger."""
    config = None  # configuration data, also serves as flag
    # for preventing restart at closedown
    conn = None  # AMQP receiver
    uuid = None  # my UUID
    restarting = None
    args = ()
    debug = None
    _running = False

    def __init__(self, app, *args, hidden=False, nursery=None, idle_proc=None, **cfg):
        """\
            Connect to an AMQP server. See qbroker.config.DEFAULT_CONFIG for
            all recognized parameters.

            >>> u = Unit("my_nice_server",
            ...         server=dict(
            ...            login="foo",
            ...            password="bar",
            ...            virtualhost="/test"))

            You need to call .start() to actually initiate a connection.

            Arguments:
                app:
                    name of this program. All apps of the same name should
                    implement the same API.
                args:
                    Arguments this code has been called with, for
                    introspection.
                hidden:
                    set if you're a debugger and don't want to actively
                    register anything.
                idle_proc:
                    async procedure that runs while attempting to
                    reconnect.
                cfg:
                    Configuration. See :obj:`qbroker.config.DEFAULT_CONFIG`
                    for the defaults.
            """

        self.app = app
        self.args = args

        if nursery is None:
            raise RuntimeError("I need a nursery")
        self.nursery = nursery
        self._queue = trio.Queue(999)
        self._stop = trio.Event()
        self._connected = trio.Event()
        self.nursery.start_soon(self._queue_run)
        self.hidden = hidden
        self._idle = None  # cancel scope of backgound while-disconnected task
        self.idle_proc = idle_proc

        self.uuid = uuidstr()

        if 'cfg' in cfg:
            cfg = cfg['cfg']
        if 'amqp' in cfg:
            cfg = cfg['amqp']
        self.cfg = combine_dict(cfg, DEFAULT_CONFIG)

        self._endpoints = {}
        self._reg_endpoints = set()

        self.codec = self.cfg.codec

        if self.cfg.handlers.debug:
            from .debug import Debugger
            self.debug = Debugger(self)

        if not self.hidden:
            self.on_rpc(self._alert_ping, "qbroker.ping", call_conv=CC_DATA, multiple=True)
            self.on_rpc(self._reply_ping, "qbroker.ping")
            self.on_rpc(
                self._alert_ping, "qbroker.app." + self.app, call_conv=CC_DATA, multiple=True
            )
            self.on_rpc(self._reply_ping, "qbroker.app." + self.app)
        if self.debug is not None:
            self.on_rpc(
                self._reply_debug, "qbroker.debug.app." + self.app, call_conv=CC_MSG, debug=True
            )
            self.on_rpc(
                self._reply_debug, "qbroker.debug.uuid." + self.uuid, call_conv=CC_MSG, debug=True
            )
            # uuid: done in conn setup

    async def __aenter__(self):
        if self._running:
            raise RuntimeError("This broker is already running")
        self._running = True
        try:
            await self.nursery.start(self._keep_connected)
        except BaseException:
            self._running = False
            raise
        return self

    async def __aexit__(self, *tb):
        self.nursery.cancel_scope.cancel()

        with trio.open_cancel_scope(shield=True):
            try:
                if self.conn is not None:
                    await self.conn.aclose()
            except BaseException as exc:
                logger.debug("Conn ended", exc_info=exc)
                raise
            finally:
                self.conn = None
                self._running = False

    def __enter__(self):
        raise RuntimeError("You need to use 'asny with'")

    def __exit__(self, *tb):
        raise RuntimeError("You need to use 'asny with'")

    async def _run_idle(self, task_status=trio.TASK_STATUS_IGNORED):
        """
        Run the "idle proc" under a separate scope
        so that it can be cancelled when the connection comes back.
        """
        try:
            with trio.open_cancel_scope() as s:
                self._idle = s
                await self.idle_proc()
        finally:
            self._idle = None

    async def _keep_connected(self, task_status=trio.TASK_STATUS_IGNORED):
        """Task which keeps a connection going"""

        class TODOexception(Exception):
            pass

        self.restarting = None
        while not self._stop.is_set():
            try:
                self._reg_endpoints = set()
                async with Connection(self.cfg, self.uuid).connect(self) as conn:
                    self.restarting = False
                    self.conn = conn
                    self._connected.set()
                    if self._idle is not None:
                        self._idle.cancel()
                        self._idle = None
                    await self._do_regs()
                    task_status.started()
                    task_status = trio.TASK_STATUS_IGNORED
                    await conn.is_disconnected.wait()
            except TODOexception:
                self._connected.clear()
                logger.exception("Error. TODO Reconnecting after a while.")
            finally:
                c, self.conn = self.conn, None
                if c is not None:
                    with trio.open_cancel_scope(shield=True, deadline=trio.current_time() + 1):
                        await c.aclose()

            self.restarting = True
            if self._stop.is_set():
                break
            if self.idle_proc is not None:
                await self.nursery.start(self._run_idle)

            with trio.move_on_after(10):
                await self._stop.wait()

    async def _queue_run(self, task_status=trio.TASK_STATUS_IGNORED):
        task_status.started()
        async for typ, arg in self._queue:
            if self.conn is None:  # do it later
                continue
            if typ == "reg":
                await self._do_register(arg)
            elif typ == "wait":
                arg.set()
            else:
                raise RuntimeError("Unlnown message '%s' (%s)" % (typ, arg))

    async def wait_queue(self):
        """
        Registrations may be synchronous (they can be used as function decorators).

        To make sure that your code proceeds only after all previous registrations
        have been acknowledged by the server, call this coroutine.

        TODO: use this to catch registration errors.
        """
        ev = trio.Event()
        self._queue.put_nowait(("wait", ev))
        await ev.wait()

    ## client

    async def rpc(self, *args, **kwargs):
        """A remote procedure call returns one reply.
        """
        return await self.conn.rpc(*args, **kwargs)

    async def stream(self, *args, **kwargs):
        """A remote procedure call that returns more than one reply.
        """
        # yield from self.conn.poll(*args, **kwargs)  # py3.7
        async with aclosing(self.conn.stream(*args, **kwargs)) as p:
            async for r in p:
                yield r

    async def alert(self, *args, **kwargs):
        """An alert call ("pubsub") sends one request to multiple recipients. No reply.
        """
        await self.conn.alert(*args, **kwargs)

    async def poll(self, *args, **kwargs):
        """A poll call expects replies from more than one client.
        """
        # yield from self.conn.poll(*args, **kwargs)  # py3.7
        async with aclosing(self.conn.poll(*args, **kwargs)) as p:
            async for r in p:
                yield r

    async def poll_first(self, *args, **kwargs):
        """An alert call returns the first reply.
        """
        async with aclosing(self.conn.poll(*args, **kwargs)) as p:
            async for r in p:
                return r

    ## server

    async def _do_regs(self):
        for ep in self._endpoints.values():
            if ep.tag in self._reg_endpoints:
                continue
            await self._do_register(ep)

    async def _do_register(self, ep):
        try:
            self._reg_endpoints.add(ep.tag)
            if self.conn is not None:
                await self.conn.register(ep)
        except BaseException:
            self._reg_endpoints.remove(ep.tag)
            raise

    async def register(self, *a, **kw):
        """
        Async code to register a function with this broker.
        """
        ep = RPCservice(*a, **kw)
        if ep.tag in self._endpoints:
            raise RuntimeError("aleady registered")
        self._endpoints[ep.tag] = ep
        await self._do_register(ep)
        return ep

    def on_rpc(self, fn=None, name=None, **kw):
        """\
            Decorator/sync function to register an RPC listener.

            Example::

                @broker.rpc(call_conv=CC_MSG)
                async def call_name(msg):
                    return {'result':'I got a message!'}

            The function may be sync, async, or a Trio task.

            See :class:`qbroker.rpc.RPCservice` for possible keyword arguments.
            """

        def _register(*a, _direct=False, **kw):
            ep = RPCservice(*a, **kw)
            if ep.tag in self._endpoints:
                raise RuntimeError("aleady registered")
            self._endpoints[ep.tag] = ep
            self._queue.put_nowait(("reg", ep))
            return ep if _direct else ep.fn

        if name is not None or callable(fn):
            # either used without parentheses or not as a decorator
            return _register(fn, name=name, _direct=True, **kw)

        if fn is not None:
            return partial(_register, name=fn, **kw)
        elif kw:
            return partial(_register, **kw)
        else:
            return _register

    async def unregister(self, ep):
        if isinstance(ep, str):
            ep = self._endpoints[ep]
        try:
            del self._endpoints[ep.tag]
        except KeyError:
            # multiple removals are benign
            pass
        else:
            if self.conn is not None:
                await self.conn.unregister(ep)

    def _alert_ping(self, msg):
        if isinstance(msg, Mapping):
            if msg.get('app', self.app) != self.app:
                return
            if msg.get('uuid', self.uuid) != self.uuid:
                return
        return dict(
            app=self.app,
            uuid=self.uuid,
            args=self.args,
        )

    def _reply_ping(self, msg):
        return dict(
            app=self.app,
            args=self.args,
            uuid=self.uuid,
            endpoints=list(self._endpoints.keys()),
        )

    async def _reply_debug(self, msg):
        return await self.debug.run(msg)

    def debug_env(self, **data):
        if self.debug is None:
            return
        if not data:
            return self.debug.env
        for k, v in data.items():
            if v is not None:
                self.debug.env[k] = v
            else:
                self.debug.env.pop(k, None)

    ## cleanup, less interesting (hopefully)

    def __del__(self):
        self._kill_conn(deleting=True)

    async def aclose(self):
        self._stop.set()
        self.nursery.cancel()

    def close(self):
        self._kill_conn()

    def _kill_conn(self, deleting=False):
        self.cfg = None
        c, self.conn = self.conn, None
        if c:  # pragma: no cover
            try:
                c._kill()
            except Exception:
                if not deleting:
                    logger.exception("closing connection")

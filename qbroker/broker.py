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
from traceback import print_exc
from . import CC_MSG,CC_DATA,CC_DICT
from .conn import Connection
from .msg import RequestMsg,PollMsg,AlertMsg
from .rpc import RPCservice
from .config import DEFAULT_CONFIG
from .util import uuidstr, combine_dict
from collections.abc import Mapping
from aioamqp.exceptions import ChannelClosed
from async_generator import asynccontextmanager
from functools import partial

import logging
logger = logging.getLogger(__name__)

class Broker:
    """The basic QBroker messenger."""
    config = None # configuration data, also serves as flag
                  # for preventing restart at closedown
    conn = None # AMQP receiver
    uuid = None # my UUID
    restarting = None
    args = ()
    debug = None

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
        self._idle = None # cancel scope of backgound while-disconnected task
        self.idle_proc = idle_proc

        self.uuid = uuidstr()

        if 'cfg' in cfg:
            cfg = cfg['cfg']
        if 'amqp' in cfg:
            cfg = cfg['amqp']
        self.cfg = combine_dict(cfg, DEFAULT_CONFIG)

        self.rpc_endpoints = {}
        self.alert_endpoints = {}
        self.rpc_calls= {}
        self.alert_calls= {}
        self._did_reg_rpc = set()
        self._did_reg_alert = set()

        self.codec = self.cfg.codec

        if self.cfg.handlers.debug:
            from .debug import Debugger
            self.debug = Debugger(self)

        if not self.hidden:
            self.register_alert(self._alert_ping, "qbroker.ping",call_conv=CC_DATA)
            self.register_rpc(self._reply_ping,"qbroker.ping")
            self.register_alert(self._alert_ping, "qbroker.app."+self.app, call_conv=CC_DATA)
            self.register_rpc(self._reply_ping,"qbroker.app."+self.app)
        if self.debug is not None:
            self.register_rpc(self._reply_debug, "qbroker.debug."+self.app, call_conv=CC_MSG)
            # uuid: done in conn setup

    async def __aenter__(self):
        await self.nursery.start(self._keep_connected)
        return self

    async def __aexit__(self, *tb):
        if self.conn is not None:
            with trio.open_cancel_scope(shield=True):
                try:
                    await self.conn.aclose()
                finally:
                    self.conn = None
                    self.nursery.cancel_scope.cancel()

    async def _run_idle(self,task_status=trio.TASK_STATUS_IGNORED):
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

    async def _keep_connected(self,task_status=trio.TASK_STATUS_IGNORED):
        """Task which keeps a connection going"""
        class TODOexception(Exception):
            pass
        while not self._stop.is_set():
            try:
                self._did_reg_rpc = set()
                self._did_reg_alert = set()

                async with Connection(self.cfg, self.uuid).connect(self) as conn:
                    self.conn = conn
                    self._connected.set()
                    if self._idle is not None:
                        self._idle.cancel()
                        self._idle = None
                    await self._do_regs()
                    task_status.started()
                    await conn.is_disconnected.wait()
            except TODOexception:
                self._connected.clear()
                logger.exception("Error. TODO Reconnecting after a while.")
            finally:
                self.conn = None

            if self._stop.is_set():
                break
            if self.idle_proc is not None:
                await self.nursery.start(self._run_idle)

            with trio.move_on_after(10):
                await self._stop.wait()

        self.nursery.cancel_scope.cancel()

    async def _queue_run(self, task_status=trio.TASK_STATUS_IGNORED):
        task_status.started()
        async for typ,arg in self._queue:
            if self.conn is None:
                continue
            if typ == "reg_rpc":
                await self._do_register_rpc(arg)
            elif typ == "reg_alert":
                await self._do_register_alert(arg)
            elif typ == "unreg_rpc":
                await self._do_unregister_rpc(arg)
            elif typ == "unreg_alert":
                await self._do_unregister_alert(arg)
            elif typ == "wait":
                arg.set()
            else:
                raise RuntimeError("Unlnown message '%s' (%s)" % (typ, arg))

    async def wait_queue(self):
        ev = trio.Event()
        self._queue_event("wait",ev)
        await ev.wait()

    ## client

    async def rpc(self, *args, **kwargs):
        return await self.conn.rpc(*args, **kwargs)

    async def poll_one(self, *args, **kwargs):
        return await self.conn.poll_one(*args, **kwargs)

    async def alert(self, *args, **kwargs):
        await self.conn.alert(*args, **kwargs)

    async def poll(self, *args, **kwargs):
        # yield from self.conn.poll(*args, **kwargs)  # py3.7
        async with aclosing(self.conn.poll(*args, **kwargs)) as p:
            for r in p:
                yield r

    ## server

    async def _do_regs(self):
        for ep in self.rpc_endpoints.values():
            await self._do_register_rpc(ep)
        for ep in self.alert_endpoints.values():
            await self._do_register_alert(ep)

    async def _do_register_rpc(self, ep):
        if ep.name not in self._did_reg_rpc:
            try:
                self._did_reg_rpc.add(ep.name)
                await self.conn.register_rpc(ep)
            except BaseException:
                self._did_reg_rpc.remove(ep.name)
                raise

    async def _do_register_alert(self, ep):
        if ep.name not in self._did_reg_alert:
            try:
                self._did_reg_alert.add(ep.name)
                await self.conn.register_alert(ep)
            except BaseException:
                self._did_reg_alert.remove(ep.name)
                raise

    async def _do_unregister_rpc(self, ep):
        try:
            self._did_reg_rpc.remove(ep.name)
        except KeyError:
            pass
        else:
            await self.conn.unregister_rpc(ep)

    async def _do_unregister_alert(self, ep):
        try:
            self._did_reg_alert.remove(ep.name)
        except KeyError:
            pass
        else:
            await self.conn.unregister_alert(ep)

    def register_rpc(self, *a, _alert=False, call_conv=CC_MSG, durable=None, ttl=None):
        """
        Code to register a function with this broker.
        """
        fn = RPCservice(*a, call_conv=call_conv, durable=durable, ttl=ttl)
        if fn.is_alert is None:
            fn.is_alert = _alert
        elif fn.is_alert != _alert:
            raise RuntimeError("You can' change an RPC to an alert (or vice versa)")

        if _alert:
            epl = self.alert_endpoints
            q = 'reg_alert'
        else:
            epl = self.rpc_endpoints
            q = 'reg_rpc'
        if fn.name in epl:
            raise RuntimeError("'%s' is already registered" % (fn.name,))
        epl[fn.name] = fn
        self._queue_event(q,fn)

    def on_rpc(self, *a, **kw):
        """\
            Decorator to register an RPC listener.

            Example::
                
                @broker.rpc(call_conv=CC_MSG)
                async def call_name(msg):
                    return {'result':'I got a message!'}

            The function may be sync, async, or a Trio task.
            """

        if len(a) > 1:
            raise RuntimeError("too many arguments")
        if len(a) == 1 and not isinstance(a[0], str):
            # called without parentheses
            self.register_rpc(a[0], **kw)
            return a[0]

        if len(a):
            if not isinstance(a[0], str):
                raise RuntimeError("Call with a function name!")
            return partial(self.register_rpc,name=a[0],**kw)
        else:
            return partial(self.register_rpc,**kw)

    def register_alert(self, *a, **kw):
        """Register an alert listener.
           See meth:`rpc` for calling conventions."""
        return self.register_rpc(*a, _alert=True, **kw)

    def on_alert(self, *a, **kw):
        """Decorator to register an alert listener.
           See register_rpc for calling conventions."""
        return self.rpc(*a, _alert=True, **kw)

    def unregister_rpc(self, fn, _alert=False):
        if not isinstance(fn,str):
            if hasattr(fn,'name'):
                fn = fn.name
            else:
                fn = fn.__module__+'.'+fn.__name__
        if _alert:
            epl = self.alert_endpoints
        else:
            epl = self.rpc_endpoints

        fn = epl.pop(fn)
        if fn.is_alert != _alert:
            raise RuntimeError("register/unregister alert: %s/%s" % (fn.is_alert,_alert))
        if _alert:
            self._queue_event("unreg_rpc",fn)
        else:
            self._queue_event("unreg_alert",fn)

    def unregister_alert(self, fn):
        return self.unregister_rpc(fn, _alert=True)

    def _queue_event(self, name, arg):
        self._queue.put_nowait((name, arg))

    def _alert_ping(self,msg):
        if isinstance(msg,Mapping):
            if msg.get('app',self.app) != self.app:
                return
            if msg.get('uuid',self.uuid) != self.uuid:
                return
        return dict(
            app=self.app,
            uuid=self.uuid,
            args=self.args,
            )

    def _reply_ping(self,msg):
        return dict(
            app=self.app,
            args=self.args,
            uuid=self.uuid,
            rpc=list(self.rpc_endpoints.keys()),
            alert=list(self.alert_endpoints.keys()),
            rpc_calls=list(self.rpc_calls.keys()),
            alert_calls=list(self.alert_calls.keys()),
            )
        
    async def _reply_debug(self,msg):
        return await self.debug.run(msg)

    def debug_env(self, **data):
        if self.debug is None:
            return
        if not data:
            return self.debug.env
        for k,v in data.items():
            if v is not None:
                self.debug.env[k] = v
            else:
                self.debug.env.pop(k,None)
        
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
        c,self.conn = self.conn,None
        if c: # pragma: no cover
            try:
                c._kill()
            except Exception:
                if not deleting:
                    logger.exception("closing connection")


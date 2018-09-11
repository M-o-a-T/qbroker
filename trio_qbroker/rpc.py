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
import inspect
from collections.abc import Mapping
from contextlib import suppress
from trio_amqp.exceptions import AmqpClosedConnection

from . import CC_MSG, CC_DICT, CC_DATA, CC_TASK
from .util import uuidstr


async def coro_wrapper(proc, *a, **kw):
    """\
        This code is responsible for turning whatever callable you pass in
        into a "yield from"-style coroutine.
        """
    proc = proc(*a, **kw)
    if inspect.isawaitable(proc):
        proc = await proc
    return proc


class RPCservice(object):
    """\
        This object wraps one specific RPC service.

        Arguments:
            fn:
                The function to call when a packet arrives.
                The function may be synchronous or asynchronous.

            name:
                The name to register the function as. If not set,
                auto-generate from the function's module and name,
                replacing underlines by dots.

            exchange:
                The exchange to attach to. Default: 'rpc'

            call_conv:
                Calling convention for the function. Possible values:
                    CC_MSG:
                        The message instance itself is passed.

                    CC_DATA:
                        The data element of the message is passed.

                    CC_DICT:
                        The message must be a dictionary. The function is
                        called with the message's elements.

                    CC_TASK:
                        Async functions must accept a ``task_status``
                        argument as per Trio conventions. The sole
                        parameter is the message. The function must call
                        either ``await msg.ack()`` or ``await
                        msg_reject()`` as soon as possible, and may call
                        ``await msg.reply(…)`` arbitrarily often.

                        Sync functions are started in a separate thread.
                        The sole parameter is the message. The function
                        **must** call either ``msg.ack()`` or ``msg_reject()``
                        as soon as possible, and *may* call ``msg.reply(…)``
                        arbitrarily often.

                Code which does not use ``CC_TASK`` is expected to
                terminate reasonably quickly. Any result (other than
                ``None``) is packaged as a reply and transmitted to the
                called.

            durable:
                Name of a persistent message queue to create/use. If True,
                the name is constant but auto-generated. If False, a
                non-persistent queue will be created.

            ttl:
                Time after which unprocessed messages are dead-lettered.

            multiple:
                Flag whether incoming message will be sent to all
                listeners. If False (the default), only one server will
                read the message.
        """

    _mode = None

    # for qbroker.conn.Connection.register() et al.
    channel = None
    queue = None

    def __new__(cls, fn, name=None, **kw):
        if isinstance(fn, RPCservice):
            return fn
        return object.__new__(cls)

    def __init__(
            self,
            fn,
            name=None,
            exchange=None,
            call_conv=CC_MSG,
            durable=None,
            ttl=None,
            multiple=False,
            debug=False
    ):
        if isinstance(fn, RPCservice):
            return
        if name is None:
            # name = (fn.__module__.strip('_')+'.'+fn.__name__.strip('_')).
            # .replace('_','.').replace('..','.')
            name = fn.__name__.strip('_').replace('_', '.').replace('..', '.')
        self.fn = fn
        self.name = name
        self.call_conv = call_conv
        self.durable = durable
        self.uuid = uuidstr()
        self.ttl = ttl
        self.multiple = multiple
        self.debug = debug
        if exchange is None:
            exchange = "alert" if self.multiple else "rpc"
        self.exchange = exchange

    @property
    def tag(self):
        return "%s.%s" % (self.type, self.name)

    @property
    def type(self):
        if self.multiple:
            return "alert"
        else:
            return "rpc"

    async def _run(self, fn, msg, task_status=trio.TASK_STATUS_IGNORED):
        task_status.started()
        try:
            res = await fn(msg)
        except Exception as exc:
            await msg.error(exc, _exit=self.debug)
        else:
            if res is not None:
                await msg.reply(res)
        finally:
            with trio.open_cancel_scope(shield=True, deadline=trio.current_time() + 1):
                with suppress(AmqpClosedConnection):
                    await msg.aclose()

    async def run(self, msg):
        if self.call_conv == CC_DICT:
            a = ()
            k = msg.data
            if not isinstance(k, Mapping):
                assert k is None, k
                k = {}
        elif self.call_conv == CC_DATA:
            a = (msg.data,)
            k = {}
        else:
            a = (msg,)
            k = {}

        if self.call_conv == CC_TASK:
            await msg.conn.nursery.start(self._run, self.fn, msg)
        else:
            try:
                res = await coro_wrapper(self.fn, *a, **k)
                if res is not None:
                    await msg.reply(res)
            except Exception as exc:
                await msg.error(exc, _exit=self.debug)
            finally:
                with trio.open_cancel_scope(shield=True, deadline=trio.current_time() + 1):
                    with suppress(AmqpClosedConnection):
                        await msg.aclose()

    def __str__(self):
        if self.is_alert:
            n = "ALERT"
        elif self.is_alert is not None:
            n = "RPC"
        else:
            n = '?'
        return "‹%s %s %s %s›" % (n, self.name, self.call_conv, self.fn)

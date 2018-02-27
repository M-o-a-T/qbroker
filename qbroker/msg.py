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
from time import time

from . import CC_MSG, CC_DICT, CC_DATA
from .util import uuidstr, _NOTGIVEN
#from aioamqp.properties import Properties
from .util import attrdict
Properties = attrdict
from .codec import get_codec, DEFAULT
from .codec.registry import BaseCodec, register_obj

import logging
logger = logging.getLogger(__name__)

obj_codec = BaseCodec()
obj_codec.code_lists = 2

__all__ = ['RequestCancelledError', 'MsgError', 'RequestMsg', 'ResponseMsg', 'AlertMsg', 'PollMsg']

_types = {}
_fmap = {}


def fmap(s):
    r = _fmap.get(s, _NOTGIVEN)
    if r is _NOTGIVEN:
        _fmap[s] = r = s.replace('-', '_')
    return r


class RequestCancelledError(Exception):
    def __init__(self, rpc):
        self.rpc = rpc

    def __repr__(self):
        return '<%s %s>' % (self.__class__.__name__, self.rpc)


class FieldCollect(type):
    """\
        A metaclass which coalesces "fields" class attributes from the base
        classes into one coherent set
        """

    def __new__(meta, name, bases, dct):
        # Grab all known field names
        s = set()
        for b in bases:
            s.update(getattr(b, 'fields', ()))
        b = dct.get('fields', "")
        if b:
            if isinstance(b, str):
                b = b.split(" ")
            assert not s.intersection(b), (s, b)
            s.update(b)
        dct['fields'] = s

        res = super(FieldCollect, meta).__new__(meta, name, bases, dct)
        t = dct.get('type', None)
        if t is not None:
            _types[t] = res
        return res


class _MsgPart(object, metaclass=FieldCollect):
    """\
        A message part.
        """

    def dump(self):
        """Convert myself to a dict"""
        obj = {}
        for f in self.fields:
            try:
                obj[f] = getattr(self, fmap(f))
            except AttributeError:
                pass
        return obj

    def _load(self, props):
        """Load myself from a proplist"""
        if props.headers is None:
            return
        for f in self.fields:
            v = props.headers.get(f, _NOTGIVEN)
            if v is not _NOTGIVEN:
                setattr(self, fmap(f), v)

    def __eq__(self, other):
        for f in "type version data error".split():  # self.fields:
            a = getattr(self, f, _NOTGIVEN)
            b = getattr(other, f, _NOTGIVEN)
            if a == b:
                continue
            return False  # pragma: no cover
        return True


class MsgError(RuntimeError, _MsgPart):
    """Proxy for a remote error"""
    fields = "status id part message cls"
    exc = None

    def __init__(self, data=None):
        if data is not None:
            for f, v in data.items():
                setattr(self, fmap(f), v)

    def dump(self):
        """Convert myself to a dict"""
        if isinstance(self.err, bytes):
            return self.err
        return super().dump()

    @property
    def failed(self):
        """Is this message really an error?"""
        if self.status in ('ok', 'warn'):  # pragma: no cover ## XXX TODO
            return False
        if self.status in ('error', 'fail'):
            return True
        raise RuntimeError("Unknown error status: " + str(self.status))  # pragma: no cover

    def _load(self, props):
        """Load myself from a proplist"""
        super()._load(props)

        v = props.headers.get('err', _NOTGIVEN)
        if v is not _NOTGIVEN:
            exc = obj_codec.decode(v)
            self.exc = exc
            self.cls = exc.__class__.__name__
            if getattr(self, 'message', None) is None:
                self.message = str(exc)

    @classmethod
    def build(cls, exc, eid):
        obj = cls()
        obj.status = "error"
        obj.eid = eid
        obj.cls = exc.__class__.__name__
        obj.message = str(exc)
        obj.exc = exc
        return obj

    def __repr__(self):
        if self.exc is not None:
            return repr(self.exc)
        return "%s(%s)" % (self.cls, repr(self.message))

    def __str__(self):
        if self.exc is not None:
            return str(self.exc)
        return self.message

    def __hash__(self):
        return id(self)


@register_obj
class _MsgError(object):
    cls = MsgError
    clsname = "m_err"

    map = {'s': 'status', 'i': 'id', 'm': 'message', 'c': 'cls', 'e': 'exc'}

    @staticmethod
    def encode(obj):
        res = {}
        for a, b in _MsgError.map.items():
            v = getattr(obj, b, None)
            if v is not None:
                res[a] = v
        return res

    @staticmethod
    def decode(**kv):
        res = MsgError()
        for a, b in _MsgError.map.items():
            v = kv.get(a, None)
            if v is not None:
                setattr(res, b, v)
        return res


class BaseMsg(_MsgPart):
    version = 1
    debug = False
    fields = "type version debug message-id"
    _timer = None

    data = None
    error = None
    reply_to = None
    debug = None

    def __init__(self, data=None, debug=False):
        self.data = data
        self.message_id = uuidstr()
        self.debug = debug

    def __repr__(self):  # pragma: no cover
        return "%s._load(%s)" % (self.__class__.__name__, repr(self.__dict__))

    def dump(self, conn, codec):
        props = Properties()
        obj = super().dump()
        for f in 'type message-id reply-to correlation-id'.split(' '):
            m = obj.pop(f, None)
            if m is not None:
                setattr(props, fmap(f), m)

        data = self.data
        if codec is get_codec(DEFAULT):
            if isinstance(data, str) and data != "":
                codec = get_codec("text/plain")
            elif isinstance(data, bytes) and data != b"":
                codec = get_codec("application/binary")

        props.timestamp = int(time())
        props.user_id = conn.cfg.server['login']
        props.content_type = codec.CODEC
        props.app_id = conn.uuid
        # props.delivery_mode = 2
        if self.error is not None:
            obj['error'] = obj_codec.encode(self.error)
        if obj:
            props.headers = obj

        return codec.encode(data), props

    def set_error(self, *a, **k):
        self.error = MsgError.build(*a, **k)

    @staticmethod
    def load(data, env, props, type=None, **kwargs):
        t = type or props.type
        if t is None:
            t = 'alert'  # message from non-qbroker
        res = _types[t]._load(data, props)

        for f in 'type message-id reply-to user-id timestamp content-type app-id correlation-id'.split(
                ' '):
            ff = fmap(f)
            m = getattr(props, ff, _NOTGIVEN)
            if m is not _NOTGIVEN:
                setattr(res, ff, m)
        if getattr(res, 'routing_key', None) is None:
            res.routing_key = env.routing_key
        res.delivery_tag = env.delivery_tag

        for k, v in kwargs.items():
            setattr(res, k, v)
        return res

    @classmethod
    def _load(cls, msg, props):
        obj = cls()
        super(BaseMsg, obj)._load(props)
        obj.data = msg
        if props.headers is not None and 'error' in props.headers:
            obj.error = obj_codec.decode(props.headers['error'])
        return obj

    @property
    def failed(self):
        return self.error is not None and self.error.failed

    def raise_if_error(self):
        if self.error and self.error.failed:
            if self.error.exc is not None:  # error passed the codec
                raise self.error.exc
            raise self.error


class AlertMsg(BaseMsg):
    """An alert which is not replied to"""
    fields = "routing-key"
    type = "alert"

    def __init__(self, name=None, routing_key=None, uuid=None, **kwargs):
        # The reply-queue UUID is ignored on purpose.
        # since an alert doesn't *get* replies we don't set the field,
        # bit the caller may set it anyway.
        super().__init__(**kwargs)
        self.routing_key = name
        self.dest = routing_key or name


class _RequestMsg(AlertMsg):
    """A request packet. The remaining fields are data elements."""
    fields = "reply-to"

    def __init__(self, uuid=None, **kwargs):
        super().__init__(**kwargs)
        self.reply_to = uuid
        self._q = trio.Queue(99)  # TODO get from config

    async def read(self):
        msg = await self._q.get()
        if msg is None:
            raise RequestCancelledError(self)
        return msg

    async def recv_reply(self, reply):
        """Client side: Incoming reply."""
        self._q.put_nowait(reply)

    async def cancel(self):
        self._q.put_nowait(None)


class ServerMsg(BaseMsg):
    type = "server"
    fields = "routing-key"

    _acked = None
    _replied = None

    def make_response(self, conn=None):
        if conn is None:
            conn = self.conn
        return ResponseMsg(conn, self)

    async def reply(self, data, codec=None):
        if self._acked is False:
            raise RuntimeError("I already rejected this message")
        reply = self.make_response()
        reply.data = data
        await reply._send()
        self._replied = True

    async def error(self, exc, _exit=None):
        """Return an error to the caller.
        
        If _exit is set, the error has been raised by your handler and will
        only be returned if debugging is active and wanted. Otherwise it
        will propagate within the server.
        """
        if _exit is False:
            if not self.conn.cfg.handlers['debug']:
                raise
            if not self.debug:
                logger.exception("Problem with %s: %s", self.routing_key, type(exc), exc_info=exc)
                if self._acked is None:
                    await self.reject()
                return
        reply = self.make_response()
        logger.debug(
            "Problem with %s: forwarding %s to client", self.routing_key, type(exc), exc_info=exc
        )
        reply.set_error(exc, self.routing_key)
        await reply._send()
        if self._replied is None:
            self._replied = False

    async def ack(self):
        """Acknowledge the message.

        If you call :meth:`ack` manually, you're responsible for replying, either
        by calling :meth:`reply` or by returning a non:obj:`None` value
        from your handler.
        """
        if self._acked is not None:
            raise RuntimeError("I already processed this message")
        logger.debug("Accepting %s", self.delivery_tag)
        await self.channel.basic_client_ack(self.delivery_tag)
        self._acked = True

    async def reject(self):
        """Reject the message.

        You cannot both reply to a message and reject it.
        """
        if self._acked is not None:
            raise RuntimeError("I already processed this message")
        elif self._replied:
            raise RuntimeError("I already replied w/o error")
        logger.debug("Rejecting %s", self.delivery_tag)
        await self.channel.basic_reject(self.delivery_tag)
        self._acked = False

    async def aclose(self, data=None):
        """Close the interaction. Do not call from your handler!

        This method will send a reply with the value your handler returns.
        To suppress that, return :obj:`None`.
        """
        acked = self._acked
        if self._acked is None:
            await self.ack()
        if self.reply_to is None or acked is False:
            return
        if data is None:
            # Don't send a reply unless it's an RPC and nothing at all has
            # been sent so far.
            if self._replied is not None:
                return
            if acked is not None:
                return
            if self.type != "rpc":
                return
        await self.reply(data)


class RequestMsg(_RequestMsg):
    _timer = "rpc"  # lookup key for the timeout
    type = "rpc"


class PollMsg(_RequestMsg):
    """An alert which requests replies"""
    _timer = "poll"  # lookup key for the timeout
    type = "alert"


class StreamMsg(_RequestMsg):
    """An RPC call which requests multiple replies"""
    _timer = "stream"  # lookup key for the timeout
    type = "rpc"


class ResponseMsg(BaseMsg):
    type = "reply"
    fields = "correlation-id"

    def __init__(self, conn=None, request=None, codec=None):
        self.conn = conn
        if codec is None:
            codec = get_codec(DEFAULT)
        elif isinstance(codec, str):
            codec = get_codec(codec)
        self.codec = codec
        if request is not None:
            self.correlation_id = request.message_id
            self.reply_to = request.reply_to
            self.channel = request.reply_channel
            self.exchange = request.reply_exchange
        super().__init__()

    async def _send(self):
        reply, props = self.dump(self.conn, codec=self.codec)
        logger.debug("Reply %s to %s: %s", self.reply_to, self.exchange, reply)
        await self.channel.publish(reply, self.exchange, self.reply_to, properties=props)

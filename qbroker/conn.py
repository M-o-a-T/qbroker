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
import trio_amqp
import weakref
import math  # inf
from collections.abc import Mapping
from contextlib import contextmanager
from async_generator import asynccontextmanager

from . import CC_DICT,CC_DATA,CC_MSG
from .msg import _RequestMsg,PollMsg,RequestMsg,BaseMsg
from .codec import get_codec
from .util import import_string

import logging
logger = logging.getLogger(__name__)

class DeadLettered(RuntimeError):
    def __init__(self, exchange,routing_key):
        self.exchange = exchange
        self.routing_key = routing_key

    def __str__(self):
        return "Dead: queue=%s route=%s" % (self.exchange, self.routing_key)

    def __reduce__(self):
        return (self.__class__,(self.exchange,self.routing_key),{})

class _ch(object):
    """Helper object"""
    channel = None
    exchange = None
    queue = None

class Connection(object):
    amqp = None # connection
    alert_bc = False
    is_disconnected = None

    def __init__(self, cfg, uuid):
        self.cfg = cfg
        self.uuid = uuid

        codec = cfg.codec
        if isinstance(codec,str):
            codec = get_codec(codec)
        self.codec = codec

        cfg.timeout.connect = float(cfg.timeout.connect)
        server = cfg.server
        if isinstance(server.ssl,str):
            server.ssl = server.ssl.lower() == 'true'
        try:
            server.port = int(cfg.port)
        except AttributeError:
            pass

        lim = self.cfg.limits
        self.max_rpc = lim.rpc.queue
        self.max_alert = lim.alert.queue
        self.rpc_workers = lim.rpc.workers
        self.alert_workers = lim.alert.workers

        self.rpcs = {} # RPC listeners
        self.alerts = {} # Alert+Poll listeners
        self.replies = {} # RPC requests

        self.q_rpc = trio.Queue(self.max_rpc)
        self.q_alert = trio.Queue(self.max_alert)

    @property
    def nursery(self):
        return self.amqp.nursery

    @asynccontextmanager
    async def connect(self, broker):
        self.broker = weakref.ref(broker)
        self.is_disconnected = trio.Event()

        timeout = self.cfg.timeout.connect
        if not timeout:
            timeout = math.inf
        with trio.open_cancel_scope() as scope:
            scope.timeout = timeout
            async with trio_amqp.connect_amqp(**self.cfg.server) as amqp:
                scope.timeout = math.inf

                self.amqp = amqp
                nursery = self.nursery
                try:
                    for _ in range(self.rpc_workers):
                        await nursery.start(self._work_rpc)
                    for _ in range(self.alert_workers):
                        await nursery.start(self._work_alert)

                    await self.setup_channels()

                    yield self
                finally:
                    self.is_disconnected.set()
                    nursery.cancel_scope.cancel()
                    self.amqp = None

    async def _work_rpc(self, task_status=trio.TASK_STATUS_IGNORED):
        task_status.started()
        async for msg in self.q_rpc:
            await self._on_rpc(*msg)

    async def _work_alert(self, task_status=trio.TASK_STATUS_IGNORED):
        task_status.started()
        async for msg in self.q_alert:
            await self._on_alert(*msg)

    async def _setup_one(self,name,typ,callback=None, q=None, route_key=None, exclusive=None, alt=None):
        """\
            Register a channel. Internal helper.
            """
        b = self.broker()
        cfg = b.cfg
        ch = _ch()
        setattr(self,'_ch_'+name,ch)
        logger.debug("setup RPC for %s",name)
        ch.channel = await self.amqp.channel()
        ch.exchange = cfg['exchanges'][name]
        logger.debug("Chan %s: exchange %s", ch.channel,cfg['exchanges'][name])
        if exclusive is None:
            exclusive = (q is not None)
        d = {}
        if alt is not None:
            d["alternate-exchange"] = cfg['exchanges'][alt]
        try:
            await ch.channel.exchange_declare(cfg['exchanges'][name], typ, auto_delete=False, durable=True, passive=False, arguments=d)
        except aioamqp.exceptions.ChannelClosed as exc:
            if exc.code != 406: # PRECONDITION_FAILED
                raise
            ch.channel = await self.amqp.channel()
            await ch.channel.exchange_declare(cfg['exchanges'][name], typ, passive=True)
            logger.warning("passive: %s",repr(exc))

        if q is not None:
            assert callback is not None
            d = {}
            ttl = cfg['ttl'].get(name,0)
            if ttl:
                d["x-dead-letter-exchange"] = cfg['queues']['dead']
                d["x-message-ttl"] = int(1000*cfg['ttl'][name])
            ch.queue = await ch.channel.queue_declare(cfg['queues'][name]+q, auto_delete=True, passive=False, exclusive=exclusive, arguments=d)
            await ch.channel.basic_qos(prefetch_count=1,prefetch_size=0,connection_global=False)
            logger.debug("Chan %s: read %s", ch.channel,cfg['queues'][name]+q)
            await ch.channel.basic_consume(queue_name=cfg['queues'][name]+q, callback=callback)
            if route_key is not None:
                logger.debug("Chan %s: bind %s %s %s", ch.channel,cfg['exchanges'][name], route_key, ch.queue['queue'])
                await ch.channel.queue_bind(ch.queue['queue'], cfg['exchanges'][name], routing_key=route_key)
                pass
        else:
            assert callback is None

        logger.debug("setup RPC for %s done",name)

    async def setup_channels(self):
        """Configure global channels"""
        b = self.broker()
        # See doc/qbroker.rst
        await self._setup_one("alert",'topic', self._on_alert_in, b.uuid)
        await self._setup_one("rpc",'topic', self._on_rpc_in, b.uuid, 'qbroker.uuid.'+b.uuid, alt="dead")
        await self._setup_one("reply",'direct', self._on_reply, b.uuid, b.uuid)
        if b.cfg.handlers['dead']:
            await self._setup_one("dead",'topic', self._on_dead_rpc, "rpc", exclusive=False, route_key='#')

    async def _on_dead_rpc(self, channel,body,envelope,properties):
        try:
            codec = get_codec(properties.content_type)
            msg = codec.decode(body)
            msg = BaseMsg.load(msg,envelope,properties)
            reply = msg.make_response()
            reply_to = getattr(msg, 'reply_to',None)
            exc = envelope.exchange_name
            if exc.startswith("dead"):
                exc = properties.headers['x-death'][0]['exchange']
            exc = DeadLettered(exc, envelope.routing_key)
            reply.set_error(exc, envelope.routing_key, "reply")
            reply,props = reply.dump(self)
            reply = self.codec.encode(reply)
            await self._ch_reply.channel.publish(reply, self._ch_reply.exchange, reply_to, properties=props)
        finally:
            await channel.basic_client_ack(envelope.delivery_tag)
        
    async def _on_rpc_in(self, *params):
        self.q_rpc.put_nowait(params)

    async def _on_alert_in(self, *params):
        self.q_alert.put_nowait(params)

    async def _on_alert(self, channel,body,envelope,properties):
        logger.debug("read alert %s on %s: %s",envelope.delivery_tag, envelope.routing_key,body)
        try:
            codec = get_codec(properties.content_type)
            msg = codec.decode(body)
            msg = BaseMsg.load(msg,envelope,properties)
            try:
                rpc = self.alerts[msg.routing_key]
            except KeyError:
                n = msg.routing_key
                while True:
                    i = n.rfind('.')
                    if i < 1:
                        rpc = self.alerts.get('#',None)
                        if rpc is not None:
                            break
                        raise
                    n = n[:i]
                    rpc = self.alerts.get(n+'.#',None)
                    if rpc is not None:
                        break
            msg.codec = codec
            if rpc.call_conv == CC_DICT:
                a=(); k=msg.data
                if not isinstance(k,Mapping):
                    assert k == ''
                    k = {}
            elif rpc.call_conv == CC_DATA:
                a=(msg.data,); k={}
            else:
                a=(msg,); k={}

            reply_to = getattr(msg, 'reply_to',None)
            if reply_to:
                try:
                    data = await rpc.run(*a,**k)
                except Exception as exc:
                    reply = msg.make_response()
                    logger.exception("error on alert %s: %s", envelope.delivery_tag, body)
                    reply.set_error(exc, rpc.name,"reply")
                else:
                    if data is None:
                        return
                    reply = msg.make_response()
                    reply.data = data

                codec = msg.codec
                if codec is None:
                    codec = self.codec
                elif isinstance(codec,str):
                    codec = get_codec(codec)
                reply,props = reply.dump(self, codec=codec)
                reply = codec.encode(reply)
                await self._ch_reply.channel.publish(reply, self._ch_reply.exchange, reply_to, properties=props)
            else:
                try:
                    await rpc.run(*a,**k)
                except Exception as exc:
                    logger.exception("error on alert %s: %s", envelope.delivery_tag, body)
                reply = None

        except Exception as exc:
            try:
                await self._ch_alert.channel.basic_reject(envelope.delivery_tag)
            except Exception as exc:
                logger.exception("problem with rpc reject %s: %s", envelope.delivery_tag, body)
            else:
                logger.exception("problem with rpc %s: %s", envelope.delivery_tag, body)
        else:
            logger.debug("acked alert %s: %s",envelope.delivery_tag, reply)
            await channel.basic_client_ack(envelope.delivery_tag)

    async def _on_rpc(self, channel,body,envelope,properties):
        logger.debug("read rpc %s on %s: %s",envelope.delivery_tag,envelope.routing_key,body)
        try:
            codec = get_codec(properties.content_type)
            msg = codec.decode(body)
            msg = BaseMsg.load(msg,envelope,properties)
            msg.codec = codec
            rpc = self.rpcs[msg.routing_key]
            reply = msg.make_response()
            try:
                if rpc.call_conv == CC_DICT:
                    a=(); k=msg.data or {}
                elif rpc.call_conv == CC_DATA:
                    a=(msg.data,); k={}
                else:
                    a=(msg,); k={}
                reply.data = await rpc.run(*a,**k)
            except Exception as exc:
                logger.exception("error on rpc %s: %s", envelope.delivery_tag, body)
                reply.set_error(exc, rpc.name,"reply")

            codec = msg.codec
            if codec is None:
                codec = self.codec
            elif isinstance(codec,str):
                codec = get_codec(codec)
            reply,props = reply.dump(self,codec=codec)
            await channel.publish(reply, self._ch_reply.exchange, msg.reply_to, properties=props)
        except Exception as exc:
            try:
                await channel.basic_reject(envelope.delivery_tag)
            except Exception as exc:
                logger.exception("problem with rpc reject %s: %s", envelope.delivery_tag, body)
            else:
                logger.exception("problem with rpc %s: %s", envelope.delivery_tag, body)
        else:
            try:
                await channel.basic_client_ack(envelope.delivery_tag)
            except Exception as exc:
                logger.exception("problem with rpc ack: %s", envelope.delivery_tag)
            else:
                logger.debug("acked rpc %s to %s: %s",envelope.delivery_tag, msg.correlation_id, reply)

    async def _on_reply(self, channel,body,envelope,properties):
        logger.debug("read reply %s for %s: %s",envelope.delivery_tag, properties.correlation_id, body)
        try:
            codec = get_codec(properties.content_type)
            msg = codec.decode(body)
            msg = BaseMsg.load(msg,envelope,properties)
            req = self.replies[msg.correlation_id]
            try:
                await req.recv_reply(msg)
            except Exception as exc: # pragma: no cover
                if not f.done():
                    f.set_exception(exc)
        except Exception as exc:
            await self._ch_reply.channel.basic_reject(envelope.delivery_tag)
            logger.exception("problem with message %s: %s", envelope.delivery_tag, body)
        else:
            logger.debug("ack reply %s",envelope.delivery_tag)
            await channel.basic_client_ack(envelope.delivery_tag)

    # client

    def _pack_rpc(self,name, data='', *, MsgClass=None, dest=None, uuid=None, codec=None, call_conv=CC_MSG):
        """Package data and metadata into one message object."""
        msg = RequestMsg(name, self, data=data, call_conv=call_conv)
        assert uuid is None or dest is None
        if dest is not None:
            dest = 'qbroker.app.'+dest
        elif uuid is not None:
            dest = 'qbroker.uuid.'+uuid

        msg.dest = dest
        msg.codec = codec
        return msg

    @contextmanager
    def _accept_reply(self, msg):
        """
        Context manager to cleanly register a receiver for a message ID
        """
        if msg.message_id in self.replies:
            raise RuntimeError("Message ID is already known (%s)" % msg.message_id)
        try:
            self.replies[msg.message_id] = msg
            yield msg
        finally:
            del self.replies[msg.message_id]
        

    async def rpc(self, *args, **kwargs):
        """Send an RPC request.

        Returns the response.
        """
        msg = self._pack_rpc(*args, MsgClass=RequestMsg, **kwargs)

        if msg.message_id in self.replies:
            raise RuntimeError("Message ID is already known (%s)" % msg.message_id)

        with self._accept_reply(msg):
            await self._send(msg)
            res = await msg.read()

        res.raise_if_error()
        return res.data

    async def poll_one(self, *args, **kwargs):
        """Broadcast an RPC request.

        Returns or raises the first response.
        """
        msg = self._pack_rpc(*args, MsgClass=PollMsg, **kwargs)

        if msg.message_id in self.replies:
            raise RuntimeError("Message ID is already known (%s)" % msg.message_id)

        with self._accept_reply(msg):
            await self._send(msg)
            res = await msg.read()

        res.raise_if_error()
        return res.data

    async def alert(self, *args, **kwargs):
        """Send a broadcast alert.

        There is no reply.
        """
        msg = self._pack_rpc(*args, MsgClass=AlertMsg, **kwargs)

        if msg.message_id in self.replies:
            raise RuntimeError("Message ID is already known (%s)" % msg.message_id)

        await self._send(msg)

    async def poll(self, *args, min_replies=0, max_replies=math.inf, result_conv=CC_DATA, max_delay=math.inf, **kwargs):
        """Send a broadcast that accepts multiple replies.

        This is an async iterator. If :param:`result_conv` is
        :const:`CC_MSG`, this call yields the raw message. Otherwise
        (default), it returns the data element / raises the incoming error.
        """
        msg = self._pack_rpc(*args, MsgClass=PollMsg, **kwargs)

        n = 0
        with trio.move_on_after(max_delay) as timeout:
            with self._accept_reply(msg):
                await seld._send(msg)
                while n < max_replies:
                    res = await msg.read()
                    n += 1
                    if result_conv != CC_MSG:
                        res.raise_if_error()
                        res = res.data
                    yield res
        if timeout.cancelled_caught() and n < min_replies:
            raise trio.TooSlowError

    async def _send(self, msg, dest=None, codec=None):
        """Send a request-type message (i.e. something that's not a reply).
        """
        assert isinstance(msg,_RequestMsg)
        cfg = self.cfg

        if dest is None:
            dest = msg.routing_key
        if codec is None:
            codec = self.codec

        data,props = msg.dump(self, codec=codec)
        logger.debug("Send %s to %s: %s", dest, cfg.exchanges[msg._exchange], data)
        await getattr(self,'_ch_'+msg._exchange).channel.publish(data, cfg.exchanges[msg._exchange], dest, properties=props)
        
    # Server

    async def register_rpc(self,rpc):
        assert rpc.name not in self.rpcs
        ch = self._ch_rpc
        cfg = self.cfg

        rpc.channel = await self.amqp.channel()
        d = {}
        if cfg.ttl.rpc or rpc.ttl:
            d["x-dead-letter-exchange"] = cfg.queues['dead']
            d["x-message-ttl"] = int(1000*(rpc.ttl if rpc.ttl else cfg.ttl.rpc))
        self.rpcs[rpc.name] = rpc
        try:
            rpc.queue = await rpc.channel.queue_declare(cfg.queues['rpc']+rpc.name, auto_delete=not rpc.durable, durable=rpc.durable, passive=False, arguments=d)
            logger.debug("Chan %s: bind %s %s %s", ch.channel,cfg.exchanges['rpc'], rpc.name, rpc.queue['queue'])
            await rpc.channel.queue_bind(rpc.queue['queue'], cfg.exchanges['rpc'], routing_key=rpc.name)

            await rpc.channel.basic_qos(prefetch_count=1,prefetch_size=0,connection_global=False)
            logger.debug("Chan %s: read %s", rpc.channel,rpc.queue['queue'])
            await rpc.channel.basic_consume(queue_name=rpc.queue['queue'], callback=self._on_rpc_in, consumer_tag=rpc.uuid)
        except BaseException:
            logger.error("RPC failed on %s: %s", self,rpc)
            del self.rpcs[rpc.name]
            raise

    async def unregister_rpc(self,rpc):
        ch = self._ch_rpc
        cfg = self.cfg
        if isinstance(rpc,str):
            rpc = self.rpcs.pop(rpc)
        else:
            del self.rpcs[rpc.name]
        assert rpc.queue is not None
        logger.debug("Chan %s: unbind %s %s %s", ch.channel,cfg['exchanges']['rpc'], rpc.name, rpc.queue['queue'])
        await rpc.channel.queue_unbind(rpc.queue['queue'], cfg['exchanges']['rpc'], routing_key=rpc.name)
        logger.debug("Chan %s: noread %s", rpc.channel,rpc.queue['queue'])
        await rpc.channel.basic_cancel(consumer_tag=rpc.uuid)

    async def register_alert(self,rpc):
        assert rpc.name not in self.alerts
        dn = n = rpc.name
        if rpc.name.endswith('.#'):
            n = n[:-2]
            dn = n+'._all_'
        if len(n) > 1 and '#' in n:
            raise RuntimeError("I won't find that")

        self.alerts[rpc.name] = rpc
        if rpc.name.endswith('.#'):
            self.alert_bc = True
        ch = None

        try:
            if rpc.durable:
                if isinstance(rpc.durable,str):
                    dn = rpc.durable
                else:
                    dn = self.broker().config['amqp']['queues']['msg']+dn
                ch = await self.amqp.channel()
                d = {}
                if rpc.ttl is not None:
                    d["x-message-ttl"] = int(1000*rpc.ttl)
                q = await ch.queue_declare(dn, auto_delete=False, passive=False, exclusive=False, durable=True, arguments=d)
                await ch.basic_consume(queue_name=q['queue'], callback=self._on_alert_in)
            else:
                ch = self._ch_alert.channel
                q = self._ch_alert.queue
            await ch.queue_bind(q['queue'], self._ch_alert.exchange, routing_key=rpc.name)
        except BaseException:
            del self.alerts[rpc.name]
            if ch is not None:
                await ch.close()
            raise

        rpc.ch = ch
        rpc.q = q

    async def unregister_alert(self,rpc):
        if isinstance(rpc,str):
            rpc = self.alerts.pop(rpc)
        else:
            del self.alerts[rpc.name]
        ch = self._ch_alert
        if rpc.durable:
            await rpc.ch.close()
        else:
            await rpc.ch.queue_unbind(rpc.q['queue'], ch.exchange, routing_key=rpc.name)

    async def aclose(self):
        a,self.amqp = self.amqp,None
        if a is not None:
            logger.debug("Disconnecting %s",self)
            try:
                await a.aclose()
            except BaseException:
                a.close()
                raise
            logger.debug("Disconnected %s",self)

    def close(self):
        self.amqp = None
        a,self.amqp_transport = self.amqp_transport,None
        if a is not None:
            logger.debug("Killing %s",self)
            a.close()
            logger.debug("Killed %s",self)


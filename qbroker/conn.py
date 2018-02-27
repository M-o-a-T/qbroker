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
from trio_amqp.exceptions import AmqpClosedConnection
import weakref
import math  # inf
from contextlib import contextmanager, suppress
from async_generator import asynccontextmanager, aclosing

from . import CC_DICT,CC_DATA,CC_MSG
from .msg import _RequestMsg,PollMsg,RequestMsg,BaseMsg,AlertMsg,StreamMsg
from .codec import get_codec, DEFAULT
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
                except BaseException as exc:
                    logger.debug("Problem in connection", exc_info=exc)
                    raise
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

    async def _setup_one(self,name,exchange_mode, callback=None, q=None, route_key=None, exclusive=None, alt=None):
        """\
            Register "client" channels for sending requests / receiving replies.

            The other way, i.e. the "server" side, is set up in :meth:`register`.
            """
        b = self.broker()
        cfg = b.cfg
        ch = _ch()
        setattr(self,'_ch_'+name,ch)
        logger.debug("setup RPC for %s",name)
        ch.channel = await self.amqp.channel()
        ch.exchange = cfg['exchanges'][name]
        logger.debug("Chan %s: exchange %s", ch.channel, cfg.exchanges[name])
        if exclusive is None:
            exclusive = (q is not None)
        d = {}
        if alt is not None:
            d["alternate-exchange"] = cfg.exchanges[alt]
        try:
            await ch.channel.exchange_declare(cfg.exchanges[name], exchange_mode, auto_delete=False, durable=True, passive=False, arguments=d)
        except aioamqp.exceptions.ChannelClosed as exc:
            if exc.code != 406: # PRECONDITION_FAILED
                raise
            ch.channel = await self.amqp.channel()
            await ch.channel.exchange_declare(cfg.exchanges[name], exchange_mode, passive=True)
            logger.warning("passive: %s",repr(exc))

        if q is not None:
            assert callback is not None
            d = {}
            ttl = cfg['ttl'].get(name,0)
            if ttl:
                d["x-dead-letter-exchange"] = cfg.queues['dead']
                d["x-message-ttl"] = int(1000*cfg.ttl[name])
            ch.queue = await ch.channel.queue_declare(cfg.queues[name]+q, auto_delete=True, passive=False, exclusive=exclusive, arguments=d)
            await ch.channel.basic_qos(prefetch_count=1,prefetch_size=0,connection_global=False)
            logger.debug("Chan %s: read %s", ch.channel,cfg.queues[name]+q)
            await ch.channel.basic_consume(queue_name=cfg.queues[name]+q, callback=callback)
            if route_key is not None:
                logger.debug("Chan %s: bind %s %s %s", ch.channel,cfg.exchanges[name], route_key, ch.queue['queue'])
                await ch.channel.queue_bind(ch.queue['queue'], cfg.exchanges[name], routing_key=route_key)
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
        """
        This handler is responsible for receiving dead-lettered messages.
        It builds an error reply and sends it to the client, ensuring that
        the error is discovered instantly, instead of waiting for a timeout.
        """
        try:
            codec = get_codec(properties.content_type)
            msg = codec.decode(body)
            msg = BaseMsg.load(msg,envelope,properties, conn=self, type="server", reply_channel=self._ch_reply.channel, reply_exchange=self._ch_reply.exchange)
            reply = msg.make_response(self)
            reply_to = getattr(msg, 'reply_to',None)
            exc = envelope.exchange_name
            if exc.startswith("dead"):
                exc = properties.headers['x-death'][0]['exchange']
            exc = DeadLettered(exc, envelope.routing_key)
            if reply_to is None:
                # usually, this is no big deal: call debug(), not exception().
                logger.debug("Undeliverable one-way message", exc_info=exc)
                return
            reply.set_error(exc, envelope.routing_key)
            reply,props = reply.dump(self, codec=self.codec)
            logger.debug("DeadLetter %s to %s", envelope.routing_key, self._ch_reply.exchange)
            await self._ch_reply.channel.publish(reply, self._ch_reply.exchange, reply_to, properties=props)
        finally:
            with trio.open_cancel_scope(shield=True,deadline=trio.current_time()+1):
                await channel.basic_client_ack(envelope.delivery_tag)

    async def _on_rpc_in(self, *params):
        self.q_rpc.put_nowait(params)

    async def _on_alert_in(self, *params):
        self.q_alert.put_nowait(params)

    async def _on_alert(self, channel,body,envelope,properties):
        await self._dispatch("alert", channel,body,envelope,properties)

    async def _on_rpc(self, channel,body,envelope,properties):
        await self._dispatch("rpc", channel,body,envelope,properties)

    async def _dispatch(self, mode,channel,body,envelope,properties):

        try:
            routing_key = properties.headers['routing-key']
        except (KeyError,AttributeError):
            routing_key = envelope.routing_key
        if routing_key != envelope.routing_key:
            logger.debug("read %s %s on %s for %s: %s",mode,envelope.delivery_tag, envelope.routing_key, routing_key,body)
        else:
            logger.debug("read %s %s for %s: %s",mode,envelope.delivery_tag, routing_key,body)
        try:
            codec = get_codec(properties.content_type)
            msg = codec.decode(body)
            msg = BaseMsg.load(msg,envelope,properties, channel=channel, type='server', reply_channel=self._ch_reply.channel, reply_exchange=self._ch_reply.exchange, conn=self)

            n = mode+'.'+msg.routing_key
            try:
                rpc = self.rpcs[n]
            except KeyError:
                while True:
                    i = n.rfind('.')
                    if i < 1:
                        raise
                    n = n[:i]
                    rpc = self.rpcs.get(n+'.#',None)
                    if rpc is not None:
                        break
            await rpc.run(msg)

        except KeyError:
            logger.info("Unknown message %s %s on %s for %s: %s",mode,envelope.delivery_tag, envelope.routing_key, routing_key,body)
            await channel.basic_reject(envelope.delivery_tag)
            
        except BaseException:
            with trio.open_cancel_scope(shield=True):
                with suppress(AmqpClosedConnection):
                    await channel.basic_reject(envelope.delivery_tag)
            raise

    async def _on_reply(self, channel,body,envelope,properties):
        logger.debug("read reply %s for %s: %s",envelope.delivery_tag, properties.correlation_id, body)
        try:
            codec = get_codec(properties.content_type)
            msg = codec.decode(body)
            msg = BaseMsg.load(msg,envelope,properties, conn=self, channel=channel)
            req = self.replies[msg.correlation_id]
            await req.recv_reply(msg)
        except KeyError as exc:
            logger.warning("Undelivered reply %s: %s", msg.correlation_id, msg)
            await channel.basic_client_ack(envelope.delivery_tag)
        except Exception as exc:
            await self._ch_reply.channel.basic_reject(envelope.delivery_tag)
            logger.exception("problem with reply %s: %s", envelope.delivery_tag, body)
        else:
            logger.debug("ack reply %s",envelope.delivery_tag)
            await channel.basic_client_ack(envelope.delivery_tag)

    # client

    def _pack_rpc(self,name, data=None, *, MsgClass=None, exchange=None, dest=None, uuid=None, codec=None, **kwargs):
        """
        Package data and metadata into one message object.
        
        Arguments:
            name:
                the dispatch key to use for processing the message at the
                recipient
            data:
                the actual payload to transmit
            dest:
                The name of the server to deliver the message to.
                Mutually exclusive with ``uuid``.
            uuid:
                The UUID of the server to deliver the message to.
                Mutually exclusive with ``dest``.
            codec:
                The codec to be used to encode the message's body.
            exchange:
                The exchange to send the message to. The default is the
                exchange configured for ``rpc``  or ``alert``, depending on
                the message type.
            result_conv:
                Calling convention used for returning the reply or replies.

                CC_MSG:
                    The message is not modified (other than decoding the
                    payload) and includes all metadata. The caller is
                    responsible for recognizing errors.
                CC_DATA:
                    The decoded payload is returned. The caller will receive an
                    exception if the message contains an error.
                    This is the default.

        """

        if dest is not None:
            if uuid is not None:
                raise RuntimeError("You cannot specify both uuid and dest")
            dest = 'qbroker.app.'+dest
        elif uuid is not None:
            dest = 'qbroker.uuid.'+uuid
        else:
            dest = name
        msg = MsgClass(name=name, data=data, routing_key=dest, uuid=self.uuid, **kwargs)

        if exchange is None:
            exchange = self.cfg.exchanges[msg.type]
        if codec is None:
            codec = DEFAULT
        if isinstance(codec, str):
            codec = get_codec(codec)
        msg._c_exchange = exchange
        msg._c_codec = codec
        return msg

    @contextmanager
    def _accept_reply(self, msg):
        """
        Context manager to cleanly register a receiver for a message ID
        """
        if msg.message_id in self.replies:  # pragma: no cover
            raise RuntimeError("Message ID is already known (%s)" % msg.message_id)
        try:
            self.replies[msg.message_id] = msg
            yield msg
        finally:
            del self.replies[msg.message_id]
        

    async def rpc(self, *args, result_conv=CC_DATA, **kwargs):
        """Send an RPC request.

        Returns the response.
        """
        msg = self._pack_rpc(*args, MsgClass=RequestMsg, **kwargs)

        with self._accept_reply(msg):
            await self._send(msg)
            res = await msg.read()

        if result_conv == CC_DATA:
            res.raise_if_error()
            return res.data
        elif result_conv == CC_MSG:
            return res
        else:
            raise RuntimeError("I do not know how to return %s (%s?)" % (msg, result_conv))

    async def alert(self, *args, **kwargs):
        """Send a broadcast alert.

        There is no reply, thus no reply-to UUID is transmitted.
        """
        msg = self._pack_rpc(*args, MsgClass=AlertMsg, **kwargs)

        if msg.message_id in self.replies:  # pragma: no cover
            raise RuntimeError("Message ID is already known (%s)" % msg.message_id)

        await self._send(msg)

    async def poll(self, *args, min_replies=0, max_replies=math.inf, max_delay=math.inf, _MsgClass=PollMsg, result_conv=CC_DATA, **kwargs):
        """Send a broadcast that accepts replies from multiple clients.

        This is an async iterator. If :param:`result_conv` is
        :const:`CC_MSG`, this call yields the raw message. Otherwise
        (default), it returns the data element / raises the incoming error.
        """
        msg = self._pack_rpc(*args, MsgClass=_MsgClass, **kwargs)

        n = 0
        with trio.move_on_after(max_delay) as timeout:
            with self._accept_reply(msg):
                await self._send(msg)
                while n < max_replies:
                    res = await msg.read()
                    n += 1
                    if result_conv == CC_DATA:
                        res.raise_if_error()
                        res = res.data
                    yield res
        if timeout.cancelled_caught and n < min_replies:
            raise trio.TooSlowError

    async def stream(self, *args, **kwargs):
        async with aclosing(self.poll(*args, _MsgClass=StreamMsg, **kwargs)) as s:
            async for x in s:
                yield x

    async def _send(self, msg):
        """Send a request-type message (i.e. something that's not a reply).
        """
        assert isinstance(msg,BaseMsg)

        data,props = msg.dump(self, codec=msg._c_codec)
        logger.debug("Send %s to %s: %s", msg.routing_key, msg._c_exchange, data)
        await getattr(self,'_ch_'+msg.type).channel.publish(data, msg._c_exchange, msg.dest, properties=props)
        
    # Server

    async def register(self,ep):
        assert ep.tag not in self.rpcs
        ch = getattr(self,'_ch_'+ep.type)
        cfg = self.cfg

        dn = n = ep.name
        if ep.name.endswith('.#'):
            n = n[:-2]
            dn = n+'._all_'
        if len(n) > 1 and '#' in n:
            raise RuntimeError("I won't find that")

        if ep.tag in self.rpcs:
            raise RuntimeError("multiple registration of "+ep.tag)

        self.rpcs[ep.tag] = ep
        chan = None
        try:
            ep._c_channel = chan = await self.amqp.channel()
            d = {}
            ttl = ep.ttl or cfg.ttl[ep.type]
            if ttl:
                d["x-dead-letter-exchange"] = cfg.queues['dead']
                d["x-message-ttl"] = int(1000*ttl)

            if ep.durable:
                if isinstance(ep.durable,str):
                    dn = ep.durable
                else:
                    dn = self.cfg.queues['msg']+ep.tag
                chan = await self.amqp.channel()
                q = await chan.queue_declare(dn, auto_delete=False, passive=False, exclusive=False, durable=True, arguments=d)
            elif ep.type == "rpc":
                chan = await self.amqp.channel()
                q = await chan.queue_declare(cfg.queues[ep.type]+ep.name, auto_delete=True, durable=False, passive=False, arguments=d)
            else:
                chan = self._ch_alert.channel
                q = self._ch_alert.queue
            logger.debug("Chan %s: bind %s %s %s", ch.channel,ep.exchange, ep.name, q['queue'])
            await chan.queue_bind(q['queue'], ep.exchange, routing_key=ep.name)

            await chan.basic_qos(prefetch_count=1,prefetch_size=0,connection_global=False)
            logger.debug("Chan %s: read %s", ch,q['queue'])
            await chan.basic_consume(queue_name=q['queue'], callback=self._on_rpc_in if ep.type == "rpc" else self._on_alert_in)

            ep._c_channel = chan
            ep._c_queue = q

        except BaseException:  # pragma: no cover
            del self.rpcs[ep.tag]
            if chan is not None:
                del ep._c_channel
                with trio.open_cancel_scope(shield=True, deadline=trio.current_time()+1):
                    await chan.close()
            raise

    async def unregister(self,ep):
        cfg = self.cfg

        if isinstance(ep,str):
            ep = self.rpcs.pop(ep)  # pragma: no cover
        else:
            del self.rpcs[ep.tag]
        chan = ep._c_channel
        q = ep._c_queue
        assert q is not None
        logger.debug("Chan %s: unbind %s %s %s", chan,ep.exchange, ep.name, q['queue'])

        if chan is self._ch_alert.channel:
            await chan.queue_unbind(q['queue'], ep.exchange, routing_key=ep.name)
            await chan.basic_cancel(consumer_tag=ep.uuid)
        else:
            await chan.close()

    async def aclose(self):
        a,self.amqp = self.amqp,None
        if a is not None:
            logger.debug("Disconnecting %s",self)
            try:
                await a.aclose()
            except BaseException:  # pragma: no cover
                a.close()
                raise
            logger.debug("Disconnected %s",self)

    def close(self):
        a,self.amqp = self.amqp,None
        if a is not None:
            logger.debug("Killing %s",self)
            a.close()


#!/usr/bin/env python3
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
import qbroker
from qbroker import CC_DATA
from tests.util import load_cfg
import signal
import pprint
import json

import logging
import sys
logging.basicConfig(stream=sys.stderr, level=logging.INFO)

import os
cfg = load_cfg(os.environ.get("QBROKER","test.cfg"))
u=None
cf=None

channels = {'alert':'topic', 'rpc':'topic', 'reply':'direct'}

class mon:
    def __init__(self,u,typ,name):
        self.u = u
        self.typ = typ
        self.name = u.config['amqp']['exchanges'][name]

    async def start(self, task_status=trio.TASK_STATUS_IGNORED):
        async with u.conn.amqp.new_channel() as channel:
            await channel.exchange_declare(self.name, self.typ, passive=True)
            queue_name = 'mon_'+self.name+'_'+self.u.uuid
            queue = (await channel.queue_declare(queue_name, auto_delete=True, passive=False, exclusive=True))
            await channel.basic_qos(prefetch_count=1,prefetch_size=0,connection_global=False)
            await channel.queue_bind(self.queue_name, self.name, routing_key='#')
            async with channel.new_consumer(queue_name=self.queue_name) as cons:
                task_status.started()

                async for msg in cons:
                    (body,envelope,properties) = msg
                    if properties.content_type == 'application/json' or properties.content_type.startswith('application/json+'):
                        body = json.loads(body.decode('utf-8'))

                    if self.name == 'alert':
                        if envelope.routing_key == 'qbroker.start':
                            c = channels['reply']
                            await jobs.put(BindMe,c, c.queue_name, c.name, routing_key=body['uuid'])
                        elif envelope.routing_key == 'qbroker.stop':
                            c = channels['reply']
                            await jobs.put(UnBindMe,c, c.queue_name, c.name, routing_key=body['uuid'])

                    m = {'body':body, 'prop':{}, 'env':{}}
                    for p in dir(properties):
                        if p.startswith('_'):
                            continue
                        v = getattr(properties,p)
                        if v is not None:
                            m['prop'][p] = v
                    for p in dir(envelope):
                        if p.startswith('_'):
                            continue
                        v = getattr(envelope,p)
                        if v is not None:
                            m['env'][p] = v
                    pprint.pprint(m)
                    await self.channel.basic_client_ack(delivery_tag = envelope.delivery_tag)

class BindMe:
    def __init__(self,c,*a,**k):
        self.c = c
        self.a = a
        self.k = k
    async def run(self):
        await self.c.channel.queue_bind(*self.a,**self.k)

class UnBindMe:
    def __init__(self,c,*a,**k):
        self.c = c
        self.a = a
        self.k = k
    async def run(self):
        await self.c.channel.queue_unbind(*self.a,**self.k)

##################### main loop

jobs=None

async def mainloop():
    global jobs
    jobs = trio.Queue(99)

    with qbroker.open_broker("example.monitor", cfg=cfg) as _u:
        global u
        u = _u
        global cf
        cf=u.config['amqp']['server']
        print(cf['host'],cf['virtualhost'])

        async with trio.open_nursery() as n:
            for c,t in channels.items():
                channels[c] = m = mon(u,t,c)
                await n.start(m.start)
            async for j in jobs:
                await j.run()

def main():
    trio.run(mainloop)

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print("Terminated.", file=sys.stderr)



#!/usr/bin/env python3
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
cfg = load_cfg(os.environ.get("QBROKER", "test.cfg"))
u = None
cf = None

class mon:
    def __init__(self, u):
        self.u = u
        self.name = "firehose"

    async def start(self, task_status=trio.TASK_STATUS_IGNORED):
        async with u.conn.amqp.new_channel() as channel:
            await channel.exchange_declare("amq.rabbitmq.trace", "topic", passive=True)
            queue_name = 'mon_' + self.name + '_' + self.u.uuid
            queue = (
                await
                channel.queue_declare(queue_name, auto_delete=True, passive=False, exclusive=True)
            )
            await channel.basic_qos(prefetch_count=1, prefetch_size=0, connection_global=False)
            await channel.queue_bind(queue_name, "amq.rabbitmq.trace", routing_key='#')
            async with channel.new_consumer(queue_name=queue_name) as cons:
                task_status.started()
                async for msg in cons:
                    (body, envelope, properties) = msg
                    ph = properties.headers['properties'].get('content_type', '')
                    if ph == 'application/json' or ph.startswith('application/json+'):
                        body = json.loads(body.decode('utf-8'))

                    m = {'body': body}
                    e, k = envelope.routing_key.split('.', 1)
                    m[e] = k
                    m['header'] = properties.headers

                    pprint.pprint(m)
                    await channel.basic_client_ack(delivery_tag=envelope.delivery_tag)

##################### main loop

async def mainloop():
    async with qbroker.open_broker("example.firehose", cfg=cfg) as _u:
        global u
        u = _u
        global cf
        cf = u.cfg['server']
        print(cf['host'], cf['virtualhost'])

        m = mon(u)
        await m.start()

def main():
    trio.run(mainloop)

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print("Terminated.", file=sys.stderr)

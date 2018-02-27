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
from pprint import pprint
from traceback import print_exc
from async_generator import aclosing

import logging
import sys
logging.basicConfig(stream=sys.stderr, level=logging.INFO)

import os
cfg = load_cfg(os.environ.get("QBROKER", "test.cfg"))
u = None


async def cb(data):
    data['_broadcast'] = True
    pprint(data)
    try:
        res = await u.rpc('qbroker.ping', _uuid=data['uuid'])
    except Exception:
        print_exc()
    else:
        pprint(res)


async def example(app=None):
    async with qbroker.open_broker("example.list_servers", cfg=cfg) as _u:
        global u
        u = _u
        await trio.sleep(1)
        d = {}
        if app is not None:
            d['app'] = app
        async with aclosing(u.poll("qbroker.ping", call_conv=CC_DATA, timeout=2, _data=d)) as r:
            async for msg in r:
                await cb(msg)


def main():
    trio.run(example, *sys.argv[1:])


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print("Terminated.", file=sys.stderr)

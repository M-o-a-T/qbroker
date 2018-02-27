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

"""
This code simulates a concurrent server.
It listens for "example.server" events, thinks about them, and returns a
reply.

The number of concurrently-running calls is limited (globally) by 
the "config.amqp.limits.rpc.workers" setting.
"""

import trio
import qbroker
from qbroker import CC_DICT
from tests.util import load_cfg

import logging
import sys
logging.basicConfig(stream=sys.stderr, level=logging.INFO)

import os
cfg = load_cfg(os.environ.get("QBROKER", "test.cfg"))

async def hello(name="Joe", **kw):
    print("working for", name)
    await trio.sleep(5)  # simulate doing some work in parallel
    print("DONE working for", name)
    return "Hello %s!" % name

async def example():
    async with qbroker.open_broker("example.server", cfg=cfg) as u:
        u.register_rpc(hello, "example.hello", call_conv=CC_DICT)
        await trio.sleep(200)

def main():
    trio.run(example)

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print("Terminated.", file=sys.stderr)

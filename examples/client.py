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
This is a sample client.

Usage:
    env PYTHONPATH=. client.py routing_key data…
"""

import trio
import qbroker
from tests.util import load_cfg
from traceback import print_exc
import logging
import sys
import json
logging.basicConfig(stream=sys.stderr, level=logging.INFO)

import os
cfg = load_cfg(os.environ.get("QBROKER", "test.cfg"))
u = None

async def example(type="example.client", content=""):
    async with qbroker.open_broker(type, cfg=cfg) as u:
        rc = 0
        await trio.sleep(0.2)  # allow monitor to attach
        i = type.find('::')
        if i > 0:
            dest = type[i + 2:]
            type = type[:i]
        else:
            dest = None
        if content is None:
            content = ''
        elif content == '-':
            content = sys.stdin.read()
        try:
            content = json.loads(content)
        except ValueError:
            print("Warning: content is not JSON, sending as string", file=sys.stderr)
        try:
            res = (await u.rpc(type, data=content, dest=dest))
            print(res)
        except Exception:
            print_exc()
            rc = 2

def main(type="example.client", content=""):
    trio.run(example, type, content)

if __name__ == '__main__':
    main(*sys.argv[1:])

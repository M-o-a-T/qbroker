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
from tests.util import load_cfg
import logging
import sys
from pprint import pprint
logging.basicConfig(stream=sys.stderr, level=logging.INFO)
import json

import os
cfg = load_cfg(os.environ.get("QBROKER","test.cfg"))
u=None

async def example():
    async with qbroker.open_broker("example.send_alert", cfg=cfg) as _u:
        global u
        u = _u
        await trio.sleep(0.2) # allow monitor to attach
        async for r in u.alert(sys.argv[1],_data=json.loads(' '.join(sys.argv[2:])), timeout=3):
            pprint(r.data)

def main():
    trio.run(example)
if __name__ == '__main__':
    try:
        if len(sys.argv) < 3:
            print("Usage: %s route.key {some.json.expression}" % (sys.argv[0],),file=sys.stderr)
            sys.exit(1)
        main()
    except KeyboardInterrupt:
        print("Terminated.", file=sys.stderr)


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
from tests.util import load_cfg
import logging
import sys
from pprint import pprint
logging.basicConfig(stream=sys.stderr, level=logging.INFO)

import os
cfg = load_cfg(os.environ.get("QBROKER", "test.cfg"))
u = None

async def example():
    async with qbroker.open_broker("example.ping", cfg=cfg) as _u:
        global u
        u = _u
        await trio.sleep(0.2)  # allow monitor to attach
        async for r in u.alert("dabroker.ping", timeout=3):
            pprint(r.data)

def main():
    trio.run(example)

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        pass

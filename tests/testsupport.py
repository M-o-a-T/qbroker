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

import os
import trio_qbroker as qbroker
import inspect

TIMEOUT = 0.5

from .util import load_cfg
CFG = "test.cfg"
for x in range(3):
    if os.path.exists(CFG):
        break
    CFG = os.path.join(os.pardir, CFG)
cfg = load_cfg(CFG)


def unit(name):
    f = inspect.currentframe().f_back
    n = f.f_code.co_name
    if n.startswith("test_"):
        n = n[5:]
    fn = f.f_code.co_filename
    if fn.endswith('.py'):
        fn = fn[:-3]
    i = fn.index("/test_")
    if i > -1:
        fn = fn[i + 6:]
    return qbroker.open_broker("test.%s.%s.%s" % (fn, n, name), cfg=cfg)

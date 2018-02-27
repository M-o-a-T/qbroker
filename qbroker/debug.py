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

import inspect
import weakref

import logging
logger = logging.getLogger(__name__)


class Debugger(object):
    """QBroker insert for debugging."""

    def __init__(self, broker):
        self.broker = weakref.ref(broker)
        self.env = {}

    async def run(self, msg):
        """Evaluate a debugger command."""
        msg.codec = 'application/json+repr'
        args = msg.data or {}
        cmd = args.pop('cmd', None)

        if cmd is None:
            if args:
                raise RuntimeError("Need 'cmd' parameter")
            return dict(
                (x[4:], getattr(self, x).__doc__) for x in dir(self) if x.startswith("run_")
            )
        return await getattr(self, 'run_' + cmd)(**args)

    async def run_env(self, **args):
        """Dump the debugger's environment"""
        return dict((k, v) for k, v in self.env.items() if k != '__builtins__')

    async def run_eval(self, code=None, mode="eval", **args):
        """\
            Evaluate @code (string). @mode may be 'exec', 'single' or 'eval'/'vars'
            (default: 'eval').
            All other arguments are used as local variables.
            Non-local variables are persistent. The result is returned.

            The mode 'vars' behaves like 'eval' but applies vars() to the result.
            """
        do_vars = False
        if mode == "vars":
            mode = "eval"
            do_vars = True

        code = compile(code, "(debug)", mode)
        loc = args.copy()

        self.env['broker'] = self.broker()
        try:
            res = eval(code, self.env, loc)
            if inspect.iscoroutine(res):
                res = await res
        finally:
            del self.env['broker']

        for k, v in loc.items():
            if k not in args:
                self.env[k] = v
        if do_vars:
            try:
                r = vars(res)
            except TypeError:
                r = {}
                for k in dir(res):
                    v = getattr(res, k)
                    if not callable(v):
                        r[k] = v
            r['__obj__'] = str(res)
            res = r
        return res

    async def run_ping(self):
        """Return 'pong'"""
        return "pong"

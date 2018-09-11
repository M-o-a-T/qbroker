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
"""Collect codec info"""

from importlib import import_module
import os
import pkgutil

import logging
logger = logging.getLogger(__name__)

codecs = {}
default_codec = None
DEFAULT = "DEFAULT"


def get_codec(name):
    global default_codec

    if not codecs:

        def _check(m):
            global default_codec
            try:
                if isinstance(m, str):
                    m = import_module(m)
            except ImportError as ex:
                raise ImportError(m) from ex  # pragma: no cover
                # not going to ship a broken file for testing this
            else:
                try:
                    codecs[m.CODEC] = m
                except AttributeError:
                    pass
                else:
                    if getattr(m, 'DEFAULT', False):
                        default_codec = m

        for a, b, c in pkgutil.walk_packages((os.path.dirname(__file__),), __package__ + '.'):
            _check(b)

    if name == DEFAULT:
        return default_codec

    return codecs[name]

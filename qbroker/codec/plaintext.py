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

CODEC = "text/plain"

DEBUG = False


def encode(data):
    if not data:
        data = b""
    elif isinstance(data, str):
        data = data.encode('utf-8')
    elif not isinstance(data, bytes):  # pre-encoded. Oh well.
        raise RuntimeError("Need Unicode, not %s" % str(type(data)))
    # else:
    #     data.decode('utf-8') # assertion only
    return data


def decode(data):
    data = data.decode('utf-8')
    return data

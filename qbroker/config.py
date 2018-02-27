#!/usr/bin/env python
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

DEFAULT_CONFIG = dict(
    server=dict(
        host='localhost',
        login='guest',
        password='guest',
        virtualhost='qbroker',
        ssl=False,
    ),
    exchanges=dict(  # all are persistent
        alert='alert',  # topic: broadcast messages, may elicit multiple replies
        rpc='rpc',  # topic: RPC requests, will trigger exactly one reply
        reply='reply',  # direct: all replies go here
        dead=
        'dead',  # fanout: dead messages (TTL expires, reject, RPC/alert unrouteable, …)
    ),
    queues=dict(
        alert='alert_',  # plus the unit UUID. Nonpersistent.
        msg='msg.',  # plus the routing key. Persistent alerts.
        rpc='rpc_',  # plus the command name. Persistent.
        reply='reply_',  # plus the unit UUID.
        dead='dead',  # no add-on. Persistent. No TTL here!
    ),
    handlers=dict(
        dead=False,  # add a handler for dead messages
        debug=False,  # add hooks to introspect/debug the connection
    ),
    codec='DEFAULT',
    ttl=dict(
        rpc=10,  # seconds before being dead lettered, when not read
        alert=10,  # durable queues only
    ),
    limits=dict(
        rpc=dict(
            workers=10,  # parallel handlers
            queue=99,  # max nr of waiting tasks
        ),
        alert=dict(
            workers=5,  # parallel handlers
            queue=99,  # max nr of waiting tasks
        ),
    ),
    timeout=dict(
        connect=30,  # open link
        rpc=15,  # waiting for RPC reply
        poll=30,  # waiting for alert replies
        reconnect=30,  # before retrying
    ),
    retries=dict(
        connect=0,  # initially
        rpc=0,  # when timed out
        poll=0,  # if no reply
        reconnect=9999,  # retries before giving up
    ),
)

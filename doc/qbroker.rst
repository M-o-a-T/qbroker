==============
AMQP structure
==============

Rationale
=========

The design should be reasonably efficient, i.e. messages don't go where
they're not wanted anyway.

The whole system must be monitor-able. This includes snooping on any RPC
exchange.

Broadcasts may be used. They may even be replied to, i.e. a broadcast
sender might expect many responses. This will aid in discoverability.

RPC messages are read by one client, which will process it and send one
answer. If no client is present, RabbitMQ offers a timeout queue, i.e.
unprocessed requests get re-queued to where a dedicated error logger+replier
can process them.
(Doing this is application specific, thus not part of QBroker.)

The routing key for requests and alerts is the service to be addressed,
i.e. "web.users.list" to retrieve a list of web users. Replies use the
content of the request's reply-to field.

--------------
AMQP structure
--------------

Exchanges
=========

rpc
---

RPC requests go here.

This is a topic exchange. The topic is the request's name.

reply
-----

RPC replies go here.

This is a direct exchange. The topic is the recipient's UUID.

A debugger reads replies by binding to the UUID, which it reads from
the alert message the client emits when started.

alert
-----

Broadcasts go here.

This is a topic exchange. The topic is the broadcast's name.

dead
----

Fan-out exchange for non-deliverable or timed-out messages.

Queues
======

rpc_COMMAND
-----------

This queue is attached to the ``rpc`` exchange. RPC servers listen to
messages on this queue and **must** send exactly one reply.

reply_UUID
----------

This is each client's non-persistent queue for replies.

alert_UUID
----------

This is each client's non-persistent queue for alerts.

dead
----

This queue is attached to the ``dead`` exchange. A dead-reply
collector should listen to this queue and process non-deliverable messages.

Message structure
=================

Messages are always encoded as dicts. All messages must include a header.
The body and error parts are both optional.

header
------

Meta information about the message. This is stored in the AMQP header.

* headers.version

  The protocol version. Currently 1.

* app-id

  The ID of the application (QBroker unit) which generated the message.
  This is random and unique.

* message-id

  Some unique random string to identify each message.

* reply-to

  in requests, the routing key with which replies should be sent.

* headers.routing-key

  A dotted name, like "qbroker.info", which identifies the message destination.
  Present in requests and alerts. Usually matches the message's actual routing
  key, but may not.

* correlation-id

  The UUID to which replies should be addressed. Matches the message-id of
  the request it is sent in answer of.

* headers.debug

  A flag. If true, replies are supposed to include debugging information.

* content-type

  `QBroker` supports pluggable codecs.

  There is no negotiation WRT which codec to use.

  * application/json

    Your basic dumb JSON.

  * application/json+obj

    A JSON variant which can encode select Python objects. See
    `qbroker.codec.json_obj` on how to encode your own objects.

  * application/json+repr

    Like json+obj, but non-encodeable objects are represented as dicts
    instead of triggering an error.

  * text/plain

    UTF-8 data.

  * application/binary

    raw bytes.

You can specify which codec to use by default in the connect call, but
the default needs at least the same power as the JSON codec because the
startup and teardown messages are dicts.

The `alert` and `rpc` methods understand a ``codec`` parameter.

Replies default to using the same codec the requests were transmitted as.
If you need to change it, your callback needs to use the `CC_MSG` calling
convention; set ``msg.codec`` to the codec you require for your reply, or
to ``None`` to use the connection's default codec.

body
----

The actual request or response payload. Usually another dict.

The contents are specific to the message and its encoding.

Message destinations
--------------------

The message's routing key usually matches the queue's, as all destinations
are supposed to be equal, i.e. a RPC request of type "foo.bar" should be
understood and processed equally by all processes which listen to that
queue.

Since the real world frequently is more complicated, `QBroker` supports
specialized routing keys. Specifically:

* qbroker.ping

  Read by all instances.

* qbroker.uuid.INSTANCE_UUID

  Read by exactly one instance.

* qbroker.app.APP.NAME

  Read by all instances with that exact app name.

* qbroker.debug.APP.NAME

  Call code to debug your application. See next section.

---------
Debugging
---------

`QBroker` supports hooks for debugging your code. 

Enabling debugging
------------------

In your configuration, set cfg['amqp']['handlers']['debug'] to something
Python considers to be true.

Usage
-----

RPC calls to ``qbroker.debug.YOUR.APP.NAME`` will return the command's
result, whatever that is. Exceptions will be propagated.

A call without arguments will return a hash of available methods with
their docstring. Otherwise, the method's name is supplied in the "cmd"
parameter.

The debug object contains an environment which is initally empty. You may
add arbitrary values by using the connection's ``debug_env`` method.

There are two predefined methods:

* ping

  simply returns "pong"

* exec

  Evaluates the code passed in the ``code`` argument.

  ``mode`` can be set to "single" to evaluate a statement instead of an
  expression, or "exec" to compile a code block. All other arguments are
  available to the statement, as local variables. Non-local variables which
  your code sets are persistent.

You can monkey-patch the debugger to add your own methods; simply name them
"run_" plus the method's name.


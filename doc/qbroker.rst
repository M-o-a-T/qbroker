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

These are QBroker's standard exchanges. A client may decide to send to /
listen on any non-standard exchange, in order to be compatible with
non-QBroker peers.

The actual queue names are configurable.

rpc
---

RPC requests go here. An RPC request shall be received (and acted upon) by exactly
one client; if there is no such client, a dead-letter processor shall
reply with an error message. This is done by sending each topic to a
dedicated queue; that queue forwards to the dead-letter exchange when a
message times out.

This is a topic exchange. The topic is the request's name.

A debugger may simply listen to the topics it's interested in.

reply
-----

RPC replies go here.

This is a direct exchange. The topic is the recipient's UUID.

A debugger reads replies by binding to the requestor's UUID, which it
receives from the alert message that the client emits when starts.

alert
-----

Broadcasts go here. Broadcasts are defined as messages that get replied to
by any number of clients. A client that's interested in alerts binds its
private, non-durable alert queue to the topics it's interested in.

This is a topic exchange. The topic is the broadcast's name.

A debugger may simply listen to the topics it's interested in.

dead
----

for non-deliverable or timed-out messages.

This is a simple fan-out exchange which usually sends to one dedicated
queue which dead-letter-redirectors listen to.

A debugger may listen to this exchange on a private queue, but typically
that's not necessary because messages on this exchange are promptly
converted to an error and redirected to the `reply` exchange where the
debugger will get a copy.

Queues
======

These are QBroker's standard queues. A client may decide to listen on any
non-standard queue, in order to be compatible with non-QBroker peers.

The actual queue names / prefixes are configurable.

rpc.COMMAND
-----------

This queue is attached to the ``rpc`` exchange. RPC servers listen to
messages on this queue. A typical RPC call triggers exactly one reply,
though the requestor may be able to accept multiple responses (or none).

reply.UUID
----------

This is each client's non-persistent queue for receiving replies.

alert.UUID
----------

This is each client's non-persistent queue for receiving alerts. A typical
alert triggers zero or one response.

dead
----

This queue is attached to the ``dead`` exchange. Any number of dead-reply
collectors should listen to this queue and process non-deliverable messages.
Typically, a system monitoring process does this in addition to its regular
job, though any long-running client or server may do the job.

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

  The UID of the application (QBroker instance) which generated the
  message. This string is random and unique. It does **not** change when
  reconnecting after a service interrupt.

* message-id

  Some unique random string to identify each message.

* reply-to

  in requests, the routing key with which replies should be sent.

* headers.routing-key

  A dotted name, like "qbroker.info", which identifies the message destination.
  This field is present in requests and alerts. The routing key used for
  delivering the message usually matches this field, but in some circumstances
  you might use a different key (e.g. test a specific server, or to avoid
  compatibility issues with non-QBroker messages on the same exchange).

* correlation-id

  The UUID to which replies should be addressed. Matches the message-id of
  the request it is sent in answer of.

* headers.debug

  A flag. If true, replies are supposed to include debugging information.

* content-type

  QBroker supports pluggable codecs.

  There is no negotiation WRT which codec to use. JSON is nice for
  human-readable messages while MsgPack is faster and more compact.

  * application/json

    Your basic JSON.

  * application/json+obj

    A JSON variant which can encode selected Python objects. See
    :mod:`qbroker.codec.json_obj` on how to encode your own objects.

  * application/json+repr

    Like json+obj, but non-encodeable objects are represented as dicts
    instead of triggering an error.

  * application/msgpack…

    Like above, but use MsgPack instead of JSON.

  * text/plain

    UTF-8 data. No, other string representations are not supported!

  * application/binary

    Raw bytes.

You can specify whether to use JSON or MsgPack in the connect call.

QBroker may or may not auto-switch to ``text/plain`` or
``application/binary`` when you send a string.

body
----

The actual request or response payload.

The contents are specific to the message type and its encoding.

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

  Read by this instance only.

* qbroker.app.APP.NAME

  Read by all instances with that app name.

* qbroker.debug.ping

  Alert queue, read by all introspectable instances.

* qbroker.debug.uuid.INSTANCE_UUID

  or

* qbroker.debug.app.APP.NAME

  RPC interface to introspect and/or debug your application. See next section.

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

RPC commands to ``qbroker.debug.uuid.INSTANCE_UUID`` or
``qbroker.debug.app.APP.NAME`` will return the command's result, whatever
that is. Exceptions will be propagated.

A call without arguments will return a hash of available methods with
their docstring. Otherwise, the method's name is supplied in the "cmd"
parameter.

The debug object contains an environment which is initally empty. You may
add arbitrary values by using the connection's ``debug_env`` method.

There are two predefined methods:

* ping

  replies with "pong".

* env

  Dump the debugger's environment.

* exec

  Evaluates the Python passed in the ``code`` argument.


  ``mode`` can be set to ``exec``, ``single`` or ``eval``/``vars``
  (default: ``eval``). See the documentation of :func:`eval` for details.

  If set to ``vars``, the call behaves as if you used ``eval`` but applies
  :func:`vars`\() to the result before returning it.

  All arguments other than ``code`` and ``mode`` are available to the
  evaluated statement, as local variables. The code's global variables are
  set to the debugger environment. Non-local variables which your code sets
  are added to the environment.

You can monkey-patch the debugger to add your own methods; simply name them
"run_" plus the method's name and add them to ``broker.debug``. They should
be asynchronous.

Unlike the rest of QBroker, debug messages will not be interpreted by the
common RPC dispatcher or vice versa.


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

Meta information about the message. Some of the data replicates information
in the AMQP header.

* version

  The protocol version. Currently 1.

* type

  request / reply / alert

* message-id

  Some unique string.

* name

  A dotted name, like "qbroker.info". Used for message routing.

* reply-to

  The UUID to which replies should be addressed.

  Required for requests, optional for alerts.

* in-reply-to

  The ``message-id`` of the request or alert this message is a reply to.

  Required for replies, forbidden for other messages.

* debug

  A flag. If true, replies are supposed to include debugging information.

body
----

The actual request or response payload. Usually another dict.

The contents are message specific.

error
-----

If there was an error processing a request, this element encodes relevant
information, possibly including a stack trace.

* status

  One of "ok", "warn", "error" or "fail".

  * ok

    There is no problem.
	
	This is typically used to cancel an error state on a message board
    or similar.

  * warn

    Some problem has been encountered and fixed. The request was processed.

  * error

    There is a problem. Retrying may or may not exhibit the same error.
	(External connection got dropped, database unavailable, …)

  * fail

	There is a problem which can't be fixed without intervention. (Message
	without required content, wrong protocol version, host not found …)
    
* id

  A unique identifier for the type of error encountered.

* part

  A unique identified for the subsystem that encountered an error.

* message

  Something human-readable. Any specific (status,id,part) combination should
  result in the exact same error message (disregarding memory addresses and
  timestamps).


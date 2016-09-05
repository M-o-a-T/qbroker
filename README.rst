QBroker is short for Quick Access Broker.

QBroker is Python library which provides an easy-to-use interface for asynchronous, debuggable RPC via AMQP.
It uses Python 3.4 asyncio features and provides wrappers for use with native threads or gevent tasklets.

Example scripts, including an event/RPC monitor, are provided.

The QBroker data format is straightforward. Writing an adapter in a different programming language
is straightforward, given JSON and AMQP libraries. You can add codecs for your classes, or replace
JSON with another marshalling system.

QBroker supports the time-to-live and dead-lettering extensions of RabbitMQ, but does not depend on them.
Its support for monitoring does not rely on RabbitM's "firehose" tracing feature.

QBroker supports (as-yet rudimentary) introspection.

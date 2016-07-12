# -*- coding: utf-8 -*-
from __future__ import absolute_import, print_function, division, unicode_literals
##
## This file is part of QBroker, an easy to use RPC and broadcast
## client+server using AMQP.
##
## QBroker is Copyright © 2016 by Matthias Urlichs <matthias@urlichs.de>,
## it is licensed under the GPLv3. See the file `README.rst` for details,
## including optimistic statements by the author.
##
## This paragraph is auto-generated and may self-destruct at any time,
## courtesy of "make update". The original is in ‘utils/_boilerplate.py’.
## Thus, please do not remove the next line, or insert any blank lines.
##BP

import pytest
import pytest_asyncio.plugin
import os
import asyncio
from qbroker.proto import ProtocolClient,ProtocolInteraction
from qbroker.proto.lines import LineProtocol
from qbroker.util.tests import load_cfg
import unittest
from unittest.mock import Mock

D=0.2

class EchoServerClientProtocol(asyncio.Protocol):
	def __init__(self,loop):
		self._loop = loop

	def connection_made(self, transport):
		peername = transport.get_extra_info('peername')
		print('Connection from:',peername)
		self.transport = transport

	def data_received(self, data):
		message = data.decode()
		print("Message:",message)
		self._loop.call_later(3*D, self._echo,data)

	def _echo(self,data):
		self.transport.write(data)

	def connection_lost(self, exc):
		print("Conn closed:",exc)

class EchoClientProtocol(asyncio.Protocol):
	def __init__(self, future):
		self.future = future
		self.done = False

	def connection_made(self, transport):
		self.transport = transport
		transport.write(b"Hello!\n");

	def data_received(self, data):
		assert data == b"Hello!\n"
		self.done = True
		self.transport.close()

	def connection_lost(self, exc):
		self.future.set_result(self.done)

@pytest.yield_fixture
def echoserver(loop, unused_tcp_port):
	# Each client connection will create a new protocol instance
	coro = loop.create_server(lambda: EchoServerClientProtocol(loop), '127.0.0.1', unused_tcp_port)
	server = loop.run_until_complete(coro)
	server.port = unused_tcp_port
	yield server
	server.close()
	loop.run_until_complete(server.wait_closed())

@pytest.mark.run_loop
@asyncio.coroutine
def test_echo(loop, echoserver):
	f = asyncio.Future(loop=loop)
	coro = loop.create_connection(lambda: EchoClientProtocol(f),
							  '127.0.0.1', echoserver.port)
	yield from coro
	yield from f
	assert f.result() is True

class LinesTester(ProtocolInteraction):
	def __init__(self,conn_store=None,**k):
		self.conn_store = conn_store
		super().__init__(**k)

	@asyncio.coroutine
	def interact(self):
		if self.conn_store is not None:
			self.conn_store.append(self._protocol)
		self.send("whatever\nwherever")
		yield from asyncio.sleep(D/2, loop=self._loop)
		self.send("whenever")
		m = (yield from self.recv())
		n = (yield from self.recv())
		o = (yield from self.recv())
		assert m == "whatever"
		assert n == "wherever"
		assert o == "whenever"
	
@pytest.mark.run_loop
@asyncio.coroutine
def test_lines(echoserver, loop):
	"""Use the "lines" protocol to go through the basic protocol features"""
	c = ProtocolClient(LineProtocol, "127.0.0.1",echoserver.port, loop=loop)
	yield from c.run(LinesTester(loop=loop))
	assert len(c.conns) == 1
	fff=[]
	tp = LinesTester(fff,loop=loop,conn=c)
	e = asyncio.ensure_future(tp.run(), loop=loop)
	# give 'e' time to start up
	yield from asyncio.sleep(D/3, loop=loop)
	# make sure close() waits for 'e' and doesn't break it
	yield from c.close()
	assert e.done()
	e.result()
	assert len(c.conns) == 0
	# create an idle connection which the next step can re-use
	eee=[]
	yield from c.run(LinesTester(eee, loop=loop))
	assert len(c.conns) == 1

	# now do two at the same time, and abort
	fff.pop()
	ff = c.run(tp)
	f = asyncio.ensure_future(ff, loop=loop)
	yield from asyncio.sleep(D, loop=loop)
	with pytest.raises(RuntimeError):
		yield from c.run(tp)
	g = asyncio.ensure_future(c.run(LinesTester(loop=loop)), loop=loop)
	yield from asyncio.sleep(D, loop=loop)
	yield from f
	# check that the connection is reused
	assert len(eee) == 1
	assert len(fff) == 1
	assert eee[0] == fff[0]

	c.MAX_IDLE = 0
	hhh = []
	hh = c.run(LinesTester(hhh, loop=loop))
	h = asyncio.ensure_future(hh, loop=loop)
	yield from asyncio.sleep(D/3, loop=loop)
	assert len(hhh) == 1
	assert eee[0] != hhh[0]
	c.abort()
	assert len(c.conns) == 0
	with pytest.raises(asyncio.CancelledError):
		yield from g
	with pytest.raises(asyncio.CancelledError):
		g.result()
	with pytest.raises(asyncio.CancelledError):
		yield from h

	# let the mainloop process things
	yield from asyncio.sleep(D/2, loop=loop)


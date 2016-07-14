#!/usr/bin/env python3
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

import asyncio
from qbroker.unit import Unit, CC_DATA
from qbroker.util.tests import load_cfg
import signal
import pprint
import json

import logging
import sys
logging.basicConfig(stream=sys.stderr, level=logging.INFO)

u=Unit("qbroker.monitor", **load_cfg("test.cfg")['config'])

channels = {'alert':'topic', 'rpc':'topic', 'reply':'direct'}

class mon:
	def __init__(self,u,typ,name):
		self.u = u
		self.typ = typ
		self.name = u.config['amqp']['exchanges'][name]

	@asyncio.coroutine
	def start(self):
		self.channel = (yield from u.conn.amqp.channel())
		yield from self.channel.exchange_declare(self.name, self.typ, auto_delete=False, passive=False)
		self.queue_name = 'mon_'+self.name+'_'+self.u.uuid
		self.queue = (yield from self.channel.queue_declare(self.queue_name, auto_delete=True, passive=False, exclusive=True))
		yield from self.channel.basic_qos(prefetch_count=1,prefetch_size=0,connection_global=False)
		yield from self.channel.queue_bind(self.queue_name, self.name, routing_key='#')
		yield from self.channel.basic_consume(queue_name=self.queue_name, callback=self.callback)
	
	@asyncio.coroutine
	def callback(self, channel,body,envelope,properties):
		if properties.content_type == 'application/json':
			body = json.loads(body.decode('utf-8'))

		if self.name == 'alert':
			if envelope.routing_key == 'qbroker.start':
				c = channels['reply']
				yield from jobs.put(BindMe(c, c.queue_name, c.name, routing_key=body['uuid']))
			elif envelope.routing_key == 'qbroker.stop':
				c = channels['reply']
				loop.call_later(10,jobs.put_nowait,UnBindMe(c, c.queue_name, c.name, routing_key=body['uuid']))

		m = {'body':body, 'prop':{}, 'env':{}}
		for p in dir(properties):
			if p.startswith('_'):
				continue
			v = getattr(properties,p)
			if v is not None:
				m['prop'][p] = v
		for p in dir(envelope):
			if p.startswith('_'):
				continue
			v = getattr(envelope,p)
			if v is not None:
				m['env'][p] = v
		pprint.pprint(m)
		yield from self.channel.basic_client_ack(delivery_tag = envelope.delivery_tag)

class BindMe:
	def __init__(self,c,*a,**k):
		self.c = c
		self.a = a
		self.k = k
	@asyncio.coroutine
	def run(self):
		yield from self.c.channel.queue_bind(*self.a,**self.k)

class UnBindMe:
	def __init__(self,c,*a,**k):
		self.c = c
		self.a = a
		self.k = k
	@asyncio.coroutine
	def run(self):
		yield from self.c.channel.queue_unbind(*self.a,**self.k)

##################### main loop

loop=None
jobs=None
quitting=False

class StopMe:
	@asyncio.coroutine
	def run(self):
		global quitting
		quitting = True

@asyncio.coroutine
def mainloop():
	yield from u.start()
	for c,t in channels.items():
		channels[c] = m = mon(u,t,c)
		yield from m.start()
	while not quitting:
		j = (yield from jobs.get())
		yield from j.run()
	yield from u.stop()

def _tilt():
	loop.remove_signal_handler(signal.SIGINT)
	loop.remove_signal_handler(signal.SIGTERM)
	jobs.put(StopMe())

def main():
	global loop
	global jobs
	jobs = asyncio.Queue()
	loop = asyncio.get_event_loop()
	loop.add_signal_handler(signal.SIGINT,_tilt)
	loop.add_signal_handler(signal.SIGTERM,_tilt)
	loop.run_until_complete(mainloop())

if __name__ == '__main__':
	main()


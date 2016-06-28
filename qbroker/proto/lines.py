# -*- coding: utf-8 -*-
from __future__ import absolute_import, print_function, division, unicode_literals
##
## This file is part of QBroker, a distributed data access manager.
##
## QBroker is Copyright © 2016 by Matthias Urlichs <matthias@urlichs.de>,
## it is licensed under the GPLv3. See the file `README.rst` for details,
## including optimistic statements by the author.
##
## This paragraph is auto-generated and may self-destruct at any time,
## courtesy of "make update". The original is in ‘utils/_boilerplate.py’.
## Thus, please do not remove the next line, or insert any blank lines.
##BP

"""\
	This code implements a simple line-based protocol.
	"""

import asyncio
import struct
from time import time
from . import Protocol

import logging
logger = logging.getLogger(__name__)

class LineProtocol(Protocol):
	MAX_LENGTH=10*1024
	LF=b'\n'

	def __init__(self, loop=None):
		super().__init__(loop=loop)
		self.data = b""

	def received(self, data):
		self.data += data
		while True:
			lf = self.data.find(self.LF)
			if lf == -1:
				break
			data = self.data[:lf]
			self.data = self.data[lf+len(self.LF):]
			yield data.decode('utf-8')
		if len(self.data) > self.MAX_LENGTH:
			raise RuntimeError("message too long in %s" % repr(self()))

	def send(self, data):
		self.transport.write(data.encode('utf-8') + self.LF)


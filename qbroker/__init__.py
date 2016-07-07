#!/usr/bin/env python
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

__VERSION__ = (0,4,2)

# Not using gevent is not yet supported
# mainly because you can't kill/cancel OS threads from within Python
USE_GEVENT=True

## change the default encoding to UTF-8
## this is a no-op in PY3
# PY2 defaults to ASCII, which requires adding spurious .encode("utf-8") to
# absolutely everything you might want to print / write to a file
import sys
try:
	reload(sys)
except NameError:
	# py3 doesn't have reload()
	pass
else:
	# py3 also doesn't have sys.setdefaultencoding
	sys.setdefaultencoding("utf-8")

import asyncio
try:
	asyncio.ensure_future
except AttributeError:
	asyncio.ensure_future = asyncio.async

def unit(app, cfg="/etc/qbroker.cfg", **args):
	"""Return the QBroker unit for this app."""
	from qbroker.unit import Unit
	return Unit(app,cfg, **args)


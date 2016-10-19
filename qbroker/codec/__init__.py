# -*- coding: utf-8 -*-
from __future__ import absolute_import, print_function, division, unicode_literals
##
##  This file is part of MoaT, the Master of all Things.
##
##  MoaT is Copyright © 2007-2015 by Matthias Urlichs <matthias@urlichs.de>,
##  it is licensed under the GPLv3. See the file `README.rst` for details,
##  including optimistic statements by the author.
##
##  This program is free software: you can redistribute it and/or modify
##  it under the terms of the GNU General Public License as published by
##  the Free Software Foundation, either version 3 of the License, or
##  (at your option) any later version.
##
##  This program is distributed in the hope that it will be useful,
##  but WITHOUT ANY WARRANTY; without even the implied warranty of
##  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
##  GNU General Public License (included; see the file LICENSE)
##  for more details.
##
##  This header is auto-generated and may self-destruct at any time,
##  courtesy of "make update". The original is in ‘scripts/_boilerplate.py’.
##  Thus, do not remove the next line, or insert any blank lines above.
##BP

"""Collect codec info"""

from importlib import import_module
import os
import pkgutil

import logging
logger = logging.getLogger(__name__)

codecs = {}
default_codec = None
DEFAULT="DEFAULT"
def get_codec(name):
	global default_codec

	if not codecs:
		def _check(m):
			global default_codec
			try:
				if isinstance(m,str):
					m = import_module(m)
			except ImportError as ex:
				raise ImportError(m) from ex # pragma: no cover
				# not going to ship a broken file for testing this
			else:
				try:
					codecs[m.CODEC] = m
				except AttributeError:
					pass
				else:
					if getattr(m,'DEFAULT',False):
						default_codec = m

		for a,b,c in pkgutil.walk_packages((os.path.dirname(__file__),), __package__+'.'):
			_check(b)

	if name == DEFAULT:
		return default_codec
		
	return codecs[name]


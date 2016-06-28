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

# generic test setup

from yaml import safe_load

import logging,sys,os
logger = logging.getLogger("tests")

def test_init(who):
	if os.environ.get("TRACE","0") == '1':
		level = logging.DEBUG
	else:
		level = logging.WARN

	logger = logging.getLogger(who)
	logging.basicConfig(stream=sys.stderr,level=level)

	return logger

def load_cfg(cfg):
	"""load a config file"""
	global cfgpath
	if os.path.exists(cfg):
		pass
	elif os.path.exists(os.path.join("tests",cfg)):
		cfg = os.path.join("tests",cfg)
	elif os.path.exists(os.path.join(os.pardir,cfg)):
		cfg = os.path.join(os.pardir,cfg)
	else:
		raise RuntimeError("Config file '%s' not found" % (cfg,))

	cfgpath = cfg
	with open(cfg) as f:
		cfg = safe_load(f)

	return cfg

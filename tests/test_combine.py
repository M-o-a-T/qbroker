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

from qbroker.util import combine_dict
from py.test import raises

def test_basic():
	d1 = combine_dict(
		dict(
			a=1,
		), dict(
			b=2,
		), dict(
			c=3,
			a=4,
		)
	)
	d2 = dict(
			a=1,
			b=2,
			c=3,
	)
	assert d1 == d2

def test_complex():
	x = dict( a=dict(x=10))
	y = dict( b=2, a=dict(x=99,y=20))
	z = dict( c=dict(xx=11), a=dict(z=30))
	d1 = combine_dict(x,y,z)
	d2 = dict(
			a=dict(x=10,y=20,z=30),
			b=2,
			c=dict(xx=11),
	)
	assert d1 == d2
	assert d1['c'] is z['c']
	assert not (d1['a'] is x['a'])
	assert not (d1['a'] is y['a'])
	assert not (d1['a'] is z['a'])

def test_no_merge():
	
	with raises((AssertionError,AttributeError)):
		combine_dict(
			dict(
				a=dict(b=3),
			), dict(
				a=2,
			)
		)
	with raises((AssertionError,AttributeError)):
		combine_dict(
			dict(
				a=2,
			), dict(
				a=dict(b=3),
			)
		)


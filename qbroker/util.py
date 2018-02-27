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

# Utility code

from base64 import b64encode
from collections.abc import Mapping
from importlib import import_module
from datetime import datetime

import pytz
UTC = pytz.UTC
with open("/etc/localtime", 'rb') as tzfile:
    TZ = pytz.tzfile.build_tzinfo(str('local'), tzfile)

class _NOTGIVEN:
    pass

def uuidstr(u=None):
    if u is None:
        import uuid
        u=uuid.uuid1()
    return b64encode(u.bytes, altchars=b'-_').decode('ascii').rstrip('=')

def import_string(name):
    """Import a module, or resolve an attribute of a module."""
    name = str(name)
    try:
        return import_module(name)
    except ImportError:
        if '.' not in name:
            raise
        module, obj = name.rsplit('.', 1)
        try:
            return getattr(import_string(module),obj)
        except AttributeError:
            raise AttributeError(name)

def combine_dict(*d):
    res = attrdict()
    keys = {}
    if len(d) == 0:
        return res
    if len(d) == 1 and isinstance(d[0], attrdict):
        return d[0]
    for kv in d:
        for k,v in kv.items():
            if k not in keys:
                keys[k] = []
            keys[k].append(v)
    for k,v in keys.items():
        if not isinstance(v[0],Mapping):
            for vv in v[1:]:
                assert not isinstance(vv,Mapping)
            res[k] = v[0]
        else:
            res[k] = combine_dict(*v)
    return res

class _missing: pass

class attrdict(dict):
    """A dictionary which can be accessed via attributes, for convenience"""
    def __init__(self,*a,**k):
        super(attrdict,self).__init__(*a,**k)
        self._done = set()

    def __getattr__(self,a):
        if a.startswith('_'):
            return super(attrdict,self).__getattr__(a)
        try:
            return self[a]
        except KeyError:
            raise AttributeError(a)
    def __setattr__(self,a,b):
        if a.startswith("_"):
            super(attrdict,self).__setattr__(a,b)
        else:
            self[a]=b
    def __delattr__(self,a):
        del self[a]

# Default timeout for the cache.
def format_dt(value, format='%Y-%m-%d %H:%M:%S'):
    if isinstance(value,(int,float)):
        value = datetime.utcfromtimestamp(value)
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.astimezone(TZ).strftime(format)


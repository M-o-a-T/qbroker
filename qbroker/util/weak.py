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

from weakref import WeakValueDictionary,KeyedRef,_IterationGuard


class OptWeakValueDictionary(WeakValueDictionary):
    """Mapping class that references values weakly, if possible.
    """
    def __getitem__(self, key):
        if self._pending_removals:
            self._commit_removals()
        o = self.data[key]
        if isinstance(o,KeyedRef):
            o = o()
            if o is None:
                raise KeyError(key)
        return o

    def __contains__(self, key):
        if self._pending_removals:
            self._commit_removals()
        try:
            o = self.data[key]
        except KeyError:
            return False
        if isinstance(o,KeyedRef):
            o = o()
            if o is None:
                return False
        return True

    def __setitem__(self, key, value):
        if self._pending_removals:
            self._commit_removals()
        try:
            self.data[key] = KeyedRef(value, self._remove, key)
        except TypeError:
            self.data[key] = value

    def copy(self):
        if self._pending_removals:
            self._commit_removals()
        new = type(self)()
        for key, val in self.data.items():
            if isinstance(val, KeyedRef):
                val = val()
                if val is None:
                    continue
            new[key] = val
        return new

    __copy__ = copy

    def __deepcopy__(self, memo):
        from copy import deepcopy
        if self._pending_removals:
            self._commit_removals()
        new = type(self)()
        for key, val in self.data.items():
            if isinstance(val, KeyedRef):
                val = val()
                if val is None:
                    continue
            if val is not None:
                new[deepcopy(key, memo)] = val
        return new

    def get(self, key, default=None):
        if self._pending_removals:
            self._commit_removals()
        try:
            o = self.data[key]
        except KeyError:
            return default
        if isinstance(o,KeyedRef):
            o = o()
            if o is None:
                return default
        return o

    def items(self):
        if self._pending_removals:
            self._commit_removals()
        with _IterationGuard(self):
            for k, v in self.data.items():
                if isinstance(v,KeyedRef):
                    v = v()
                    if v is None:
                        continue
                yield k, v

    def keys(self):
        if self._pending_removals:
            self._commit_removals()
        with _IterationGuard(self):
            for k, v in self.data.items():
                if not isinstance(v,KeyedRef) or v() is not None:
                    yield k

    __iter__ = keys

    def itervaluerefs(self):
        """Return an iterator that yields the weak references to the values.

        The references are not guaranteed to be 'live' at the time
        they are used, so the result of calling the references needs
        to be checked before being used.  This can be used to avoid
        creating references that will cause the garbage collector to
        keep the values around longer than needed.

        """
        if self._pending_removals:
            self._commit_removals()
        with _IterationGuard(self):
            yield from self.data.values()

    def values(self):
        if self._pending_removals:
            self._commit_removals()
        with _IterationGuard(self):
            for v in self.data.values():
                if isinstance(v,KeyedRef):
                    v = v()
                    if v is None:
                        continue
                yield v

    def popitem(self):
        if self._pending_removals:
            self._commit_removals()
        while True:
            key, v = self.data.popitem()
            if isinstance(v,KeyedRef):
                v = v()
                if v is None:
                    continue
            return key, v

    def pop(self, key, *args):
        if self._pending_removals:
            self._commit_removals()
        try:
            o = self.data.pop(key)
        except KeyError:
            o = _NOTGIVEN
        else:
            if isinstance(o,KeyedRef):
                o = o()
                if o is None:
                    o = _NOTGIVEN
        if o is _NOTGIVEN:
            if args:
                return args[0]
            else:
                raise KeyError(key)
        else:
            return o

    def setdefault(self, key, default=None):
        o = self.data.get(key,_NOTGIVEN)
        if isinstance(o,KeyedRef):
            o = o()
            if o is None:
                o = _NOTGIVEN
        if o is _NOTGIVEN:
            if self._pending_removals:
                self._commit_removals()
            try:
                self.data[key] = KeyedRef(default, self._remove, key)
            except TypeError:
                self.data[key] = default
            return default
        else:
            return o

    def update(*args, **kwargs):
        if not args:
            raise TypeError("descriptor 'update' of 'WeakValueDictionary' "
                            "object needs an argument")
        self, *args = args
        if len(args) > 1:
            raise TypeError('expected at most 1 arguments, got %d' % len(args))
        dct = args[0] if args else None
        if self._pending_removals:
            self._commit_removals()
        d = self.data
        if dct is not None:
            if not hasattr(dct, "items"):
                dct = dict(dct)
            for key, value in dct.items():
                try:
                    d[key] = KeyedRef(value, self._remove, key)
                except TypeError:
                    d[key] = value
        if len(kwargs):
            self.update(kwargs)

    def valuerefs(self):
        """Return a list of weak references to the values.

        The references are not guaranteed to be 'live' at the time
        they are used, so the result of calling the references needs
        to be checked before being used.  This can be used to avoid
        creating references that will cause the garbage collector to
        keep the values around longer than needed.

        """
        if self._pending_removals:
            self._commit_removals()
        return list(self.data.values())


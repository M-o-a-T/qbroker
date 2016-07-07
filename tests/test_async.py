# -*- coding: UTF-8 -*-

import asyncio
import os
import pytest
import sys
import unittest
import qbroker.util.async

#MY_DIR = os.path.abspath(os.path.dirname(__file__))
#sys.path.append(MY_DIR)

class Main(qbroker.util.async.Main):
    @asyncio.coroutine
    def start(self):
        super().start()
        self.loop.call_later(1,self._tilt)

class TestMain(unittest.TestCase):
    def test_main(self):
        m = Main()
        m.run()
        

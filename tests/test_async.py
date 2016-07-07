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
    def at_start(self):
        super().at_start()
        self.add_cleanup(self.HALT)
        self.loop.call_later(1,self.stop)
    
    def HALT(self):
        self.X = 1

class TestMain(unittest.TestCase):
    def test_main(self):
        m = Main()
        m.run()
        assert m.X == 1
        

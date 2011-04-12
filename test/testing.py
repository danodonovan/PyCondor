#!/usr/bin/env python
# encoding: utf-8
"""
testing.py

Created by Daniel O'Donovan on 2011-04-12.
Copyright (c) 2011 Harvard Medical School. All rights reserved.
"""

import unittest

from condor import CondorJob

class TestCondor(unittest.TestCase):

    def setUp(self):
        self.job = CondorJob( 'test.submit', db_path='job.db', verbose=True)


    def testSubmit( self ):

        job.submit()

if __name__ == '__main__':

    unittest.main()
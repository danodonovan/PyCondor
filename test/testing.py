#!/usr/bin/env python
# encoding: utf-8
"""
testing.py

Created by Daniel O'Donovan on 2011-04-12.
Copyright (c) 2011 Harvard Medical School. All rights reserved.
"""

import sys, os

import unittest
from StringIO import StringIO

# As we don't keep tests in test dir
sys.path.append( '.' )
from condor import CondorJob
from condor_dag import CondorDag

class TestCondor(unittest.TestCase):

    def setUp(self):
        """ Setup a test environment """
        self.job = CondorJob( 'test/test.submit', db_path='job.db', verbose=True)

#     def tearDown(self):
#         self.job.kill()

    def testSubmit( self ):
        """ Test job submission CONDOR """
        saved_stdout = sys.stdout
        try:
            out = StringIO()
            sys.stdout = out

            self.job.submit()

            output = out.getvalue().strip()
            assert output.find('Submitting job(s)')

        finally:

            sys.stdout = saved_stdout


    def testKill( self ):
        """ Test job remove CONDOR """
        saved_stdout = sys.stdout
        try:
            out = StringIO()
            sys.stdout = out

            self.job.kill()

            output = out.getvalue().strip()
            assert output.find('job(s) have been marked for removal.')

        finally:

            sys.stdout = saved_stdout


class TestDag(unittest.TestCase):

    def setUp(self):
        """ Setup a test environment """
        self.job = CondorDag( 'test/test.dag', db_path='job.db', verbose=True)


    def testSubmit( self ):
        """ Test job submission DAG """
        saved_stdout = sys.stdout
        try:
            out = StringIO()
            sys.stdout = out

            self.job.submit()

            output = out.getvalue().strip()
            assert output.find('Submitting job(s)')

        finally:

            sys.stdout = saved_stdout

    def testKill( self ):
        """ Test job remove DAG """
        saved_stdout = sys.stdout
        try:
            out = StringIO()
            sys.stdout = out

            self.job.kill()

            output = out.getvalue().strip()
            assert output.find('job(s) have been marked for removal.')

        finally:

            sys.stdout = saved_stdout



if __name__ == '__main__':

    unittest.main()
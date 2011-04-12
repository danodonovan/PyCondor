#!/usr/bin/env python
# encoding: utf-8
"""
condor_new.py

TBD Change the name of this file

Created by Daniel O'Donovan on 2011-04-11.
Copyright (c) 2011 Harvard Medical School. All rights reserved.
"""

import sys, os
import time

import subprocess

# import anydbm as db_manager
import shelve as db_manager

# import unittest
# 
# class untitledTests(unittest.TestCase):
#     def setUp(self):
#         pass

submit_exe = 'condor_submit'
condor_submit_success_string = 'job(s) submitted to cluster'

class CondorDbInfo:
    """ Class to hold information about the condor job 
    """
    def __init__(self, condor_id, parent_id, **kwargs ):

        self.condor_id = condor_id
        self.parent_id = parent_id
        self.status_history = []
        self.current_status = 'New job'
        self.notes = {}

        for key in kwargs:
            self.notes[key] = kwargs[key]

    def __repr__( self ):
        return "CondorDbInfo()"

    def __str__(self):
        if self.condor_id and self.current_status:
            return "CondorDbInfo: condor %10.1f - %s" % \
                    ( self.condor_id, self.current_status )
        if self.condor_id and self.parent_id and self.current_status:
            return "CondorDbInfo: condor dag child %10.1f (%10.1f) - %s" % \
                    ( self.condor_id, self.parent_id, self.current_status )
        else:
            return "CondorDbInfo: condor new"

    def record( self, information ):
        """ update self in light of new status information
              record (old) current status
        """
        now = time.strftime('%Y.%b.%d:%H-%M-%S', time.gmtime())
        self.status_history.append( self.current_status )
        self.current_status = (now, information)

class BaseJobModel(object):
    """ Model for tracking in database: 
            Will start a new default DB if one is not specified
            or specified DB is not found. 
    """
    def __init__(self, db_path=None):

        # non condor parameters
        self.info = None

        if not db_path:
            self._open_db( 'dev.db' )
        else:
            self._open_db( db_path )

    def __repr__( self ):
        return "BaseJobModel()"

    def __str__(self):
        if self.info:
            return "BaseJobModel: condor %10.1f - %s" % \
                ( self.info.condor_id, self.info.current_status )
        return "BaseJobModel: Not submitted"

    def _open_db( self, path ):
        """ Open the DB for condor job tracking """
        self.db = db_manager.open(path, 'c')

    def _call_condor( self, args ):
        """ Generic function for calling host OS (and so condor) """

        if type( args ) != list: args = [args]

        try:
            submit = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        except OSError, e:
            if self.verbose:
                print 'DEBUG _call_condor - cannot find %s' % ' '.join( args )
                print 'DEBUG Are you running in the correct environment?'
                print 'DEBUG %s' % e
            raise( OSError )

        stdOut = submit.stdout.readlines()
        stdErr = submit.stderr.read()

        return (stdOut, stdErr)

    def _store_state( self ):
        """ Store the current state information for this job """
        if self.info: self.db[ str(self.info.condor_id) ] = self.info

    def _read_state( self ):
        """ Read and return the current state information for this job """
        if self.info: return self.db[ str(self.info.condor_id) ]

    def submit( self ):
        """ Submits a condor job to using the script provided to init
            - simply calls condor_submit on given submission script
            - collects condor job id if successful
        """

        (stdOut, stdErr) = self._call_condor( [submit_exe, self.script] )

        if not len(stdErr) and len(stdOut):
            for line in stdOut:
                if line.find(condor_submit_success_string) > 0:
                    condor_id = float( line.split(condor_submit_success_string)[-1] )
                    parent_id = None
                    self.info = CondorDbInfo( condor_id, parent_id )

        else:
            if self.verbose:
                print 'DEBUG Unexpected output from %s:' % submit_exe
                for line in stdOut:
                    print 'DEBUG %s' % line
            return

        # store state of job
        self.info.record( 'Submitted' )
        self._store_state()


    ## These functions should not be tied to a given condor / dag type
    def status( self ):
        """ Checks to see if the job is running, idle, held or done
        """

        if not self.info:
            if self.verbose:
                print 'DEBUG Status: Job not yet submitted to grid'
            return

        (stdOut, stdErr) = self._call_condor( [ 'condor_q', '-format', 
                                                '%s', 'substr("?IRXCH",JobStatus,1)', 
                                                '%f' % self.info.condor_id] )

        if len(stdOut) > 0:
            stdOut = stdOut[0]

        if len( stdErr ):
            if self.verbose:
                print 'DEBUG CondorJob reported status error:\n%s' % (stdErr)
            return

        if not len( stdOut ):   job_status = 'Completed'

        elif stdOut == 'I':     job_status = 'Idle'

        elif stdOut == 'R':     job_status = 'Running'

        elif stdOut == 'H':     job_status = 'Held'

        else:                   job_status = 'Unknown: %s' % stdOut

        if self.verbose: print 'DEBUG CondorJob %s %s' % (self.info.condor_id, job_status)

        # store state of job
        self.info.record( job_status )
        self._store_state()

    def kill( self ):
        """ Kill job """

        if not self.info:
            if self.verbose: print 'DEBUG Status: Job not yet submitted to grid'
            return

        (stdOut, stdErr) = self._call_condor( ['condor_rm', '%f' % self.info.condor_id] )

        if len( stdErr ):
            job_status = 'not killed'
            if self.verbose: print 'DEBUG CondorJob %s %s\n%s' % (self.info.condor_id, job_status, stdErr)
            return

        job_status = 'Killed'

        # store state of job
        self.info.record( job_status )
        self._store_state()


class CondorJob(BaseJobModel):
    """ Simple Class - given that we already have a valid submission 
        script, submit it and track it.
    """

    def __init__(self, script, **kwargs ):

        if 'db_path' in kwargs:
            BaseJobModel.__init__(self, kwargs['db_path'] )
        else:
            BaseJobModel.__init__(self)

        self.verbose = 'verbose' in kwargs.keys()

        if not os.path.isfile( script ):
            if self.verbose:
                print 'DEBUG Cannot find file %s' % script
            raise
        else:
            self.script = script


if __name__ == '__main__':
    # unittest.main()

    job = CondorJob( 'test.submit', db_path='job.db', verbose=True)

    job.submit()
    print job

    time.sleep( 10 )

    job.status()
    print job

    time.sleep( 20 )

    job.status()
    print job




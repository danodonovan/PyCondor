#!/usr/bin/env python
# encoding: utf-8
"""
untitled.py

Created by Daniel O'Donovan on 2011-04-09.
Copyright (c) 2011 Harvard Medical School. All rights reserved.
"""

import os
import random, time
import subprocess

class JobModel(object):
    """ Model for tracking in database
    """
    def __init__(self, db_path=None):
        #super(JobModel, self).__init__()

        ## setting non-condor parameters
        # sort of unique ID to keep track of job
        rand                  = random.uniform( 0., 1. )
        self.internal_id      = time.time() + rand
        self.internal_id_str  = time.strftime('%Y%b%d_%H-%M-%S', time.gmtime()) + '6.5%f' % rand
        # non condor parameters
        self.submitted  = False
        self.condor_id  = None

        self.job_status = None

        if not db_path:
            self._init_db()
        else:
            self._attach_db( db_path )

    def _init_db( self ):
        """ Initiate the DB for condor job tracking """
        print 'Initiating'

    def _attach_db( self, path ):
        """ Attach to existing DB for condor job tracking """
        print 'Attaching %s' % path

    def _call_condor( self, args ):
        """ Generic function for calling host OS (and so condor) """

        if type( args ) != list: args = [args]

        try:
            submit = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        except OSError, e:
            if self.verbose:
                print '&&& _call_condor - cannot find %s' % ' '.join( args )
                print '&&& Are you running in the correct environment?'
                print '&&& %s' % e
            raise( OSError )

        stdOut = submit.stdout.readlines()
        stdErr = submit.stderr.read()

        return (stdOut, stdErr)


class CondorJob(JobModel):
    """ Simple Class - given that we already have a valid submission 
        script, submit it and track it.
    """

    def __init__(self, script, **kwargs ):
        #super(CondorJob, self).__init__()
        if 'db_path' in kwargs:
            JobModel.__init__(self, kwargs['db_path'] )
        else:
            JobModel.__init__(self)

        self.verbose = 'verbose' in kwargs.keys()

        if not os.path.isfile( script ):
            if self.verbose:
                print '&&& Cannot find file %s' % script
            raise
        else:
            self.script = script


    def submit( self ):
        """ Submits a condor job to using the script provided to init
            - simply calls condor_submit on given submission script
            - collects condor job id if successful
        """

        (stdOut, stdErr) = self._call_condor( ['condor_submit', self.script] )

        if self.verbose:
            print 'stdOut', stdOut
            print 'stdErr', stdErr

        if len( stdErr ) == 0:
            self.submitted = True
        else:
            if self.verbose:
                print '&&& CondorJob %s reported submission error:\n%s' % (self.internal_id_str, stdErr)
            raise

        if len( stdOut ) > 1:
            """ Successful submit looks something like
            Submitting job(s).
            1 job(s) submitted to cluster 14738433. """
            try:
                self.condor_id = float( stdOut[1].split('job(s) submitted to cluster')[-1] )
            except ValueError:
                if self.verbose:
                    print '&&& Unexpected output from condor_submit:'
                    for line in stdOut:
                        print '&&& %s' % line
                raise

    def status( self ):
        """ Checks to see if the job is running, idle, held or done
        """

        if not self.submitted:
            if self.verbose:
                print '&&& Status: Job %s not yet submitted to grid' % self.internal_id_str
            return

        (stdOut, stdErr) = self._call_condor( [ 'condor_q', '-format', 
                                                '%s', 'substr("?IRXCH",JobStatus,1)', 
                                                '%f' % self.condor_id] )
        stdOut = stdOut[0]

        if len( stdErr ) != 0:
            if self.verbose:
                print '&&& CondorJob %s reported status error:\n%s' % (self.internal_id_str, stdErr)
            return

        if len( stdOut ) == 0:
            if self.verbose:
                print '&&& CondorJob %s completed' % (self.internal_id_str)
            self.job_status = 'Completed'
        elif stdOut == 'I':
            if self.verbose:
                print '&&& CondorJob %s idle' % (self.internal_id_str)
            self.job_status = 'Idle'
        elif stdOut == 'R':
            if self.verbose:
                print '&&& CondorJob %s running' % (self.internal_id_str)
            self.job_status = 'Running'
        elif stdOut == 'H':
            if self.verbose:
                print '&&& CondorJob %s held' % (self.internal_id_str)
            self.job_status = 'Held'
        else:
            if self.verbose:
                print '&&& CondorJob %s undetermined status %s' % (self.internal_id_str, stdOut)
            self.job_status = 'Unknown: %s' % stdOut


    def kill( self, onlyIfHeld=False ):
        """ Kill job """

        if onlyIfHeld:
            self.status()
            if self.job_status != 'Held':
                return

        (stdOut, stdErr) = self._call_condor( ['condor_rm', '%f' % self.condor_id] )

        if len( stdErr ) != 0:
            if self.verbose:
                print '&&& CondorJob %s not killed, error:\n%s' % (self.internal_id_str, stdErr)
            return








if __name__ == '__main__':

    #job = CondorJob( 'test_code/test.submit', verbose=True)
    job = CondorJob( 'test_code/test.submit', db_path='/here/db', verbose=True)

    print job.internal_id_str

    job.submit()

    time.sleep( 10 )

    job.status()

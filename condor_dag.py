#!/usr/bin/env python
# encoding: utf-8
"""
condor_dag.py

Created by Daniel O'Donovan on 2011-04-11.
Copyright (c) 2011 Harvard Medical School. All rights reserved.
"""

import sys
import os

from condor_job import JobModel

class CondorDat(JobModel):
    """ Class similar to CondorJob - but DAG specific """

    def __init__(self, dag, **kwargs ):

        if 'db_path' in kwargs:
            JobModel.__init__(self, kwargs['db_path'] )
        else:
            JobModel.__init__(self)

        self.verbose = 'verbose' in kwargs.keys()

        if not os.path.isfile( dag ):
            if self.verbose:
                print '&&& Cannot find DAG file %s' % dag
            raise
        else:
            self.dag = dag

    def submit( self ):
        """ Submits a condor dag
            - simply calls condor_submit_dag on given DAG file
            - collects condor job id
            - records DAG log file for future status updates
        """

        (stdOut, stdErr) = self._call_condor( ['condor_submit_dag', self.dag] )

        if self.verbose:
            print 'stdOut', stdOut
            print 'stdErr', stdErr

        if len( stdErr ) == 0:
            self.submitted = True
            self._store_state( 'DAG submitted' )
        else:
            if self.verbose:
                print '&&& CondorDag %s reported submission error:\n%s' % (self.internal_id_str, stdErr)
                self._store_state( 'DAG submit failed' )
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
                    self._store_state( stdOut )
                raise



if __name__ == '__main__':

    pass


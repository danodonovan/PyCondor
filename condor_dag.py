#!/usr/bin/env python
# encoding: utf-8
"""
condor_dag.py

Created by Daniel O'Donovan on 2011-04-11.
Copyright (c) 2011 Harvard Medical School. All rights reserved.
"""

import sys
import os

from condor_new import BaseJobModel

submit_exe = 'condor_submit_dag'
condor_submit_success_string = 'job(s) submitted to cluster'
condor_dag_log_string = 'Log of the life of condor_dagman itself'

class CondorDag(BaseJobModel):
    """ Class similar to CondorJob - but DAG specific """

        def __init__(self, script, **kwargs ):

            if 'db_path' in kwargs:
                BaseJobModel.__init__(self, kwargs['db_path'] )
            else:
                BaseJobModel.__init__(self)

        self.verbose = 'verbose' in kwargs.keys()

        if not os.path.isfile( dag ):
            if self.verbose:
                print '&&& Cannot find DAG file %s' % dag
            raise
        else:
            self.dag = dag


    def submit( self ):
        """ ** Over riding condor_submit to record dag log file path ** 
            Submits a condor dag
            - simply calls condor_submit_dag on given DAG file
            - collects condor job id
            - records DAG log file for future status updates
        """

        (stdOut, stdErr) = self._call_condor( [submit_exe, self.dag] )

        if not len(stdErr) and len(stdOut):
            for line in stdOut:
                if line.find(condor_submit_success_string) > 0:
                    condor_id = float( line.split(condor_submit_success_string)[-1] )
                    parent_id = None

                if line.find(condor_dag_log_string):
                    self.dag_log = line.split(condor_dag_log_string)[-1]

            self.info = CondorDbInfo( condor_id, parent_id, dag_log=self.dag_log )

        else:
            if self.verbose:
                print 'DEBUG Unexpected output from %s:' % submit_exe
                for line in stdOut:
                    print 'DEBUG %s' % line
            return

        # store state of job
        self.info.record( 'Submitted' )
        self._store_state()


    def dag_status( self ):
        """ read the dag log file, and put child jobs into DB """

        with open( self.dag_log ) as d:

            for line in d.readlines():

                if line.find( 'Job submitted from host' ):

                    x = line.split( '(' )[-1].split( ')' )

                    child_id = float( x[0].split('.')[0] )
                    info = 'Submitted: ' + ''.join(y[1].split()[:2])

                    if not self.db.has_key( child_id ):

                        self._store_child_state( child_id, info )

    def dag_child_status( self ):
        """ Update the DAG child status """

if __name__ == '__main__':

    job = CondorJob( 'test.dag', db_path='job.db', verbose=True)

    job.submit()
    print job

    time.sleep( 10 )

    job.status()
    print job

    time.sleep( 20 )

    job.status()
    print job


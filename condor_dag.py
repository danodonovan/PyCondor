#!/usr/bin/env python
# encoding: utf-8
"""
condor_dag.py

Created by Daniel O'Donovan on 2011-04-11.
Copyright (c) 2011 Harvard Medical School. All rights reserved.
"""

import sys, os
import time
import re

from condor import BaseJobModel, CondorDbInfo


class CondorDag(BaseJobModel):
    """ Class similar to CondorJob - but DAG specific """

    def __init__(self, script, **kwargs ):

        if 'db_path' in kwargs:
            BaseJobModel.__init__(self, script, kwargs['db_path'] )
        else:
            BaseJobModel.__init__(self, script)

        self.verbose = 'verbose' in kwargs.keys()

        self.log = {}

        self.submit_exe = 'condor_submit_dag'

        self.condor_log_string = 'Log of the life of condor_dagman itself'

    def _parse_submit_output( self, stdOut, stdErr ):

        if self.verbose: print 'DEBUG %s' % type( stdOut )
        if self.verbose: print 'DEBUG stdOut %s' % stdOut
        if self.verbose: print 'DEBUG stdErr %s' % stdErr

        (condor_id, parent_id) = (None, None)

        if not len(stdErr) and len(stdOut):
            for line in stdOut:

                if self.condor_submit_success_string in line:
                    condor_id = float( line.split(self.condor_submit_success_string)[-1] )

                if self.condor_log_string in line:
                    self.dag_log = line.split(self.condor_log_string)[-1].split(':')[-1].strip()

            if not condor_id:
                if self.verbose: print 'DEBUG : no condor_id'

            if not self.dag_log:
                if self.verbose: print 'DEBUG : no daglog'

        else:
            if self.verbose:
                print 'DEBUG Unexpected output from %s:' % self.submit_exe
                for line in stdOut:
                    print 'DEBUG %s' % line
            return (None, None)

        return (condor_id, parent_id)


if __name__ == '__main__':

    job = CondorDag( 'test.dag', db_path='job.db', verbose=True)

    job.submit()
    print job

    time.sleep( 10 )

    job.status()
    print job

    time.sleep( 20 )

    job.status()
    print job


#     job.dag_log_parse()
#     for p in job.log: print p
# 
#     time.sleep( 20 )
# 
#     job.dag_log_parse()
#     for p in job.log: print p


## Scrap -
## Dagman doesn't log it's children
# 
#     def dag_log_parse( self ):
#         """ Inquisitive coding - this function may evolve 
#             Would be nice to use SQLAlchemy to track everything here
#         """
# 
#         tag_num = re.compile( r'^[0-9]{3}' )
#         tag_id  = re.compile( r'\(.+\)' )
# 
#         if not self.dag_log:
#             if self.verbose: print 'DEBUG - no dag_log'
#             return
# 
#         if not os.path.isfile( self.dag_log ):
#             if self.verbose: print 'DEBUG - no dag_log file %s' % self.dag_log
#             return
# 
#         with open( self.dag_log ) as d:
# 
#             for line in d.readlines():
# 
#                 dagid_m = tag_num.search( line, 0 )
#                 if dagid_m:
# 
#                     dagid_i = int(line[ dagid_m.start() : dagid_m.end() ])
# 
#                     conid_m = tag_id.search( line )
#                     if conid_m:
#                         conid_i = int(float(line[ conid_m.start()+1 : conid_m.end()-5 ]))
#                     else:
#                         if self.verbose: print 'DEBUG %s' % 'No Condor ID?'
# 
#                     if not self.log.has_key( dagid_i ):
#                         self.log[ dagid_i ] = [ conid_i ]
#                     else:
#                         self.log[ dagid_i ].append( conid_i )

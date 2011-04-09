#!/usr/bin/env python
# encoding: utf-8
"""
condor_class.py

Created by Daniel O'Donovan on 2011-02-06.
Copyright (c) 2011 Harvard University. All rights reserved.
"""

import sys, os
import subprocess, time, random
import tempfile

# import xml.dom.minidom

from condor_submit_template import createSubmitScript

class CondorJob(object):
    """ Simple class for a single Condor Job 
    """

    def __init__(self, **kwargs):
        super(CondorJob, self).__init__()

        ## setting non-condor parameters
        # sort of unique ID to keep track of job
        rand                  = random.uniform( 0., 1. )
        self.internal_id      = time.time() + rand
        self.internal_id_str  = time.strftime('%Y%b%d_%H-%M-%S', time.gmtime()) + '6.5%f' % rand
        # non condor parameters
        self.submitted  = False
        self.condor_id  = None

        self.job_status = None

        ## setting condor parameters
        self.args = {}

        required_arguments = [
            'Executable',
            'Output',
            'Error',
            'Log',
            'Should_transfer_files',
            'When_to_transfer_output',
            'Transfer_output_files',
            'Notification',
            'Priority',
            'Requirements',
            'Periodic_remove',
            'X509userproxy',
            'Universe', ]

    self.verbose = 'verbose' in kwargs.keys()

    # set the required arguments, as in keyword args
    for key in kwargs:
        if key in required_arguments:
            if self.verbose:
                print 'Setting required argument %s' % key,

            self.args[key] = str( kwargs[key] )

            if self.verbose:
                print 'set to  %s' % self.args[key]


    # set required args that weren't specified to defaults
    for key in required_arguments:
        if key not in self.args.keys() and not in kwargs:
            if self.verbose:
                print 'Missing required key %s' % key,

            if   key == 'outputFile': self.args[key] = 'job_%s.out' % self.internal_id_str
            elif key == 'errorFile':  self.args[key] = 'job_%s.err' % self.internal_id_str
            elif key == 'logFile':    self.args[key] = 'job_%s.log' % self.internal_id_str
            elif key == 'priority':   self.args[key] = '100'
            elif key == 'timeout':    self.args[key] = '21600'
            elif key == 'notify_User':self.args[key] = ''
            elif key == 'universe':   self.args[key] = 'vanilla'
            elif key == 'should_transfer_files':  self.args[key] = ''
            elif key == 'when_to_transfer_output':self.args[key] = 'ON_EXIT'

            elif key == 'periodic_remove':        
                self.args[key] = '((JobStatus == 2) && ((CurrentTime - EnteredCurrentStatus) > $(Timeout) ))'

            elif key == 'requirements':
                self.args[key] = '(Arch == "X86_64") && (OpSys == "LINUX")'

            elif key == 'transfer_input_files':
                if type( transfer_input_files ) == list:
                    self.args[key] = kwargs[key]
                else:
                    self.args[key] = [kwargs[key]]

            elif key == 'transfer_output_files':
                if type( transfer_output_files ) == list:
                    self.args[key] = kwargs[key]
                else:
                    self.args[key] = [kwargs[key]]


            if self.verbose:
                print 'set to  %s' % self.args[key]

        else: # end key not in self.args.keys() and not in kwargs:
            print 'Peculiar key %s' % key

    # set all other arguments in keyword args
    for key in kwargs:
        if key not in self.args.keys():
            if self.verbose:
                print 'Extra argument %s' % keys

            self.args[key] = kwargs[key]

            if self.verbose:
                print 'set to  %s' % self.args[key]


    def _call_condor( self, args ):
    """ Generic function for calling host OS (and so condor) """

        if type( args ) != list: args = [args]

        try:
            submit = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        except OSError:
            print '&&& CondorJob - cannot find %s' % ' '.join( args )
            print '&&& Are you running in the correct environment?'
            return

        stdOut = submit.stdout.readlines()
        stdErr = submit.stderr.read()

        return (stdOut, stdErr)


    def submit( self, writeSubmitFile=None ):
        """ Submits a job to GRID 
          - simply calls condor_submit on given submission script
          - collects condor job id if successful
        """

        submitString = createSubmitScript( self )

        if writeSubmitFile:
            fd = open( writeSubmitFile, 'w' ).write( submitString )
            submitFile = writeSubmitFile
        else:
            fd = tempfile.NamedTemporaryFile( delete=False )
            fd.write( submitString )
            fd.seek( 0 )
            submitFile = fd.name

        if not os.path.isfile( submitFile ):
            print '&&& Cannot find file %s' % submitFile
            return

        (stdOut, stdErr) = self._call_condor( ['condor_submit', submitFile] )

        if len( stdErr ) == 0:
            self.submitted = True
        else:
            print '&&& CondorJob %s reported submission error:\n%s' % (self.internal_id_str, stdErr)
            return

        if len( stdOut ) > 1:
            """ Successful submit looks something like
            Submitting job(s).
            1 job(s) submitted to cluster 14738433. """
            try:
                self.condor_id = float( stdOut[1].split('job(s) submitted to cluster')[-1] )
            except ValueError:
                print '&&& Unexpected output from condor_submit:'
                for line in stdOut:
                    print '&&& %s' % line

        if not writeSubmitFile:
            os.unlink( fd.name )


    def status( self ):
        """ Checks to see if the job is running, idle, held or done
        """

        (stdOut, stdErr) = self._call_condor( [ 'condor_q', '-format', 
                                                '%s', 'substr("?IRXCH",JobStatus,1)', 
                                                '%f' % self.condor_id] )
        stdOut = stdOut[0]

        # (stdOut, stdErr) = self._call_condor( ['condor_q', '-xml', self.condor_id] )
        # dom = xml.dom.minidom.parseString( stdOut )

        if len( stdErr ) != 0:
            print '&&& CondorJob %s reported status error:\n%s' % (self.internal_id_str, stdErr)
            return

        if len( stdOut ) == 0:
            print '&&& CondorJob %s completed' % (self.internal_id_str)
            self.job_status = 'Completed'
        elif stdOut == 'I':
            print '&&& CondorJob %s idle' % (self.internal_id_str)
            self.job_status = 'Idle'
        elif stdOut == 'R':
            print '&&& CondorJob %s running' % (self.internal_id_str)
            self.job_status = 'Running'
        elif stdOut == 'H':
            print '&&& CondorJob %s held' % (self.internal_id_str)
            self.job_status = 'Held'
        else:
            print '&&& CondorJob %s unknown status %s' % (self.internal_id_str, stdOut)
            self.job_status = 'Unknown: %s' % stdOut


    def kill( self, onlyIfHeld=False ):
        """ Kill job """

        if onlyIfHeld:
            self.status()
            if self.job_status != 'Held':
                return

        (stdOut, stdErr) = self._call_condor( ['condor_rm', '%f' % self.condor_id] )

        if len( stdErr ) != 0:
            print '&&& CondorJob %s not killed, error:\n%s' % (self.internal_id_str, stdErr)
            return




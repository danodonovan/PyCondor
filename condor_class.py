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

import xml.dom.minidom

from condor_submit_template import createSubmitScript

class CondorJob(object):
  """ Simple class to cover a single Condor Job 
      should be able  to begin execution
                      log and update job status
  """

  def __init__(self, executable, x509userproxy, arguments, 
        outputFile=None, errorFile=None, logFile=None, initialDir='.',
        priority=100, timeout=21600, notification='Never', notify_User='', universe='vanilla', 
        should_transfer_files=False, when_to_transfer_output='ON_EXIT',
        periodic_remove='((JobStatus == 2) && ((CurrentTime - EnteredCurrentStatus) > $(Timeout) ))',
        requirements='(Arch == "X86_64") && (OpSys == "LINUX")', 
        transfer_input_files=None, transfer_output_files=None, other_commands=None):
    super(CondorJob, self).__init__()

    # sort of unique ID to keep track of job
    rand                  = random.uniform( 0., 1. )
    self.internal_id      = time.time() + rand
    self.internal_id_str  = time.strftime('%Y%b%d_%H-%M-%S', time.gmtime()) + '6.5%f' % rand

    ## These are generic condor arguments
    self.c_Executable     = executable
    self.c_Timeout        = timeout
    self.c_X509userproxy  = x509userproxy
    self.c_Priority       = priority
    self.c_Notification   = notification
    self.c_Notify_User    = notify_User
    self.c_Universe       = universe
    self.c_Should_transfer_files    = 'YES' if should_transfer_files else 'NO'
    self.c_When_to_transfer_output  = when_to_transfer_output
    self.c_Periodic_remove          = periodic_remove
    self.c_Requirements             = requirements

    ## These are more condor job specific
    if type( arguments ) == list:
      self.c_Arguments  = arguments
    else:
      print '&&& CondorJob \'arguments\' must be a list'
      self.c_Arguments  = []

    self.c_Output = outputFile if outputFile else 'job_%s.out' % self.internal_id_str
    self.c_Error  = errorFile if errorFile else 'job_%s.err' % self.internal_id_str
    self.c_Log    = logFile if logFile else 'job_%s.log' % self.internal_id_str

    self.c_initialdir  = initialDir

    if type( transfer_input_files ) == list:
      self.c_Transfer_input_files = transfer_input_files
    else:
      self.c_Transfer_input_files = [transfer_input_files]

    if type( transfer_output_files ) == list:
      self.c_Transfer_output_files   = transfer_output_files
    else:
      self.c_Transfer_output_files   = [transfer_output_files]

    self.c_Other  = other_commands

    # non condor parameters
    self.submitted  = False
    self.condor_id  = None

    self.job_status = None

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




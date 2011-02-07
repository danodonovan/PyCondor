#!/usr/bin/env python
# encoding: utf-8
"""
test_CondorJob.py

Created by Daniel O'Donovan on 2011-02-06.
Copyright (c) 2011 Harvard University. All rights reserved.
"""

import sys
sys.path.append( '../' )

from condor_class import CondorJob

if __name__ == '__main__':

  executable = 'helloWorld.sh'
  x509userproxy = '$ENV(X509_USER_PROXY)'
  arguments = ['one', 'two', 'three']
  # outputFile='hw.out'
  # errorFile='hw.err'
  # logFile='hw.log'
  should_transfer_files=True
  transfer_input_files = 'input.txt'
  transfer_output_files = 'results_data_out'

  job = CondorJob( executable, x509userproxy, arguments,
      should_transfer_files=should_transfer_files,
      transfer_input_files=transfer_input_files, transfer_output_files=transfer_output_files )

  # job = CondorJob( executable, x509userproxy, arguments, outputFile, errorFile, logFile, 
  #     should_transfer_files=should_transfer_files,
  #     transfer_input_files=transfer_input_files, transfer_output_files=transfer_output_files )

  # job.submit( writeSubmitFile='test.submit' )

  job.submit( )
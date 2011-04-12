#! /bin/sh

# Check for proper number of command line args.

EXPECTED_ARGS=3
E_BADARGS=-1

if [ $# -ne $EXPECTED_ARGS ]
then
  echo "Usage: `basename $0` {arg 1} {arg 2} {arg 3}"
  echo "ERROR - need to supply arguments!!" >> results_data_out
  exit $E_BADARGS
fi

# Stdout testing
echo "*** Hello World!"
echo "*** This script will eventually replace refine.csh"
echo "*** But for now - I'm process id $$ on" `hostname`
echo "*** Running as: $0" "$@"

# Stderr testing
echo "&&& This is sent to standard error" 1>&2

echo "sleep 60"

# checking for transferred files
echo "This should be returned" >> results_data_out
echo `cat input.txt` >> results_data_out

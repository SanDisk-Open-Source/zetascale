#!/bin/sh -x

DEBUG=
#DEBUG=true ||

# Wrapper for running tests which require the sdfagent to work.
#
# XXX: We should search for a : (like mpi) and assign everything to the LHS as
# mpi/sdfagent arguments and everything on the RHS as test program

# $Id: agent_wrapper.sh 1662 2008-06-17 02:12:14Z drew $

$DEBUG echo "TOP is $TOP" 1>&2

TIMEOUT=40

# Bourne shell provides no wait that blocks on any number of processes and 
# still returns status, so only wait on one process (the sleeper) and make
# all others subshells which kill it.
(sleep $TIMEOUT; ($DEBUG echo "timeout complete" 1>&2)) &
TIMEOUTPID=$! 

# Killing the subshell causes it to terminate correctly but the child process
# continues to run reparented to init.  So run commands as background
# children of the subshell, trap signals, and forward signals.
#(mpirun -np 2 $@ &
#    pid=$!; let "pid0 = $pid + 2"; let "pid1 = $pid + 3"; trap "kill $pid0 $pid1" SIGTERM; wait $pid0; wait $pid1; status=$?;
#    kill -TERM $TIMEOUTPID; 
#    ($DEBUG echo "$1 done" 1>&2); exit $status
#) &
(mpirun -np 2 $@ --msg_mpi 2 --log=sdf/sdfmsg=debug &
    pid=$!; trap "kill $pid" SIGTERM; wait $pid; status=$?;
    kill -TERM $TIMEOUTPID; 
    ($DEBUG echo "$1 done" 1>&2); exit $status
) &
MPIPID=$!
$DEBUG echo "MPI started PID $MPIPID" 1>&2

# example2_sdfclient never terminates without the startup delay
#sleep 4

# Note that the client retries so agent crashing immediately causes 
# client to hang.
#echo "Running $@" 1>&2
#($@ &
#    pid=$!; trap "kill $pid" SIGTERM; wait $pid; status=$?;
#    kill -TERM $TIMEOUTPID; 
#    ($DEBUG echo "test done" 1>&2); exit $status
#) &
#TESTPID=$!

wait $TIMEOUTPID
TIMEOUT_STATUS=$?
$DEBUG echo "Timeout done status $TIMEOUT_STATUS" 1>&2

kill $MPIPID
#kill $TESTPID

wait $MPIPID
MPI_STATUS=$?

#wait $TESTPID
#TEST_STATUS=$?


# 143 = SIGTERM from kill.  Not 143 means somethign bad since we don't exit
if [ $MPI_STATUS -ne 143 ]; then
    STATUS=$MPI_STATUS
# 143 = SIGTERM from kill.  Not 143 means timeout was cancelled
elif [ $TIMEOUT_STATUS -ne 143 ]; then
    STATUS=$TIMEOUT_STATUS
# Test should have terminated with status 0.
else
    STATUS=0 
fi

echo "MPI $MPI_STATUS TIMEOUT $TIMEOUT_STATUS" 1>&2

#exit $STATUS
exit $STATUS

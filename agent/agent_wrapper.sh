#!/bin/bash -x

# Wrapper for running tests which require the sdfagent to work.
#
# $Id: agent_wrapper.sh 5462 2009-01-08 18:39:39Z darryl $

#
# Arguments are split at ':'.  Where no : exists all arguments are 
# test program arguments; otherwise arguments before are agent
# args and arguments after test args.
#
# Not coincidentally, this is the option separator for MPI
#
# Examples:
# agent_wrapper.sh a.out b c
#	Runs a.out with b c
# agent_wrapper.sh --log default=debug : a.out b c
# 	Runs agent with --log default=debug
#       Runs a.out with b c
#

# Config parameters
TIMEOUT=60
CLIENT_DELAY=15
DEBUG=
# To disable debug logging
#DEBUG=true ||

# Capture my PID to pass to children
RUNNER="runner=$$"

$DEBUG echo "TOP is $TOP" 1>&2
$DEBUG echo "PROPERTIES_FILE is $PROPERTIES_FILE" 1>&2

declare -a TEST_ARGS
declare -a AGENT_ARGS

# XXX: Should probably parse test args out as a third part
while [ $# -gt 0 -a "$1" != ":" ]; do
    TEST_ARGS[${#TEST_ARGS[@]}]=$1
    shift
done

if [ $# -ne 0 ]; then
    # Discard :
    shift

    AGENT_ARGS=(${TEST_ARGS[@]})
    TEST_ARGS=()

    while [ $# -gt 0 ]; do
	TEST_ARGS[${#TEST_ARGS[@]}]=$1
	shift
    done
fi

AGENT_ARGS[${#AGENT_ARGS[@]}]=--nop
AGENT_ARGS[${#AGENT_ARGS[@]}]=$RUNNER

# Needed to run the tests in multi-node
AGENT_ARGS[${#AGENT_ARGS[@]}]=--msg_mpi
AGENT_ARGS[${#AGENT_ARGS[@]}]=2

AGENT_ARGS[${#AGENT_ARGS[@]}]=--property_file
AGENT_ARGS[${#AGENT_ARGS[@]}]=$PROPERTIES_FILE

AGENT_ARGS[${#AGENT_ARGS[@]}]=--recover
AGENT_ARGS[${#AGENT_ARGS[@]}]=0

$DEBUG echo AGENT_ARGS ${#AGENT_ARGS[@]} "${AGENT_ARGS[@]}"
$DEBUG echo TEST_ARGS ${#TEST_ARGS[@]} "${TEST_ARGS[@]}"


# Bourne shell provides no wait that blocks on any number of processes and 
# still returns status, so only wait on one process (the sleeper) and make
# all others subshells which kill it.
(sleep $TIMEOUT &
    pid=$!; trap "kill $pid" SIGTERM; wait $pid; status=$?
    ($DEBUG echo "timeout complete" 1>&2); exit $status
) &
TIMEOUTPID=$! 

# Killing the subshell causes it to terminate correctly but the child process
# continues to run reparented to init.  So run commands as background
# children of the subshell, trap signals, and forward signals.
(mpirun -x PROPERTIES_FILE -np 2 ${TOP}/build/sdf/agent/sdfagent ${AGENT_ARGS[@]} &
    pid=$!; trap "kill $pid" SIGTERM; wait $pid; status=$?;
    kill -TERM $TIMEOUTPID; 
    ($DEBUG echo "agent done" 1>&2); exit $status
) &
MPIPID=$!
$DEBUG echo "MPI started PID $MPIPID" 1>&2

# example2_sdfclient never terminates without the startup delay
sleep $CLIENT_DELAY

# Note that the client retries so agent crashing immediately causes 
# client to hang.
echo "Running ${TEST_ARGS[@]}" 1>&2
(${TEST_ARGS[@]} &
    pid=$!; trap "kill $pid" SIGTERM; wait $pid; status=$?;
    kill -TERM $TIMEOUTPID; 
    ($DEBUG echo "test done" 1>&2); exit $status
) &
TESTPID=$!

wait $TIMEOUTPID
TIMEOUT_STATUS=$?
$DEBUG echo "Timeout done status $TIMEOUT_STATUS" 1>&2

kill $MPIPID
kill $TESTPID

wait $MPIPID
MPI_STATUS=$?

wait $TESTPID
TEST_STATUS=$?

# 143 = SIGTERM from kill.  Not 143 means timeout fired
if [ $TIMEOUT_STATUS -ne 143 ]; then
    STATUS=1
# 143 = SIGTERM from kill.  Not 143 means other signal
elif [ $MPI_STATUS -ne 143 ]; then
    STATUS=$MPI_STATUS
# Test should have terminated with status 0.
else
    STATUS=$TEST_STATUS
fi

# MPI does not cleanly shutdown the agents so they continue to spin forever
# slurping CPU. Kill them.
#
# XXX: Something different needs to happen if we give MPI a multi-node 
# configuration.
ps axo pid,cmd,args | grep -- "--nop $RUNNER" | grep -v grep | \
    awk '{print $1}' | xargs kill 

echo "TEST $TEST_STATUS MPI $MPI_STATUS TIMEOUT $TIMEOUT_STATUS" 1>&2

exit $STATUS

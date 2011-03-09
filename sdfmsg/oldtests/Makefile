# $Id: Makefile,v 1.1 2008/05/22 09:48:07 drew Exp drew $
#
# sdf messaging test
#
# The default build is a standalone program for the testing of the message queues
# and internode communciation over InfinPath, Ethernet or interprocess with shared 
# memory. You will have a bin sdf_msg in ../../build/sdf/sdfmsg/
# ------------------------------------------------------------------
# You can run the test on a single machine by the following command
# if you are in the build/sdf/sdfmsg/tests directory
# > mpirun -np 2 ./sdf_msg --msg_mpi 2
# this starts 2 processes. The output is directed to the screen
#
# you can also gain the plat_log output with the following
# > mpirun -np 2 sdf_msg --log sdf/sdfmsg=trace or =debug
# with two nodes and over a connection ie: Infinipath or Ethernet
#
# you must specify a hostfile that outlines the nodes you plan to
# kick the processes off on. The command line will look something like
# > mpirun -np 2 --hostfile my_hostfile sdf_msg --log sdf/sdfmsg=trace
# 
# ------------------------------------------------------------------
# use the BIG_BUILD=1 with make test to run the full unit tests
#
#

TOP = ../../..

PROG = \
	$(TESTS)

ifndef BIG_BUILD
SRCS = sdf_msg_motor.c sdf_msg_protocol.c 
TESTS = sdf_msg
else
TESTS = \
        fcnl_consistency_test1 \
        fcnl_bigsize_fth_test1 \
        fcnl_bigsize_test1 \
        fcnl_flush_test1 \
        fcnl_membership_test1 \
        fcnl_metadata_test1 \
        fcnl_mgnt_test1 \
        fcnl_mixed_thread_test1 \
        fcnl_queue_order_fth_test2 \
        fcnl_queue_order_pth_test2 \
        fcnl_queue_order_fth_test1 \
        fcnl_queue_order_pth_test1 \
        fcnl_receive_queue_test1 \
	fcnl_replication_test1 \
	fcnl_send_queue_test1 \
	fcnl_simplexsr_test1 \
	fcnl_system_test1    \
	fcnl_multi_protocol_test1 \
	fcnl_single_protocol_test1 \
	fcnl_mixed_thread_test2 \
	fcnl_multiptl_sequential_test1 \
	fcnl_singleptl_sequential_test1 \
        fcnl_multiptl_quantity_test1 \
	fcnl_multiptl_sequential_test2 \
	fcnl_multiptl_sequential_test3 \
	fcnl_multiptl_sequential_test4 \
	fcnl_singleptl_chkmsg_test1 \
	fcnl_singleptl_chksize_test1 \
	fcnl_singleptl_sequential_test2 \
	fcnl_singleptl_sequential_test3 \
        fcnl_presstest_test1 \
	fcnl_multinode_test1 \
	fcnl_multinode_test2 \
	fcnl_multinode_test3 \
        fcnl_multinode_test4 \
	fcnl_multinode_multiptl_test1 \
	fcnl_multinode_multiptl_test2 \
        fcnl_multinode_doubleside_test1 \
        fcnl_multinode_doubleside_test2 \
        fcnl_multinode_doubleside_test3 \
        fcnl_multinode_doubleside_test4 \
	fcnl_fth_mbx_test1
endif

TESTS_BROKEN = \
        fcnl_multinode_doubleside_test1 \
        fcnl_multinode_doubleside_test2 \
        fcnl_multinode_doubleside_test3 \
        fcnl_multinode_doubleside_test4 \
	fcnl_wrapper_test1

ifndef BIG_BUILD
LIB_SRCS = Utilfuncs.c
else
LIB_SRCS = \
	fcnl_bigsize_fth_lib1.c fcnl_metadata_lib1.c fcnl_replication_lib1.c \
	fcnl_bigsize_lib1.c fcnl_mgnt_lib1.c  fcnl_send_queue_lib1.c \
	fcnl_consistency_lib1.c  fcnl_queue_order_fth_lib1.c  fcnl_multi_protocol_lib1.c\
        fcnl_single_protocol_lib1.c fcnl_queue_order_fth_lib2.c fcnl_queue_order_pth_lib2.c\
	fcnl_simplexsr_lib1.c Utilfuncs.c  fcnl_flush_lib1.c \
	fcnl_queue_order_pth_lib1.c fcnl_mixed_thread_lib1.c fcnl_mixed_thread_lib2.c \
	fcnl_system_lib1.c fcnl_membership_lib1.c fcnl_receive_queue_lib1.c \
	slinkedlist.c fcnl_multiptl_sequential_lib1.c fcnl_singleptl_sequential_lib1.c \
        fcnl_multiptl_quantity_lib1.c fcnl_multiptl_sequential_lib2.c fcnl_multiptl_sequential_lib3.c \
	fcnl_multiptl_sequential_lib4.c fcnl_singleptl_chkmsg_lib1.c fcnl_singleptl_chksize_lib1.c \
        fcnl_singleptl_sequential_lib2.c fcnl_singleptl_sequential_lib3.c \
        fcnl_presstest_lib1.c fcnl_multinode_lib1.c fcnl_multinode_lib2.c fcnl_multinode_lib3.c \
        fcnl_multinode_lib4.c fcnl_multinode_multiptl_lib1.c fcnl_multinode_multiptl_lib2.c \
        fcnl_multinode_doubleside_lib1.c fcnl_multinode_doubleside_lib2.c \
        fcnl_multinode_doubleside_lib3.c fcnl_multinode_doubleside_lib4.c \
        fcnl_fth_mbx_lib1.c log.c
# fcnl_wrapper_lib1.c
endif

LIB = libsdfmsgtest.a

LIBS =  \
	.././libsdfmsgqueue.a \
	libsdfmsgtest.a \
	../../fth/libfth.a \
	../../utils/libutils.a \
	../../fth/libfth.a \
	../../misc/libmisc.a \
	../../platform/libplatform.a \
        ../../sdfappcommon/libsdfappcommon.a \
	../../sdftcp/libsdfmtp.a \
	-lpthread -lrt

USE_MPI = 1

#   mpirun tests with --stop arg
#
#STOP=--stop

#sdf_msg.run: TESTRUNNER = mpirun -np 2

TARGETRUN := $(foreach testitem, $(TESTS), $(testitem).run)
$(TARGETRUN): TESTRUNNER = $(SHELL) testscript.sh

fcnl_multinode_doubleside_test1.run : TESTRUNNER = /bin/sh testscript.sh 4
fcnl_multinode_doubleside_test2.run : TESTRUNNER = /bin/sh testscript.sh 4
fcnl_multinode_doubleside_test3.run : TESTRUNNER = /bin/sh testscript.sh 4
fcnl_multinode_doubleside_test4.run : TESTRUNNER = /bin/sh testscript.sh 4
include $(TOP)/Makefile.inc

# only bypass warnings as errors if you really need to
#WERROR=

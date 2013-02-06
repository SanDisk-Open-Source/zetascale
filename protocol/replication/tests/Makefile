#$Id: Makefile 13389 2010-05-03 12:40:01Z drew $

TOP = ../../../..

TESTS_PROG = \
    fcnl_framework_async_get_put_stress \
    fcnl_framework_async_shard \
    fcnl_framework_async_test \
    fcnl_framework_home_node_test \
    fcnl_framework_recover_test \
    fcnl_framework_replica_node_test \
    fcnl_framework_seqno_test \
    fcnl_framework_sync_test \
    fcnl_rms_get_put_test \
    fcnl_sn_null_test \
    fcnl_rms_get_lease_test \
    fcnl_rms_simple_test \
    fcnl_rms_fault_in_test \
    fcnl_rms_multi_put_test \
    fcnl_rms_put_crash_get_test \
    fcnl_rms_put_crash_expire_put_test \
    fcnl_rms_crash_switch_over_test \
    fcnl_framework_cursor_test \
    fcnl_sn_multi_shard_test \
    fcnl_sn_multi_crash_test \
    fcnl_sn_multi_shard_test2 \
    fcnl_framework_state_machine_test \
    key_lock_test

TESTS_VALGRIND = \
    fcnl_framework_state_machine_test_valgrind \
    key_lock_test_valgrind

TESTS = \
    $(TESTS_PROG) \
    fcnl_framework_state_machine_test_nplus1 \
    fcnl_framework_state_machine_test_liveness \
    fcnl_framework_state_machine_test_preference_liveness \
    $(TESTS_VALGRIND)

TESTS_BROKEN_PROG = \
    fcnl_framework_msg_timeout_test \
    fcnl_rms_get_put_del_test \
    fcnl_rtf_stress_test \
    fcnl_sn_all_crash_test \
    fcnl_sn_get_put_test \
    fcnl_sn_lock_test1 \
    fcnl_sn_lock_test2 \
    fcnl_sn_ss_mc_test \
    #fcnl_sn_async_test

TESTS_BROKEN = \
    $(TESTS_BROKEN_PROG)

PROG = $(TESTS_PROG) $(TESTS_BROKEN_PROG)

LIB = \
    libreplica.a

LIB_SRCS = \
    test_common.c \
    test_flash.c \
    test_framework.c \
    test_generator.c \
    test_model.c \
    test_node.c \
    tlmap3.c

# XXX: drew 2009-08-21 Some one has added a plethora of libraries
# in the wrong order and it happened to work because something higher
# on the list was pulling in the dependencies for the next part of 
# the list.
#
# That changed and we became screwed.  So I just link against
# both sets of libraries twice and everything resolves itself.
LIBS_SDF = \
        ../../../agent/libsdfagent.a \
        ../../../shared/libsdfshared.a \
        ../../../api/libsdf.a \
        ../../../protocol/action/libsdfaction.a \
        ../../../protocol/home/libsdfhome.a \
        ../../../protocol/libsdfprotocol.a \
        ../../../ssd/libsdfssd.a \
        ../../../ssd/clipper/libclipper.a \
        ../../../protocol/action/libsdfaction.a \
        ../../../ssd/fifo/libfifo.a \
        ../../../utils/libutils.a \
        ../../../fth/libfth.a \
        ../../../misc/libmisc.a \
        ../../../platform/libplatform.a \
        ../../../sdfmsg/libsdfmsgqueue.a \
        ../../../sdftcp/libsdfmtp.a \
        ../../../protocol/replication/libsdfreplication.a \
        ../../../ecc/libecc.a

LIBS = \
        libreplica.a \
	$(LIBS_SDF) \
	$(LIBS_SDF) \
        -lpthread -lm -lrt -lcunit -levent -laio

SUBDIRS = t

USE_MPI = 1

EXTRACLEAN = $(patsubst %.run,$(BUILD)%.shmem,$(TESTS) $(TESTS_BROKEN))

SHMEM = $(abspath $(BUILD)$(patsubst %.run,%.shmem,$@)):128m
TIMEOUT = --timeout 10
TESTARGS = $(TIMEOUT)
#Simplest possible run for least spew
fcnl_framework_sync_test.run: TESTARGS = $(TIMEOUT) --iterations 100 --log sdf/prot/replication/test=info
fcnl_framework_async_test.run: TESTARGS = $(TIMEOUT) --iterations 100
fcnl_framework_state_machine_test.run: TESTARGS = $(SHMEM) $(TIMEOUT) --2way
key_lock_test.run: TESTARGS = $(TIMEOUT) --plat/shmem/file $(SHMEM)

TESTPROG_fcnl_framework_state_machine_test_nplus1 = fcnl_framework_state_machine_test
fcnl_framework_state_machine_test_nplus1.run: TESTARGS = $(SHMEM) $(TIMEOUT) --nplus1

TESTPROG_fcnl_framework_state_machine_test_liveness = fcnl_framework_state_machine_test
fcnl_framework_state_machine_test_liveness.run: TESTARGS = $(SHMEM) $(TIMEOUT) --2way --replication_lease_liveness --new_tests

TESTPROG_fcnl_framework_state_machine_test_preference_liveness = fcnl_framework_state_machine_test
fcnl_framework_state_machine_test_preference_liveness.run: TESTARGS = $(SHMEM) $(TIMEOUT) --2way --replication_initial_preference --replication_lease_liveness

TIMEOUT_VALGRIND = --timeout 10
TESTRUNNER_VALGRIND = $(VALGRINDDIR)/bin/valgrind --error-exitcode=2

fcnl_framework_state_machine_test_valgrind.run: TESTRUNNER = $(TESTRUNNER_VALGRIND)
TESTPROG_fcnl_framework_state_machine_test_valgrind = fcnl_framework_state_machine_test
fcnl_framework_state_machine_test_valgrind.run: TESTARGS = $(SHMEM) $(TIMEOUT_VALGRIND) --2way --replication_lease_liveness --new_tests 

key_lock_test_valgrind.run: TESTRUNNER = $(VALGRINDDIR)/bin/valgrind --error-exitcode=2 --leak-check=full --show-reachable=yes
TESTPROG_key_lock_test_valgrind = key_lock_test
key_lock_test_valgrind.run: TESTARGS = $(TIMEOUT_VALGRIND)

include $(TOP)/Makefile.inc

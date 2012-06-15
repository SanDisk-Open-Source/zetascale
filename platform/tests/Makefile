# $Id: Makefile 13916 2010-05-29 00:29:12Z drew $

TOP = ../../..

TESTS_PROG = \
	aio_test \
	alloc_arenatest \
	alloc_stacktest \
	asserttest \
	closuretest \
        cpu_peertest \
	logtest \
	mbox_schedulertest \
	oncetest \
	opttest \
	shmemtest_aoset \
	shmemtest_multi \
	shmemtest_one \
	shmemtest_sbt \
	timer_dispatchertest \
        spin_rwtest \
	fast_logtest

TESTS = $(TESTS_PROG) \
	aio_test_wc \
	aio_test_wc_delay \
	aio_test_wc_valgrind \
	aio_test_error \
	aio_test_error_valgrind

# shmemtest_phys needs the actual device driver to probe and that's a 
# misadventure requiring root privledges, etc. Punt on automating it.
TESTS_BROKEN_PROG = \
	shmemtest_plat_alloc \
	shmemtest_phys

TESTS_BROKEN = \
	$(TESTS_BROKEN_PROG)

PROG = $(TESTS_PROG) $(TESTS_PROG_BROKEN)

EXTRACLEAN = $(patsubst %,$(BUILD)%_backing,$(TESTS))

LIBS = \
	../../misc/libmisc.a \
	../../fth/libfth.a \
	../libplatform.a \
	../../fth/libfth.a \
	../../ecc/libecc.a \
	../../fth/libfth.a \
	../../sdfappcommon/libsdfappcommon.a \
	../../fth/libfth.a \
	../../utils/libutils.a \
	-laio -lrt -lpthread

SUBDIRS = \
	#mem_debug

TIMEOUT = --timeout 10
TIMEOUT_VALGRIND = --timeout 5
TESTRUNNER_VALGRIND = $(VALGRINDDIR)/bin/valgrind --error-exitcode=2 --leak-check=full --show-reachable=yes

aio_test.run: TESTARGS = --plat/shmem/file $(abspath $(BUILD)alloc_arenatest_backing):16m --file $(abspath $(BUILD)aio_test_file) $(TIMEOUT)

TESTPROG_aio_test_wc = aio_test
aio_test_wc.run: TESTARGS = --plat/shmem/file $(abspath $(BUILD)alloc_arenatest_backing):16m --file $(abspath $(BUILD)aio_test_file) --mode wc $(TIMEOUT)

TESTPROG_aio_test_wc_delay = aio_test
aio_test_wc.run: TESTARGS = --plat/shmem/file $(abspath $(BUILD)alloc_arenatest_backing):16m --file $(abspath $(BUILD)aio_test_file) --mode wc $(TIMEOUT) --plat/aio/wc/delay_submit

aio_test_wc_valgrind.run: TESTRUNNER = $(TESTRUNNER_VALGRIND)
TESTPROG_aio_test_wc_valgrind = aio_test
aio_test_wc_valgrind.run: TESTARGS = --plat/shmem/file $(abspath $(BUILD)alloc_arenatest_backing):16m --file $(abspath $(BUILD)aio_test_file) --mode wc $(TIMEOUT_VALGRIND)

TESTPROG_aio_test_error = aio_test
aio_test_error.run: TESTARGS = --plat/shmem/file $(abspath $(BUILD)alloc_arenatest_backing):16m --file $(abspath $(BUILD)aio_test_file) --mode error $(TIMEOUT)

aio_test_error_valgrind.run: TESTRUNNER = $(TESTRUNNER_VALGRIND)
TESTPROG_aio_test_error_valgrind = aio_test
aio_test_error_valgrind.run: TESTARGS = --plat/shmem/file $(abspath $(BUILD)alloc_arenatest_backing):16m --file $(abspath $(BUILD)aio_test_file) --mode error $(TIMEOUT_VALGRIND)

alloc_arenatest.run: TESTARGS = --plat/shmem/file $(abspath $(BUILD)alloc_arenatest_backing):16m
asserttest.run: TESTRUNNER = $(SHELL) ./invert_no_core.sh
shmemtest_one.run: TESTARGS = --plat/shmem/file $(abspath $(BUILD)shmemtest_one_backing):16m
shmemtest_sbt.run: TESTARGS = --plat/shmem/file $(abspath $(BUILD)shmemtest_sbt_backing):16m

# Run with multiple segments in order to exercise segment switching 
# corner case.
shmemtest_multi.run: TESTARGS = --plat/shmem/file $(abspath $(BUILD)shmemtest_multi_backing0):16k --plat/shmem/file $(abspath $(BUILD)shmemtest_multibacking_1):16m  --runtime 1 --len 1000 --plat/shmem/address_space=18m 

# XXX: drew 2008-09-24 Currently address space randomization breaks multi-process shmem.  Turn it off for multi-process shmem
# shmemtest_multi.run: TESTRUNNER = setarch `uname -p` -RL

mbox_schedulertest.run: TESTARGS = --plat/shmem/file $(abspath $(BUILD)mbox_schedulertest_backing):1m
timer_dispatchertest.run: TESTARGS = --plat/shmem/file $(abspath $(BUILD)timer_dispatchertest_backing):1m

shmemtest_phys.run: TESTARGS = --plat/shmem/physmem /dev/physmem

logtest.run: TESTARGS = --plat/shmem/file $(abspath $(BUILD)logtest_backing):2g


include $(TOP)/Makefile.inc

# Makefile debugging
#
show-%:
	@echo $*=$($*)

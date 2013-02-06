# $Id: Makefile 13831 2010-05-25 02:41:49Z drew $

TOP = ../..
LIB = libplatform.a

# Platform level functionality - allocation, scheduling, shared memory, etc.
PLATFORM_SRCS = \
	aio_api.c \
	aio_libaio.c \
	aio_error_bdb.c \
	aio_error_control.c \
	aio_wc.c \
	alloc_stack.c \
	aoset.c \
	attr.c \
	coredump_filter.c \
	cpu_peers.c \
	event.c \
	fd_dispatcher.c \
	ffdc_log.c \
	fth_scheduler.c \
	get_exe.c \
	logging.c \
	mbox_scheduler.c \
	mem_debug.cc \
	memory_fault.c \
	memory_size.c \
	mprobe.c \
	msg.c \
	prng.c \
        alloc.c \
        malloccheck.c \
	shmem_alloc.c \
	shmem.c \
	shmem_debug.c \
	shmem_global.c \
	shmem_ptrs.c \
	shmem_test.c \
	stats.c \
	strarray_alloc.c \
	string.c \
	timer_dispatcher.c \
	tmp_path.c\
        util_trace.c

# (mostly) macro generated  wrappers for system calls and library functions
WRAPPER_SRCS = \
        assert.c \
        epoll.c \
	fcntl.c \
	fd.c \
	file.c \
	fork.c \
	mman.c \
	mutex.c \
	printf.c \
	process.c \
	random.c \
	rwlock.c \
	socket.c \
	sysvipc.c \
	thread.c \
	time.c

LIB_SRCS = $(PLATFORM_SRCS) $(WRAPPER_SRCS) $(MISC_SRCS)

PROG = ffdc_reader

PROG_ffdc_reader_SRCS = ffdc_reader.c

LIBS = \
	./libplatform.a \
	../fth/libfth.a \
	../utils/libutils.a \
	-lpthread

# FFDC generated source files
FFDC_SRC = ffdc_log.c ffdc_log.dbg ffdc_log.h ffdc_reader.c
# All ffdc temporary files
FFDC_TMP = $(patsubst %,$(BUILD)tmp/%,$(FFDC_SRC)) $(BUILD)tmp/ffdc_log.mc
FFDC_PARSER = ffdc_parser.pl

vpath %.c ../misc
vpath %.h ../misc

#PROG = shmemd

SUBDIRS = \
	shmemd \
	tests \
        tool

# For MISC_SRCS
include ../misc/Makefile.inc

include $(TOP)/Makefile.pre

# Put the temporary copy of new ffdc files in a subdirectory incase 
# $(BUILD) is empty
EXTRACLEAN = $(FFDC_SRC) $(BUILD)ffdc.stamp $(FFDC_TMP)

$(BUILD)ffdc_log.d $(BUILD)ffdc_reader.d: | $(MAKESTAMP)

ifneq (,$(BUILD))
.PHONY: ffdc.stamp
ffdc.stamp: $(BUILD)ffdc.stamp
endif

ifdef PRODUCTION
FFDC_NEWER := echo "ffdc_log.mc changed"; exit 1
else
FFDC_NEWER := cp $(BUILD)tmp/ffdc_log.mc .
endif

$(BUILD)ffdc.stamp: $(FFDC_PARSER) $(MAKEFILES)
	mkdir -p $(BUILD)tmp
	cmp ffdc_log.mc $(BUILD)tmp/ffdc_log.mc; \
	if [ $$? -ne 0 ]; then \
	    rm -f $(FFDC_TMP); \
	    cp ffdc_log.mc $(BUILD)tmp/ffdc_log.mc; \
	fi
	cd $(BUILD)tmp; $(PERL) $(abspath $(FFDC_PARSER)) $(abspath $(TOP))
	for file in $(FFDC_SRC); do \
		cp $(BUILD)tmp/$$file .; \
	done
	cmp ffdc_log.mc $(BUILD)tmp/ffdc_log.mc; \
	if [ $$? -ne 0 ]; then \
	    $(FFDC_NEWER); \
        fi;
	touch $(@)

include $(TOP)/Makefile.post

# $Id: Makefile 1293 2008-05-16 21:06:53Z jim $

TOP = ../../../..

TESTS = mem_debug_test

TESTS_BROKEN = mem_debug_stress_test

PROG = $(TESTS) $(TESTS_BROKEN)

LIBS = \
        libshtest.a \
        ../../../misc/libmisc.a \
        ../../../fth/libfth.a \
        ../../libplatform.a \
        ../../../fth/libfth.a \
        -lcunit \
        -lpthread \
	-lrt

LIB = libshtest.a

LIB_SRCS = \
        test_mem_debug_one.c \
        testall.c

include $(TOP)/Makefile.inc

CPP_INCLUDES += -I$(CUNITDIR)/include/CUnit

mem_debug_test.run: TESTARGS = --log platform=fatal
mem_debug_stress_test.run: TESTARGS = --plat/test/mem_debug/total=2000


# $Id$

TOP := ../..

LIB_SRCS := ecc_recover.c lfsr.c discrete_math.c

LIB := libecc.a

TESTS := ecc_test

TESTS_BROKEN := 

LIBS := ./libecc.a ../misc/libmisc.a

PROG := $(TESTS) $(TESTS_BROKEN)

all: $(TESTS)

ecc_test.run: TESTARGS := -max-test-size 1k -v

%.s: %.c
	cc -S $(CFLAGS) $< > $@

%.i: %.c
	cc -E $< > $@

.PHONY: s i

ifdef BUILD
BUILD_PFX := $(BUILD)/
else
BUILD_PFX :=
endif
s: $(patsubst %.c,$(BUILD_PFX)%.s,$(wildcard *.c))

i: $(patsubst %.c,$(BUILD_PFX)%.i,$(wildcard *.c))

include $(TOP)/Makefile.inc

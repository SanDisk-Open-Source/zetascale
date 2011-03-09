#$Id: Makefile 328 2008-02-22 10:20:46Z darpan $

TOP = ../../..

PROG = \
	sdfclienttest \
#	stockTradingApp

TESTS = $(PROG)

LIBS =	\
	../../../sdfclient/libsdfclient.a \
	../libsdfshared.a \
	../../../sdfcc/libsdfcc.a \
	../../utils/libutils.a \
	../../platform/libplatform.a \
	-lpthread

sdfclienttest.run: TESTARGS = .
# stockTradingApp.run: TESTARGS = .

include $(TOP)/Makefile.inc



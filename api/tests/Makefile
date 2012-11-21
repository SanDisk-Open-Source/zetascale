TOP = ../../..

PROG = \
       fdf_static_test \
       fdf_prop_test \
       sdf_stress_test \
       fdf_multi_test 
	

TESTS = $(PROG)

LIBS_SDF = \
        $(TOP)/sdf/agent/libsdfagent.a \
        $(TOP)/sdf/shared/libsdfshared.a \
        $(TOP)/sdf/api/libsdf.a \
        $(TOP)/sdf/protocol/action/libsdfaction.a \
        $(TOP)/sdf/protocol/home/libsdfhome.a \
        $(TOP)/sdf/protocol/libsdfprotocol.a \
        $(TOP)/sdf/protocol/replication/libsdfreplication.a \
        $(TOP)/sdf/ssd/libsdfssd.a \
        $(TOP)/sdf/ssd/clipper/libclipper.a \
        $(TOP)/sdf/ssd/fifo/libfifo.a \
        $(TOP)/sdf/ssd/libsdfssd.a \
        $(TOP)/sdf/ecc/libecc.a \
        $(TOP)/sdf/utils/libutils.a \
        $(TOP)/sdf/sdfmsg/libsdfmsgqueue.a \
        $(TOP)/sdf/sdftcp/libsdfmtp.a \
        $(TOP)/sdf/platform/tool/hotkey/libhotkey.a \
        $(TOP)/sdf/misc/libmisc.a \
        $(TOP)/sdf/fth/libfth.a \
        $(TOP)/sdf/platform/libplatform.a \
        $(TOP)/crypto/libcryptopp.a \
        -lpthread -lrt -lnsl -lutil -lm -laio -levent

LIBS = $(LIBS_FTH) $(LIBS_SDF) $(LIBS_SDF) 

fdf_static_test.run: TESTARGS = .

include $(TOP)/Makefile.inc

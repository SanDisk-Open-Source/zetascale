
TOP = ../../../../..

PROG = $(TESTS) 	\
    ht_rpt_test


TESTS = \
    ht_candidate_win     \
    ht_winner_lose       \
    ht_winner_lose_win   \
    ht_compete_winners   \
    ht_all_rpts \

LIB = test_main.a
LIB_SRCS = test_main.c

LIBS =  \
    $(LIB)                                           \
    ../libhotkey.a                                   \
    ../../../../shared/libsdfshared.a                \
    ../../../../platform/libplatform.a               \
    ../../../../utils/libutils.a                     \
    ../../../../sdfmsg/libsdfmsgqueue.a              \
    ../../../../sdftcp/libsdfmtp.a                   \
    ../../../../misc/libmisc.a                       \
    ../../../../fth/libfth.a                         \
    -lpthread -lrt

include $(TOP)/Makefile.inc

OBJ_CLEAN:=$(patsubst %, $(TOP)/build/$(SUBDIR)/%*, $(PROG))

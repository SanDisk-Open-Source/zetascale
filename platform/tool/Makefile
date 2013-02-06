
TOP = ../../..

PROG = util_trace_collector\
       util_trace_viewer

EXTRACLEAN = $(patsubst %,$(BUILD)%_backing,$(PROG))

LIBS = \
	../../misc/libmisc.a \
	../../fth/libfth.a \
	../libplatform.a \
	../../fth/libfth.a \
	../../utils/libutils.a \
	-lrt -lpthread

SUBDIRS=hotkey

include $(TOP)/Makefile.inc

# Makefile debugging
#
show-%:
	@echo $*=$($*)

# $Id: Makefile 9983 2009-11-02 18:25:23Z gxu $

TOP = ../..
LIB = libsdfshared.a
LIB_SRCS = \
	cmc.c \
	container.c \
	container_meta.c \
	container_props.c \
	init_sdf.c \
	internal_blk_obj_api.c \
	name_service.c \
	object.c \
	open_container_map.c \
	open_container_mgr.c \
	shard_compute.c

LIBS = \
	../utils/libutils.a \
	../platform/libplatform.a \
	../../sdf/sdfmsg/libsdfmsgqueue.a \
        ../../sdftcp/libsdfmtp.a \
	../applib/libapplib.a \
	-lpthread -lrt

#SUBDIRS = tests

#USE_MPI = 1

include $(TOP)/Makefile.inc

BROKEN = cmc.c internal_handler.c
$(call MAKE_OPTOBJS_FROM_SRCS,$(BROKEN)): OPTFLAGS += -fno-strict-aliasing

vpath %.c $(SDF_PROTOCOL_TABLES_GENERATED)

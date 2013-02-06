# sdf messaging 
#
# The libsdfmsgqueue.a defaults to require MPI libs for implementation of the multi node 
# messaging queues and overall communication. The current MPI implementation that is
# utilized is OpenMPI. www.open-mpi.org. You may change the version and desired 
# implementation. See the root dir and Makefile.vers.
# ------------------------------------------------------------------
# sdf_msg_queue: thread communication queues
# sdf_msg_mpi: mpi calls and bin init
# sdf_msg_engine: init, messaging loop and shutdown we deal with them here
# sdf_msg_mgmnt: the management thread for messaging

TOP = ../..

LIB = libsdfmsgqueue.a

LIB_SRCS = \
	sdf_fth_mbx.c \
        sdf_msg_mpi.c \
        sdf_msg_hash.c \
        sdf_msg_hmap.c \
        sdf_msg_sync.c \
        sdf_msg_mgmnt.c \
	sdf_msg_queue.c \
	sdf_msg_shmem.c \
	sdf_msg_types.c \
	sdf_msg_action.c \
        sdf_msg_engine.c \
        sdf_msg_binding.c \
	sdf_msg_wrapper.c

#SUBDIRS = tests

USE_MPI = 1

include $(TOP)/Makefile.inc

# $Id: Makefile 4241 2008-11-07 22:02:06Z drew $

# The Linux kernel build system expects a specific makefile format, while 
# the Schooner build system has something more suited to our needs. Be
# schizophrenic.

# Schooner or local make
ifeq ($(KERNELRELEASE),)

TOP = ../..

PROG = \
	memcat \
	physmem_ioctl

DEFAULT_KERNEL_DIR := /lib/modules/$(shell uname -r)/build

DRIVER := physmem_driver

EXTRACLEAN += Module.symvers modules.order
EXTRACLEAN += $(DRIVER).mod.c $(DRIVER).mod.o $(DRIVER).mod.c

# $(PROG) doesn't get built without the schooner make environment,  
# but otherwise everything will work correctly
-include $(TOP)/Makefile.inc

KERNEL_DIR ?= $(DEFAULT_KERNEL_DIR)

# FIXME: Should probably copy Makefile to $(BUILD)Makefile where 
# build is not "." and then make SUBDIRS $(BUILD) so build droppings
# end there. 

.PHONY: all
all: physmem_driver.ko

# Always re-make in the kernel environment
.PHONY: physmem_driver.ko

physmem_driver.ko:
	make -C $(KERNEL_DIR) SUBDIRS=$(CURDIR) modules

clean: clean.kernel

.PHONY: clean.kernel
clean.kernel: 
	make -C $(KERNEL_DIR) SUBDIRS=$(CURDIR) clean

# Kernel make
else # eq($(KERNELRELEASE),)

EXTRA_CFLAGS += -Wall -Werror -DPHYSMEM_DEBUG_FILL
obj-m := physmem_driver.o 

endif

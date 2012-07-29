# $Id: Makefile 9980 2009-11-02 17:27:13Z briano $

# sdfcc is dead but does unit tests which exercise the fth thread library
TOP := ..
SUBDIRS := \
	misc		\
	platform	\
	ecc		\
	protocol	\
	sdfmsg		\
	fth		\
	agent		\
	shared		\
	utils		\
	applib		\
        sdftcp          \
        ssd
	###
#	flash		\

include $(TOP)/Makefile.inc

TOP = ..

PROG = sample_program

TESTS = $(PROG)

LIBS_SDF = $(TOP)/lib/libzsdll.a \
        -lpthread -ldl -laio -levent -lsnappy

LIBS = $(LIBS_FTH) $(LIBS_SDF) $(LIBS_SDF) 

all: 
	cc -Wall sample_program.c $(LIBS_SDF) -I$(TOP)/include -o $(PROG)

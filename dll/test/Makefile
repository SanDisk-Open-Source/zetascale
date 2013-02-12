CC	= gcc
CFLAGS = -Wall -g -I../../api -I../..
OBJS	= test.o fdf_easy.o fdf_conf.o fdf_errs.o fdf_link.o fdf_dll.o

all: test

test: $(OBJS)
	$(CC) -o test $(OBJS) -ldl -lpthread -laio -lrt -levent -lm

fdf_dll.o:	../fdf_dll.c
	$(CC) -c $(CFLAGS) ../fdf_dll.c

.PHONY: clean

clean:
	rm -f $(OBJS) test

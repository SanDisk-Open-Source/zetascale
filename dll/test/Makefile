CC	= gcc
CFLAGS = -Wall -g -I../../api -I../..
OBJS	= hello.o fdf_easy.o fdf_conf.o fdf_errs.o fdf_link.o fdf_dll.o

all: hello

hello: $(OBJS)
	$(CC) -o hello $(OBJS) -ldl -lpthread -laio -lrt -levent -lm

fdf_dll.o:	../fdf_dll.c
	$(CC) -c $(CFLAGS) ../fdf_dll.c

.PHONY: clean

clean:
	rm -f $(OBJS) hello

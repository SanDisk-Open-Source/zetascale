CC	= gcc
CFLAGS = -Wall -g -I../../api -I../..
OBJS	= hello.o fdf_easy.o fdf_errs.o

all: hello

hello: $(OBJS)
	$(CC) -o hello $(OBJS) ../libfdfdll.a -ldl -lpthread -laio -lrt -levent -lm

.PHONY: clean

clean:
	rm -f $(OBJS) hello

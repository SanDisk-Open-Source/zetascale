
#CC      = gcc
CC      = cc
CFLAGS  = -g
LDFLAGS =
TARGET = stopleak

LIBS	= -lm

OBJS = hash.o tlmap.o xtlmap.o main.o

stopleak : $(OBJS)
	$(CC) $(LDFLAGS) $(LIBS) -o $@ $(OBJS)

-include $(OBJS:.o=.d)

%.o: %.c
	$(CC) -c $(CFLAGS) $*.c -o $*.o
	$(CC) -MM $(CFLAGS) $*.c > $*.d

clean:
	rm -f $(TARGET) *.o *.d

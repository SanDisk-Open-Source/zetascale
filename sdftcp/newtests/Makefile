ALL	= bw lat mbw
CC	= gcc -Wall -g -DNOSDF -I..
LD	= gcc -g
LIBS	= -lrt

all: $(ALL)

bw: bw.o msg_err.o msg_msg.o msg_tcp.o msg_util.o msg_tools.o msg_trace.o
	$(LD) -o $@ $^ $(LIBS)

lat: lat.o msg_err.o msg_msg.o msg_tcp.o msg_util.o msg_tools.o msg_trace.o
	$(LD) -o $@ $^ $(LIBS)

mbw: mbw.o msg_err.o msg_msg.o msg_tcp.o msg_util.o msg_tools.o msg_trace.o
	$(LD) -o $@ $^ $(LIBS)

%.o: ../%.c
	$(CC) -c -o $@ $<

.PHONY: clean
clean:
	-rm -f *.o $(ALL)

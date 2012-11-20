#!/bin/bash

#CFLAGS = -c -Isdf/inc -I../api -I..
CFLAGS = -c -I../api -I..

libfdfdll.a: fdf_dll.o
	ar cr libfdfdll.a fdf_dll.o

.PHONY: clean

clean:
	rm -f fdf_dll.o libfdfdll.a

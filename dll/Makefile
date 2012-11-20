#!/bin/bash

CFLAGS = -c -I../api -I..

libfdfdll.a: fdf_dll.o
	ar cr libfdfdll.a fdf_dll.o

.PHONY: clean

clean:
	rm -f fdf_dll.o libfdfdll.a

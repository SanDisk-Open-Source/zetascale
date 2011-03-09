#/bin/sh

./asyn_memslap --servers=localhost:11211 -F cmd_modified -c 64 -T 4 -x 80000000 -r 5s -t 300s #-r 5s #-v 0.01

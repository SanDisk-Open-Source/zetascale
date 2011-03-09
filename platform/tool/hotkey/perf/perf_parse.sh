#!/bin/bash

parse_perf () {
	echo 'tps:'| tee  >>perf.txt
	for fd in `ls perf_*` 
 	do
		cat $fd | grep "^run time:" | awk '{print $7}' | tee >> perf.txt
	done
}

parse_mem () {
	echo 'memory:' | tee >> perf.txt
	for fd in `ls hotkey.memory.*`
    do 
		cat $fd | grep "^mem" | awk '{print $2}' | tee >> perf.txt
	done		
}
parse_perf
parse_mem

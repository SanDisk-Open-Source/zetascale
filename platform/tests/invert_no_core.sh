#!/bin/sh -x
ulimit -c 0
$@
if [ $? -eq 0 ]; then
    exit 1
else
    exit 0
fi

#!/bin/sh -x
#
# The things that have been typed manually at the device driver to test it.
#
# $Id: test.sh 2487 2008-07-29 18:47:39Z drew $

size=10
device=/dev/physmem
driver=physmem_driver
build=${TOP}/build/sdf/physmem

# XXX: Should just stuff memcat into path
if [ -x memcat ]; then
    memcat=./memcat
elif [ -x ${build}/memcat ]; then
    memcat=${build}/memcat
elif [ -x /tmp/memcat ]; then
    memcat= /tmp/memcat
else
    echo "no memcat" 1>&2
    exit 1;
fi

sudo insmod "$driver".ko physmem_size_mb=$size 
status=$?
if [ $status -ne 0 ] ; then
    echo "insmod failed" 1>&2
    exit $status
fi

if [ ! -c "$device" ]; then
    major=`grep "$driver" /proc/devices | awk '{print $1}'`
    status=$?
    if [ $status -ne 0 ]; then
	echo "no major" 1>&2
	exit $status
    fi

    sudo mknod -m 666 $device c $major 0
    status=$?
    if [ $status -ne 0 ]; then
	echo "can't mknod" 1>&2
	exit $status;
    fi
fi

dd if=$device > raw
status=$?
if [ $status -ne 0 ]; then
    echo "read failed"
    exit $status
fi

expected=`expr $size \* 1024 \* 1024` 
size_bytes=`ls -l raw | awk '{print $5}'`
if [ $size_bytes -ne $expected ]; then
    echo "size is $size_bytes not $expected" 1>&2
    exit $status
fi

od -i raw | perl verify_od.pl
status=$?
if [ $status -ne 0 ]; then
    echo "verify failed" 1>&2
    exit $status
fi

$memcat if=$device size=$size_bytes > raw.mmap
status=$?
if [ $status -ne 0 ]; then
    echo "memcat failed" 1>&2
    exit $status
fi

cmp raw raw.mmap
if [ $status -ne 0 ]; then
    echo "mmap output didn't match read" 1>&2
    exit $status
fi

truncated_size_bytes=`expr $size_bytes - 4096`
$memcat if=$device memskip=4096 size=$truncated_size_bytes > raw-offset.mmap
status=$?
if [ $status -ne 0 ]; then
    echo "memcat skip failed" 1>&2
    exit $status
fi

dd if=raw bs=4096 skip=1 > raw-offset && cmp raw-offset raw-offset.mmap
status=$?
if [ $status -ne 0 ]; then
    echo "mmap seek output didn't match" 1>&2
    exit $status
fi

truncated_size_bytes=`expr $size_bytes - 8192`
cat /dev/zero | $memcat of=$device memskip=8192 size=$truncated_size_bytes
if [ $status -ne 0 ]; then
    echo "final offset write failed"
    exit $status
fi
dd if=/dev/zero bs=8192 count=1 of=$device
sum=`sum $device | awk '{print $1}'`
if [ $sum -ne 0 ]; then
    echo "sum $sum not 0" 1>&2
    exit $status
fi

sudo rmmod $driver
status=$?
if [ $status -ne 0 ]; then
    echo "rmmod failed" 1>&2
    exit $status
fi

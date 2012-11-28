#!/bin/bash -x

OPTIMIZE=""
if [ "$1" = "help" ]
then
    echo
    echo "$0 [optimize|help]"
    echo optimize - build library with all optimization
    echo help - display help message
    echo
    exit 0    
fi

if [ "$1" = "optmize" ]
then
    OPTIMIZE="_o" 
    make optimize
else
    make
fi

echo >tmp
for i in  ../../build/sdf/api/libsdf$OPTIMIZE.a \
	../../build/sdf/agent/libsdfagent$OPTIMIZE.a \
	../../build/sdf/shared/libsdfshared$OPTIMIZE.a \
    ../../build/sdf/protocol/action/libsdfaction$OPTIMIZE.a \
    ../../build/sdf/protocol/home/libsdfhome$OPTIMIZE.a \
	../../build/sdf/protocol/libsdfprotocol$OPTIMIZE.a \
	../../build/sdf/ssd/libsdfssd$OPTIMIZE.a \
	../../build/sdf/ssd/clipper/libclipper$OPTIMIZE.a \
	../../build/sdf/ssd/fifo/libfifo$OPTIMIZE.a \
	../../build/sdf/ssd/libsdfssd$OPTIMIZE.a \
	../../build/sdf/protocol/replication/libsdfreplication$OPTIMIZE.a \
	../../build/sdf/ecc/libecc$OPTIMIZE.a \
	../../build/sdf/utils/libutils$OPTIMIZE.a \
	../../build/sdf/sdfmsg/libsdfmsgqueue$OPTIMIZE.a \
	../../build/sdf/sdftcp/libsdfmtp$OPTIMIZE.a \
	../../build/sdf/platform/tool/hotkey/libhotkey$OPTIMIZE.a \
	../../build/sdf/misc/libmisc$OPTIMIZE.a \
	../../build/sdf/fth/libfth$OPTIMIZE.a \
	../../build/sdf/platform/libplatform$OPTIMIZE.a
# ../../build/../3rd-party/libevent-1.4.3-et/lib/libevent$OPTIMIZE.a
do
ar t $i|sed "s+^+$(dirname $i)/+" >>tmp
done

ar -sr libfdf.a $(cat tmp) ../../../3rd-party/libevent-1.4.3-et/lib/libevent.so

#ranlib libsdf.o
# -lpthread -lrt -lnsl -lutil -lm -la$OPTIMIZE.a -lsasl2

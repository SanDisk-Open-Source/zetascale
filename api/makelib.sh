#!/bin/bash -x

echo >tmp
for i in  ../../build/sdf/api/libsdf.a \
	../../build/sdf/agent/libsdfagent.a \
	../../build/sdf/shared/libsdfshared.a \
    ../../build/sdf/protocol/action/libsdfaction.a \
    ../../build/sdf/protocol/home/libsdfhome.a \
	../../build/sdf/protocol/libsdfprotocol.a \
	../../build/sdf/ssd/libsdfssd.a \
	../../build/sdf/ssd/clipper/libclipper.a \
	../../build/sdf/ssd/fifo/libfifo.a \
	../../build/sdf/ssd/libsdfssd.a \
	../../build/sdf/protocol/replication/libsdfreplication.a \
	../../build/sdf/ecc/libecc.a \
	../../build/sdf/utils/libutils.a \
	../../build/sdf/sdfmsg/libsdfmsgqueue.a \
	../../build/sdf/sdftcp/libsdfmtp.a \
	../../build/sdf/platform/tool/hotkey/libhotkey.a \
	../../build/sdf/misc/libmisc.a \
	../../build/sdf/fth/libfth.a \
	../../build/sdf/platform/libplatform.a
# ../../build/../3rd-party/libevent-1.4.3-et/lib/libevent.a
do
ar t $i|sed "s+^+$(dirname $i)/+" >>tmp
done

ar -sr libsdf.a $(cat tmp) ../../build/../3rd-party/libevent-1.4.3-et/lib/libevent.so

#ranlib libsdf.o
# -lpthread -lrt -lnsl -lutil -lm -la.a -lsasl2

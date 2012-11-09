#!/bin/bash -x

echo >tmp
for i in  ../../build/sdf/api/libsdf_o.a \
	../../build/sdf/agent/libsdfagent_o.a \
	../../build/sdf/shared/libsdfshared_o.a \
    ../../build/sdf/protocol/action/libsdfaction_o.a \
    ../../build/sdf/protocol/home/libsdfhome_o.a \
	../../build/sdf/protocol/libsdfprotocol_o.a \
	../../build/sdf/ssd/libsdfssd_o.a \
	../../build/sdf/ssd/clipper/libclipper_o.a \
	../../build/sdf/ssd/fifo/libfifo_o.a \
	../../build/sdf/ssd/libsdfssd_o.a \
	../../build/sdf/protocol/replication/libsdfreplication_o.a \
	../../build/sdf/ecc/libecc_o.a \
	../../build/sdf/utils/libutils_o.a \
	../../build/sdf/sdfmsg/libsdfmsgqueue_o.a \
	../../build/sdf/sdftcp/libsdfmtp_o.a \
	../../build/sdf/platform/tool/hotkey/libhotkey_o.a \
	../../build/sdf/misc/libmisc_o.a \
	../../build/sdf/fth/libfth_o.a \
	../../build/sdf/platform/libplatform_o.a
# ../../build/../3rd-party/libevent-1.4.3-et/lib/libevent_o.a
do
ar t $i|sed "s+^+$(dirname $i)/+" >>tmp
done

ar -sr libsdf.a $(cat tmp) ../../../3rd-party/libevent-1.4.3-et/lib/libevent.so

#ranlib libsdf.o
# -lpthread -lrt -lnsl -lutil -lm -la_o.a -lsasl2

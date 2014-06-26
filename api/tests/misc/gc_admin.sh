#!/bin/bash

set -ex

at_exit() { kill $CHLD; rm $PF; rm /tmp/zs_listen_pid.$CHLD; }
trap at_exit EXIT

WD=$(readlink -f $(dirname $0))
PORT=55555

[ -n "$ZS_PROPERTY_FILE" ] || ZS_PROPERTY_FILE=$WD/../conf/zs.prop

PF=/tmp/zs.prop.$$
sed -e '/ZS_ADMIN_PORT/d' \
	-e '/ZS_SLAB_GC/d' \
	-e '/ZS_SLAB_GC_THRESHOLD/d' \
		$ZS_PROPERTY_FILE >$PF
echo "ZS_ADMIN_PORT=$PORT" >>$PF

export ZS_PROPERTY_FILE=$PF
$WD/ZS_Listen&

CHLD=$!

while ! [ -f /tmp/zs_listen_pid.$CHLD ] && [ $((i++ < 120)) ]; do
	sleep 1
done

admin_cmd() { OK=$(echo $1|nc localhost $PORT); [[ $OK == *$2* ]] || exit 1;}

admin_cmd "gc enable" "OK"
admin_cmd "gc disable" "OK"

admin_cmd "gc enable" "OK"
admin_cmd "gc enable" "OK"
admin_cmd "gc disable" "OK"

admin_cmd "gc threshold 44" "OK"
admin_cmd "gc enable" "OK"
admin_cmd "gc disable" "OK"

admin_cmd "gc enable" "OK"
admin_cmd "gc threshold 55" "OK"
admin_cmd "gc disable" "OK"

admin_cmd "gc threshold asdf" "Invalid slab GC threshold: asdf"
admin_cmd "gc threshold -100" "Invalid slab GC threshold: -100"
admin_cmd "gc threshold 0" "OK"
admin_cmd "gc threshold" "Invalid arguments. Slab GC threshold."
admin_cmd "gc" "Invalid argument!"
admin_cmd "gc1" "Invalid command:(gc1)"
admin_cmd "gc asdf" "Invalid arguments."

#!/bin/bash

set -ex

at_exit() { kill $CHLD; rm $PF; }
trap at_exit EXIT

WD=$(readlink -f $(dirname $0))

[ -n $FDF_PROPERTY_FILE ] || FDF_PROPERTY_FILE=$WD/../conf/fdf.prop

PF=/tmp/fdf.prop.$$
sed -e '/FDF_ADMIN_PORT/d' \
	-e '/FDF_SLAB_GC/d' \
	-e '/FDF_SLAB_GC_THRESHOLD/d' \
		$FDF_PROPERTY_FILE >$PF
echo "FDF_ADMIN_PORT=55555" >>$PF

FDF_PROPERTY_FILE=$PF
$WD/FDF_Listen&

CHLD=$!
sleep 10

admin_cmd() { OK=$(echo $1|nc localhost 55555); [[ $OK == *$2* ]] || exit 1;}

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

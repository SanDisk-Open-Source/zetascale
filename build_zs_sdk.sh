#!/bin/bash

set -e
export WD=$(readlink -f $(dirname $0))

while [ $# -gt 0 ]; do
	[ "$1" == "--optimize" ] && optimize=$1
	[ "$1" == "--test" ] && run_tests=$1
	[ "$1" == "--pkg" ] && pkg=$1
	[ "$1" == "--trace" ] && TRACE=ON
	[ "$1" == "--with-jni" ] && WITHJNI=ON
	shift
done

[ -n "${ZS_SDK_VERSION}" ] || export ZS_SDK_VERSION=2.0
[ -n "${BUILD_NUMBER}" ] || export BUILD_NUMBER=$(date +%s)
#
if test -e .git; then
	BRANCH=$(git rev-parse --abbrev-ref HEAD)
	[ -n "${SVN_REVISION}" ] || export SVN_REVISION=$(git rev-list HEAD|head -c 8)
else
	[ -n "${SVN_REVISION}" ] || export SVN_REVISION=$(svn info | awk '/Last Changed Rev:/ {print $NF}')
fi
#
[ -n "${SCHOONER_RELEASE}" ] || export SCHOONER_RELEASE=${SVN_REVISION}.${BUILD_NUMBER}

#if [ "$1" == "--test" ] || [ "$2" == "--test" ]; then
#[ -d test_suite ] || svn co svn://svn.schoonerinfotech.net/schooner-trunk/ht_delivery/qa/ZS_test/ZS_test_framework test_suite
#[ -d zs_slap ] || svn://svn.schoonerinfotech.net/schooner-trunk/ht_delivery/qa/sdf_test/zs_slap zs_slap
#fi

VERSION=${ZS_SDK_VERSION}-${SCHOONER_RELEASE}
NCPU=$(cat /proc/cpuinfo|grep CPU|wc -l)
NCPU=$((NCPU*12/10))

DBG=OFF
PKG_NAME=zs_sdk-$VERSION
PKG_TEST=zs_sdk_test_package-$VERSION

[ -n "$BRANCH" ] && PKG_NAME=$PKG_NAME-$BRANCH && PKG_TEST=$PKG_TEST-$BRANCH

if [ -z "$optimize" ]; then
	PKG_NAME=$PKG_NAME-dbg
	PKG_TEST=$PKG_TEST-dbg
	DBG=ON
fi

cd $WD

SDK_DIR=$WD/zs-build/$PKG_NAME
rm -fr $SDK_DIR
mkdir -p $SDK_DIR/{config,lib,include,samples,utils}

echo "Building DEBUG=$DBG shared lib"
rm -f CMakeCache.txt
cmake $WD -DNCPU=$NCPU -DDEBUG=$DBG -DZS_REVISION="$VERSION" -DTRACE=$TRACE
make -j $NCPU

#Copy zsck utils
cp $WD/zsck/zsck $WD/zsck/zsformat $WD/zsck/zsmetafault -t $SDK_DIR/utils
#Packaging
#scp -r lab67:/schooner/backup/fdf_extra/lib/* $SDK_DIR/lib
#wget http://lab67.schoonerinfotech.net/zs/lib/libpthread.so.0 -O $SDK_DIR/lib/libpthread.so.0
cp -f $WD/output/lib/* $SDK_DIR/lib
cp -a $WD/api/zs.h $SDK_DIR/include
cp -a $WD/api/tests/sample_program.c $SDK_DIR/samples
cp -a $WD/api/tests/Makefile.sample $SDK_DIR/samples/Makefile
#cp -a $WD/doc/ZS_programming_guide.docx $SDK_DIR/docs
#cp -a $WD/doc/ZS1.2_DesignDocument.docx $SDK_DIR/docs
mkdir -p $SDK_DIR/include/common
cp -a $WD/common/zstypes.h $SDK_DIR/include/common
cp -a $WD/api/tests/conf/zs_sample.prop $SDK_DIR/config/
#check withjni option
#when withjni=true, get jni code and compiling
#if [ "is$WITHJNI" == "isON" ]
#then
#    rm -fr ZSJNI 
#    jniurl=https://10.196.60.217/svn/schooner-trunk/ht_delivery/rd/fdfjni/trunk
#    svn co $jniurl ZSJNI 
#    cd ZSJNI 
#    sed -i "/sdk$/d" bin/prepare_zssdk.sh 
#    cp -r $SDK_DIR ./zs_sdk
#
#   export ZS_LIB=$PWD/zs_sdk/lib/libzs.so
#    mvn clean && mvn install -Dmaven.test.skip=true
#    cp target/*.jar zs_sdk/lib/
#    rm -fr $SDK_DIR && mv zs_sdk $SDK_DIR
#    cd - 
#    cd ..
#fi
#


cd $SDK_DIR/..
tar --exclude=.svn --exclude=.git --exclude=libzs.a -czf $WD/$PKG_NAME.tar.gz $PKG_NAME
if [ -n "$pkg" ]; then
    rm -fr $PKG_TEST 
    mkdir -p $PKG_TEST/ZS_SDK_Test_Package/ZS_test/ZS_unit_test
    cp -av $WD/api/tests/* $PKG_TEST/ZS_SDK_Test_Package/ZS_test/ZS_unit_test
    cp -r $SDK_DIR $PKG_TEST/ZS_SDK_Test_Package/ZS_SDK
    cd $PKG_TEST
    tar --exclude=.svn --exclude=.git --exclude=libzs.a -czf $WD/$PKG_TEST.tar.gz ZS_SDK_Test_Package
fi
#rm -fr $PKG_NAME $PKG_TEST

echo -e "\n** BUILD SUCCESSFUL **\n"

#Running tests
if [ -n "$run_tests" ]; then
	cd $WD

	#export BTREE_LIB=$WD/output/lib/libbtree.so
	export ZS_LIB=$WD/output/lib/libzs.so
	export ZS_PROPERTY_FILE=$WD/api/tests/conf/zs.prop

	if [[ "$(hostname)" =~ "xen" ]]; then
		ctest
	else
		ctest -j$NCPU
	fi
fi

#echo -e "\nVariables:\nexport BTREE_LIB=$WD/output/lib/libbtree.so\nexport ZS_LIB=$WD/output/lib/libzs.so\nexport ZS_PROPERTY_FILE=$WD/api/tests/conf/zs.prop\n"
echo -e "\nVariables:\nexport ZS_LIB=$WD/output/lib/libzs.so\nexport ZS_PROPERTY_FILE=$WD/api/tests/conf/zs.prop\n"

echo -e "Package: root@$(hostname -s):$WD/$PKG_NAME.tar.gz\n"

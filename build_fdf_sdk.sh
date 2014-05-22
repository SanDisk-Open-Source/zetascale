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

[ -n "${FDF_SDK_VERSION}" ] || export FDF_SDK_VERSION=2.0
[ -n "${BUILD_NUMBER}" ] || export BUILD_NUMBER=$(date +%s)
#
if test -d .git; then
	BRANCH=$(git rev-parse --abbrev-ref HEAD)
	[ -n "${SVN_REVISION}" ] || export SVN_REVISION=$(git rev-list HEAD|head -c 8)
else
	[ -n "${SVN_REVISION}" ] || export SVN_REVISION=$(svn info | awk '/Last Changed Rev:/ {print $NF}')
fi
#
[ -n "${SCHOONER_RELEASE}" ] || export SCHOONER_RELEASE=${SVN_REVISION}.${BUILD_NUMBER}

#if [ "$1" == "--test" ] || [ "$2" == "--test" ]; then
#[ -d test_suite ] || svn co svn://svn.schoonerinfotech.net/schooner-trunk/ht_delivery/qa/FDF_test/FDF_test_framework test_suite
#[ -d fdf_slap ] || svn://svn.schoonerinfotech.net/schooner-trunk/ht_delivery/qa/sdf_test/fdf_slap fdf_slap
#fi

VERSION=${FDF_SDK_VERSION}-${SCHOONER_RELEASE}
NCPU=$(cat /proc/cpuinfo|grep CPU|wc -l)
NCPU=$((NCPU*12/10))

DBG=OFF
PKG_NAME=fdf_sdk-$VERSION
PKG_TEST=fdf_sdk_test_package-$VERSION

[ -n "$BRANCH" ] && PKG_NAME=$PKG_NAME-$BRANCH && PKG_TEST=$PKG_TEST-$BRANCH

if [ -z "$optimize" ]; then
	PKG_NAME=$PKG_NAME-dbg
	PKG_TEST=$PKG_TEST-dbg
	DBG=ON
fi

cd $WD

SDK_DIR=$WD/fdf-build/$PKG_NAME
rm -fr $SDK_DIR
mkdir -p $SDK_DIR/{config,lib,include,samples}

echo "Building DEBUG=$DBG shared lib"
rm -f CMakeCache.txt
cmake $WD -DNCPU=$NCPU -DDEBUG=$DBG -DFDF_REVISION="$VERSION" -DTRACE=$TRACE
make -j $NCPU

#Packaging
cp -f $WD/output/lib/* $SDK_DIR/lib
cp -a $WD/api/fdf.h $SDK_DIR/include
cp -a $WD/api/tests/sample_program.c $SDK_DIR/samples
cp -a $WD/api/tests/Makefile.sample $SDK_DIR/samples/Makefile
#cp -a $WD/doc/FDF_programming_guide.docx $SDK_DIR/docs
#cp -a $WD/doc/FDF1.2_DesignDocument.docx $SDK_DIR/docs
mkdir -p $SDK_DIR/include/common
cp -a $WD/common/fdftypes.h $SDK_DIR/include/common
cp -a $WD/api/tests/conf/fdf_sample.prop $SDK_DIR/config/
#check withjni option
#when withjni=true, get jni code and compiling
if [ "is$WITHJNI" == "isON" ]
then
    rm -fr fdfjni_2.0
    jniurl=http://svn.schoonerinfotech.net/svn/schooner-trunk/ht_delivery/rd/fdfjni/trunk
    svn co $jniurl FDFJNI 
    cd FDFJNI
    sed -i "/sdk$/d" bin/update_sdk
    cp -r $SDK_DIR ./fdf_sdk
	export BTREE_LIB=$PWD/fdf_sdk/lib/libbtree.so
	export FDF_LIB=$PWD/fdf_sdk/lib/libfdf.so
    mvn clean && mvn install -Dmaven.test.skip=true
    rm -fr $WD/$PKG_NAME && mv fdf_sdk $WD/$PKG_NAME
    cd - 
    cd ..
fi
#


cd $SDK_DIR/..
tar --exclude=.svn --exclude=.git --exclude=libfdf.a -czf $WD/$PKG_NAME.tar.gz $PKG_NAME
if [ -n "$pkg" ]; then
    rm -fr $PKG_TEST 
    mkdir -p $PKG_TEST/FDF_SDK_Test_Package/FDF_test/FDF_unit_test
    cp -av $WD/api/tests/* $PKG_TEST/FDF_SDK_Test_Package/FDF_test/FDF_unit_test
    cp -r $SDK_DIR $PKG_TEST/FDF_SDK_Test_Package/FDF_SDK
    cd $PKG_TEST
    tar --exclude=.svn --exclude=.git --exclude=libfdf.a -czf $WD/$PKG_TEST.tar.gz FDF_SDK_Test_Package
fi
#rm -fr $PKG_NAME $PKG_TEST

echo -e "\n** BUILD SUCCESSFUL **\n"

#Running tests
if [ -n "$run_tests" ]; then
	cd $WD

	export BTREE_LIB=$WD/output/lib/libbtree.so
	export FDF_LIB=$WD/output/lib/libfdf.so
	export FDF_PROPERTY_FILE=$WD/api/tests/conf/fdf.prop

	if [[ "$(hostname)" =~ "xen" ]]; then
		ctest
	else
		ctest -j$NCPU
	fi
fi

echo -e "\nVariables:\nexport BTREE_LIB=$WD/output/lib/libbtree.so\nexport FDF_LIB=$WD/output/lib/libfdf.so\nexport FDF_PROPERTY_FILE=$WD/api/tests/conf/fdf.prop\n"

echo -e "Package: root@$(hostname -s):$WD/$PKG_NAME.tar.gz\n"

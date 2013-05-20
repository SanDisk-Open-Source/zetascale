#!/bin/bash

set -e
export WD=$(readlink -f $(dirname $0))

[ -n "${FDF_SDK_VERSION}" ] || export FDF_SDK_VERSION=1.2
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

[ -n "$BRANCH" ] && PKG_NAME=$PKG_NAME-$BRANCH

if [ "$1" != "--optimize" ] && [ "$2" != "--optimize" ]; then
	PKG_NAME=$PKG_NAME-dbg
	DBG=ON
fi

cd $WD

SDK_DIR=$WD/fdf-build/$PKG_NAME
rm -fr $SDK_DIR
mkdir -p $SDK_DIR/{config,lib,include,samples}

echo "Building DEBUG=$DBG shared lib"
rm -f CMakeCache.txt
cmake $WD -DNCPU=$NCPU -DDEBUG=$DBG -DFDF_REVISION="$VERSION"
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
#
cd $SDK_DIR/..
tar --exclude=.svn --exclude=.git --exclude=libfdf.a -czf $WD/$PKG_NAME.tar.gz $PKG_NAME
rm -fr $PKG_NAME

echo -e "\n** BUILD SUCCESSFUL **\n"

#Running tests
if [ "$1" == "--test" ] || [ "$2" == "--test" ]; then
	cd $WD

	export BTREE_LIB=$WD/output/lib/libbtree.so
	export FDF_LIB=$WD/output/lib/libfdf.so
	export FDF_PROPERTY_FILE=$WD/api/tests/conf/fdf.prop

	ctest -j$NCPU
fi

echo -e "\nVariables:\nexport BTREE_LIB=$WD/output/lib/libbtree.so\nexport FDF_LIB=$WD/output/lib/libfdf.so\nexport FDF_PROPERTY_FILE=$WD/api/tests/conf/fdf.prop\n"

echo -e "Package: root@$(hostname -s):$WD/$PKG_NAME.tar.gz\n"

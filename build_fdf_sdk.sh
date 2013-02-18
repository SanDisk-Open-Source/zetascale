#!/bin/bash

set -xe
export WD=$(readlink -f $(dirname $0))

BUILD=$WD/fdf-build

if [ -z "${FDF_SDK_VERSION}" ]; then
	export FDF_SDK_VERSION=1.2
fi
#
if [ -z ${SVN_REVISION} ]; then
	export SVN_REVISION=$(svn info | awk '/Last Changed Rev:/ {print $NF}')
fi
#
if [ -z ${BUILD_NUMBER} ]; then
	export BUILD_NUMBER=$(date +%s)
fi
#
if [ -z ${SCHOONER_RELEASE} ]; then
	export SCHOONER_RELEASE=${SVN_REVISION}.${BUILD_NUMBER}
fi

VERSION=${FDF_SDK_VERSION}-${SCHOONER_RELEASE}
NCPU=$(cat /proc/cpuinfo|grep CPU|wc -l)

DBG=OFF
PKG_NAME=fdf_sdk-$VERSION
if [ "$1" != "--optimize" ] && [ "$2" != "--optimize" ]; then
	PKG_NAME=$PKG_NAME-dbg
	DBG=ON
fi

cd $WD

SDK_DIR=$BUILD/$PKG_NAME
rm -fr $SDK_DIR
mkdir -p $SDK_DIR/{config,lib,include,samples,tests,docs}

echo "Building DEBUG=$DBG shared lib"
rm -f CMakeCache.txt
cmake $WD -DDEBUG=$DBG -DFDF_REVISION="$VERSION"
make -j $NCPU

#Packaging
cp -fv $WD/output/lib/* $SDK_DIR/lib
cp -av $WD/api/fdf.h $SDK_DIR/include
mkdir -p $SDK_DIR/include/common
cp -va $WD/common/fdf{stats,types}.h $SDK_DIR/include/common
#
cd $SDK_DIR/..
tar --exclude=.svn --exclude=.git --exclude=libfdf.a -czf $WD/$PKG_NAME.tar.gz $PKG_NAME
rm -fr $PKG_NAME

echo -e "\n** BUILD SUCCESSFUL **\n"

echo -e "\n** Please set the following variables and modify FDF_PROPERTY_FILE\nto conform your environment in order to run 'make test':\nexport FDF_LIB=$WD/output/lib/libfdf.so\nexport FDF_PROPERTY_FILE=$WD/api/tests/conf/fdf.prop\n**\n"

#Running tests
if [ "$1" == "--test" ] || [ "$2" == "--test" ]; then
	cd $WD

	export FDF_LIB=$WD/output/lib/libfdf.so
	export FDF_PROPERTY_FILE=$WD/api/tests/conf/fdf.prop

	make test
fi


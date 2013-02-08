#!/bin/bash
#

BUILD=./fdf-build

set -xe
export mWORKSPACE=$(readlink -f $(dirname $0))
#
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
#

membrain_sdk_dir=$BUILD/fdf_sdk-${FDF_SDK_VERSION}-${SCHOONER_RELEASE}
rm -fr ${membrain_sdk_dir}
mkdir -p ${membrain_sdk_dir}/{config,lib,include,samples,tests,docs}

# FFDC log parser magic
#cd ${mWORKSPACE}/platform
#/usr/bin/perl ${mWORKSPACE}/platform/fdf_ffdc_parser.pl ${mWORKSPACE}

cd ${mWORKSPACE}

#optimized shared lib
echo "Building OPTIMIZED shared lib"
rm -f CMakeCache.txt
cmake ${mWORKSPACE} -DFDF_REVISION="${FDF_SDK_VERSION}-${SCHOONER_RELEASE}" #Default: -DDEBUG=OFF -DBUILD_SHARED=ON
make -j24
#ctest
cp -fv ${mWORKSPACE}/output/lib/* ${membrain_sdk_dir}/lib
#make clean
#
#debug shared lib
#echo "Building DEBUG shared lib"
#rm -f CMakeCache.txt
#cmake ${mWORKSPACE} -DDEBUG=ON -DBUILD_SHARED=ON  -DFDF_REVISION="${FDF_SDK_VERSION}-${SCHOONER_RELEASE}"
#make -j24
#cp -fv ${mWORKSPACE}/output/lib/* ${membrain_sdk_dir}/lib
#
#
#cp -av ${mWORKSPACE}/config/memcached.properties.default ${membrain_sdk_dir}/config
cp -av ${mWORKSPACE}/api/fdf.h ${membrain_sdk_dir}/include
#cp -av ${mWORKSPACE}/api/fdf.h ${membrain_sdk_dir}/samples
#cp -av ${mWORKSPACE}/api/tests/fdf_multi_test.c ${membrain_sdk_dir}/samples
#cp -av ${mWORKSPACE}/api/Makefile.sdk ${membrain_sdk_dir}/samples/Makefile
#
mkdir -p ${membrain_sdk_dir}/include/common
cp -va ${mWORKSPACE}/common/fdf{stats,types}.h ${membrain_sdk_dir}/include/common
#
cd ${membrain_sdk_dir}/..
tar --exclude=.svn --exclude=.git --exclude=libfdf.a --exclude=libfdfdbg.a -czf ${mWORKSPACE}/fdf_sdk-${FDF_SDK_VERSION}-${SCHOONER_RELEASE}.tar.gz fdf_sdk-${FDF_SDK_VERSION}-${SCHOONER_RELEASE}
rm -fr fdf_sdk-${FDF_SDK_VERSION}-${SCHOONER_RELEASE}

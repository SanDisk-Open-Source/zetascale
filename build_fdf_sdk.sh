#!/bin/bash
#

set -xe
export mWORKSPACE=$(readlink -f $(dirname $0))

BUILD=${mWORKSPACE}/fdf-build

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

cd ${mWORKSPACE}

CPU=$(cat /proc/cpuinfo|grep CPU|wc -l)

rm -f CMakeCache.txt

DBG=ON
if [ "$1" == "--optimize" ] || [ "$2" == "--optimize" ]; then
	DBG=OFF
fi

echo "Building DEBUG=$DBG shared lib"
cmake ${mWORKSPACE} -DDEBUG=$DBG -DFDF_REVISION="${FDF_SDK_VERSION}-${SCHOONER_RELEASE}"
make -j $CPU
cp -fv ${mWORKSPACE}/output/lib/* ${membrain_sdk_dir}/lib

#
cp -av ${mWORKSPACE}/api/fdf.h ${membrain_sdk_dir}/include
mkdir -p ${membrain_sdk_dir}/include/common
cp -va ${mWORKSPACE}/common/fdf{stats,types}.h ${membrain_sdk_dir}/include/common
#
cd ${membrain_sdk_dir}/..
tar --exclude=.svn --exclude=.git --exclude=libfdf.a -czf ${mWORKSPACE}/fdf_sdk-${FDF_SDK_VERSION}-${SCHOONER_RELEASE}.tar.gz fdf_sdk-${FDF_SDK_VERSION}-${SCHOONER_RELEASE}
rm -fr fdf_sdk-${FDF_SDK_VERSION}-${SCHOONER_RELEASE}

echo -e "\n** BUILD SUCCESSFUL **\n"

echo -e "\n** Please set the following variables and modify FDF_PROPERTY_FILE\nto conform your environment in order to run 'make test':\nexport FDF_LIB=${mWORKSPACE}/output/lib/libfdf.so\nexport FDF_PROPERTY_FILE=${mWORKSPACE}/api/tests/conf/fdf.prop\n**\n"

#Running tests
if [ "$1" == "--test" ] || [ "$2" == "--test" ]; then
	cd ${mWORKSPACE}

	export FDF_LIB=${mWORKSPACE}/output/lib/libfdf.so
	export FDF_PROPERTY_FILE=${mWORKSPACE}/api/tests/conf/fdf.prop

	make test
fi


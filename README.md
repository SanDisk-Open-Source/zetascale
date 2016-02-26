# ZetaScale

ZetaScale provides an object API with configurable attributes, and leverages flash storage for high performance and high availability.

ZetaScale was developed because many applications realize limited benefits from flash storage without extensive system level optimization. ZetaScale incorporates many of the system level optimizations that are required to exploit flash. Applications can be flash-optimized with much less effort by using ZetaScale as their storage layer.

The system level optimizations in ZetaScale include:

  - Intelligent DRAM caching.
  - Heavily optimized access paths for high performance.
  - Optimized threading to maximize concurrency and minimize response time.
  - Configurable flash management algorithms to optimize different workloads.

## Build ZetaScale

### CentOS
```sh
#yum groupinstall 'Development Tools'
#yum install libaio-devel libevent-devel snappy-devel
```
### Ubuntu
```sh
#apt-get install build-essential libaio-dev libevent-dev libsnappy-dev
```

### Install lz4
```sh
#cd /opt
#git clone https://github.com/Cyan4973/lz4.git
#cd lz4/lib
#make install
```
### debug build
```sh
#./build_zs_sdk.sh
```
### optimized build
```sh
#./build_zs_sdk.sh --optimize
```
## Basic test
```sh
#export ZS_LIB=/path/to/zetascale/library/libzs.so
#export ZS_PROPERTY_FILE=/path/to/zetascale/config/zs.prop
#cd ./api/tests/
#make test
```
## Documetation

>doc/FDF_programming_guide.docx

License
----

LGPL

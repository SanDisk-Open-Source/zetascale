#include "zs.h"
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#define ZS_MAX_KEY_LEN 256

#define OBJ_COUNT 100000

int
main()
{
    ZS_container_props_t props;
    struct ZS_state *zs_state;
    struct ZS_thread_state *thd_state;
    struct ZS_iterator *iterator;
    ZS_status_t	status;
    ZS_cguid_t cguid;
    uint64_t datalen;
    char	*key;
    char	*data = NULL;
    char	data1[256] = "data is just for testing pstats";
    int i;
    int count = 0;
    uint32_t keylen;

    char key_var[ZS_MAX_KEY_LEN] ={0};

    ZSSetProperty("ZS_REFORMAT", "1");
    if (ZSInit(&zs_state) != ZS_SUCCESS) {
		return -1;
	}
    ZSInitPerThreadState(zs_state, &thd_state);	

    ZSLoadCntrPropDefaults(&props);

    props.persistent = 1;
    props.evicting = 0;
    props.writethru = 1;
    //	props.async_writes = 0;
    props.durability_level= ZS_DURABILITY_SW_CRASH_SAFE;
    //	props.fifo_mode = 1;
    props.fifo_mode = 0;

    status = ZSOpenContainer(thd_state, "cntr", &props, ZS_CTNR_CREATE, &cguid);
    if (status != ZS_SUCCESS) {
        printf("Open Cont failed with error=%x.\n", status);
        return -1;	
    }

    for (i = 1; i <= OBJ_COUNT; i++) {
        memset(key_var, 0, ZS_MAX_KEY_LEN);
        sprintf(key_var, "key_%d", i);
        sprintf(data1, "data_%d", i);

        ZSWriteObject(thd_state, cguid, key_var, strlen(key_var) + 1, data1, strlen(data1), 0);
    }

    count = 0;
    status =  ZSEnumerateContainerObjects(thd_state, cguid, &iterator);

    while (ZSNextEnumeratedObject(thd_state, iterator, &key, &keylen, &data, &datalen) == ZS_SUCCESS) {
        ZSFreeBuffer(data);
        ZSFreeBuffer(key);
        count++;
    }
    ZSFinishEnumeration(thd_state, iterator);

    ZS_stats_t stats;
    status = ZSGetContainerStats(thd_state, cguid, &stats);
    printf("Enum count=%d Pstats count=%ld size = %ld\n",
            count, stats.cntr_stats[ZS_CNTR_STATS_NUM_OBJS], stats.cntr_stats[ZS_CNTR_STATS_USED_SPACE]);

    ZSCloseContainer(thd_state, cguid);
    ZSReleasePerThreadState(&thd_state);

    ZSShutdown(zs_state);

    if (stats.cntr_stats[ZS_CNTR_STATS_NUM_OBJS] == count) {
        return 0;
    } else {
    return 1;
    }
}

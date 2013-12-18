#include <api/fdf.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#define FDF_MAX_KEY_LEN 256

#define OBJ_COUNT 100000

int
main()
{
    FDF_container_props_t props;
    struct FDF_state *fdf_state;
    struct FDF_thread_state *thd_state;
    struct FDF_iterator *iterator;
    FDF_status_t	status;
    FDF_cguid_t cguid;
    uint64_t datalen;
    char	*key;
    char	*data = NULL;
    char	data1[256] = "data is just for testing pstats";
    int i;
    int count = 0;
    uint32_t keylen;

    char key_var[FDF_MAX_KEY_LEN] ={0};

    FDFSetProperty("FDF_REFORMAT", "1");
    FDFInit(&fdf_state);
    FDFInitPerThreadState(fdf_state, &thd_state);	

    FDFLoadCntrPropDefaults(&props);

    props.persistent = 1;
    props.evicting = 0;
    props.writethru = 1;
    //	props.async_writes = 0;
    props.durability_level= FDF_DURABILITY_SW_CRASH_SAFE;
    //	props.fifo_mode = 1;
    props.fifo_mode = 0;

    status = FDFOpenContainer(thd_state, "cntr", &props, FDF_CTNR_CREATE, &cguid);
    if (status != FDF_SUCCESS) {
        printf("Open Cont failed with error=%x.\n", status);
        return -1;	
    }

    for (i = 1; i <= OBJ_COUNT; i++) {
        memset(key_var, 0, FDF_MAX_KEY_LEN);
        sprintf(key_var, "key_%d", i);
        sprintf(data1, "data_%d", i);

        FDFWriteObject(thd_state, cguid, key_var, strlen(key_var) + 1, data1, strlen(data1), 0);
    }

    count = 0;
    status =  FDFEnumerateContainerObjects(thd_state, cguid, &iterator);

    while (FDFNextEnumeratedObject(thd_state, iterator, &key, &keylen, &data, &datalen) == FDF_SUCCESS) {
        FDFFreeBuffer(data);
        FDFFreeBuffer(key);
        count++;
    }
    FDFFinishEnumeration(thd_state, iterator);

    FDF_stats_t stats;
    status = FDFGetContainerStats(thd_state, cguid, &stats);
    printf("Enum count=%d Pstats count=%ld size = %ld\n",
            count, stats.cntr_stats[FDF_CNTR_STATS_NUM_OBJS], stats.cntr_stats[FDF_CNTR_STATS_USED_SPACE]);

    FDFCloseContainer(thd_state, cguid);
    FDFReleasePerThreadState(&thd_state);

    FDFShutdown(fdf_state);

    if (stats.cntr_stats[FDF_CNTR_STATS_NUM_OBJS] == count) {
        return 0;
    } else {
    return 1;
    }
}

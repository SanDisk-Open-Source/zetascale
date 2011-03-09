#include "common/sdftypes.h"
#include "platform/prng.h"
#include "platform/time.h"
#include "platform/assert.h"
#include "platform/stdlib.h"
#include "platform/stdio.h"
#include "platform/string.h"
#include "fth/fth.h"
#include "platform/fth_scheduler.h"

#include "../test_model.h"
#include "../test_config.h"
#include "../../../shared/shard_meta.h"

typedef struct replication_test_model rtm_t;

int
main()
{
    struct replication_test_config config;
    rtm_t *rtm = replication_test_model_alloc(&config);
    struct timeval now;
    plat_gettimeofday(&now, NULL);
    char key[100], data[100];
    struct SDF_shard_meta shard_meta;

    shard_meta.sguid = 0;
    rtm_start_create_shard(rtm, now, 0, &shard_meta);
    sprintf(key, "%s", "google");
    sprintf(data, "%s", "Sebstian");

    rtm_start_write(rtm, 0, 0, now, 1, NULL, (const void *)(key), strlen(key),
                    (const void *)data, strlen(data), NULL);
    rtm_general_op_t *op_read = rtm_start_read(rtm, 0, 0, now, 1,
                                               (const void *)(key), strlen(key));

    rtm_read_complete(op_read, now, SDF_SUCCESS, data, strlen(data));
    rtm_free(rtm);
}

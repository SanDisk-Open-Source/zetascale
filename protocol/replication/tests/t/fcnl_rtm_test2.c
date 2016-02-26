//----------------------------------------------------------------------------
// ZetaScale
// Copyright (c) 2016, SanDisk Corp. and/or all its affiliates.
//
// This program is free software; you can redistribute it and/or modify it under
// the terms of the GNU Lesser General Public License version 2.1 as published by the Free
// Software Foundation;
//
// This program is distributed in the hope that it will be useful, but WITHOUT
// ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
// FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License v2.1 for more details.
//
// A copy of the GNU Lesser General Public License v2.1 is provided with this package and
// can also be found at: http://opensource.org/licenses/LGPL-2.1
// You should have received a copy of the GNU Lesser General Public License along with
// this program; if not, write to the Free Software Foundation, Inc., 59 Temple
// Place, Suite 330, Boston, MA 02111-1307 USA.
//----------------------------------------------------------------------------

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
    int i = 0;
    char key[100][100], data[100];
    struct SDF_shard_meta shard_meta;
    shard_meta.sguid = 0;
    rtm_start_create_shard(rtm, now, 0, &shard_meta);
    for (i = 0; i < 100; i ++) {
        printf("index = %d\n", i);
        sprintf(key[i], "test %d", i);
        sprintf(data, "test_data %d", i);
        rtm_general_op_t *op = rtm_start_write(rtm, 0, i,
                                               now, 1, NULL, (const void *)(key[i]), strlen(key[i]),
                                               (const void *)data, strlen(data), NULL);
        if (op) {
            rtm_write_complete(op, now, SDF_SUCCESS);
        }
    }
    for (i = 100; i < 200; i ++) {
        printf("index = %d\n", i - 100);
        sprintf(data, "test_data %d", i - 100);
        rtm_general_op_t *op = rtm_start_read(rtm, 0, i, now, 1,
                                              (const void *)(key[i-100]), strlen(key[i-100]));
        int ret = 1;
        if (op) {
            ret = rtm_read_complete(op, now, SDF_SUCCESS, (const void *)data, strlen(data));
        }
        printf("ret = %d, %d\n", ret, i);
    }
    rtm_free(rtm);
}

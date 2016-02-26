/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

/*
 * File:   fthOpt.c
 * Author: drew
 *
 * Created on June 19, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http: //www.schoonerinfotech.com/
 *
 * $Id: fthOpt.c 5903 2009-02-18 02:05:10Z drew $
 */

#include "fth.h"
#include "platform/errno.h"
#include "platform/stdio.h"
#include "platform/stdlib.h"
#include "platform/string.h"

int
fthParseIdleMode(const char *idleType) {
    int ret;

#define item(caps, lower, description) \
    if (0 == strcmp(idleType, #lower)) {                                       \
        fthConfig.idleMode = FTH_IDLE_ ## caps;                                \
        ret = 0;                                                               \
    } else
    FTH_IDLE_MODE_ITEMS() {
#undef item
        fprintf(stderr, "Unexpected idle mode %s.  Accepted values are\n"
#define item(caps, lower, description) \
                "\t" #lower "\t[" description "]\n"
                FTH_IDLE_MODE_ITEMS(),
#undef item
                idleType);
        ret = -EINVAL;
    }

    return (ret);
}

int
fthParseAffinityCpuMask(const char *affinityCpus) {
    int ret;
    const char *after;
    uint64_t tmp;
    int cpu;
    int mask;

    ret = parse_uint64(&tmp, affinityCpus, &after);
    if (!ret && *after) {
        ret = -EINVAL;
    }
    if (ret) {
        fprintf(stderr, "Invalid fth/affinity_cpus: %s\n",
                plat_strerror(-ret));
    } else {
        fthConfig.affinityMode = FTH_AFFINITY_PER_THREAD;
        CPU_ZERO(&fthConfig.affinityCores);
        for (cpu = 0, mask = 1; tmp; ++cpu, mask <<= 1) {
            if (tmp & mask) {
                CPU_SET(cpu, &fthConfig.affinityCores);
                tmp &= ~mask;
            }
        }
    }

    return (ret);
}

enum parseMode {
    PARSE_FIRST_NUMBER,
    PARSE_RANGE_END
};

int
fthParseAffinityCpus(const char *affinityCpus) {
    int ret = 0;
    const char *ptr = affinityCpus;
    enum parseMode parseMode = PARSE_FIRST_NUMBER;
    int rangeStart = -1;
    char *after;
    int done = 0;
    cpu_set_t cpus;
    int len;
    unsigned long cpu = 0;
    int i;

    CPU_ZERO(&cpus);
    do {
        if (!*ptr) {
            len = ptr - affinityCpus;
            fprintf(stderr,
                    "Error fth/afinity_cpu expects number after \"%*.*s\"\n",
                    len, len, affinityCpus);
            ret = -EINVAL;
        }

        if (!ret) {
            cpu = strtoul(ptr, &after, 10);
            if (cpu == ULONG_MAX || ptr == after) {
                fprintf(stderr, "Error parsing fth/affinity_cpu at %s : %s\n",
                        ptr, plat_strerror(-ret));
            }
        }

        if (!ret) {
            switch (*after)  {
            case 0:
                done = 1;
                /* Fall through  */
            case ',':
                ptr = after + 1;
                switch (parseMode) {
                case PARSE_FIRST_NUMBER:
                    CPU_SET(cpu, &cpus);
                    break;
                case PARSE_RANGE_END:
                    plat_assert(rangeStart >= 0);
                    for (i = rangeStart; i <= cpu; ++i) {
                        CPU_SET(i, &cpus);
                    }
                    rangeStart = -1;
                    break;
                }
                parseMode = PARSE_FIRST_NUMBER;
                break;
            case '-':
                if (parseMode == PARSE_FIRST_NUMBER) {
                    ptr = after + 1;
                    parseMode = PARSE_RANGE_END;
                    rangeStart = cpu;
                    break;
                }
                /* Fall through */
            default:
                fprintf(stderr,
                        "Error parsing fth/affinity_cpus: unexpected %s\n",
                        ptr);
                ret = -EINVAL;
            }
        }
    } while (!ret && !done);

    if (!ret) {
        fthConfig.affinityMode = FTH_AFFINITY_PER_THREAD;
        fthConfig.affinityCores = cpus;
    }

    return (ret);
}

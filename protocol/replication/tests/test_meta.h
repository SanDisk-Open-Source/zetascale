/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

#ifndef REPLICATION_TEST_META_H
#define REPLICATION_TEST_META_H 1

/*
 * File: sdr/protocol/replication/tests/test.h
 *
 * Author: drew
 *
 * Created on October 31, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: test_meta.h 5283 2008-12-24 02:45:30Z xwang $
 */

/**
 * Replication test operation meta-data
 */

#include "platform/string.h"
#include "platform/time.h"


struct replication_test_meta {
    /** @brief Expiration time since the epoch in seconds */
    SDF_time_t exptime;

    /**
     * @brief Object creation time since the epoch in seconds
     *
     * This must be <= flushtime.
     */
    SDF_time_t createtime;

    /** @brief Object expiration time from the epoch, 0 for none */
    struct timeval exptime_val;
};

static void __inline__
replication_test_meta_init(struct replication_test_meta *meta) {
    memset(meta, 0, sizeof (*meta));
}


#endif /* ndef REPLICATION_TEST_META_H */

/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

/*
 * File:   cpu_peertest.c
 * Author: drew
 *
 * Created on January 26, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: cpu_peertest.c 6197 2009-03-11 21:06:54Z drew $
 */

/*
 * Smoke test cpu peer extraction
 */

#include "platform/assert.h"
#include "platform/platform.h"

int
main() {
    uint32_t peers;
    int ret;

    ret = plat_get_cpu_cache_peers(&peers, 0);
    plat_assert_always(!ret);
    plat_assert_always(peers);
    plat_assert_always(peers & 1);

    return (0);
}

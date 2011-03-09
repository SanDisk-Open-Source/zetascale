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

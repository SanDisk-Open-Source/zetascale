/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

#include "platform/assert.h"
#include "platform/once.h"

static int count = 0;

PLAT_ONCE(static, foo);

PLAT_ONCE_IMPL(static, foo, ++count);

int
main() {
    foo_once();
    plat_assert_always(count == 1);
    foo_once();
    plat_assert_always(count == 1);
    return (0);
}

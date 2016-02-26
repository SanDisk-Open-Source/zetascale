/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

/*
 * Copyright (c) 2012-2013, Sandisk Corporation.  All rights reserved.
 */
#include <stdio.h>
#include "test.h"
#include "tlib.h"


/*
 * Test information.
 */
static char *Name = "t5";
static char *Desc = "test 5";


/*
 * A test.
 */
static void
test(zs_t *zs)
{
    /* Initialize ZS */
    test_init(zs, Name);

    fdf.ctr_t *ctr = open_ctr(zs, "C0", ZS_CTNR_CREATE);

    show_obj(ctr, "0000000000000000", "");
    show_obj(ctr, "0000000000000001", "");
    show_obj(ctr, "0000000000000002", "");
    show_obj(ctr, "0000000000000003", "");
    show_obj(ctr, "0000000000000004", "");
    show_obj(ctr, "0000000000000005", "");
    show_obj(ctr, "0000000000000006", "");
    show_obj(ctr, "0000000000000007", "");
    show_obj(ctr, "0000000000000008", "");
    show_obj(ctr, "0000000000000009", "");

    show_objs(ctr);

    fdf.ctr_close(ctr, NULL);

    /* Close ZS */
    zs_done(zs);
}


/*
 * Initialize the test.
 */
static __attribute__ ((constructor)) void
init(void)
{
    test_info(Name, Desc, NULL, NULL, test);
}

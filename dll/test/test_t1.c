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
static char *Name = "t1";
static char *Desc = "test 1";


/*
 * A test.
 */
static void
test(zs_t *zs)
{
    /* Initialize ZS */
    test_init(zs, Name);

    /* Create containers */
    fdf.ctr_t *ctr1 = open_ctr(zs, "C0", ZS_CTNR_CREATE);
    fdf.ctr_t *ctr2 = open_ctr(zs, "C1", ZS_CTNR_CREATE);

    /* Set some objects */
    set_obj(ctr1, "white", "horse");
    set_obj(ctr2, "white", "cow");
    set_obj(ctr1, "red",   "squirrel");
    set_obj(ctr2, "green", "alligator");

    del_obj(ctr2, "white");
    set_obj(ctr2, "yellow", "yak");
    set_obj(ctr2, "yellow", "pig");

    /* Flush containers */
    flush_ctr(ctr1);
    flush_ctr(ctr2);

    /* Show some objects */
    show_obj(ctr1, "white", "horse");
    show_obj(ctr2, "white", "cow");
    show_obj(ctr1, "red",   "squirrel");
    show_obj(ctr2, "green", "alligator");

    /* Show all objects */
    show_objs(ctr1);
    show_objs(ctr2);

    /* Close containers */
    fdf.ctr_close(ctr1, NULL);
    fdf.ctr_close(ctr2, NULL);

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

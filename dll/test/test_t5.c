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
test(void)
{
    /* Initialize FDF */
    fdf_t *fdf = init_fdf(Name);

    fdf_ctr_t *ctr = open_ctr(fdf, "C0", FDF_CTNR_CREATE);

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

    fdf_ctr_close(ctr, NULL);

    /* Close FDF */
    fdf_done(fdf);
}


/*
 * Initialize the test.
 */
static __attribute__ ((constructor)) void
init(void)
{
    test_info(Name, Desc, NULL, test);
}

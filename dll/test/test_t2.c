/*
 * Copyright (c) 2012-2013, Sandisk Corporation.  All rights reserved.
 */
#include <stdio.h>
#include "test.h"
#include "tlib.h"


/*
 * Test information.
 */
static char *Name = "t2";
static char *Desc = "test 2";


/*
 * A test.
 */
static void
test(fdf_t *fdf)
{
    /* Initialize FDF */
    test_init(fdf, Name);

    /* Create containers */
    fdf_ctr_t *ctr1 = open_ctr(fdf, "C0", 0);
    fdf_ctr_t *ctr2 = open_ctr(fdf, "C1", 0);

    /* Set some objects */
    set_obj(ctr1, "purple", "penguin");
    set_obj(ctr2, "purple", "porpoise");

    /* Show some objects */
    show_obj(ctr1, "white", "horse");
    show_obj(ctr2, "white", "cow");
    show_obj(ctr1, "red",   "squirrel");
    show_obj(ctr2, "green", "alligator");

    /* Show all objects */
    show_objs(ctr1);
    show_objs(ctr2);

    /* Close containers */
    fdf_ctr_close(ctr1, NULL);
    fdf_ctr_close(ctr2, NULL);

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

/*
 * Copyright (c) 2012-2013, Sandisk Corporation.  All rights reserved.
 */
#include <stdio.h>
#include "test.h"
#include "tlib.h"


/*
 * Test information.
 */
static char *Name = "hello";
static char *Desc = "hello world";


/*
 * A test.
 */
static void
test(fdf_t *fdf)
{
    /* Initialize FDF */
    test_init(fdf, Name);

    /* Create containers */
    fdf_ctr_t *ctr1 = open_ctr(fdf, "C0", FDF_CTNR_CREATE);
    fdf_ctr_t *ctr2 = open_ctr(fdf, "C1", FDF_CTNR_CREATE);

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
    fdf_ctr_close(ctr1, NULL);
    fdf_ctr_close(ctr2, NULL);
}


/*
 * Initialize the test.
 */
static __attribute__ ((constructor)) void
init(void)
{
    test_info(Name, Desc, NULL, test);
}

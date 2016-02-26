//----------------------------------------------------------------------------
// ZetaScale
// Copyright (c) 2016, SanDisk Corp. and/or all its affiliates.
//
// This program is free software; you can redistribute it and/or modify it under
// the terms of the GNU Lesser General Public License version 2.1 as published by the Free
// Software Foundation;
//
// This program is distributed in the hope that it will be useful, but WITHOUT
// ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
// FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License v2.1 for more details.
//
// A copy of the GNU Lesser General Public License v2.1 is provided with this package and
// can also be found at: http://opensource.org/licenses/LGPL-2.1
// You should have received a copy of the GNU Lesser General Public License along with
// this program; if not, write to the Free Software Foundation, Inc., 59 Temple
// Place, Suite 330, Boston, MA 02111-1307 USA.
//----------------------------------------------------------------------------

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

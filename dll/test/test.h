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
 * Author: Johann George
 * Copyright (c) 2012-2013, Sandisk Corporation.  All rights reserved.
 */
#ifndef TEST_H
#define TEST_H


/*
 * Command line interpreter information.
 */
typedef struct clint {
    void (*do_arg)(char *arg, char **args, struct clint **clint);
    void *user;
    struct clopt {
        char *str;
        int   req;
    } opts[];
} clint_t;


/*
 * Function prototypes.
 */
void test_info(char *name, char *desc, char *help,
               clint_t *clint, void (*func)());


/*
 * Global variables.
 */
int Verbose;

#endif /* TEST_H */

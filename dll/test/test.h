/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

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

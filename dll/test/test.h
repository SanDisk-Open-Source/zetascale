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

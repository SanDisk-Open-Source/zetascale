/*
 * Copyright (c) 2012-2013, Sandisk Corporation.  All rights reserved.
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include "test.h"
#include "tlib.h"


/*
 * Test information.
 */
static char *Name = "enum1";
static char *Desc = "multiple simultaneous enumeration";


/*
 * Static variables.
 */
static int Threads = 1;
static int Objects = 100 * 1000;


/*
 * Enumerate over all elements in a container.
 */
static void *
enum_start(void *arg)
{
    char *err;
    uint64_t   num = 0;
    fdf_ctr_t *ctr = arg;

    fdf_iter_t *iter = fdf_iter_init(ctr, &err);
    if (!iter)
        die_err(err, "fdf_iter_init failed");

    for (;;) {
        char *key;
        char *data;
        uint64_t keylen;
        uint64_t datalen;

        int s = fdf_iter_next(iter, &key, &keylen, &data, &datalen, &err);
        if (s < 0)
            die_err(err, "fdf_iter_next failed");
        if (s == 0)
            break;
        num++;
    }

    if (!fdf_iter_done(iter, &err))
        die_err(err, "fdf_iter_done failed");
    if (num != Objects)
        die("enumerated %ld/%ld objects", num, Objects);
    printv("enumerated %ld/%ld objects", num, Objects);
    return NULL;
}


/*
 * Enumerate simultaneously using multiple threads.
 */
static void
enum_m(fdf_ctr_t *ctr, int num_threads)
{
    int t;
    pthread_t *threads = malloc_q(num_threads * sizeof(pthread_t));

    for (t = 0; t < num_threads; t++)
        if (pthread_create(&threads[t], NULL, enum_start, ctr) < 0)
            die("pthread_create failed");

    for (t = 0; t < num_threads; t++)
        if (pthread_join(threads[t], NULL) != 0)
            die("pthread_join failed");

    free(threads);
}


/*
 * A test.
 */
static void
test(fdf_t *fdf)
{
    test_init(fdf, Name);

    fdf_ctr_t *ctr = open_ctr(fdf, "C0", FDF_CTNR_CREATE);
    set_objs_m(ctr, 0, Objects, 16, 32, Threads);
    enum_m(ctr, Threads);
    fdf_ctr_close(ctr, NULL);
}


/*
 * Print out a usage message and exit.
 */
static void
usage(void)
{
    int i;
    const char *str[] = {
        "Usage:",
        "    test enum1 Options",
        "Options:",
        "    -h|--help",
        "       Print this message",
        "    -o|--objects N (100,000)",
        "       Set N objects",
        "    -t|--threads N (1)",
        "       Use N threads",
    };

    for (i = 0; i < sizeof(str)/sizeof(*str); i++)
        printf("%s\n", str[i]);
    exit(0);
}


/*
 * Handle an argument.
 */
static void
do_arg(char *arg, char **args, clint_t **clint)
{
    if (streq(arg, "-h") || streq(arg, "--help"))
        usage();
    else if (streq(arg, "-o") || streq(arg, "--objects"))
        Objects = atoi(args[0]);
    else if (streq(arg, "-t") || streq(arg, "--threads"))
        Threads = atoi(args[0]);
    else if (arg[0] == '-')
        die("bad option: %s", arg);
    else
        die("bad argument: %s", arg);
}


/*
 * Command line options.
 */
static clint_t Clint ={
    do_arg, NULL, {
        { "-h",        0 },
        { "--help",    0 },
        { "-o",        1 },
        { "--objects", 1 },
        { "-t",        1 },
        { "--threads", 1 },
        {},
    }
};


/*
 * Initialize the test.
 */
static __attribute__ ((constructor)) void
init(void)
{
    test_info(Name, Desc, NULL, &Clint, test);
}

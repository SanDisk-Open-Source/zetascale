/*
 * A FDF program to use some FDF functions.
 * Author: Johann George
 *
 * Copyright (c) 2012, Sandisk Corporation.  All rights reserved.
 */
#include <stdio.h>
#include <stdarg.h>
#include <stdlib.h>
#include <string.h>
#include "fdf.h"
#include "fdf_easy.h"
#include "fdf_errs.h"


/*
 * Print out an error message and exit.
 */
void
panic(char *fmt, ...)
{
    va_list alist;

    va_start(alist, fmt);
    vfprintf(stderr, fmt, alist);
    va_end(alist);
    fprintf(stderr, "\n");
    exit(1);
}


/*
 * Print out a FDF error message.
 */
static void
err_fdf(fdf_err_t err, char *fmt, ...)
{
    va_list alist;

    va_start(alist, fmt);
    vfprintf(stderr, fmt, alist);
    va_end(alist);
    fprintf(stderr, ": %s\n", fdf_errmsg(err));
    exit(1);
}


/*
 * Create a container.
 */
static fdf_ctr_t *
make_ctr(fdf_t *fdf, char *name)
{
    fdf_err_t err;

    fdf_ctr_t *ctr = fdf_ctr_init(fdf, name, &err);
    if (!ctr)
        err_fdf(err, "fdf_ctr_init failed");

    if (!fdf_ctr_open(ctr, &err))
        err_fdf(err, "fdf_ctr_open failed");
    return ctr;
}


/*
 * Set a key in a container.
 */
static void
set_key(fdf_ctr_t *ctr, char *key, char *value)
{
    fdf_err_t err;

    if (!fdf_ctr_set(ctr, key, strlen(key), value, strlen(value), &err)) {
        err_fdf(err, "fdf_ctr_set failed: %s: %s => %s",
                ctr->name, key, value);
    }
}


/*
 * Get and show a key from a container.
 */
static void
show_key(fdf_ctr_t *ctr, char *key)
{
    char *data;
    uint64_t datalen;
    fdf_err_t err;

    int s = fdf_ctr_get(ctr, key, strlen(key), &data, &datalen, &err);
    if (s < 0)
        err_fdf(err, "fdf_ctr_get failed %s: %s", ctr->name, key);

    if (s == 0)
        printf("ctr %s: object %s was not set\n", ctr->name, key);
    else {
        printf("ctr %s: %s => %.*s\n", ctr->name, key, (int)datalen, data);
        fdf_free(ctr->fdf, data);
    }
}


/*
 * Show all the keys in a container.
 */
static void
show_keys(fdf_ctr_t *ctr)
{
    fdf_err_t err;

    fdf_iter_t *iter = fdf_iter_init(ctr, &err);
    if (!iter)
        err_fdf(err, "fdf_iter_init failed");

    printf("\n%s\n", ctr->name);
    for (;;) {
        char *key;
        char *data;
        uint64_t keylen;
        uint64_t datalen;

        int s = fdf_iter_next(iter, &key, &keylen, &data, &datalen, &err);
        if (s < 0)
            err_fdf(err, "fdf_iter_next failed");
        if (s == 0)
            break;

        printf("  %.*s => %.*s\n", (int)keylen, key, (int)datalen, data);
    }

    if (!fdf_iter_done(iter, &err))
        err_fdf(err, "fdf_iter_done failed");
}


int
main(int argc, char *argv[])
{
    fdf_err_t err;

    /* Initialize FDF */
    fdf_t *fdf = fdf_init(&err);
    if (!fdf)
        err_fdf(err, "fdf_init failed");

    /* Create containers */
    fdf_ctr_t *ctr1 = make_ctr(fdf, "c0");
    fdf_ctr_t *ctr2 = make_ctr(fdf, "c1");

    /* Set some keys */
    set_key(ctr1, "white", "horse");
    set_key(ctr2, "white", "cow");
    set_key(ctr1, "red", "squirrel");
    set_key(ctr2, "green", "alligator");

    /* Show some keys */
    show_key(ctr1, "white");
    show_key(ctr2, "white");
    show_key(ctr1, "red");
    show_key(ctr2, "green");

    /* Show all keys */
    show_keys(ctr1);
    show_keys(ctr2);

    /* Delete containers and close FDF */
    fdf_ctr_done(ctr1, NULL);
    fdf_ctr_done(ctr2, NULL);
    fdf_done(fdf);
    return 0;
}

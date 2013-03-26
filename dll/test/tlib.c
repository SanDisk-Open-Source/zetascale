/*
 * Author: Johann George
 * Copyright (c) 2012-2013, Sandisk Corporation.  All rights reserved.
 */
#include <stdio.h>
#include <stdarg.h>
#include <stdlib.h>
#include <string.h>
#include "tlib.h"


/*
 * Print out an error message and exit.
 */
void
die(char *fmt, ...)
{
    va_list alist;

    va_start(alist, fmt);
    vfprintf(stderr, fmt, alist);
    va_end(alist);
    fprintf(stderr, "\n");
    exit(1);
}


/*
 * Print out an error message that was allocated and exit.
 */
void
die_err(char *err, char *fmt, ...)
{
    va_list alist;

    va_start(alist, fmt);
    vfprintf(stderr, fmt, alist);
    va_end(alist);
    if (err) {
        fprintf(stderr, ": %s", err);
        free(err);
    }
    fprintf(stderr, "\n");
    exit(1);
}


/*
 * Flush a container.
 */
void
flush_ctr(fdf_ctr_t *ctr)
{
    char *err;

    if (!fdf_ctr_flush(ctr, &err))
        die_err(err, "fdf_ctr_flush failed");
}


/*
 * Delete a container.
 */
void
delete_ctr(fdf_ctr_t *ctr)
{
    char *err;

    if (!fdf_ctr_delete(ctr, &err))
        die_err(err, "fdf_ctr_delete failed");
}


/*
 * Reopen a container.
 */
void
reopen_ctr(fdf_ctr_t *ctr, int mode)
{
    char *err;

    if (!fdf_ctr_open(ctr, mode, &err))
        die_err(err, "fdf_ctr_open failed");
}


/*
 * Open a container.
 */
fdf_ctr_t *
open_ctr(fdf_t *fdf, char *name, int mode)
{
    char *err;

    fdf_ctr_t *ctr = fdf_ctr_init(fdf, name, &err);
    if (!ctr)
        die_err(err, "fdf_ctr_init failed");

    if (!fdf_ctr_open(ctr, mode, &err))
        die_err(err, "fdf_ctr_open failed");
    return ctr;
}


/*
 * Set an object.
 */
void
set_obj(fdf_ctr_t *ctr, char *key, char *value)
{
    char *err;

    if (!fdf_obj_set(ctr, key, strlen(key), value, strlen(value), &err)) {
        die_err(err, "fdf_obj_set failed: %s: %s => %s",
                ctr->name, key, value);
    }
}


/*
 * Delete an object.
 */
void
del_obj(fdf_ctr_t *ctr, char *key)
{
    char *err;

    if (!fdf_obj_del(ctr, key, strlen(key), &err))
        die_err(err, "fdf_obj_del failed: %s: %s", ctr->name, key);
}


/*
 * Get and show an object from a container.
 */
void
show_obj(fdf_ctr_t *ctr, char *key, char *value)
{
    char *data;
    uint64_t datalen;
    char *err;

    int s = fdf_obj_get(ctr, key, strlen(key), &data, &datalen, &err);
    if (s < 0)
        die_err(err, "fdf_obj_get failed %s: %s", ctr->name, key);

    if (s == 0)
        printf("ctr %s: object %s was not set\n", ctr->name, key);
    else {
        printf("ctr %s: %s => %.*s\n", ctr->name, key, (int)datalen, data);
        fdf_free(ctr->fdf, data);
    }
}


/*
 * Show all objects in a container.
 */
void
show_objs(fdf_ctr_t *ctr)
{
    char *err;

    fdf_iter_t *iter = fdf_iter_init(ctr, &err);
    if (!iter)
        die_err(err, "fdf_iter_init failed");

    printf("\n%s\n", ctr->name);
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

        printf("  %.*s => %.*s\n", (int)keylen, key, (int)datalen, data);
    }

    if (!fdf_iter_done(iter, &err))
        die_err(err, "fdf_iter_done failed");
}


/*
 * Allocate memory and die on failure.
 */
void *
alloc(long size)
{
    char *p = malloc(size);

    if (!p)
        die("out of space");
    return p;
}


/*
 * Fill a buffer with a pattern.
 */
void
fill_patn(char *buf, int len)
{
    int i;
    char   *patn = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
    int patn_len = strlen(patn);

    for (i = 0; i < len; i++)
        buf[i] = patn[i % patn_len];
}


/*
 * Fill a buffer with a number.
 */
void
fill_uint(char *buf, int len, unsigned long num)
{
    buf += len;
    while (len--) {
        *--buf = (num % 10) + '0';
        num /= 10;
    }
}

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
#include <stdio.h>
#include <stdarg.h>
#include <stdlib.h>
#include <string.h>
#include "tlib.h"
#include "xstr.h"


/*
 * Atomic operations.
 */
#define atomic_get_inc(v)        __sync_fetch_and_add(&v, 1)
#define atomic_dec(v)     (void) __sync_sub_and_fetch(&v, 1)


/*
 * For setting a range of objects in a pthread.
 */
typedef struct {
    fdf.ctr_t *ctr;
    uint64_t   num;
    uint64_t   max;
    int        key_len;
    int        val_len;
} m_set_t;


/*
 * Allocate memory and die on failure.
 */
void *
malloc_q(long size)
{
    char *p = malloc(size);

    if (!p)
        die("out of space");
    return p;
}


/*
 * Reallocate memory and die on failure.
 */
void *
realloc_q(void *ptr, long size)
{
    char *p = realloc(ptr, size);

    if (!p)
        die("out of space");
    return p;
}


/*
 * Print out an error message and exit.
 */
void
die(char *fmt, ...)
{
    xstr_t xstr;
    va_list alist;

    xsinit(&xstr);
    va_start(alist, fmt);
    xsvprint(&xstr, fmt, alist);
    va_end(alist);
    xsprint(&xstr, "\n");
    fwrite(xstr.p, xstr.i, 1, stderr);
    xsfree(&xstr);
    exit(1);
}


/*
 * Print out an error message that was allocated and exit.
 */
void
die_err(char *err, char *fmt, ...)
{
    xstr_t xstr;
    va_list alist;

    xsinit(&xstr);
    va_start(alist, fmt);
    xsvprint(&xstr, fmt, alist);
    va_end(alist);
    if (err) {
        xsprint(&xstr, ": %s", err);
        free(err);
    }
    xsprint(&xstr, "\n");
    fwrite(xstr.p, xstr.i, 1, stderr);
    xsfree(&xstr);
    exit(1);
}


/*
 * Print out a verbose message if desired.
 */
void
printv(char *fmt, ...)
{
    xstr_t xstr;
    va_list alist;

    if (!Verbose)
        return;

    xsinit(&xstr);
    va_start(alist, fmt);
    xsvprint(&xstr, fmt, alist);
    va_end(alist);
    xsprint(&xstr, "\n");
    fwrite(xstr.p, xstr.i, 1, stdout);
    xsfree(&xstr);
}


/*
 * Flush a container.
 */
void
flush_ctr(fdf.ctr_t *ctr)
{
    char *err;

    if (!fdf.ctr_flush(ctr, &err))
        die_err(err, "fdf.ctr_flush failed");
}


/*
 * Delete a container.
 */
void
delete_ctr(fdf.ctr_t *ctr)
{
    char *err;

    if (!fdf.ctr_delete(ctr, &err))
        die_err(err, "fdf.ctr_delete failed");
}


/*
 * Reopen a container.
 */
void
reopen_ctr(fdf.ctr_t *ctr, int mode)
{
    char *err;

    if (!fdf.ctr_open(ctr, mode, &err))
        die_err(err, "fdf.ctr_open failed");
}


/*
 * Open a container.
 */
fdf.ctr_t *
open_ctr(zs_t *zs, char *name, int mode)
{
    char *err;

    fdf.ctr_t *ctr = fdf.ctr_init(zs, name, &err);
    if (!ctr)
        die_err(err, "fdf.ctr_init failed");

    if (!fdf.ctr_open(ctr, mode, &err))
        die_err(err, "fdf.ctr_open failed");

    ZS_container_props_t *p = &ctr->props;
    printv("open ctr %s mode=%d fifo=%d pers=%d evict=%d wthru=%d",
           name, mode, p->fifo_mode, p->persistent, p->evicting, p->writethru);

    return ctr;
}


/*
 * Set an object.
 */
void
set_obj(fdf.ctr_t *ctr, char *key, char *value)
{
    char *err;

    if (!zs_obj_set(ctr, key, strlen(key), value, strlen(value), &err)) {
        die_err(err, "zs_obj_set failed: %s: %s => %s",
                ctr->name, key, value);
    }
}


/*
 * Delete an object.
 */
void
del_obj(fdf.ctr_t *ctr, char *key)
{
    char *err;

    if (!zs_obj_del(ctr, key, strlen(key), &err))
        die_err(err, "zs_obj_del failed: %s: %s", ctr->name, key);
}


/*
 * Get and show an object from a container.
 */
void
show_obj(fdf.ctr_t *ctr, char *key, char *value)
{
    char *data;
    uint64_t datalen;
    char *err;

    int s = zs_obj_get(ctr, key, strlen(key), &data, &datalen, &err);
    if (s < 0)
        die_err(err, "zs_obj_get failed %s: %s", ctr->name, key);

    if (s == 0)
        printf("ctr %s: object %s was not set\n", ctr->name, key);
    else {
        printf("ctr %s: %s => %.*s\n", ctr->name, key, (int)datalen, data);
        zs_free(ctr->zs, data);
    }
}


/*
 * Show all objects in a container.
 */
void
show_objs(fdf.ctr_t *ctr)
{
    char *err;

    zs_iter_t *iter = zs_iter_init(ctr, &err);
    if (!iter)
        die_err(err, "zs_iter_init failed");

    printf("\n%s\n", ctr->name);
    for (;;) {
        char *key;
        char *data;
        uint64_t keylen;
        uint64_t datalen;

        int s = zs_iter_next(iter, &key, &keylen, &data, &datalen, &err);
        if (s < 0)
            die_err(err, "zs_iter_next failed");
        if (s == 0)
            break;

        //printf("  %.*s => %.*s\n", (int)keylen, key, (int)datalen, data);
    }

    if (!zs_iter_done(iter, &err))
        die_err(err, "zs_iter_done failed");
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


/*
 * Set objects in parallel in a container.
 */
static void *
set_objs_start(void *arg)
{
    char *err;
    m_set_t     *t = arg;
    fdf.ctr_t *ctr = t->ctr;
    int    key_len = t->key_len;
    int    val_len = t->val_len;
    char      *key = malloc_q(key_len);
    char      *val = malloc_q(val_len);
    int     id_len = min(20, key_len);
    char   *id_ptr = key + key_len - id_len;

    fill_patn(key, key_len);
    fill_patn(val, val_len);

    for (;;) {
        uint64_t num = atomic_get_inc(t->num);
        if (num >= t->max) {
            atomic_dec(t->num);
            break;
        }

        fill_uint(id_ptr, id_len, num);
        if (!zs_obj_set(ctr, key, key_len, val, val_len, &err)) {
            die_err(err, "zs_obj_set failed: key %d kl=%d dl=%d",
                    num, key_len, val_len);
        }
    }
    return NULL;
}


/*
 * Set objects in a container using multiple threads.
 */
void
set_objs_m(fdf.ctr_t *ctr, int obj_min, int obj_max,
           int key_len, int val_len, int num_threads)
{
    int t;
    pthread_t *threads = malloc_q(num_threads * sizeof(pthread_t));
    m_set_t   thr_set  = {ctr, obj_min, obj_min + obj_max, key_len, val_len};

    for (t = 0; t < num_threads; t++)
        if (pthread_create(&threads[t], NULL, set_objs_start, &thr_set) < 0)
            die("pthread_create failed");

    for (t = 0; t < num_threads; t++)
        if (pthread_join(threads[t], NULL) != 0)
            die("pthread_join failed");

    free(threads);
}

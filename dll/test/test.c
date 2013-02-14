/*
 * Test some FDF functions.
 * Author: Johann George
 *
 * Copyright (c) 2012-2013, Sandisk Corporation.  All rights reserved.
 */
#include <stdio.h>
#include <stdarg.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>
#include "fdf_easy.h"


/*
 * Macros.
 */
#define streq(a, b) (strcmp(a, b) == 0)


/*
 * Print out an allocated error message and exit.
 */
static void
die(char *err, char *fmt, ...)
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
static void
flush_ctr(fdf_ctr_t *ctr)
{
    char *err;

    if (!fdf_ctr_flush(ctr, &err))
        die(err, "fdf_ctr_flush failed");
}


/*
 * Open a container.
 */
static fdf_ctr_t *
open_ctr(fdf_t *fdf, char *name, int mode)
{
    char *err;

    fdf_ctr_t *ctr = fdf_ctr_init(fdf, name, &err);
    if (!ctr)
        die(err, "fdf_ctr_init failed");

    if (!fdf_ctr_open(ctr, mode, &err))
        die(err, "fdf_ctr_open failed");
    return ctr;
}


/*
 * Set an object.
 */
static void
set_obj(fdf_ctr_t *ctr, char *key, char *value)
{
    char *err;

    if (!fdf_obj_set(ctr, key, strlen(key), value, strlen(value), &err)) {
        die(err, "fdf_obj_set failed: %s: %s => %s",
                ctr->name, key, value);
    }
}


/*
 * Delete an object.
 */
static void
del_obj(fdf_ctr_t *ctr, char *key)
{
    char *err;

    if (!fdf_obj_del(ctr, key, strlen(key), &err))
        die(err, "fdf_obj_del failed: %s: %s", ctr->name, key);
}


/*
 * Get and show an object from a container.
 */
static void
show_obj(fdf_ctr_t *ctr, char *key, char *value)
{
    char *data;
    uint64_t datalen;
    char *err;

    int s = fdf_obj_get(ctr, key, strlen(key), &data, &datalen, &err);
    if (s < 0)
        die(err, "fdf_obj_get failed %s: %s", ctr->name, key);

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
static void
show_objs(fdf_ctr_t *ctr)
{
    char *err;

    fdf_iter_t *iter = fdf_iter_init(ctr, &err);
    if (!iter)
        die(err, "fdf_iter_init failed");

    printf("\n%s\n", ctr->name);
    for (;;) {
        char *key;
        char *data;
        uint64_t keylen;
        uint64_t datalen;

        int s = fdf_iter_next(iter, &key, &keylen, &data, &datalen, &err);
        if (s < 0)
            die(err, "fdf_iter_next failed");
        if (s == 0)
            break;

        printf("  %.*s => %.*s\n", (int)keylen, key, (int)datalen, data);
    }

    if (!fdf_iter_done(iter, &err))
        die(err, "fdf_iter_done failed");
}


/*
 * Test 2.
 */
static void
run_t2(fdf_t *fdf)
{
    /* Create containers */
    fdf_ctr_t *ctr1 = open_ctr(fdf, "C0", 0);
    fdf_ctr_t *ctr2 = open_ctr(fdf, "C1", 0);

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
 * Test 1.
 */
static void
run_t1(fdf_t *fdf)
{
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
    //sleep(3);
}


/*
 * Initialize FDF.
 */
static fdf_t *
init_fdf(char *name)
{
    char *err;
    char buf[256];
    struct stat sbuf;
    char *prop = getenv("FDF_PROPERTY_FILE");

    if (!prop) {
        snprintf(buf, sizeof(buf), "./%s.prop", name);
        if (stat(buf, &sbuf) < 0) {
            snprintf(buf, sizeof(buf), "./test.prop");
            if (stat(buf, &sbuf) < 0)
                die(NULL, "cannot determine property file");
        }
        setenv("FDF_PROPERTY_FILE", buf, 1);
        prop = buf;
    }
    setenv("FDF_LIB", "/tmp/libfdf.so", 0);

    fdf_t *fdf = fdf_init(&err);
    if (!fdf)
        die(err, "fdf_init failed");

    if (!fdf_conf(fdf, prop, &err))
        die(err, "fdf_conf_init failed");

    return fdf;
}


int
main(int argc, char *argv[])
{
    if (argc < 2)
        die(NULL, "usage: test Name");
    fdf_t *fdf = init_fdf(argv[1]);

    int i;
    for (i = 1; i < argc; i++) {
        char *name = argv[i];
        if (streq(name, "t1"))
            run_t1(fdf);
        else if (streq(name, "t2"))
            run_t2(fdf);
        else
            die(NULL, "unknown test: %s", name);
    }

    fdf_done(fdf);
    return 0;
}

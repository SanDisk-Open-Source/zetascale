/*
 * Copyright (c) 2012-2013, Sandisk Corporation.  All rights reserved.
 */
#include <stdio.h>
#include <stdlib.h>
#include "test.h"
#include "tlib.h"


/*
 * For setting a range of objects in a pthread.
 */
typedef struct thr_set {
    fdf_ctr_t *ctr;
    int        lo;
    int        hi;
    int        key_len;
    int        val_len;
} thr_set_t;


/*
 * Test information.
 */
static char *Name = "enum1";
static char *Desc = "enumeration";


/*
 * Set a range of objects in a container.
 */
static void
set_objs_range(fdf_ctr_t *ctr, int n0, int n1, int key_len, int val_len)
{
    int n;
    char *err;
    char    *key = alloc(key_len);
    char    *val = alloc(val_len);
    int   id_len = min(20, key_len);
    char *id_ptr = key + key_len - id_len;

fprintf(stderr, "set_objs_range %d to %d\n", n0, n1);
    fill_patn(key, key_len);
    fill_patn(val, val_len);

    for (n = n0; n < n1; n++) {
        fill_uint(id_ptr, id_len, n);
        if (!fdf_obj_set(ctr, key, key_len, val, val_len, &err)) {
            die_err(err, "fdf_obj_set failed: key %d kl=%d dl=%d",
                    n, key_len, val_len);
        }
    }
}


/*
 * Set objects in parallel in a container.
 */
static void *
set_objs_start(void *arg)
{
    thr_set_t *t = arg;
    set_objs_range(t->ctr, t->lo, t->hi, t->key_len, t->val_len);
    return NULL;
}


/*
 * Set objects in parallel in a container.
 */
static void
set_objs_thr(fdf_ctr_t *ctr, int num_objects,
             int key_len, int val_len, int num_threads)
{
    int t;
    int            lo = 0;
    int            num = num_objects / num_threads;
    int            rem = num_objects % num_threads;
    pthread_t *threads = alloc(num_threads * sizeof(pthread_t));

    for (t = 0; t < num_threads; t++) {
        int hi = lo + num + (t < rem);
        if (pthread_create(&threads[t], NULL, set_objs_start, NULL) < 0)
            die("pthread_create failed");
        lo = hi;
    }
    for (t = 0; t < num_threads; t++)
        if (pthread_join(threads[t], NULL) != 0)
            die("pthread_join failed");

    free(threads);
}


/*
 * A test.
 */
static void
test(void)
{
    /* Initialize FDF */
    fdf_t *fdf = init_fdf(Name);

    /* Set objects */
    fdf_ctr_t *ctr = open_ctr(fdf, "C0", FDF_CTNR_CREATE);
    set_objs_thr(ctr, 10, 16, 32, 1);
    fdf_ctr_close(ctr, NULL);

    /* Close FDF */
    fdf_done(fdf);
}


/*
 * Initialize the test.
 */
static __attribute__ ((constructor)) void
init(void)
{
    test_info(Name, Desc, NULL, test);
}

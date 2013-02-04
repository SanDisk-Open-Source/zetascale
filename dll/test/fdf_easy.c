/*
 * An easier interface to FDF.
 * Author: Johann George
 *
 * Copyright (c) 2012-2013, Sandisk Corporation.  All rights reserved.
 */
#include <errno.h>
#include <stdio.h>
#include <stdarg.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include "fdf_easy.h"
#include "fdf_easy_int.h"


/*
 * Macros.
 */
#define streq(a, b) (strcmp(a, b) == 0)
#define round(n, d) (((n)+(d)-1)/(d))


/*
 * Log an error message.
 */
static void
loge(const char *fmt, ...)
{
    va_list alist;

    va_start(alist, fmt);
    vfprintf(stderr, fmt, alist);
    va_end(alist);
    fprintf(stderr, "\n");
}


/*
 * Set errno based on a FDF error.
 */
static void
fdf_set_errno(FDF_status_t ferr)
{
    if (ferr == FDF_SUCCESS)
        errno = 0;
    else if (ferr == FDF_FAILURE_MEMORY_ALLOC)
        errno = ENOMEM;
    else if (ferr == FDF_INVALID_PARAMETER)
        errno = EINVAL;
    else
        errno = EIO;
}


/*
 * If there is a FDF error, set a string indicating the error, translate the
 * error and return 1.
 */
static int
set_err_fdf_if(char **errp, FDF_status_t ferr)
{
    if (ferr == FDF_SUCCESS)
        return 0;

    fdf_set_errno(ferr);
    fdf_aperr_(errp, NULL, ferr, 0, NULL);
    return 1;
}


/*
 * Convert a UNIX error to a string.
 */
static void
set_err_sys(char **errp, int err)
{
    fdf_aperr_(errp, NULL, 0, err, NULL);
}


/*
 * Convert a UNIX error to a string if there is one.
 */
static int
set_err_sys_if(char **errp, int err)
{
    if (!err)
        return 0;

    errno = err;
    fdf_aperr_(errp, NULL, 0, err, NULL);
    return 1;
}


/*
 * Return the current thread state.
 */
static struct FDF_thread_state *
ts_get(fdf_t *fdf, char **errp)
{
    struct FDF_thread_state *ts = pthread_getspecific(fdf->key);
    
    if (!ts) {
        FDF_status_t ferr = FDFInitPerThreadState(fdf->state, &ts);
        if (set_err_fdf_if(errp, ferr))
            return NULL;

        int err = pthread_setspecific(fdf->key, ts);
        if (set_err_sys_if(errp, err))
            return NULL;
    }
    return ts;
}


/*
 * Destroy the current thread state.
 */
static void
ts_destroy(void *p)
{
    struct FDF_thread_state *ts = p;
    FDFReleasePerThreadState(&ts);
}


/*
 * Initialize FDF.
 */
fdf_t *
fdf_init(char **errp)
{
    fdf_link_();

    fdf_t *fdf = malloc(sizeof(*fdf));
    if (!fdf) {
        set_err_sys(errp, errno);
        return NULL;
    }

    int err = pthread_key_create(&fdf->key, ts_destroy);
    if (set_err_sys_if(errp, err))
        return NULL;

    FDF_status_t ferr = FDFInit(&fdf->state);
    if (set_err_fdf_if(errp, ferr)) {
        free(fdf);
        return NULL;
    }
    return fdf;
}


/*
 * Finish with FDF.
 */
void
fdf_done(fdf_t *fdf)
{
    FDFShutdown(fdf->state);
    free(fdf);
}


/*
 * Free an object.
 */
void
fdf_free(fdf_t *fdf, void *ptr)
{
    FDFFreeBuffer(ptr);
}


/*
 * Get a durability level property.
 */
static int
prop2_durability(fdf_t *fdf, int *fail, char *pre, char *name, int def)
{
    const char *val = fdf_get_prop2(fdf, pre, name);
    if (!val)
        return def;

    if (streq(val, "PERIODIC"))
	return FDF_DURABILITY_PERIODIC;
    if (streq(val, "SW_CRASH_SAFE"))
	return FDF_DURABILITY_SW_CRASH_SAFE;
    if (streq(val, "HW_CRASH_SAFE"))
	return FDF_DURABILITY_HW_CRASH_SAFE;

    loge("bad property: %s_%s = %s", pre, name, val);
    *fail = 1;
    errno = EINVAL;
    return def;
}


/*
 * Get an unsigned integer property.
 */
static unsigned long
prop2_uint(fdf_t *fdf, int *fail, char *pre, char *name, unsigned long def)
{
    const char *val = fdf_get_prop2(fdf, pre, name);
    if (!val)
        return def;

    char *end;
    unsigned long ret = strtol(val, &end, 0);
    int c = *end;

    if (c == 'K')
        ret *= 1024;
    else if (c == 'M')
        ret *= 1024 * 1024;
    else if (c == 'G')
        ret *= 1024 * 1024 * 1024;
    else if (c != '\0') {
        loge("bad property: %s_%s = %s", pre, name, val);
        *fail = 1;
        errno = EINVAL;
        ret = def;
    }
    return ret;
}


/*
 * Get a boolean property.
 */
static int
prop2_bool(fdf_t *fdf, int *fail, char *pre, char *name, int def)
{
    const char *val = fdf_get_prop2(fdf, pre, name);
    if (!val)
        return def;
    if (streq(val, "1") || streq(val, "on") || streq(val, "true"))
        return FDF_TRUE;
    if (streq(val, "0") || streq(val, "off") || streq(val, "false"))
        return FDF_FALSE;

    loge("bad property: %s_%s = %s", pre, name, val);
    *fail = 1;
    errno = EINVAL;
    return def;
}


/*
 * Initialize a container.
 */
fdf_ctr_t *
fdf_ctr_init(fdf_t *fdf, char *name, char **errp)
{
    static int num = 1;
    fdf_ctr_t *ctr;

    ctr = malloc(sizeof(*ctr));
    if (!ctr) {
        set_err_sys(errp, errno);
        return NULL;
    }

    memset(ctr, 0, sizeof(*ctr));
    ctr->fdf = fdf;

    name = strdup(name);
    if (!name) {
        free(ctr);
        set_err_sys(errp, errno);
        return NULL;
    }

    int fail = 0;
    FDF_container_props_t *props = &ctr->props;

    props->fifo_mode  = prop2_bool(fdf, &fail, name, "FIFO_MODE",  FDF_FALSE);
    props->persistent = prop2_bool(fdf, &fail, name, "PERSISTENT", FDF_TRUE);
    props->evicting   = prop2_bool(fdf, &fail, name, "EVICTING",   FDF_FALSE);
    props->evicting   = prop2_bool(fdf, &fail, name, "WRITETHRU",  FDF_TRUE);
    props->cid        = prop2_uint(fdf, &fail, name, "CID",        0);
    props->num_shards = prop2_uint(fdf, &fail, name, "NUM_SHARDS", 1);

    props->size_kb =
        round(prop2_uint(fdf, &fail, name, "SIZE", 1024 * 1024), 1024);

    props->durability_level =
        prop2_durability(fdf, &fail, name, "DURABILITY_LEVEL",
                         FDF_DURABILITY_PERIODIC);

    if (fail) {
        set_err_sys(errp, errno);
        return NULL;
    }

    ctr->name = name;
    ctr->num  = __sync_fetch_and_add(&num, 1);
    return ctr;
}


/*
 * Finish with a FDF container.
 */
int
fdf_ctr_done(fdf_ctr_t *ctr, char **errp)
{
    struct FDF_thread_state *ts = ts_get(ctr->fdf, errp);
    if (!ts)
        return 0;

    FDF_cguid_t  cguid = ctr->cguid;
    FDF_status_t ferr  = FDFCloseContainer(ts, cguid);
    FDF_status_t ferr2 = FDFDeleteContainer(ts, cguid);
    free(ctr->name);
    free(ctr);

    if (ferr == FDF_SUCCESS)
        ferr = ferr2;
    return !set_err_fdf_if(errp, ferr);
}


/*
 * Open a FDF container.
 */
int
fdf_ctr_open(fdf_ctr_t *ctr, char **errp)
{
    struct FDF_thread_state *ts = ts_get(ctr->fdf, errp);
    if (!ts)
        return 0;

    FDF_status_t ferr = FDFOpenContainer(ts, ctr->name, &ctr->props,
                                         FDF_CTNR_CREATE|FDF_CTNR_RW_MODE,
                                         &ctr->cguid);
    return !set_err_fdf_if(errp, ferr);
}


/*
 * Get a key and value.
 */
int
fdf_ctr_get(fdf_ctr_t *ctr, char *key, uint64_t keylen,
            char **data, uint64_t *datalen, char **errp)
{
    struct FDF_thread_state *ts = ts_get(ctr->fdf, errp);
    if (!ts)
        return 0;

    FDF_status_t ferr;
    ferr = FDFReadObject(ts, ctr->cguid, key, keylen, data, datalen);
    if (ferr == FDF_SUCCESS)
        return 1;
    if (ferr == FDF_OBJECT_UNKNOWN)
        return 0;

    set_err_fdf_if(errp, ferr);
    return -1;
}


/*
 * Set a key and value.
 */
int
fdf_ctr_set(fdf_ctr_t *ctr, char *key, uint64_t keylen,
            char *data, uint64_t datalen, char **errp)
{
    struct FDF_thread_state *ts = ts_get(ctr->fdf, errp);
    if (!ts)
        return 0;

    FDF_status_t ferr;
    ferr = FDFWriteObject(ts, ctr->cguid, key, keylen, data, datalen, 0);
    return !set_err_fdf_if(errp, ferr);
}


/*
 * Prepare to iterate through a container.
 */
fdf_iter_t *
fdf_iter_init(fdf_ctr_t *ctr, char **errp)
{
    struct FDF_thread_state *ts = ts_get(ctr->fdf, errp);
    if (!ts)
        return 0;

    fdf_iter_t *iter = malloc(sizeof(*iter));
    if (!iter) {
        set_err_sys(errp, errno);
        return NULL;
    }

    iter->ctr  = ctr;
    FDF_status_t ferr;
    ferr = FDFEnumerateContainerObjects(ts, ctr->cguid, &iter->iter);
    if (set_err_fdf_if(errp, ferr)) {
        free(iter);
        return NULL;
    }
    return iter;
}


/*
 * Complete container iteration.
 */
int
fdf_iter_done(fdf_iter_t *iter, char **errp)
{
    struct FDF_thread_state *ts = ts_get(iter->ctr->fdf, errp);
    if (!ts)
        return 0;

    FDF_status_t ferr = FDF_SUCCESS;
    if (iter->iter)
        ferr = FDFFinishEnumeration(ts, iter->iter);
    iter->iter = NULL;
    free(iter);
    return !set_err_fdf_if(errp, ferr);
}


/*
 * Get the next object in a container.
 */
int
fdf_iter_next(fdf_iter_t *iter, char **key, uint64_t *keylen,
              char **data, uint64_t *datalen, char **errp)
{
    struct FDF_thread_state *ts = ts_get(iter->ctr->fdf, errp);
    if (!ts)
        return 0;

    uint32_t keylen0;
    FDF_status_t ferr;
    ferr = FDFNextEnumeratedObject(ts, iter->iter,
                                   key, &keylen0, data, datalen);
    *keylen = keylen0;

    if (ferr == FDF_SUCCESS)
        return 1;
    if (ferr == FDF_OBJECT_UNKNOWN)
        return 0;

    set_err_fdf_if(errp, ferr);
    return -1;
}

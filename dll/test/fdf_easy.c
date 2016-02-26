/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

/*
 * An easier interface to ZS.
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
#include "zs_easy.h"
#include "zs_easy_int.h"


/*
 * Configurable parameters.
 */
#define BUF_SIZE             256


/*
 * Macros.
 */
#define streq(a, b) (strcmp(a, b) == 0)
#define round(n, d) (((n)+(d)-1)/(d))


/*
 * Print out an error message in a buffer.
 */
void
zs_perr(char *buf, int len, const char *func,
         ZS_status_t zs_err, int sys_err, const char *fmt, va_list alist)
{
    int n = 0;

    if (func)
        n += snprintf(&buf[n], len-n, "%s", func);
    if (n > len - 1)
        n = len - 1;

    if (fmt) {
        if (n)
            n += snprintf(&buf[n], len-n, ": ");
        n += vsnprintf(&buf[n], len-n, fmt, alist);
        if (n > len - 1)
            n = len - 1;
    }

    if (zs_err) {
        char *errstr = zs_errmsg_(zs_err);

        if (n)
            n += snprintf(&buf[n], len-n, ": ");
        if (errstr)
            n += snprintf(&buf[n], len-n, "%s (%d)", errstr, zs_err);
        else
            n += snprintf(&buf[n], len-n, "(%d)", zs_err);
    } else if (sys_err) {
        char errbuf[BUF_SIZE];
        strerror_r(sys_err, errbuf, sizeof(errbuf));

        if (n)
            n += snprintf(&buf[n], len-n, ": ");
        n += snprintf(&buf[n], len-n, "%s (%d)", errbuf, sys_err);
    }

    if (n > len - 1) {
        n = len - 5;
        n += snprintf(&buf[n], len-n, " ...");
    }
}


/*
 * Make an error message and allocate space for it.
 */
void
zs_aperr_(char **errp, const char *func,
          ZS_status_t zs_err, int sys_err, const char *fmt, ...)
{
    char buf[BUF_SIZE];
    va_list alist;

    if (!errp)
        return;
    va_start(alist, fmt);
    zs_perr(buf, sizeof(buf), func, zs_err, sys_err, fmt, alist);
    va_end(alist);
    *errp = strdup(buf);
}


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
 * Set errno based on a ZS error.
 */
static void
zs_set_errno(ZS_status_t ferr)
{
    if (ferr == ZS_SUCCESS)
        errno = 0;
    else if (ferr == ZS_FAILURE_MEMORY_ALLOC)
        errno = ENOMEM;
    else if (ferr == ZS_INVALID_PARAMETER)
        errno = EINVAL;
    else
        errno = EIO;
}


/*
 * If there is a ZS error, set a string indicating the error, translate the
 * error and return 1.
 */
static int
set_err_zs_if(char **errp, ZS_status_t ferr)
{
    if (ferr == ZS_SUCCESS)
        return 0;

    zs_set_errno(ferr);
    zs_aperr_(errp, NULL, ferr, 0, NULL);
    return 1;
}


/*
 * Convert a UNIX error to a string.
 */
static void
set_err_sys(char **errp, int err)
{
    zs_aperr_(errp, NULL, 0, err, NULL);
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
    zs_aperr_(errp, NULL, 0, err, NULL);
    return 1;
}


/*
 * Return the current thread state.
 */
static struct ZS_thread_state *
ts_get(zs_t *zs, char **errp)
{
    struct ZS_thread_state *ts = pthread_getspecific(zs->key);
    
    if (!ts) {
        ZS_status_t ferr = ZSInitPerThreadState(zs->state, &ts);
        if (set_err_zs_if(errp, ferr))
            return NULL;

        int err = pthread_setspecific(zs->key, ts);
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
    struct ZS_thread_state *ts = p;
    ZSReleasePerThreadState(&ts);
}


/*
 * Get a durability level property.
 */
static int
prop2_durability(zs_t *zs, int *fail, char *pre, char *name, int def)
{
    const char *val = zs_get_prop2(zs, pre, name, NULL);
    if (!val)
        return def;

    if (streq(val, "PERIODIC"))
	return ZS_DURABILITY_PERIODIC;
    if (streq(val, "SW_CRASH_SAFE"))
	return ZS_DURABILITY_SW_CRASH_SAFE;
    if (streq(val, "HW_CRASH_SAFE"))
	return ZS_DURABILITY_HW_CRASH_SAFE;

    loge("bad property: %s_%s = %s", pre, name, val);
    *fail = 1;
    errno = EINVAL;
    return def;
}


/*
 * Get an unsigned integer scaled appropriately.
 */
int
zs_utoi(const char *str, unsigned long *ret)
{
    char *end;
    unsigned long val = strtol(str, &end, 0);
    int c = *end;

    if (c == 'K')
        val *= 1024;
    else if (c == 'M')
        val *= 1024 * 1024;
    else if (c == 'G')
        val *= 1024 * 1024 * 1024;
    else if (c == 'k')
        val *= 1000;
    else if (c == 'm')
        val *= 1000 * 1000;
    else if (c == 'g')
        val *= 1000 * 1000 * 1000;
    else if (c != '\0')
        return 0;

    *ret = val;
    return 1;
}


/*
 * Get an unsigned integer property.
 */
static unsigned long
prop2_uint(zs_t *zs, int *fail, char *pre, char *name, unsigned long def)
{
    unsigned long val;
    const char *str = zs_get_prop2(zs, pre, name, NULL);
    if (!str)
        return def;

    if (zs_utoi(str, &val))
        return val;

    loge("bad property: %s_%s = %s", pre, name, str);
    *fail = 1;
    errno = EINVAL;
    return def;
}


/*
 * Get a boolean property.
 */
static int
prop2_bool(zs_t *zs, int *fail, char *pre, char *name, int def)
{
    const char *val = zs_get_prop2(zs, pre, name, NULL);
    if (!val)
        return def;
    if (streq(val, "1") || streq(val, "on") || streq(val, "true"))
        return ZS_TRUE;
    if (streq(val, "0") || streq(val, "off") || streq(val, "false"))
        return ZS_FALSE;

    loge("bad property: %s_%s = %s", pre, name, val);
    *fail = 1;
    errno = EINVAL;
    return def;
}


/*
 * Initialize a container.
 */
fdf.ctr_t *
fdf.ctr_init(zs_t *zs, char *name, char **errp)
{
    static int num = 1;
    fdf.ctr_t *ctr;

    ctr = malloc(sizeof(*ctr));
    if (!ctr) {
        set_err_sys(errp, errno);
        return NULL;
    }
    memset(ctr, 0, sizeof(*ctr));

    ctr->zs = zs;

    name = strdup(name);
    if (!name) {
        free(ctr);
        set_err_sys(errp, errno);
        return NULL;
    }

    int fail = 0;
    ZS_container_props_t *props = &ctr->props;

    props->fifo_mode  = prop2_bool(zs, &fail, name, "FIFO_MODE",  ZS_FALSE);
    props->persistent = prop2_bool(zs, &fail, name, "PERSISTENT", ZS_TRUE);
    props->evicting   = prop2_bool(zs, &fail, name, "EVICTING",   ZS_FALSE);
    props->writethru  = prop2_bool(zs, &fail, name, "WRITETHRU",  ZS_TRUE);
    props->cid        = prop2_uint(zs, &fail, name, "CID",        0);
    props->num_shards = prop2_uint(zs, &fail, name, "NUM_SHARDS", 1);

    props->size_kb =
        round(prop2_uint(zs, &fail, name, "SIZE", 1024 * 1024 * 1024), 1024);

    props->durability_level =
        prop2_durability(zs, &fail, name, "DURABILITY_LEVEL",
                         ZS_DURABILITY_PERIODIC);

    if (fail) {
        set_err_sys(errp, errno);
        return NULL;
    }

    ctr->name = name;
    ctr->num  = __sync_fetch_and_add(&num, 1);
    return ctr;
}


/*
 * Flush a ZS container.
 */
int
fdf.ctr_flush(fdf.ctr_t *ctr, char **errp)
{
    struct ZS_thread_state *ts = ts_get(ctr->zs, errp);
    if (!ts)
        return 0;

    ZS_status_t ferr = ZSFlushContainer(ts, ctr->cguid);
    return !set_err_zs_if(errp, ferr);
}


/*
 * Delete a ZS container.
 */
int
fdf.ctr_delete(fdf.ctr_t *ctr, char **errp)
{
    struct ZS_thread_state *ts = ts_get(ctr->zs, errp);
    if (!ts)
        return 0;

    ZS_cguid_t cguid = ctr->cguid;
    ZS_status_t ferr = ZSDeleteContainer(ts, cguid);
    free(ctr->name);
    free(ctr);

    return !set_err_zs_if(errp, ferr);
}


/*
 * Close a ZS container.
 */
int
fdf.ctr_close(fdf.ctr_t *ctr, char **errp)
{
    struct ZS_thread_state *ts = ts_get(ctr->zs, errp);
    if (!ts)
        return 0;

    ZS_cguid_t cguid = ctr->cguid;
    ZS_status_t ferr = ZSCloseContainer(ts, cguid);
    free(ctr->name);
    free(ctr);

    return !set_err_zs_if(errp, ferr);
}


/*
 * Open a ZS container.
 */
int
fdf.ctr_open(fdf.ctr_t *ctr, int mode, char **errp)
{
    struct ZS_thread_state *ts = ts_get(ctr->zs, errp);
    if (!ts)
        return 0;

    ZS_status_t ferr = ZSOpenContainer(ts, ctr->name, &ctr->props, mode,
                                         &ctr->cguid);
    return !set_err_zs_if(errp, ferr);
}


/*
 * Get an object.
 */
int
zs_obj_get(fdf.ctr_t *ctr, char *key, uint64_t keylen,
            char **data, uint64_t *datalen, char **errp)
{
    struct ZS_thread_state *ts = ts_get(ctr->zs, errp);
    if (!ts)
        return 0;

    ZS_status_t ferr;
    ferr = ZSReadObject(ts, ctr->cguid, key, keylen, data, datalen);
    if (ferr == ZS_SUCCESS)
        return 1;
    if (ferr == ZS_OBJECT_UNKNOWN)
        return 0;

    set_err_zs_if(errp, ferr);
    return -1;
}


/*
 * Set a key and value.
 */
int
zs_obj_set(fdf.ctr_t *ctr, char *key, uint64_t keylen,
            char *data, uint64_t datalen, char **errp)
{
    struct ZS_thread_state *ts = ts_get(ctr->zs, errp);
    if (!ts)
        return 0;

    ZS_status_t ferr;
    ferr = ZSWriteObject(ts, ctr->cguid, key, keylen, data, datalen, 0);
    return !set_err_zs_if(errp, ferr);
}


/*
 * Get an object.
 */
int
zs_obj_del(fdf.ctr_t *ctr, char *key, uint64_t keylen, char **errp)
{
    struct ZS_thread_state *ts = ts_get(ctr->zs, errp);
    if (!ts)
        return 0;

    ZS_status_t ferr = ZSDeleteObject(ts, ctr->cguid, key, keylen);
    return !set_err_zs_if(errp, ferr);
}


/*
 * Prepare to iterate through a container.
 */
zs_iter_t *
zs_iter_init(fdf.ctr_t *ctr, char **errp)
{
    struct ZS_thread_state *ts = ts_get(ctr->zs, errp);
    if (!ts)
        return 0;

    zs_iter_t *iter = malloc(sizeof(*iter));
    if (!iter) {
        set_err_sys(errp, errno);
        return NULL;
    }
    memset(iter, 0, sizeof(*iter));

    iter->ctr  = ctr;
    ZS_status_t ferr;
    ferr = ZSEnumerateContainerObjects(ts, ctr->cguid, &iter->iter);
    if (set_err_zs_if(errp, ferr)) {
        free(iter);
        return NULL;
    }
    return iter;
}


/*
 * Complete container iteration.
 */
int
zs_iter_done(zs_iter_t *iter, char **errp)
{
    struct ZS_thread_state *ts = ts_get(iter->ctr->zs, errp);
    if (!ts)
        return 0;

    ZS_status_t ferr = ZS_SUCCESS;
    if (iter->iter)
        ferr = ZSFinishEnumeration(ts, iter->iter);
    iter->iter = NULL;
    free(iter);
    return !set_err_zs_if(errp, ferr);
}


/*
 * Get the next object in a container.
 */
int
zs_iter_next(zs_iter_t *iter, char **key, uint64_t *keylen,
              char **data, uint64_t *datalen, char **errp)
{
    struct ZS_thread_state *ts = ts_get(iter->ctr->zs, errp);
    if (!ts)
        return 0;

    uint32_t keylen0;
    ZS_status_t ferr;
    ferr = ZSNextEnumeratedObject(ts, iter->iter,
                                   key, &keylen0, data, datalen);
    *keylen = keylen0;

    if (ferr == ZS_SUCCESS)
        return 1;
    if (ferr == ZS_OBJECT_UNKNOWN)
        return 0;

    set_err_zs_if(errp, ferr);
    return -1;
}


/*
 * Free an object.
 */
void
zs_free(zs_t *zs, void *ptr)
{
    ZSFreeBuffer(ptr);
}


/*
 * Load a ZS property file.
 */
int
zs_load_prop_file(zs_t *zs, const char *file, char **errp)
{
    ZS_status_t ferr = ZSLoadProperties(file);
    return !set_err_zs_if(errp, ferr);
}


/*
 * Set a ZS property.
 */
void
zs_set_prop(zs_t *zs, const char *key, const char *val)
{
    ZSSetProperty(key, val);
}


/*
 * Get a ZS property with the key given in two parts.
 */
const char *
zs_get_prop2(zs_t *zs, const char *lkey, const char *rkey, const char *def)
{
    char buf[64];
    char    *key = buf;
    int lkey_len = strlen(lkey);
    int rkey_len = strlen(rkey);

    if (lkey_len + rkey_len >= sizeof(buf)) {
        key = malloc(lkey_len + 1 + rkey_len + 1);
        if (!key)
            return def;
    }

    memcpy(key, lkey, lkey_len);
    key[lkey_len] = '_';
    memcpy(key + lkey_len + 1, rkey, rkey_len);
    key[lkey_len + 1 + rkey_len] = '\0';

    const char *val = ZSGetProperty(key, def);
    if (key != buf)
        free(key);
    return val;
}


/*
 * Get a ZS property.
 */
const char *
zs_get_prop(zs_t *zs, const char *key, const char *def)
{
    return ZSGetProperty(key, def);
}


/*
 * Start ZS.
 */
int
zs_start(zs_t *zs, char **errp)
{
    ZS_status_t ferr = ZSInit(&zs->state);
    return !set_err_zs_if(errp, ferr);
}


/*
 * Finish with ZS.
 */
void
zs_done(zs_t *zs)
{
    if (zs->state)
        ZSShutdown(zs->state);
    free(zs);
}


/*
 * Initialize ZS.
 */
zs_t *
zs_init(int start, char **errp)
{
    zs_t *zs = malloc(sizeof(*zs));
    if (!zs) {
        set_err_sys(errp, errno);
        return NULL;
    }
    memset(zs, 0, sizeof(*zs));

    int err = pthread_key_create(&zs->key, ts_destroy);
    if (set_err_sys_if(errp, err))
        return NULL;

    if (start && !zs_start(zs, errp)) {
        free(zs);
        return NULL;
    }
    return zs;
}

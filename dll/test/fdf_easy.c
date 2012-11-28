/*
 * An easier interface to FDF.
 * Author: Johann George
 *
 * Copyright (c) 2012,  Sandisk Corporation.  All rights reserved.
 */
#include <stdio.h>
#include <stdarg.h>
#include <stdlib.h>
#include <string.h>
#include "fdf_easy.h"


/*
 * Initialize an error return.
 */
static inline void
err_init(fdf_err_t **errpp, fdf_err_t *errlp)
{
    if (!*errpp)
        *errpp = errlp;
    **errpp = FDF_SUCCESS;
}


/*
 * Initialize an error return.
 */
static inline int
err_good(fdf_err_t *errp, FDF_status_t ss)
{
    if (*errp == FDF_SUCCESS)
        *errp = ss;
    return *errp == FDF_SUCCESS;
}


/*
 * Initialize FDF.
 */
fdf_t *
fdf_init(fdf_err_t *errp)
{
    fdf_t *fdf;
    fdf_err_t errl;
    FDF_status_t ss;

    err_init(&errp, &errl);
    fdf = malloc(sizeof(*fdf));
    if (!fdf) {
        *errp = FDF_FAILURE_MEMORY_ALLOC;
        return NULL;
    }

    ss = FDFInit(&fdf->state);
    if (!err_good(errp, ss)) {
        free(fdf);
        return NULL;
    }

    ss = FDFInitPerThreadState(fdf->state, &fdf->thread);
    if (!err_good(errp, ss)) {
        fdf_done(fdf);
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
 * Initialize a container.
 */
fdf_ctr_t *
fdf_ctr_init(fdf_t *fdf, char *name, fdf_err_t *errp)
{
    static int num = 0;
    fdf_ctr_t *ctr;
    fdf_err_t errl;

    err_init(&errp, &errl);
    ctr = malloc(sizeof(*ctr));
    if (!ctr) {
        *errp = FDF_FAILURE_MEMORY_ALLOC;
        return NULL;
    }

    memset(ctr, 0, sizeof(*ctr));
    ctr->fdf = fdf;

    name = strdup(name);
    if (!name) {
        *errp = FDF_FAILURE_MEMORY_ALLOC;
        free(ctr);
        return NULL;
    }

    FDF_container_props_t *props = &ctr->props;

    props->size_kb          = 1024 * 1024;
    props->fifo_mode        = FDF_FALSE;
    props->persistent       = FDF_TRUE;
    props->evicting         = FDF_FALSE;
    props->writethru        = FDF_TRUE;
    props->durability_level = FDF_FULL_DURABILITY;
    props->cid              = 0;
    props->num_shards       = 1;

    ctr->name = name;
    ctr->num  = __sync_fetch_and_add(&num, 1);
    return ctr;
}


/*
 * Finish with a FDF container.
 */
int
fdf_ctr_done(fdf_ctr_t *ctr, fdf_err_t *errp)
{
    FDF_cguid_t               cguid = ctr->cguid;
    struct FDF_thread_state *thread = ctr->fdf->thread;

    FDF_status_t ss  = FDFCloseContainer(thread, cguid);
    FDF_status_t ss2 = FDFDeleteContainer(thread, cguid);
    free(ctr->name);
    free(ctr);

    if (ss == FDF_SUCCESS)
        ss = ss2;
    if (errp)
        *errp = ss;
    return ss == FDF_SUCCESS;
}


/*
 * Open a FDF container.
 */
int
fdf_ctr_open(fdf_ctr_t *ctr, fdf_err_t *errp)
{
    fdf_err_t errl;
    struct FDF_thread_state *thread = ctr->fdf->thread;

    err_init(&errp, &errl);
    *errp = FDFOpenContainer(thread, ctr->name,
                             &ctr->props, FDF_CTNR_CREATE|FDF_CTNR_RW_MODE,
                             &ctr->cguid);
    return *errp == FDF_SUCCESS;
}


/*
 * Get a key and value.
 */
int
fdf_ctr_get(fdf_ctr_t *ctr, char *key, uint64_t keylen,
            char **data, uint64_t *datalen, fdf_err_t *errp)
{
    fdf_err_t errl;

    err_init(&errp, &errl);
    *errp = FDFReadObject(ctr->fdf->thread, ctr->cguid,
                          key, keylen, data, datalen);
    if (*errp == FDF_SUCCESS)
        return 1;
    if (*errp == FDF_OBJECT_UNKNOWN)
        return 0;
    return -1;
}


/*
 * Set a key and value.
 */
int
fdf_ctr_set(fdf_ctr_t *ctr, char *key, uint64_t keylen,
            char *data, uint64_t datalen, fdf_err_t *errp)
{
    fdf_err_t errl;

    err_init(&errp, &errl);
    *errp = FDFWriteObject(ctr->fdf->thread, ctr->cguid,
                           key, keylen, data, datalen, 0);
    return *errp == FDF_SUCCESS;
}


/*
 * Prepare to iterate through a container.
 */
fdf_iter_t *
fdf_iter_init(fdf_ctr_t *ctr, fdf_err_t *errp)
{
    fdf_err_t errl;
    FDF_status_t ss;
    fdf_iter_t *iter;

    err_init(&errp, &errl);
    iter = malloc(sizeof(*iter));
    if (!iter) {
        *errp = FDF_FAILURE_MEMORY_ALLOC;
        return NULL;
    }

    iter->ctr  = ctr;
    ss = FDFEnumerateContainerObjects(ctr->fdf->thread,
                                      ctr->cguid, &iter->iter);
    if (!err_good(errp, ss)) {
        free(iter);
        return NULL;
    }
    return iter;
}


/*
 * Complete container iteration.
 */
int
fdf_iter_done(fdf_iter_t *iter, fdf_err_t *errp)
{
    fdf_err_t errl;
    FDF_status_t ss;

    err_init(&errp, &errl);
    if (iter->iter)
        ss = FDFFinishEnumeration(iter->ctr->fdf->thread, iter->iter);
    iter->iter = NULL;
    free(iter);
    return err_good(errp, ss);
}


/*
 * Get the next object in a container.
 */
int
fdf_iter_next(fdf_iter_t *iter, char **key, uint64_t *keylen,
              char **data, uint64_t *datalen, fdf_err_t *errp)
{
    fdf_err_t errl;
    FDF_status_t ss;
    uint32_t keylen0;

    err_init(&errp, &errl);
    ss = FDFNextEnumeratedObject(iter->ctr->fdf->thread, iter->iter,
                                 key, &keylen0, data, datalen);
    *keylen = keylen0;
    *errp = ss;

    if (ss == FDF_SUCCESS)
        return 1;
    else if (ss == FDF_OBJECT_UNKNOWN)
        return 0;
    else
        return -1;
}


#if 0
/*
 * Return a string corresponding to a particular FDF message.
 */
char *
fdf_errmsg(FDF_status_t ss)
{
    return FDFStrError(ss);
}
#endif

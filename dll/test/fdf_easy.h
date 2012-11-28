/*
 * An easier interface to FDF.
 * Author: Johann George
 *
 * Copyright (c) 2012,  Sandisk Corporation.  All rights reserved.
 */
#include "fdf.h"


/*
 * Type definitions.
 */
typedef FDF_status_t fdf_err_t;


/*
 * FDF information.
 */
typedef struct {
    struct FDF_state        *state;
    struct FDF_thread_state *thread;
} fdf_t;


/*
 * FDF container information.
 */
typedef struct {
    fdf_t                   *fdf;
    char                    *name;
    int                      num;
    FDF_cguid_t              cguid;
    FDF_container_props_t    props;
} fdf_ctr_t;


/*
 * FDF container iterator.
 */
typedef struct {
    fdf_ctr_t           *ctr;
    struct FDF_iterator *iter;
} fdf_iter_t;


/*
 * Function prototypes.
 */
void        fdf_done(fdf_t *fdf);
void        fdf_free(fdf_t *fdf, void *ptr);
fdf_t      *fdf_init(fdf_err_t *errp);
fdf_ctr_t  *fdf_ctr_init(fdf_t *fdf, char *name, fdf_err_t *errp);
fdf_iter_t *fdf_iter_init(fdf_ctr_t *ctr, fdf_err_t *errp);

int fdf_ctr_done(fdf_ctr_t *ctr, fdf_err_t *errp);
int fdf_ctr_open(fdf_ctr_t *ctr, fdf_err_t *errp);
int fdf_iter_done(fdf_iter_t *iter, fdf_err_t *errp);
int fdf_ctr_set(fdf_ctr_t *ctr, char *key, uint64_t keylen,
                char *data, uint64_t datalen, fdf_err_t *errp);
int fdf_ctr_get(fdf_ctr_t *ctr, char *key, uint64_t keylen,
                char **data, uint64_t *datalen, fdf_err_t *errp);
int fdf_iter_next(fdf_iter_t *iter, char **key, uint64_t *keylen,
                          char **data, uint64_t *datalen, fdf_err_t *errp);

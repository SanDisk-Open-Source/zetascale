/*
 * An easier interface to FDF: header.
 * Author: Johann George
 *
 * Copyright (c) 2012-2013, Sandisk Corporation.  All rights reserved.
 */
#ifndef FDF_EASY_H
#define FDF_EASY_H

#include <pthread.h>
#include "fdf.h"


/*
 * FDF information.
 */
typedef struct {
    struct FDF_state *state;
    pthread_key_t     key;
    struct fdf_prop  *prop;
    char             *prop_data;
} fdf_t;


/*
 * FDF container information.
 */
typedef struct {
    fdf_t                 *fdf;
    char                  *name;
    int                    num;
    FDF_cguid_t            cguid;
    FDF_container_props_t  props;
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
fdf_t      *fdf_init(int start, char **errp);
fdf_ctr_t  *fdf_ctr_init(fdf_t *fdf, char *name, char **errp);
fdf_iter_t *fdf_iter_init(fdf_ctr_t *ctr, char **errp);
const char *fdf_get_prop(fdf_t *fdf, char *name);
const char *fdf_get_prop2(fdf_t *fdf, char *prefix, char *name);

int fdf_start(fdf_t *fdf, char **errp);
int fdf_ctr_flush(fdf_ctr_t *ctr, char **errp);
int fdf_ctr_close(fdf_ctr_t *ctr, char **errp);
int fdf_ctr_delete(fdf_ctr_t *ctr, char **errp);
int fdf_iter_done(fdf_iter_t *iter, char **errp);
int fdf_conf(fdf_t *fdf, char *path, char **errp);
int fdf_ctr_open(fdf_ctr_t *ctr, int mode, char **errp);
int fdf_set_prop(fdf_t *fdf, const char *prop, const char *value);
int fdf_obj_del(fdf_ctr_t *ctr, char *key, uint64_t keylen, char **errp);
int fdf_obj_set(fdf_ctr_t *ctr, char *key, uint64_t keylen,
                char *data, uint64_t datalen, char **errp);
int fdf_obj_get(fdf_ctr_t *ctr, char *key, uint64_t keylen,
                char **data, uint64_t *datalen, char **errp);
int fdf_iter_next(fdf_iter_t *iter, char **key, uint64_t *keylen,
                          char **data, uint64_t *datalen, char **errp);

#endif /* FDF_EASY_H */

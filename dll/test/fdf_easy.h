//----------------------------------------------------------------------------
// ZetaScale
// Copyright (c) 2016, SanDisk Corp. and/or all its affiliates.
//
// This program is free software; you can redistribute it and/or modify it under
// the terms of the GNU Lesser General Public License version 2.1 as published by the Free
// Software Foundation;
//
// This program is distributed in the hope that it will be useful, but WITHOUT
// ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
// FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License v2.1 for more details.
//
// A copy of the GNU Lesser General Public License v2.1 is provided with this package and
// can also be found at: http://opensource.org/licenses/LGPL-2.1
// You should have received a copy of the GNU Lesser General Public License along with
// this program; if not, write to the Free Software Foundation, Inc., 59 Temple
// Place, Suite 330, Boston, MA 02111-1307 USA.
//----------------------------------------------------------------------------

/*
 * An easier interface to ZS: header.
 * Author: Johann George
 *
 * Copyright (c) 2012-2013, Sandisk Corporation.  All rights reserved.
 */
#ifndef ZS_EASY_H
#define ZS_EASY_H

#include <pthread.h>
#include "zs.h"


/*
 * ZS information.
 */
typedef struct {
    struct ZS_state *state;
    pthread_key_t     key;
    struct zs_prop  *prop;
    char             *prop_data;
} zs_t;


/*
 * ZS container information.
 */
typedef struct {
    zs_t                 *zs;
    char                  *name;
    int                    num;
    ZS_cguid_t            cguid;
    ZS_container_props_t  props;
} fdf.ctr_t;


/*
 * ZS container iterator.
 */
typedef struct {
    fdf.ctr_t           *ctr;
    struct ZS_iterator *iter;
} zs_iter_t;


/*
 * Function prototypes.
 */
void        zs_done(zs_t *zs);
void        zs_free(zs_t *zs, void *ptr);
void        zs_set_prop(zs_t *zs, const char *key, const char *val);
zs_t      *zs_init(int start, char **errp);
fdf.ctr_t  *fdf.ctr_init(zs_t *zs, char *name, char **errp);
zs_iter_t *zs_iter_init(fdf.ctr_t *ctr, char **errp);
const char *zs_get_prop(zs_t *zs, const char *key, const char *def);
const char *zs_get_prop2(zs_t *zs, const char *lkey,
                          const char *rkey, const char *def);

int zs_start(zs_t *zs, char **errp);
int fdf.ctr_flush(fdf.ctr_t *ctr, char **errp);
int fdf.ctr_close(fdf.ctr_t *ctr, char **errp);
int fdf.ctr_delete(fdf.ctr_t *ctr, char **errp);
int zs_iter_done(zs_iter_t *iter, char **errp);
int zs_utoi(const char *str, unsigned long *ret);
int fdf.conf(zs_t *zs, char *path, char **errp);
int fdf.ctr_open(fdf.ctr_t *ctr, int mode, char **errp);
int zs_load_prop_file(zs_t *zs, const char *file, char **errp);
int zs_obj_del(fdf.ctr_t *ctr, char *key, uint64_t keylen, char **errp);
int zs_obj_set(fdf.ctr_t *ctr, char *key, uint64_t keylen,
                char *data, uint64_t datalen, char **errp);
int zs_obj_get(fdf.ctr_t *ctr, char *key, uint64_t keylen,
                char **data, uint64_t *datalen, char **errp);
int zs_iter_next(zs_iter_t *iter, char **key, uint64_t *keylen,
                          char **data, uint64_t *datalen, char **errp);

#endif /* ZS_EASY_H */

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
#ifndef TLIB_H
#define TLIB_H

#include <stdio.h>
#include "zs_easy.h"
#include "test.h"


/*
 * Macros.
 */
#define streq(a, b) (strcmp(a, b) == 0)
#define min(a, b)   ((a) < (b) ? (a) : (b))


void die(char *fmt, ...);
void printv(char *fmt, ...);
void show_objs(fdf.ctr_t *ctr);
void flush_ctr(fdf.ctr_t *ctr);
void delete_ctr(fdf.ctr_t *ctr);
void fill_patn(char *buf, int len);
void test_init(zs_t *zs, char *name);
void del_obj(fdf.ctr_t *ctr, char *key);
void die_err(char *err, char *fmt, ...);
void reopen_ctr(fdf.ctr_t *ctr, int mode);
void set_obj(fdf.ctr_t *ctr, char *key, char *value);
void show_obj(fdf.ctr_t *ctr, char *key, char *value);
void fill_uint(char *buf, int len, unsigned long num);
void set_objs_m(fdf.ctr_t *ctr, int obj_min, int obj_max,
                int key_len, int val_len, int num_threads);

void      *malloc_q(long size);
void      *realloc_q(void *ptr, long size);
fdf.ctr_t *open_ctr(zs_t *zs, char *name, int mode);

#endif /* TLIB_H */

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

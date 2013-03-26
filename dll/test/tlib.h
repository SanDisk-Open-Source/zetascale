/*
 * Author: Johann George
 * Copyright (c) 2012-2013, Sandisk Corporation.  All rights reserved.
 */
#ifndef TLIB_H
#define TLIB_H

#include <stdio.h>
#include "fdf_easy.h"
#include "test.h"


/*
 * Macros.
 */
#define streq(a, b) (strcmp(a, b) == 0)
#define min(a, b)   ((a) < (b) ? (a) : (b))


void die(char *fmt, ...);
void show_objs(fdf_ctr_t *ctr);
void flush_ctr(fdf_ctr_t *ctr);
void delete_ctr(fdf_ctr_t *ctr);
void fill_patn(char *buf, int len);
void test_init(fdf_t *fdf, char *name);
void del_obj(fdf_ctr_t *ctr, char *key);
void die_err(char *err, char *fmt, ...);
void reopen_ctr(fdf_ctr_t *ctr, int mode);
void set_obj(fdf_ctr_t *ctr, char *key, char *value);
void show_obj(fdf_ctr_t *ctr, char *key, char *value);
void fill_uint(char *buf, int len, unsigned long num);

void      *alloc(long size);
fdf_ctr_t *open_ctr(fdf_t *fdf, char *name, int mode);

#endif /* TLIB_H */

/*
 * An easier interface to FDF.
 * Author: Johann George
 *
 * Copyright (c) 2012-2013, Sandisk Corporation.  All rights reserved.
 */
#include "fdf.h"
#include "fdf_easy.h"


/*
 * Property.
 */
typedef struct fdf_prop {
    struct fdf_prop *next;
    const char      *name;
    const char      *value;
} fdf_prop_t;


/*
 * Function prototypes.
 */
void fdf_link_(void);
void fdf_conf_done_(fdf_t *fdf);
void fdf_aperr_(char **errp, const char *func,
                FDF_status_t fdf_err, int sys_err, const char *fmt, ...);
char *fdf_errmsg_(FDF_status_t ferr);

/*
 * An easier interface to ZS.
 * Author: Johann George
 *
 * Copyright (c) 2012-2013, Sandisk Corporation.  All rights reserved.
 */
#include "zs.h"
#include "zs_easy.h"


/*
 * Property.
 */
typedef struct zs_prop {
    struct zs_prop *next;
    const char      *name;
    const char      *value;
} zs_prop_t;


/*
 * Function prototypes.
 */
void zs_link_(void);
void fdf.conf_done_(zs_t *zs);
void zs_aperr_(char **errp, const char *func,
                ZS_status_t zs_err, int sys_err, const char *fmt, ...);
char *zs_errmsg_(ZS_status_t ferr);

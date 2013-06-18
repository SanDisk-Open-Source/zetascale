/*
 * File  : fdf_internal_btree.h
 * Author: Manavalan Krishnan
 *
 * Created on June 12
 *
 * SanDisk Proprietary Material, © Copyright 2012 SanDisk, all rights reserved.
 * http://www.sandisk.com
 *
 */
#ifndef __FDF_INTERNAL_CB_H
#define __FDF_INTERNAL_CB_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdio.h>
#include <stdint.h>
#include <inttypes.h>
#include <stdint.h>
#include "common/fdftypes.h"
#include "common/fdfstats.h"

/* Data structure to exchange stats from Btree layer */
typedef struct FDF_ext_stat {
    uint64_t       estat; /* external stat */
    uint64_t       fstat; /* FDF stat */
    uint64_t       ftype;  /* FDF Type of the state */
    uint64_t       value; /* Value */
}FDF_ext_stat_t;

/* Structure to hold callbacks registered by external modules and FDF apps
   that FDF can call in to external libraries to manage them */
typedef FDF_status_t (ext_cb_stats_t)(FDF_cguid_t cguid,FDF_ext_stat_t **stats, uint32_t *n_stats );

typedef enum {
    FDF_EXT_MODULE_BTREE,
    FDF_EXT_MODULE_APP
}FDF_EXT_MODULE;

typedef struct FDF_ext_cb { 
    /* Call back to get stats from external module */
    ext_cb_stats_t *stats_cb;
}FDF_ext_cb_t;

FDF_status_t FDFRegisterCallbacks(struct FDF_state *fdf_state, FDF_ext_cb_t *cb);

#ifdef __cplusplus
}
#endif
#endif // __FDF_INTERNAL_CB_H

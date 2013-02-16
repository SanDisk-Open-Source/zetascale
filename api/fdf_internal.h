/*
 * File  : fdf_internal.h
 * Author: Manavalan Krishnan
 *
 * Created on Feb 13
 *
 * SanDisk Proprietary Material, © Copyright 2012 SanDisk, all rights reserved.
 * http://www.sandisk.com
 *
 */
#ifndef __FDF_INTERNAL_H
#define __FDF_INTERNAL_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdio.h>
#include <stdint.h>
#include <inttypes.h>
#include <stdint.h>
#include "common/fdftypes.h"
#include "common/fdfstats.h"




/* Stats and Admin port*/
typedef enum { 
    FDF_STATS_TYPE_APP_REQ,
    FDF_STATS_TYPE_FLASH,
    FDF_STATS_TYPE_OVERWRITES,
    FDF_STATS_TYPE_CACHE_TO_FLASH,
    FDF_STATS_TYPE_FLASH_TO_CACHE,
    FDF_STATS_TYPE_FLASH_RC,
    FDF_STATS_TYPE_FLASH_MANAGER,
    FDF_STATS_TYPE_PER_CACHE
}FDF_STATS_TYPE;

typedef enum {
    STATS_PRINT_TYPE_SHORT,
    STATS_PRINT_TYPE_DETAILED
}STATS_PRINT_TYPE;

typedef struct {
    uint16_t admin_port;
    uint16_t num_threads;
    struct FDF_state *fdf_state;
}admin_config_t;

typedef struct {
    char *value;
    size_t length;
} cmd_token_t;

typedef struct fdf_stats_info {
    char stat_token[32];
    char desc[64];
    uint16_t category;
}fdf_stats_info_t;

/* Function declaration for Admin and Stats */
FDF_status_t dump_container_stats_by_cguid(struct FDF_thread_state *thd_state,
                                           FDF_cguid_t cguid, int stats_type);
FDF_status_t dump_container_stats_by_name(struct FDF_thread_state *thd_state,
                                                 char *cname, int stats_type);
FDF_status_t dump_all_container_stats(struct FDF_thread_state *thd_state,
                                                               int stats_type);
FDF_status_t fdf_start_admin_thread( struct FDF_state *fdf_state );

/* FDF internal functions */
void fdf_get_flash_map(struct FDF_thread_state *thd_state, FDF_cguid_t cguid,
                       char *buf, int *size);
FDF_cguid_t FDFGetCguid (char *cname );
char *FDFGetContainerName(FDF_cguid_t cguid);
char *get_access_type_stats_desc(int stat );
char *get_flash_type_stats_desc(int stat );
char *get_cache_type_stats_desc(int stat );
int get_cache_type_stats_category(int stat );
void enable_stats_auto_dump() ;
void disable_stats_auto_dump() ;
int is_auto_dump_enabled() ;







#ifdef __cplusplus
}
#endif

#endif // __FDF_H

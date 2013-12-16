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
#include "api/fdf_internal_cb.h"

#define STATS_BUFFER_SIZE 1024
#define MCD_FTH_STACKSIZE 81920

#define FDF_PRODUCT_NAME	"Flash Data Fabric"
#define FDF_LICENSE_PATH	"/opt/sandisk/fdf/license"
#define FDF_LICENSE_CHECK_PERIOD	0

typedef struct FDF_state {
    uint64_t           cguid_cntr;
} FDF_state_t;

/* Stats and Admin port*/
typedef enum { 
    FDF_STATS_TYPE_APP_REQ,
    FDF_STATS_TYPE_FLASH,
    FDF_STATS_TYPE_OVERWRITES,
    FDF_STATS_TYPE_CACHE_TO_FLASH,
    FDF_STATS_TYPE_FLASH_TO_CACHE,
    FDF_STATS_TYPE_FLASH_RC,
    FDF_STATS_TYPE_FLASH_MANAGER,
    FDF_STATS_TYPE_PER_CACHE,
    FDF_STATS_TYPE_CONTAINER_FLASH
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

typedef struct cmd_token {
    char *value;
    size_t length;
} cmd_token_t;

typedef struct fdf_stats_info {
    char stat_token[64];
    char desc[128];
    uint16_t category;
}fdf_stats_info_t;

#ifdef FLIP_ENABLED
enum {
	FDF_IOCTL_FLIP=1,
};
#endif

struct FDF_thread_state;

/* External call backs */
FDF_ext_cb_t *ext_cbs;

/* Function declaration for Admin and Stats */
FDF_status_t dump_container_stats_by_cguid(struct FDF_thread_state *thd_state,
                                           FDF_cguid_t cguid, int stats_type);
FDF_status_t dump_container_stats_by_name(struct FDF_thread_state *thd_state,
                                                 char *cname, int stats_type);
FDF_status_t dump_all_container_stats(struct FDF_thread_state *thd_state,
                                                               int stats_type);
FDF_status_t fdf_start_admin_thread( struct FDF_state *fdf_state );

/* FDF internal functions */

 /**
 * @brief Create and open a physical container.
 *
 * @param fdf_thread_state <IN> The FDF context for which this operation applies
 * @param cname <IN> container name
 * @param properties <IN> container properties
 * @param flags <IN> container open options
 * @param cguid <OUT> container GUID
 * @return FDF_SUCCESS on success
 */
FDF_status_t FDFOpenPhysicalContainer(
    struct FDF_thread_state *fdf_thread_state,
    char                    *cname,
    FDF_container_props_t   *properties,
    uint32_t                 flags,
    FDF_cguid_t             *cguid
    );

/**
 * @brief Close a phyiscal container.
 *
 * @param fdf_thread_state <IN> The FDF context for which this operation applies
 * @param cguid <IN> container CGUID
 * @return FDF_SUCCESS on success
 */
FDF_status_t FDFClosePhysicalContainer(
    struct FDF_thread_state *fdf_thread_state,
    FDF_cguid_t              cguid
    );

/**
 * @brief Delete a physical container
 *
 * @param fdf_thread_state <IN> The FDF context for which this operation applies
 * @param cguid <IN> container CGUID
 * @return FDF_SUCCESS on success
 */
FDF_status_t FDFDeletePhysicalContainer(
    struct FDF_thread_state *fdf_thread_state,
    FDF_cguid_t              cguid
    );

/*
 * Check if we could allow an operation to start
 * @param [in] void
 * @retval FDF_status_t, FDF_SUCCESS for success
 */
FDF_status_t is_fdf_operation_allowed(void);


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
void set_stats_autodump_interval(int interval);
int get_autodump_interval();
char *FDFGetNextContainerName(struct FDF_thread_state *fdf_thread_state, struct FDFCMapIterator **iterator, FDF_cguid_t *pcguid);
FDF_status_t async_command_delete_container(FDF_cguid_t cguid);
void get_async_delete_stats( uint32_t *num_deletes,uint32_t *num_prog);
FDF_status_t fdf_delete_container_async_end(
                                struct FDF_thread_state *fdf_thread_state,
                                                         FDF_cguid_t cguid);
FDF_status_t fdf_delete_container_async_start(
                                struct FDF_thread_state *fdf_thread_state,
                                FDF_cguid_t cguid, FDF_container_mode_t mode );
void init_async_cmd_handler(int num_thds, struct FDF_state *fdf_state);
void wait_for_container_del();

extern int fdf_instance_id;
char *get_bool_str( int val);
char *get_durability_str(FDF_durability_level_t dura);
void log_properties_file(const char *path, int log_level);
FDF_status_t change_log_level(char *level);
char *get_log_level();

bool licd_start(const char *, int, struct FDF_state *);
void wait_for_licd_start();
bool is_license_valid();

#ifdef __cplusplus
}
#endif

#endif // __FDF_H

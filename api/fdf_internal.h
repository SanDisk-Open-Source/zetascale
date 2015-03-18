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
#ifndef __ZS_INTERNAL_H
#define __ZS_INTERNAL_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdio.h>
#include <stdint.h>
#include <inttypes.h>
#include <stdint.h>
#include "common/zstypes.h"
#include "common/fdfstats.h"
#include "api/fdf_internal_cb.h"

#define STATS_BUFFER_SIZE 1024
#define MCD_FTH_STACKSIZE 81920

#define ZS_PRODUCT_NAME	"ZetaScale"
#define ZS_LICENSE_PATH	"/opt/sandisk/zs/license"
#define ZS_LICENSE_CHECK_PERIOD	0

typedef struct ZS_state {
    uint64_t           cguid_cntr;
} ZS_state_t;

/* Stats and Admin port*/
typedef enum { 
    ZS_STATS_TYPE_APP_REQ,
    ZS_STATS_TYPE_FLASH,
    ZS_STATS_TYPE_OVERWRITES,
    ZS_STATS_TYPE_CACHE_TO_FLASH,
    ZS_STATS_TYPE_FLASH_TO_CACHE,
    ZS_STATS_TYPE_FLASH_RC,
    ZS_STATS_TYPE_FLASH_MANAGER,
    ZS_STATS_TYPE_PER_CACHE,
    ZS_STATS_TYPE_CONTAINER_FLASH,
    ZS_STATS_TYPE_BTREE,
}ZS_STATS_TYPE;


typedef enum {
    STATS_PRINT_TYPE_SHORT,
    STATS_PRINT_TYPE_DETAILED
}STATS_PRINT_TYPE;

typedef struct {
    uint16_t admin_port;
    uint16_t num_threads;
    struct ZS_state *zs_state;
}admin_config_t;

typedef struct cmd_token {
    char *value;
    size_t length;
} cmd_token_t;

typedef struct zs_stats_info {
    char stat_token[64];
    char desc[128];
    uint16_t category;
}zs_stats_info_t;

#ifdef FLIP_ENABLED
enum {
	ZS_IOCTL_FLIP=1,
};
#endif

struct ZS_thread_state;

/* External call backs */
ZS_ext_cb_t *ext_cbs;

/* Function declaration for Admin and Stats */
ZS_status_t dump_container_stats_by_cguid(struct ZS_thread_state *thd_state,
                                           ZS_cguid_t cguid, int stats_type);
ZS_status_t dump_container_stats_by_name(struct ZS_thread_state *thd_state,
                                                 char *cname, int stats_type);
ZS_status_t dump_all_container_stats(struct ZS_thread_state *thd_state,
                                                               int stats_type);

ZS_status_t log_summary_stats(struct ZS_thread_state *thd_state, FILE *fp);
ZS_status_t log_container_props(struct ZS_thread_state *thd_state, FILE *fp);
ZS_status_t log_container_stats(struct ZS_thread_state *thd_state, FILE *fp);
ZS_status_t log_flash_stats(struct ZS_thread_state *thd_state, FILE *fp);
ZS_status_t log_all_container_stats(struct ZS_thread_state *thd_state, FILE *fp, int stats_type);

ZS_status_t zs_start_admin_thread( struct ZS_state *zs_state );

/* ZS internal functions */

 /**
 * @brief Create and open a physical container.
 *
 * @param zs_thread_state <IN> The ZS context for which this operation applies
 * @param cname <IN> container name
 * @param properties <IN> container properties
 * @param flags <IN> container open options
 * @param cguid <OUT> container GUID
 * @return ZS_SUCCESS on success
 */
ZS_status_t ZSOpenPhysicalContainer(
    struct ZS_thread_state *zs_thread_state,
    char                    *cname,
    ZS_container_props_t   *properties,
    uint32_t                 flags,
    ZS_cguid_t             *cguid
    );

/**
 * @brief Close a phyiscal container.
 *
 * @param zs_thread_state <IN> The ZS context for which this operation applies
 * @param cguid <IN> container CGUID
 * @return ZS_SUCCESS on success
 */
ZS_status_t ZSClosePhysicalContainer(
    struct ZS_thread_state *zs_thread_state,
    ZS_cguid_t              cguid
    );

/**
 * @brief Delete a physical container
 *
 * @param zs_thread_state <IN> The ZS context for which this operation applies
 * @param cguid <IN> container CGUID
 * @return ZS_SUCCESS on success
 */
ZS_status_t ZSDeletePhysicalContainer(
    struct ZS_thread_state *zs_thread_state,
    ZS_cguid_t              cguid
    );

/*
 * Check if we could allow an operation to start
 * @param [in] void
 * @retval ZS_status_t, ZS_SUCCESS for success
 */
ZS_status_t is_zs_operation_allowed(void);

ZS_status_t ZSWriteObjects(
	struct ZS_thread_state  *zs_thread_state,
	ZS_cguid_t          cguid,
	char                **key,
	uint32_t             keylen,
	char                **data,
	uint64_t             datalen,
	uint32_t             count,
	uint32_t             flags
	);

void zs_get_flash_map(struct ZS_thread_state *thd_state, ZS_cguid_t cguid,
                       char *buf, int *size);
ZS_cguid_t ZSGetCguid (char *cname );
char *ZSGetContainerName(ZS_cguid_t cguid);
char *get_access_type_stats_desc(int stat );
char *get_btree_stats_desc(int stat );
char *get_flash_type_stats_desc(int stat );
char *get_cache_type_stats_desc(int stat );
int get_cache_type_stats_category(int stat );
void enable_stats_auto_dump() ;
void disable_stats_auto_dump() ;
int is_auto_dump_enabled() ;
void set_stats_autodump_interval(int interval);
int get_autodump_interval();
FILE *open_stats_dump_file();

bool is_btree_loaded();
char *ZSGetNextContainerName(struct ZS_thread_state *zs_thread_state, struct ZSCMapIterator **iterator, ZS_cguid_t *pcguid);
ZS_status_t async_command_delete_container(ZS_cguid_t cguid);
void get_async_delete_stats( uint32_t *num_deletes,uint32_t *num_prog);
ZS_status_t zs_delete_container_async_end(
                                struct ZS_thread_state *zs_thread_state,
                                                         ZS_cguid_t cguid);
ZS_status_t zs_delete_container_async_start(
                                struct ZS_thread_state *zs_thread_state,
                                ZS_cguid_t cguid, ZS_container_mode_t mode );
void init_async_cmd_handler(int num_thds, struct ZS_state *zs_state);
void wait_for_container_del();

extern unsigned max_num_containers;
extern int zs_instance_id;
char *get_bool_str( int val);
char *get_durability_str(ZS_durability_level_t dura);
void log_properties_file(const char *path, int log_level);
ZS_status_t change_log_level(char *level);
char *get_log_level();

bool licd_start(const char *, int, struct ZS_state *);
void wait_for_licd_start();
bool is_license_valid(bool);

/* Keep track of max bracket slab count */
#define MAX_TRX_BRACKET_SLAB_CNT 1000000

#ifdef __cplusplus
}
#endif

#endif // __ZS_H

/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

#ifndef PLATFORM_STATS_H
#define PLATFORM_STATS_H 1

/*
 * File:   $HeadURL: svn://svn.schoonerinfotech.net/schooner-trunk/trunk/sdf/platform/stats.h $
 * Author: drew
 *
 * Created on December 10, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: stats.h 5170 2008-12-18 01:42:16Z drew $
 */

/**
 * Stats subsystem.  Hierchical stats dovetail into the logging subsystem
 * and can be used for pseudo-random white box test termination
 * conditions as when specific corner cases have been successfully
 * stimulated.
 */

#include "platform/defs.h"
#include "platform/logging.h"
#include "platform/types.h"

struct plat_stat;
struct plat_stat_setter;

struct plat_stat_get_value {
    char *path;
    int64_t value;
};

struct plat_stat_get {
    int nvalue;
    struct plat_stat_get_value values[0];
};

enum plat_stat_set {
    PLAT_STAT_SET,
    PLAT_STAT_ADD,
    PLAT_STAT_SUB
};

__BEGIN_DECLS

/**
 * @brief Register stat collection function
 *
 * The non-null subset of prefix, middle, and suffix are concatenated
 * with '/'.  These are provided in place of a single string so that it's
 * easier to programatically generate stats.
 *
 * @param level <IN> Priority at which this stat is logged in
 * #plat_stat_log
 *
 * @param get_fn <IN> Function to collect stat on pull.  NULL for
 * a stat which is updated via #plat_stat_set
 *
 * @param exra <IN> Additional argument for get_fn
 */
struct plat_stat *plat_stat_register(const char *prefix,
                                     const char *middle,
                                     const char *suffix,
                                     enum plat_log_level level,
                                     int64_t (*get_fn)(void *extra),
                                     char *extra);
/**
 * @brief Remove registered stat on shutdown.
 *
 * @param stat <IN> Stat returned by #plat_stat_register
 */
void plat_stat_remove(struct plat_stat *stat);

/**
 * @brief Register a function which will push a group of individual stats.
 *
 * The setter shall call #plat_stat_set on zero or more statistics.
 *
 * It must be freed with #plat_stat_setter_remove
 */
struct plat_stat_setter *
plat_stat_setter_register(void (*set_fn)(void *extra), void *extra);

/**
 * @brief Deregister a setter function
 */
void plat_stat_setter_remove(struct plat_stat_setter *setter);

/**
 * Push stat
 *
 * @param plat_stat <IN> Return from #plat_stat_register.  plat_stat
 * must not have been removed prior to this call.
 *
 * @param new_value <IN>  Handling is relative to how; use
 * #PLAT_STAT_SET to set
 *
 * @return Old value
 */
int64_t plat_stat_set(struct plat_stat *plat_stat, int64_t new_value,
                      enum plat_stat_set how);

/**
 * @brief Log all statistics
 */
void plat_stat_log();

/**
 * @brief Return statistics
 *
 * @return Dynamically allocated statistics structure which user frees
 * with #plat_tat_get_free
 */
struct plat_stat_get *plat_stat_get_alloc();

/**
 * @brief Free return from #plat_stat_get_alloc()
 *
 * @param arg <IN> free
 */
void plat_stat_get_free(struct plat_stat_get *arg);

/**
 * @brief Return white space delimited stats
 *
 * The string has key, value pairs in the form
 *
 * %skey1 value1%s
 *
 * where the first %s is replaced by the prefix argument
 * and second the suffix argument.
 *
 * @param prefix <IN> Prefix for each line.  NULL means ""
 * @Param suffix <IN> Suffix for each line.  NULL means "\n"
 * @return Dynamically allocated string which user frees with
 * #plat_stat_str_get_free
 */
char *plat_stat_str_get_alloc(const char *prefix, const char *suffix);

/**
 * @brief Free return from #plat_stat_str_get_alloc
 */
void plat_stat_str_get_free(char *buf);

__END_DECLS

#endif /* ndef PLATFORM_STATS_H */

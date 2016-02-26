/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

/*
 * File:   $HeadURL: svn://svn.schoonerinfotech.net/schooner-trunk/trunk/sdf/platform/stats.c $
 * Author: drew
 *
 * Created on December 10, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: stats.c 15015 2010-11-10 23:09:06Z briano $
 */

/**
 * Stats subsystem implementation
 */

#define PLATFORM_INTERNAL 1

#include <sys/queue.h>
#include <libgen.h>

#include "platform/assert.h"
#include "platform/errno.h"
#include "platform/logging.h"
#include "platform/rwlock.h"
#include "platform/stats.h"
#include "platform/stdio.h"
#include "platform/stdlib.h"
#include "platform/string.h"
#include "platform/types.h"

struct plat_stats {
    /** @brief Lock, held on stats_list maniupulation */
    plat_rwlock_t lock;

    /** @brief Number of stats on list */
    int nstats;

    /** @brief List of all stats */
    TAILQ_HEAD(/* no name */, plat_stat) stats_list;

    TAILQ_HEAD(/* no name */, plat_stat_setter) setters_list;
};

struct plat_stat {
    /** @brief Path to stat (slash delimted) */
    char *path;

    /** @brief Called to pull stat */
    int64_t (*get_fn)(void *extra);

    /** @brief Argument to get_fn */
    char *extra;

    /** @brief Current value for push stats */
    int64_t value;

    /** @brief Log category */
    int log_cat;

    /** @brief Default log level */
    enum plat_log_level level;

#ifdef notyet
    /** @brief Reference count */
    int ref_count;
#endif

    TAILQ_ENTRY(plat_stat) list;
};

struct plat_stat_setter {
    /** @brief Called to pull stats */
    void (*fn)(void *extra);

    /** @brief Argument to fn */
    void *extra;

    TAILQ_ENTRY(plat_stat_setter) list;
};

static struct plat_stats plat_stats = {
    .lock = PLAT_RWLOCK_INITIALIZER,
    .nstats = 0,
    .stats_list = TAILQ_HEAD_INITIALIZER(plat_stats.stats_list),
    .setters_list = TAILQ_HEAD_INITIALIZER(plat_stats.setters_list)
};

static int ps_dump_locked(int (*fn)(void *extra, struct plat_stat *stat),
                          void *extra);
static int ps_dump_log(void *extra, struct plat_stat *stat);
static int ps_dump_get(void *extra, struct plat_stat *stat);
static int ps_dump_str(void *extra, struct plat_stat *stat);

struct plat_stat *
plat_stat_register(const char *prefix, const char *middle, const char *suffix,
                   enum plat_log_level level, int64_t (*get_fn)(void *extra),
                   char *extra) {
    int failed;
    struct plat_stat *stat;
#ifdef notyet
    struct plat_stat *tmp;
#endif
    char *path;

    path = NULL;
    failed = -1 == sys_asprintf(&path, "%s%s%s%s%s",
                                prefix ? prefix : "",
                                prefix && (middle || suffix) ? "/" : "",
                                middle ? middle : "",
                                (middle && suffix) ? "/" : "",
                                suffix ? suffix : "");

    plat_rwlock_wrlock(&plat_stats.lock);

    stat = NULL;

#ifdef notyet
    /*
     * Need to add reference counting for this, and then the semantics
     * aren't right for statistics with a pull function.
     */
    if (!failed) {
        TAILQ_FOREACH(tmp, &plat_stats.stats_list, list) {
            if (0 == strcmp(tmp->path, path)) {
                stat = tmp;
                break;
            }
        }
    }
#endif

    if (!failed && !stat) {
        stat = sys_malloc(sizeof(*stat));
        failed = !stat;
        if (!failed) {
            memset(stat, 0, sizeof(*stat));
            stat->get_fn = get_fn;
            stat->extra = extra;
            stat->path = path;
            path = NULL;
            stat->log_cat = plat_log_add_category(stat->path);
            stat->level = level;
            failed = stat->log_cat < 0;
        }

        if (!failed) {
            TAILQ_INSERT_TAIL(&plat_stats.stats_list, stat, list);
        } else {
            sys_free(stat);
            stat = NULL;
        }
    }

    plat_rwlock_unlock(&plat_stats.lock);

    if (path) {
        sys_free(path);
    }

    return (stat);
}

void
plat_stat_remove(struct plat_stat *stat) {
    if (stat) {
        plat_rwlock_wrlock(&plat_stats.lock);
        TAILQ_REMOVE(&plat_stats.stats_list, stat, list);
        sys_free(stat->path);
        sys_free(stat);
        plat_rwlock_unlock(&plat_stats.lock);
    }
}

struct plat_stat_setter *
plat_stat_setter_register(void (*fn)(void *extra), void *extra) {
    struct plat_stat_setter *setter;

    setter = sys_malloc(sizeof (*setter));
    if (setter) {
        setter->fn = fn;
        setter->extra = extra;
        plat_rwlock_wrlock(&plat_stats.lock);
        TAILQ_INSERT_TAIL(&plat_stats.setters_list, setter, list);
        plat_rwlock_unlock(&plat_stats.lock);
    }

    return (setter);
}

void
plat_stat_setter_remove(struct plat_stat_setter *setter) {
    plat_rwlock_wrlock(&plat_stats.lock);
    TAILQ_REMOVE(&plat_stats.setters_list, setter, list);
    plat_rwlock_unlock(&plat_stats.lock);
    sys_free(setter);
}

int64_t
plat_stat_set(struct plat_stat *stat, int64_t new_value,
              enum plat_stat_set how) {
    int64_t ret = 0; /* placate GCC */

    switch (how) {
    case PLAT_STAT_SET:
        do {
            ret = stat->value;
        } while (__sync_val_compare_and_swap(&stat->value, ret, new_value) !=
                 ret);
        break;

    case PLAT_STAT_ADD:
        ret = __sync_fetch_and_add(&stat->value, new_value);
        break;

    case PLAT_STAT_SUB:
        ret = __sync_fetch_and_sub(&stat->value, new_value);
        break;

    default:
        plat_assert_always(0);
    }

    return (ret);
}

void
plat_stat_log() {
    plat_rwlock_rdlock(&plat_stats.lock);

    ps_dump_locked(&ps_dump_log, NULL);

    plat_rwlock_unlock(&plat_stats.lock);

}

struct plat_stat_get *
plat_stat_get_alloc() {
    int failed;
    struct plat_stat_get *get;

    plat_rwlock_rdlock(&plat_stats.lock);

    get = sys_malloc(sizeof (*get) +
                     sizeof (get->values[0]) * plat_stats.nstats);
    failed = !get;

    if (!failed) {
        get->nvalue = 0;
        failed = ps_dump_locked(&ps_dump_get, get);
    }

    plat_rwlock_unlock(&plat_stats.lock);

    if (!failed && get) {
        plat_stat_get_free(get);
        get = NULL;
    }

    return (get);
}

void
plat_stat_get_free(struct plat_stat_get *arg) {
    int i;

    if (arg) {
        for (i = 0; i < arg->nvalue; ++i) {
            sys_free(arg->values[i].path);
        }

        sys_free(arg);
    }
}

enum {
    /** @brief Grow increment */
    STR_GET_GROW = 1024
};

/** @brief State for #plat_stat_str_get_alloc */
struct plat_stat_str_get {
    /** @brief Prepended to each line (may be NULL) */
    const char *prefix;

    /** @brief Appended to each line (may be NULL) */
    const char *suffix;

    /** @brief Buffer allocated via sys_malloc */
    char *buf;

    /** @brief Current append pointer */
    char *fill;

    /** @brief Current allocation length */
    int alloc_len;
};

char *
plat_stat_str_get_alloc(const char *prefix, const char *suffix) {
    int failed;
    struct plat_stat_str_get get;

    get.fill = get.buf = sys_malloc(1024);
    failed = !get.buf;

    if (!failed) {
        get.prefix = prefix;
        get.suffix = suffix;
        get.alloc_len = STR_GET_GROW;

        plat_rwlock_rdlock(&plat_stats.lock);
        failed = ps_dump_locked(&ps_dump_str, &get);
        plat_rwlock_unlock(&plat_stats.lock);
    }

    if (failed) {
        sys_free(get.buf);
        get.buf = NULL;
    }

    return (get.buf);
}

void
plat_stat_str_get_free(char *buf) {
    sys_free(buf);
}

/**
 * @brief Collect all stats and apply a given output function to each
 *
 * As a precondition, the caller must hold a read lock on plat_stats.  The
 * iteration over stats stops with the first non-zero return from the
 * output function
 *
 * @param fn <IN> Output function
 * @param extra <IN> passed to ever function call
 * @return return from last function visited (0 implies all stats were visited)
 */
static int
ps_dump_locked(int (*fn)(void *extra, struct plat_stat *stat), void *extra) {
    int ret = 0;
    struct plat_stat *stat;
    struct plat_stat_setter *setter;

    TAILQ_FOREACH(setter, &plat_stats.setters_list, list) {
        (*setter->fn)(setter->extra);
    }

    TAILQ_FOREACH(stat, &plat_stats.stats_list, list) {
        if (stat->get_fn) {
            stat->value = (*stat->get_fn)(stat->extra);
        }
        ret = (*fn)(extra, stat);
        if (ret) {
            break;
        }
    }

    return (ret);
}

/** @brief Worker for #ps_dump_locked implementing #plat_stat_log */
static int
ps_dump_log(void *extra, struct plat_stat *stat) {
    /* XXX: drew 2008-12-15 Shouldn't log strings for stats in binary log */
    plat_log_msg(21045, stat->log_cat, stat->level, "%s %ld",
                 stat->path, stat->value);
    return (0);
}

/** @brief Worker for #ps_dump_locked implementing #plat_stat_get_alloc */
static int
ps_dump_get(void *extra, struct plat_stat *stat) {
    struct plat_stat_get *stats_get = (struct plat_stat_get *)extra;
    struct plat_stat_get_value *get_value =
        &stats_get->values[stats_get->nvalue];
    int ret;

    get_value->path = sys_strdup(stat->path);
    get_value->value = stat->value;

    if (get_value->path) {
        ret = 0;
        /* This is a coverity false positive. */ 
        ++stats_get->nvalue;
    } else {
        ret = -ENOMEM;
    }

    return (ret);
}

/** @brief Worker for #ps_dump_locked implementing #plat_stat_str_get_alloc */
static int
ps_dump_str(void *extra, struct plat_stat *stat) {
    struct plat_stat_str_get *get = (struct plat_stat_str_get *)extra;
    int ret = 0;

    int retry;
    int remain;
    int status;
    char *tmp;

    char * b_name;
    char * d_name;
    char * b_tmp = plat_strdup( stat->path );
    char * d_tmp = plat_strdup( stat->path );
    char d_buf[16];

    /*
     * XXX: drew 2009-06-10 Need to undo this hack so we know 
     * what things are other than memory.  It doesn't save 
     * appreciable space in the log files.
     */
    memset( d_buf, ' ', sizeof(d_buf) );
    d_buf[15] = '\0';
    
    if ( NULL == b_tmp || NULL == d_tmp ) {
        return 0;
    }
    b_name = basename( b_tmp );

    tmp = dirname( d_tmp );
    d_name = basename( tmp );
    memcpy( d_buf, d_name, 15 < strlen(d_name) ? 15 : strlen(d_name) );

    do {
        retry = 0;
        remain = (get->buf + get->alloc_len - get->fill);

        if ( 0 == strcmp( b_name, "allocated_count" ) ) {
            status = snprintf( get->fill, (size_t)remain, "\r\nSTAT %s %11ld", 
                               d_buf, stat->value );
        }
        else {
            status = snprintf( get->fill, (size_t)remain, " %11ld",
                               stat->value );
        }

        if (status < 0) {
            ret = -errno;
        } else if (status < remain) {
            get->fill += status;
        } else {
            get->alloc_len += STR_GET_GROW;
            tmp = sys_realloc(get->buf, get->alloc_len);
            if (tmp) {
                get->fill = tmp + (get->fill - get->buf);
                get->buf = tmp;
                retry = 1;
            } else {
                ret = -ENOMEM;
            }
        }
    } while (retry);

    plat_free( b_tmp );
    plat_free( d_tmp );

    return (ret);
}

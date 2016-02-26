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
 * File:   $URL: svn://svn.schoonerinfotech.net/schooner-trunk/trunk/sdf/platform/aio_wc.c $
 * Author: drew
 *
 * Created on March 17, 2010
 *
 * (c) Copyright 2010, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: aio_wc.c 15015 2010-11-10 23:09:06Z briano $
 */

/**
 * Write combining paio_api implementation
 *
 * Write combining works by maintaining a per context+file descriptor
 * red-black tree of scheduled writes and constantly iterates over
 * those with a unidirectional elevator, maintaining maximum in-flight
 * IO count and total size limits.
 *
 * Currently new IOs are only started as side effects to the paio_submit
 *
 * For v1, reads are handled as a pass-through and are ignored
 *
 * Note that only flush policy separates "write combining" and
 * "write back caching".
 */

#include <sys/uio.h>
#include "sys/queue.h"
#include "sys/tree.h"

#include "platform/alloc.h"
#include "platform/logging.h"
#include "platform/string.h"
#include "platform/time.h"
#include "platform/types.h"

#include "fth/fth.h"
#include "fth/fthLock.h"

#include "platform/aio_api.h"
#include "platform/aio_api_internal.h"
#include "platform/aio_libaio.h"
#include "platform/aio_wc.h"

#define LOG_CAT PLAT_LOG_CAT_PLATFORM_AIO_WC

enum {
    PAIO_WC_DEFAULT_IO_LIMIT = 100,

    PAIO_WC_DEFAULT_BYTE_LIMIT = 50 * 1024 * 1024
};

struct paio_wc_context;
struct paio_wc_fd;
struct paio_wc_extent;

/** @brief API */
struct paio_wc_api {
    /* Must be first for type-pun */
    struct paio_api base;

    /** @brief Configuration */
    struct paio_wc_config config;

    /** @brief Wrapped API */
    struct paio_api *wrapped_api;

    /** @brief Reference count for robustness */
    int ref_count;
};

/** @brief Context, owns one #paio_wc_fd per fd with pending ops */
struct paio_wc_context {
    /* Must be first for type-pun */
    struct paio_context base;

    /** @brief Configuration */
    struct paio_wc_config config;

    /** @brief Lock */
    struct fthLock lock;

    struct {
        /**
         * @brief List of per file-descriptor states
         *
         * XXX: drew 2010-03-17 Note linear lookup performance if number of
         * file descriptors becomes large
         */
        TAILQ_HEAD(, paio_wc_fd) list;

        /** @brief Number of entries in fd list */
        int count;
    } fd;

    struct {
        /** @brief Wrapped context */
        struct paio_context *context;

        unsigned initialized : 1;
    } wrapped;

    /** @brief Completed user submitted extents */
    struct {
        /** @brief Allocated count of array */
        int max;

        /** @brief number of valid completed events */
        int count;

        /** @brief max sized array of events */
        struct io_event *array;
    } events;
};

RB_HEAD(PAIO_WC_EXTENT_TREE, paio_wc_extent);

/** @brief Per fd-state, paintains a tree of paio_wc_extent per extent */
struct paio_wc_fd {
    struct paio_wc_context *context;

    /** @brief File descriptor */
    int fd;

    struct {
        /** @brief Tree of extents */
        struct PAIO_WC_EXTENT_TREE tree;

        /** @brief Current extent used in the uni-directional elevator */
        struct paio_wc_extent *current;

        /** @brief Extent count */
        int count;
    } queued_extents;

    struct {
        /** @brief List of all paio_wc_extent structures currently submitted */
        TAILQ_HEAD(, paio_wc_extent) list;

        /** @brief Number of extents currently submitted for writing */
        int count;

        /** @brief Total size of writes in-flight in bytes */
        long total_bytes;
    } in_flight_writes;

    int ref_count;

    /** @brief Entry in context->fd.list */
    TAILQ_ENTRY(paio_wc_fd) fd_list_entry;
};

/** @brief Where paio_wc_extent is attached to paio_wc_extent.fd */
enum paio_wc_extent_where {
    /** @brief With no pointers from fd */
    PAIOWCE_WHERE_NONE = 0,
    /** @brief In fd->queued_extents.tree */
    PAIOWCE_WHERE_QUEUED,
    /** @brief in fd->in_flight_writes_list */
    PAIOWCE_WHERE_IN_FLIGHT
};

enum {
    /* EXT */
    PAIO_WC_EXTENT_MAGIC = 0x545845
};

struct paio_wc_extent {
    /** @brief Magic number so we can validate sensible returns */
    plat_magic_t magic;

    /** @brief Parent fd */
    struct paio_wc_fd *fd;

    /** @brief File offset */
    off_t offset;

    /** @brief Total length */
    size_t len;

    struct {
        /**
         * @brief User iocbs associated with this extent
         *
         * Note that iocbs may not be sorted
         */
        struct iocb **iocbs;

        /** @brief Current number of valid iocbs */
        int count;
    } user_ios;

    /** @brief Connection to paio_wc_extent.fd */
    enum paio_wc_extent_where where;

    /** @brief Entry in state->queued_extents.tree */
    RB_ENTRY(paio_wc_extent) queued_extents_tree_entry;

    /** @brief Entry in state->in_flight_writes.list */
    TAILQ_ENTRY(paio_wc_extent) in_flight_writes_list_entry;

    /**
     * @brief IO submitted to state->context->wrapped_context.
     *
     * For single user_ios.count == 1 the buffer comes from the only IO;
     * in all other cases it's dynamically allocated.
     */
    struct iocb submitted_iocb;

    /** @brief This IO has been submitted, so submitted_iocb is valid  */
    unsigned submitted : 1;
};

enum {
    /** @brief Number of events to get back from wrapped getevents */
    PAIO_GETEVENTS_COUNT = 256
};

/* API prototypes */
static void paio_wc_api_destroy(struct paio_api *api);
static void paio_wc_api_ref_count_dec(struct paio_wc_api *api);
static int paio_wc_setup(struct paio_api *api_base, int maxevents,
                         struct paio_context **ctxp);
static int paio_wc_destroy(struct paio_context *ctx_base);
static int paio_wc_submit(struct paio_context *ctx_base, long nr,
                          struct iocb *ios[]);
static int paio_wc_cancel(struct paio_context *ctx_base, struct iocb *iocb,
                          struct io_event *evt);
static long paio_wc_getevents(struct paio_context *ctx_base, long min_nr,
                              long nr, struct io_event *events,
                              struct timespec *timeout);

/* Other internal paio_wc prototypes */
static struct paio_wc_fd *paio_wc_get_fd_locked(struct paio_wc_context *context,
                                                int fd);
static int paio_wc_do_submit_locked(struct paio_wc_context *context);
static void paio_wc_event_locked(struct paio_wc_context *context,
                                 const struct io_event *event_arg);
static void paio_wc_write_event_locked(struct paio_wc_context *context,
                                       const struct io_event *event_arg);
static void paio_wc_grow_events_locked(struct paio_wc_context *context,
                                       int increment);

/* paio_wc_fd prototypes */
static int paio_wc_fd_submit_locked(struct paio_wc_fd *fd);
static int paio_wc_fd_add_write_locked(struct paio_wc_fd *fd,
                                       struct iocb *iocb);
static void paio_wc_fd_free_locked(struct paio_wc_fd *fd);
static void paio_wc_fd_ref_count_dec_locked(struct paio_wc_fd *fd);

/* paio_wc_extent prototypes */
static int paio_wc_extent_submit_locked(struct paio_wc_extent *extent);
static int
paio_wc_extent_merge_adjacent_locked(struct paio_wc_extent *extent_arg);
static int paio_wc_extent_merge_locked(struct paio_wc_extent *extent,
                                       struct paio_wc_extent *next);
static void paio_wc_extent_free_locked(struct paio_wc_extent *extent);
static void paio_wc_extent_remove_queued_locked(struct paio_wc_extent *extent);
static void
paio_wc_extent_remove_in_flight_locked(struct paio_wc_extent *extent);
static int paio_wc_extent_cmp(const struct paio_wc_extent *lhs,
                              const struct paio_wc_extent *rhs);

RB_PROTOTYPE_STATIC(PAIO_WC_EXTENT_TREE, paio_wc_extent, queued_extents_tree_entry,
                    paio_wc_extent_cmp)

/* Kludge around bsd tree header being unable to specify these are unused */
static struct paio_wc_extent *
PAIO_WC_EXTENT_TREE_RB_FIND(struct PAIO_WC_EXTENT_TREE *,
                            struct paio_wc_extent *) __attribute__((unused));
static struct paio_wc_extent *
PAIO_WC_EXTENT_TREE_RB_NFIND(struct PAIO_WC_EXTENT_TREE *,
                             struct paio_wc_extent *) __attribute__((unused));

struct paio_api *
paio_wc_create(const struct paio_wc_config *config,
               struct paio_api *wrapped_api) {
    struct paio_wc_api *ret;

    if (plat_calloc_struct(&ret)) {
        ret->base.api_destroy = &paio_wc_api_destroy;

        ret->base.io_setup = &paio_wc_setup;
        ret->base.io_destroy = &paio_wc_destroy;
        ret->base.io_submit = &paio_wc_submit;
        ret->base.io_cancel = &paio_wc_cancel;
        ret->base.io_getevents = &paio_wc_getevents;

        ret->config = *config;

        ret->wrapped_api = wrapped_api;

        ret->ref_count = 1;
    }

    /* Coverity does not like this safe type-pun */
#if 0
    return (&ret->base);
#else
    /* But may accept this */
    return ((struct paio_api *)ret);
#endif
}

void
paio_wc_config_init(struct paio_wc_config *config) {
    memset(config, 0, sizeof(*config));
    config->io_limit = PAIO_WC_DEFAULT_IO_LIMIT;
    config->byte_limit = PAIO_WC_DEFAULT_BYTE_LIMIT;
}

void
paio_wc_config_destroy(struct paio_wc_config *config) {
    /* nop */
}

static void
paio_wc_api_destroy(struct paio_api *api_base) {
    struct paio_wc_api *api = (struct paio_wc_api *)api_base;

    if (api) {
        paio_wc_api_ref_count_dec(api);
    }
}

static void
paio_wc_api_ref_count_dec(struct paio_wc_api *api) {
    int after;

    after = __sync_sub_and_fetch(&api->ref_count, 1);
    plat_assert(after >= 0);
    if (!after) {
        plat_free(api);
    }
}

static int
paio_wc_setup(struct paio_api *api_base, int maxevents,
              struct paio_context **ctxp) {
    struct paio_wc_api *api = (struct paio_wc_api *)api_base;
    struct paio_wc_context *context;
    int ret;

    if (!plat_calloc_struct(&context)) {
        errno = ENOMEM;
        ret = -1;
    } else {
        (void) __sync_fetch_and_add(&api->ref_count, 1);
        context->base.api = api_base;

        context->config = api->config;
        fthLockInit(&context->lock);
        TAILQ_INIT(&context->fd.list);

        ret = paio_setup(api->wrapped_api, maxevents,
                         &context->wrapped.context);
        if (!ret) {
            context->wrapped.initialized = 1;
        }
    }

    if (ret && context) {
        paio_wc_destroy(&context->base);
        context = NULL;
    }

    if (context) {
        *ctxp = &context->base;
    } else {
        *ctxp = NULL;
    }

    return (ret);
}

static int
paio_wc_destroy(struct paio_context *ctx_base) {
    struct paio_wc_context *context =
        (struct paio_wc_context *)ctx_base;
    struct paio_wc_fd *fd;
    struct paio_wc_fd *next_fd;

    struct paio_wc_api *api;
    int ret;

    if (context) {
        /*
         * XXX: drew 2010-03-19 Coverity isn't smart enough to figure
         * out that context->fd.list.lh_first is changing each time when
         * we do this instead
         * while ((fd = TAILQ_FIRST(&context->fd.list)))
         */
        TAILQ_FOREACH_SAFE(fd, &context->fd.list, fd_list_entry, next_fd) {
            paio_wc_fd_free_locked(fd);
        }

        if (context->wrapped.initialized) {
            ret = paio_destroy(context->wrapped.context);
            context->wrapped.initialized = 0;
        } else {
            ret = 0;
        }

        if (context->events.array) {
            plat_free(context->events.array);
        }

        api = (struct paio_wc_api *)context->base.api;
        plat_free(context);
        paio_wc_api_ref_count_dec(api);
    } else {
        ret = EINVAL;
    }

    return (ret);
}

static int
paio_wc_submit(struct paio_context *ctx_base, long nr, struct iocb *ios[]) {
    struct paio_wc_context *context =
        (struct paio_wc_context *)ctx_base;
    fthWaitEl_t *unlock;
    struct paio_wc_fd *fd;
    int i;
    int ret;
    int status;

    unlock = fthLock(&context->lock, 1 /* write lock */, NULL);

    /* Queue writes and pass non-writes through */
    for (i = ret = 0, status = 1; status == 1 && i < nr; ++i) {
        if (ios[i]->aio_lio_opcode != IO_CMD_PWRITE) {
            status = paio_submit(context->wrapped.context, 1, &ios[i]);
        } else {
            fd = paio_wc_get_fd_locked(context, ios[i]->aio_fildes);
            if (fd) {
                status = paio_wc_fd_add_write_locked(fd, ios[i]) == -1 ?
                    -1 : 1;
                paio_wc_fd_ref_count_dec_locked(fd);
            } else {
                errno = ENOMEM;
                status = -1;
            }
        }
        if (!ret && status == -1) {
            ret = -1;
        } else if (status >= 0) {
            ret += status;
        }
    }

    if (!context->config.delay_submit_until_getevents) {
        /* Attempt to maintain requested number of in-flight writes */
        paio_wc_do_submit_locked(context);
    }

    fthUnlock(unlock);


    return (ret);
}

static int
paio_wc_cancel(struct paio_context *ctx_base, struct iocb *iocb,
               struct io_event *evt) {
#ifdef notyet
    struct paio_wc_context *context =
        (struct paio_wc_context *)ctx_base;
#endif /* def notyet */
    int ret;

    /*
     * FIXME: drew 2010-03-17 This should be a straight pass-through for
     * read-ops.
     *
     * Write-ops can be cancelled when in
     * context->events.array: normal completion
     * fd->queued_extents: remove and cancel normalling
     * fd->in_flight_writes: cancel IO or block on normal completion
     */
    ret = -1;
    errno = ENOSYS;

    return (ret);
}

static long
paio_wc_getevents(struct paio_context *ctx_base, long min_nr, long nr,
                  struct io_event *events_arg, struct timespec *timeout) {
    struct paio_wc_context *context =
        (struct paio_wc_context *)ctx_base;
    struct io_event *events;
    long ret;
    long status;
    int do_copy_out;
    fthWaitEl_t *unlock;
    int i;

    events = plat_calloc(PAIO_GETEVENTS_COUNT, sizeof (*events));
    ret = !events ? -1 : 0;

    status = 1;
    do_copy_out = 1;
    /* XXX: drew 2010-03-19 This loop invariant is becoming contorted */
    while (ret >= 0 && (do_copy_out || status > 0) && ret < nr) {
        unlock = fthLock(&context->lock, 1 /* write lock */, NULL);
        if (do_copy_out) {
            plat_assert(ret >= 0);

            while (ret < nr && context->events.count > 0) {
                events_arg[ret] = context->events.array[0];
                --context->events.count;
                memmove(context->events.array, context->events.array + 1,
                        context->events.count *
                        sizeof (context->events.array[0]));
                ++ret;
            }
            do_copy_out = 0;
        }

        paio_wc_do_submit_locked(context);

        fthUnlock(unlock);

        if (status > 0 && ret < nr) {
            status = paio_getevents(context->wrapped.context,
                                    ret < min_nr ? 1 : 0,
                                    PAIO_GETEVENTS_COUNT, events, timeout);
        } else {
            status = 0;
        }

        if (!ret && status == -1) {
            ret = -1;
        } else if (status > 0) {
            unlock = fthLock(&context->lock, 1 /* write lock */, NULL);
            for (i = 0; i < status; ++i) {
                paio_wc_event_locked(context, &events[i]);
            }
            fthUnlock(unlock);
            do_copy_out = 1;
        }
    }

    if (events) {
        plat_free(events);
    }

    return (ret);
}

/**
 * @brief Get or allocate #paio_wc_fd structure
 *
 * A new #paio_wc_fd structure is allocated when none are found
 *
 * @param context <INOUT> context with context->lock held for writing
 * @param fd <IN> fd associated with the existing or new state
 * @return #paio_wc_fd with one additional reference count, NULL on failure.
 */
static struct paio_wc_fd *
paio_wc_get_fd_locked(struct paio_wc_context *context, int fd_arg) {
    struct paio_wc_fd *fd;

    for (fd = TAILQ_FIRST(&context->fd.list);
         fd && fd->fd != fd_arg;
         fd = TAILQ_NEXT(fd, fd_list_entry)) {
    }

    if (!fd && plat_calloc_struct(&fd)) {
        fd->context = context;
        fd->fd = fd_arg;
        RB_INIT(&fd->queued_extents.tree);
        TAILQ_INIT(&fd->in_flight_writes.list);
        TAILQ_INSERT_TAIL(&context->fd.list, fd,
                          fd_list_entry);
        ++context->fd.count;
    }

    if (fd) {
        (void) __sync_fetch_and_add(&fd->ref_count, 1);
    }

    return (fd);
}

/**
 * @brief Iterate over file descriptors and start writes
 *
 * @param context <IN> context with context->lock held for writing.
 */
static int
paio_wc_do_submit_locked(struct paio_wc_context *context) {
    int ret;
    int status;
    struct paio_wc_fd *fd;
    struct paio_wc_fd *next_fd;

    for (ret = status = 0, fd = TAILQ_FIRST(&context->fd.list);
         status >= 0 && fd; fd = next_fd) {
        next_fd = TAILQ_NEXT(fd, fd_list_entry);
        status = paio_wc_fd_submit_locked(fd);
        if (!ret && status == -1) {
            ret = -1;
        } else if (status >= 0) {
            ret += status;
        }
    }

    return (ret);
}

/**
 * @brief Process one event
 *
 * @param context <INOUT> context with context->lock held for writing
 * @param event_arg <IN> event returned from getevents
 */
static void
paio_wc_event_locked(struct paio_wc_context *context,
                     const struct io_event *event_arg) {
    if (event_arg->obj->aio_lio_opcode == IO_CMD_PWRITE) {
        paio_wc_write_event_locked(context, event_arg);
    /* Everything else is handled as a pass-through */
    } else {
        paio_wc_grow_events_locked(context, 1);
        plat_assert(context->events.max >= context->events.count + 1);
        context->events.array[context->events.count] = *event_arg;
        ++context->events.count;
    }
}

/**
 * @brief Process one write event
 *
 * @param context <INOUT> context with context->lock held for writing
 * @param event_arg <IN> event returned from getevents with
 * event_arg->obj->aio_lio_opcode == IO_CMD_PWRITE
 */
static void
paio_wc_write_event_locked(struct paio_wc_context *context,
                           const struct io_event *event_arg) {
    struct paio_wc_extent *extent;
    int i;

    plat_assert(event_arg->obj->aio_lio_opcode == IO_CMD_PWRITE);
    extent = PLAT_DEPROJECT(struct paio_wc_extent, submitted_iocb,
                            event_arg->obj);
    plat_assert(extent->magic.integer == PAIO_WC_EXTENT_MAGIC);
    plat_assert(extent->submitted);
    plat_assert(extent->submitted_iocb.u.c.offset == extent->offset);
    plat_assert(extent->submitted_iocb.u.c.nbytes == extent->len);

    paio_wc_grow_events_locked(context, extent->user_ios.count);
    plat_assert(context->events.max >=
                context->events.count + extent->user_ios.count);

    /* XXX: drew 2010-03-17 Will coverity see the exit attr on plat_fatal? */
    for (i = 0; i < extent->user_ios.count; ++i) {
        context->events.array[context->events.count] = *event_arg;
        context->events.array[context->events.count].obj =
            extent->user_ios.iocbs[i];
        if (extent->offset + event_arg->res <
            extent->user_ios.iocbs[i]->u.c.offset) {
            context->events.array[context->events.count].res = 0;
            plat_log_msg(21833, LOG_CAT,
                         PLAT_LOG_LEVEL_WARN, "paio_wc_context %p user iocb %p"
                         " short write %lu of %lu",
                         context, extent->user_ios.iocbs[i],
                         0UL, extent->user_ios.iocbs[i]->u.c.nbytes);
        } else if (extent->offset + event_arg->res >=
                   extent->user_ios.iocbs[i]->u.c.offset +
                   extent->user_ios.iocbs[i]->u.c.nbytes) {
            context->events.array[context->events.count].res =
                extent->user_ios.iocbs[i]->u.c.nbytes;
        } else {
            context->events.array[context->events.count].res =
                extent->offset + event_arg->res -
                extent->user_ios.iocbs[i]->u.c.offset;
            plat_log_msg(21833, LOG_CAT,
                         PLAT_LOG_LEVEL_WARN, "paio_wc_context %p user iocb %p"
                         " short write %lu of %lu",
                         context, extent->user_ios.iocbs[i],
                         context->events.array[context->events.count].res,
                         extent->user_ios.iocbs[i]->u.c.nbytes);
        }

        if (extent->user_ios.count != 1) {
            memcpy(extent->user_ios.iocbs[i]->u.c.buf,
                   (char *)extent->submitted_iocb.u.c.buf +
                   extent->user_ios.iocbs[i]->u.c.offset - extent->offset,
                   extent->user_ios.iocbs[i]->u.c.nbytes);
        }
        ++context->events.count;
    }

    paio_wc_extent_free_locked(extent);
}

/**
 * @brief Grow events array
 *
 * Fatal on failure because we've already received an event from the kernel
 * and need to queue it for processing
 *
 * @param context <INOUT> context with context->lock held for writing
 * @param increment <IN> number of new events to add
 */
static void
paio_wc_grow_events_locked(struct paio_wc_context *context, int increment) {
    int new_count;

    new_count = context->events.count + increment;

    if (new_count <= context->events.max) {
    } else if (!context->events.array) {
        context->events.array =
            plat_alloc(new_count * sizeof (context->events.array[0]));
        context->events.max = new_count;
    } else {
        context->events.array =
            plat_realloc(context->events.array,
                         new_count * sizeof (context->events.array[0]));

        /** Have no place to put the user's events so we're done */
        if (!context->events.array) {
            plat_fatal("no memory");
        }
        context->events.max = new_count;
    }
}

/**
 * @brief Submit all writes up to limits
 *
 * @param fd <IN> has fd->context.lock held for writing
 */
static int
paio_wc_fd_submit_locked(struct paio_wc_fd *fd) {
    int ret;
    int status;

    ++fd->ref_count;

    ret = 0;
    status = 1;
    while (status > 0 && fd->queued_extents.current &&
           (fd->in_flight_writes.count + 1 <= fd->context->config.io_limit) &&
           (fd->in_flight_writes.total_bytes +
            fd->queued_extents.current->len <= fd->context->config.byte_limit ||
            !fd->in_flight_writes.count)) {
        status = paio_wc_extent_submit_locked(fd->queued_extents.current);
        if (!ret && status == -1) {
            ret = -1;
        } else if (status >= 0) {
            ret += status;
        }
    }

    paio_wc_fd_ref_count_dec_locked(fd);

    return (ret);
}

/**
 * @brief Add write to fd
 *
 * @param fd <IN> has fd->context.lock held for writing
 * @return -1 on failure, otherwise on success
 */
static int
paio_wc_fd_add_write_locked(struct paio_wc_fd *fd, struct iocb *iocb) {
    struct paio_wc_extent *extent;
    struct paio_wc_extent *existing;
    int failed;

    plat_assert(iocb->aio_fildes == fd->fd);
    plat_assert(iocb->aio_lio_opcode == IO_CMD_PWRITE);

    /*
     * Insert new extent and then combine with all adjacent events
     *
     * XXX: drew 2010-03-17 Not the most efficient approach, although
     * the CPU cost should still be negligible
     */
    failed = !plat_calloc_struct(&extent);
    if (!failed) {
        extent->magic.integer = PAIO_WC_EXTENT_MAGIC;
        extent->fd = fd;
        extent->offset = iocb->u.c.offset;
        extent->len = iocb->u.c.nbytes;

        extent->user_ios.iocbs = (struct iocb **)plat_calloc(1, sizeof (iocb));
        failed = !extent->user_ios.iocbs;
        if (!failed) {
            extent->user_ios.count = 1;
            extent->user_ios.iocbs[0] = iocb;
        }
        /* XXX: drew 2010-03-17 May be simpler to have a check-free function */
        ++fd->ref_count;
    }

    if (!failed) {
        existing = PAIO_WC_EXTENT_TREE_RB_INSERT(&fd->queued_extents.tree,
                                                 extent);
        if (!existing) {
            if (!fd->queued_extents.current) {
                fd->queued_extents.current = extent;
            }
            ++fd->queued_extents.count;
            extent->where = PAIOWCE_WHERE_QUEUED;
        } else {
            failed = paio_wc_extent_merge_locked(existing, extent);
            if (!failed) {
                extent = existing;
            }
        }
    }

    if (!failed) {
        paio_wc_extent_merge_adjacent_locked(extent);
    }

    if (failed && extent) {
        paio_wc_extent_free_locked(extent);
    }

    return (failed ? -1 : 0);
}

/**
 * @brief Free #paio_wc_fd
 *
 * @param fd <IN> fd being freed. fd->context->lock must be held for writing
 */
static void
paio_wc_fd_free_locked(struct paio_wc_fd *fd) {
    struct paio_wc_extent *extent;
    struct paio_wc_extent *next_extent;

    /**
     * defer delete of fd.  Reference count is held exclusively by
     * extents, so deleting all will make count go to zero.
     */
    ++fd->ref_count;

    while (!RB_EMPTY(&fd->queued_extents.tree)) {
        plat_assert(fd->queued_extents.count > 0);
        paio_wc_extent_free_locked(RB_ROOT(&fd->queued_extents.tree));
    }
    plat_assert(!fd->queued_extents.count);

    /*
     * XXX: drew 2010-03-19 Coverity isn't smart enough to figure
     * out that context->fd.list.lh_first is changing each time when
     * we do this instead
     * while ((extent = TAILQ_FIRST(&fd->in_flight_writes.list)))
     */
    TAILQ_FOREACH_SAFE(extent, &fd->in_flight_writes.list,
                       in_flight_writes_list_entry, next_extent) {
        plat_assert(fd->in_flight_writes.count > 0);
        paio_wc_extent_free_locked(extent);
    }

    paio_wc_fd_ref_count_dec_locked(fd);
}

/**
 * @brief Decrement #paio_wc_fd reference count
 *
 * As a side effect, fd may be freed
 *
 * @param fd <IN> fd->context->lock must be held for writing
 */
static void
paio_wc_fd_ref_count_dec_locked(struct paio_wc_fd *fd) {
    int after;
    struct paio_wc_context *context;

    after = __sync_sub_and_fetch(&fd->ref_count, 1);
    plat_assert(after >= 0);
    if (!after) {
        plat_assert(RB_EMPTY(&fd->queued_extents.tree));
        plat_assert(!fd->queued_extents.count);
        plat_assert(TAILQ_EMPTY(&fd->in_flight_writes.list));
        plat_assert(!fd->in_flight_writes.count);

        context = fd->context;
        plat_assert(!TAILQ_EMPTY(&context->fd.list));
        plat_assert(context->fd.count > 0);

        TAILQ_REMOVE(&context->fd.list, fd, fd_list_entry);
        --context->fd.count;

        plat_free(fd);
    }
}

/**
 * @brief Submit extent for IO
 *
 * @return number of extents written on success, -1 on failure
 */
static int
paio_wc_extent_submit_locked(struct paio_wc_extent *extent) {
    struct iocb *iocb;
    char *buf;
    int i;
    int ret;

    plat_assert(!extent->submitted);

    if (extent->user_ios.count == 1) {
        buf = extent->user_ios.iocbs[0]->u.c.buf;
    } else {
        buf = plat_alloc(extent->len);
        if (buf) {
            /*
             * XXX: drew 2010-03-17 Should use writev if we ever get a
             * kernel new enough to support aio writes.
             */
            for (i = 0; i < extent->user_ios.count; ++i) {
                iocb = extent->user_ios.iocbs[i];
                plat_assert(iocb->aio_lio_opcode == IO_CMD_PWRITE);
                plat_assert(iocb->aio_fildes == extent->fd->fd);

                plat_assert(iocb->u.c.offset >= extent->offset);
                plat_assert(iocb->u.c.offset <= extent->offset + extent->len);
                plat_assert(iocb->u.c.offset + iocb->u.c.nbytes <=
                            extent->offset + extent->len);

                memcpy(buf + (iocb->u.c.offset - extent->offset),
                       iocb->u.c.buf, iocb->u.c.nbytes);
            }
        }
    }

    if (!buf) {
        ret = -1;
    } else {
        io_prep_pwrite(&extent->submitted_iocb, extent->fd->fd, buf,
                       extent->len, extent->offset);
        /* iocb is treated as a single entry array */
        iocb = &extent->submitted_iocb;
        ret = paio_submit(extent->fd->context->wrapped.context, 1 /* nr */,
                          &iocb);
        if (ret == 1) {
            ret = extent->user_ios.count;

            paio_wc_extent_remove_queued_locked(extent);
            plat_assert(extent->where == PAIOWCE_WHERE_NONE);

            extent->where = PAIOWCE_WHERE_IN_FLIGHT;
            extent->submitted = 1;

            TAILQ_INSERT_TAIL(&extent->fd->in_flight_writes.list,
                              extent, in_flight_writes_list_entry);
            ++extent->fd->in_flight_writes.count;
            extent->fd->in_flight_writes.total_bytes += extent->len;
        }

        /*
         * XXX: drew 2010-03-19 We want a ffdc log type which
         * expands to a string in the text output case and an
         * integer in log messages
         */
        plat_log_msg(21832, LOG_CAT,
                     ret > 0 ?  PLAT_LOG_LEVEL_TRACE : PLAT_LOG_LEVEL_WARN,
                     "paio_wc_context %p submit write fd %d buf %p"
                     " nbytes %ld offset %lld from %d user ios"
                     " inflight ios %d bytes %ld = %d %s",
                     extent->fd->context, iocb->aio_fildes, iocb->u.c.buf,
                     iocb->u.c.nbytes, iocb->u.c.offset, extent->user_ios.count,
                     extent->fd->in_flight_writes.count,
                     extent->fd->in_flight_writes.total_bytes, ret,
                     ret == -1 ? plat_strerror(errno) : "");
    }

    return (ret);
}

/**
 * @brief Merge as many nodes as possible on either side of extent_arg
 *
 * @param extent_arg <IN> extent to merge. extent_arg->fd->context->lock
 * must be held for writing.  extent_arg may become invalid after a return.
 *
 * @return 0 on success, non-zero on failure.  Note that even on failure
 * some nodes may be merged, although the tree state will remain
 * valid.
 */
static int
paio_wc_extent_merge_adjacent_locked(struct paio_wc_extent *extent_arg) {
    struct paio_wc_extent *extent;
    struct paio_wc_extent *prev;
    struct paio_wc_extent *next;
    int ret;
    int done;

    extent = extent_arg;

    /* Merge left */
    ret = done = 0;
    while (!ret && !done) {
        done = 1;
        prev = PAIO_WC_EXTENT_TREE_RB_PREV(extent);
        if (prev && prev->offset + prev->len >= extent->offset) {
            ret = paio_wc_extent_merge_locked(prev, extent);
            if (!ret) {
                extent = prev;
                done = 0;
            }
        }
    }

    /* Merge right */
    done = 0;
    while (!ret && !done) {
        done = 1;
        next = PAIO_WC_EXTENT_TREE_RB_NEXT(extent);
        if (next && extent->offset + extent->len >= next->offset) {
            ret = done = paio_wc_extent_merge_locked(extent, next);
        }
    }

    return (ret);
}

/**
 * @brief Merge extent and next nodes together
 *
 * @param extent <IN> extent->fd->context->lock must be held for writing,
 * and extent->where must be PAIOWCE_WHERE_QUEUED.  extent is retained.
 *
 * @param next <IN> next->fd must match extent->fd and next->where
 * must be PAIOWCE_WHERE_NONE or PAIOWCE_WHERE_QUEUED.  next must
 * be after extent and be adjacent or overlapping to it.  On success,
 * next is consumed.
 *
 * @return non-zero on failure, 0 on success
 */
static int
paio_wc_extent_merge_locked(struct paio_wc_extent *extent,
                            struct paio_wc_extent *next) {
    int ret;
    int new_count;
    struct iocb **new_iocbs;

    plat_assert(extent->fd == next->fd);
    plat_assert(extent->where == PAIOWCE_WHERE_QUEUED);
    plat_assert(next->where != PAIOWCE_WHERE_IN_FLIGHT);
    plat_assert(extent->offset <= next->offset);
    plat_assert(extent->offset + extent->len >= next->offset);

    new_count = extent->user_ios.count + next->user_ios.count;
    new_iocbs = plat_realloc(extent->user_ios.iocbs,
                             new_count * sizeof (extent->user_ios.iocbs[0]));
    if (!new_iocbs) {
        ret = -1;
        errno = ENOMEM;
    } else {
        ret = 0;
        extent->len = next->offset + next->len - extent->offset;
        extent->user_ios.iocbs = new_iocbs;
        memcpy(extent->user_ios.iocbs + extent->user_ios.count,
               next->user_ios.iocbs,
               next->user_ios.count * sizeof (extent->user_ios.iocbs[0]));
        /* XXX: drew 2010-03-17 We could sort for clarity in debugging */
        extent->user_ios.count = new_count;

        paio_wc_extent_free_locked(next);
    }

    return (ret);
}

/**
 * @brief Free extent, removing from parent structure
 *
 * @param extent <IN> extent->fd->context->lock is held for writing.
 * Freed on return.  extent->fd may be freed as a side effect.
 */
static void
paio_wc_extent_free_locked(struct paio_wc_extent *extent) {
    struct paio_wc_fd *fd;

    if (extent) {
        fd = extent->fd;
        switch (extent->where) {
        case PAIOWCE_WHERE_NONE:
            break;
        case PAIOWCE_WHERE_QUEUED:
            paio_wc_extent_remove_queued_locked(extent);
            break;
        case PAIOWCE_WHERE_IN_FLIGHT:
            paio_wc_extent_remove_in_flight_locked(extent);
            break;
        }

        /* XXX: drew 2010-03-17 Insure that event->iocb is not referenced */

        if (extent->user_ios.iocbs) {
            plat_free(extent->user_ios.iocbs);
        }

        if (extent->user_ios.count > 1 && extent->submitted &&
            extent->submitted_iocb.u.c.buf) {
            plat_free(extent->submitted_iocb.u.c.buf);
        }

        plat_free(extent);
        paio_wc_fd_ref_count_dec_locked(fd);
    }
}

/**
 * @brief Remove extent from extent->fd->queued_extents
 *
 * Side effects include potentailly advancing
 * extent->fd->queued_extents.current.
 *
 * @param extent <IN> extent with extent->fd->context->lock held for
 * writing and extent->fd->where == PAIOWCE_WHERE_QUEUED
 */
static void
paio_wc_extent_remove_queued_locked(struct paio_wc_extent *extent) {
    struct paio_wc_extent *next;

    plat_assert(extent->where == PAIOWCE_WHERE_QUEUED);
    plat_assert(!RB_EMPTY(&extent->fd->queued_extents.tree));
    plat_assert(extent->fd->queued_extents.count > 0);

    next = PAIO_WC_EXTENT_TREE_RB_NEXT(extent);
    PAIO_WC_EXTENT_TREE_RB_REMOVE(&extent->fd->queued_extents.tree, extent);
    --extent->fd->queued_extents.count;

    if (extent->fd->queued_extents.current != extent) {
    } else if (next) {
        extent->fd->queued_extents.current = next;
    } else {
        extent->fd->queued_extents.current =
            RB_MIN(PAIO_WC_EXTENT_TREE, &extent->fd->queued_extents.tree);
    }

    extent->where = PAIOWCE_WHERE_NONE;
}

/**
 * @brief Remove extent from extent->fd->in_flight_writes
 *
 * @param extent <IN> extent with extent->fd->context->lock held for
 * writing and extent->fd->where == PAIOWCE_WHERE_IN_FLIGHT
 */
static void
paio_wc_extent_remove_in_flight_locked(struct paio_wc_extent *extent) {
    plat_assert(extent->where == PAIOWCE_WHERE_IN_FLIGHT);
    plat_assert(extent->fd->in_flight_writes.count > 0);
    plat_assert(!TAILQ_EMPTY(&extent->fd->in_flight_writes.list));
    plat_assert(extent->fd->in_flight_writes.total_bytes >= extent->len);

    TAILQ_REMOVE(&extent->fd->in_flight_writes.list, extent,
                 in_flight_writes_list_entry);
    --extent->fd->in_flight_writes.count;
    extent->fd->in_flight_writes.total_bytes -= extent->len;

    extent->where = PAIOWCE_WHERE_NONE;
}

static int
paio_wc_extent_cmp(const struct paio_wc_extent *lhs,
                   const struct paio_wc_extent *rhs) {
    int ret;

    if (lhs->offset < rhs->offset) {
        ret = -1;
    } else if (lhs->offset > rhs->offset) {
        ret = 1;
    } else {
        plat_assert(lhs->offset == rhs->offset);
        ret = 0;
    }

    return (ret);
}

RB_GENERATE_STATIC(PAIO_WC_EXTENT_TREE, paio_wc_extent,
                   queued_extents_tree_entry, paio_wc_extent_cmp)

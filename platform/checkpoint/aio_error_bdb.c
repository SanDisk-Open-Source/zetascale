/*
 * File:   $URL: svn://svn.schoonerinfotech.net/schooner-trunk/trunk/sdf/platform/checkpoint/aio_error_bdb.c $
 * Author: drew
 *
 * Created on May 19, 2010
 *
 * (c) Copyright 2010, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: aio_error_bdb.c 14174 2010-06-17 01:35:38Z drew $
 */

/**
 * Error injection paio_api implementation using bdb for persistence
 *
 * This works by maintaining a set of persistent bdb databases one
 * per file in which the scheduled errors are stored.  Persistence
 * is specifically needed to simulate errors where reads fail
 * until over-written as on media with uncorrectable bit rot.
 */

#include <libaio.h>
#include <sys/queue.h>

#include "platform/aio_api.h"
#include "platform/aio_api_internal.h"
#include "platform/aio_error_control.h"
#include "platform/aio_error_control_internal.h"
#include "platform/logging.h"
#include "platform/stat.h"
#include "platform/stdio.h"
#include "platform/unistd.h"

#include "fth/fth.h"
#include "fth/fthLock.h"

#include "platform/aio_error_bdb.h"

#define LOG_CAT PLAT_LOG_CAT_PLATFORM_AIO_ERROR_BDB

/** @brief API */
struct paio_error_bdb_api {
    /** @brief base aio interface fields */
    struct paio_api base;

    /** @brief error control API implementation */
    struct paio_error_control error_control;

    /** @brief Configuration */
    struct paio_error_bdb_config config;

    /** @brief Wrapped API */
    struct paio_api *wrapped_api;

    /* Reference count for robustness */
    int ref_count;

    fthLock_t lock;


    TAILQ_HEAD(, paio_error_bdb_context) context_list;

#ifndef notyet
    TAILQ_HEAD(, paio_error_bdb_error) error_list;
#endif

    TAILQ_HEAD(, paio_error_bdb_fd) fd_list;
};

struct paio_error_bdb_error;

/** @brief Context */
struct paio_error_bdb_context {
    struct paio_context base;

    struct {
        /** @brief Wrapped context */
        struct paio_context *context;

        unsigned initialized : 1;
    } wrapped;

    TAILQ_ENTRY(paio_error_bdb_context) context_list_entry;
};

/** @param Single scheduled error */
struct paio_error_bdb_error {
    /** @brief Parent api, having global list of errors, etc. */
    struct paio_error_bdb_api *api;

    /** @brief type of error */
    enum paio_ec_type error_type;

    char filename[PATH_MAX];

    /** @brief error region start offset */
    off_t start;

    /** @param error region length */
    off_t len;

#ifndef notyet
    /* XXX: drew 2010-05-20 Goes away with bdb backing */
    TAILQ_ENTRY(paio_error_bdb_error) error_list_entry;
#endif
};

struct paio_error_bdb_fd {
    /** @brief Parent api, having global list of file descriptors, etc. */
    struct paio_error_bdb_api *api;

    /** @brief File descriptor */
    int fd;

    /** @brief Last observed device */
    dev_t device;

    /** @brief Last observed inode */
    ino_t inode;

    /** @brief Associated filename */
    char filename[PATH_MAX];

    TAILQ_ENTRY(paio_error_bdb_fd) fd_list_entry;
};

static void paio_error_bdb_api_destroy(struct paio_api *api);
static void paio_error_bdb_api_ref_count_dec(struct paio_error_bdb_api *api);
static int paio_error_bdb_api_get_fd_locked(struct paio_error_bdb_api *api,
                                            int fd,
                                            struct paio_error_bdb_fd **out);


static int paio_error_bdb_setup(struct paio_api *api_base, int maxevents,
                                struct paio_context **ctxp);
static int paio_error_bdb_destroy(struct paio_context *ctx_base);
static int paio_error_bdb_submit(struct paio_context *ctx_base, long nr,
                                 struct iocb *ios[]);
static int paio_error_bdb_cancel(struct paio_context *ctx_base, struct iocb *iocb,
                                 struct io_event *evt);
static long paio_error_bdb_getevents(struct paio_context *ctx_base, long min_nr,
                                     long nr, struct io_event *events,
                                     struct timespec *timeout);

static int paio_error_bdb_set_error(struct paio_error_control *ec_base,
                                    enum paio_ec_type error_type,
                                    const char *filename, off_t start,
                                    off_t len);

static void paio_error_bdb_process_event(struct paio_error_bdb_context *context,
                                         struct io_event *event);
static int paio_error_bdb_error_process_locked(struct paio_error_bdb_error *error,
                                               struct io_event *event);
static void paio_error_bdb_error_apply_locked(struct paio_error_bdb_error *error,
                                              struct io_event *event);
static void paio_error_bdb_error_free_locked(struct paio_error_bdb_error *error);

struct paio_api *
paio_error_bdb_create(struct paio_error_control **error_control,
                      const struct paio_error_bdb_config *config,
                      struct paio_api *wrapped_api) {
    struct paio_error_bdb_api *ret;

    if (plat_calloc_struct(&ret)) {
        ret->base.api_destroy = &paio_error_bdb_api_destroy;

        ret->base.io_setup = &paio_error_bdb_setup;
        ret->base.io_destroy = &paio_error_bdb_destroy;
        ret->base.io_submit = &paio_error_bdb_submit;
        ret->base.io_cancel = &paio_error_bdb_cancel;
        ret->base.io_getevents = &paio_error_bdb_getevents;

        ret->error_control.set_error = &paio_error_bdb_set_error;

        paio_error_bdb_config_dup(&ret->config, config);

        ret->wrapped_api = wrapped_api;

        ret->ref_count = 1;

        fthLockInit(&ret->lock);

        TAILQ_INIT(&ret->context_list);
#ifndef notyet
        /* XXX: drew 2010-05-20 Goes away with bdb backing */
        TAILQ_INIT(&ret->error_list);
#endif
        TAILQ_INIT(&ret->fd_list);

        *error_control = &ret->error_control;
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
paio_error_bdb_config_init(struct paio_error_bdb_config *config) {
    memset(config, 0, sizeof(config));
    paio_error_control_config_init(&config->error_control_config);
}

void
paio_error_bdb_config_destroy(struct paio_error_bdb_config *config) {
    if (config->directory) {
        plat_free(config->directory);
    }
    paio_error_control_config_destroy(&config->error_control_config);
}

void
paio_error_bdb_config_dup(struct paio_error_bdb_config *dest,
                          const struct paio_error_bdb_config *src) {
    if (src->directory) {
        dest->directory = plat_strdup(src->directory);
        plat_assert(dest->directory);
    }
    paio_error_control_config_dup(&dest->error_control_config,
                                  &src->error_control_config);
}

static void
paio_error_bdb_api_destroy(struct paio_api *api_base) {
    struct paio_error_bdb_api *api = (struct paio_error_bdb_api *)api_base;

    if (api) {
        paio_error_bdb_api_ref_count_dec(api);
    }
}

static void
paio_error_bdb_api_ref_count_dec(struct paio_error_bdb_api *api) {
    int after;
#ifdef notyet
    /* XXX: drew 2010-05-20 Goes away with bdb backing */
    struct paio_error_bdb_error *error;
    struct paio_error_bdb_error *next_error;
#endif /* def notyet */

    after = __sync_sub_and_fetch(&api->ref_count, 1);
    plat_assert(after >= 0);
    if (!after) {
        paio_error_bdb_config_destroy(&api->config);
        plat_assert(TAILQ_EMPTY(&api->context_list));

#ifdef notyet
        /* XXX: drew 2010-05-20 Goes away with bdb backing */
        TAILQ_FOREACH_SAFE(error, &api->error_list, error_list_entry,
                           next_error) {
            paio_error_bdb_error_free_locked(error);
        }
#endif /* def notyet */

        plat_free(api);
    }
}

static int 
paio_error_bdb_api_get_fd_locked(struct paio_error_bdb_api *api, int fd,
                                 struct paio_error_bdb_fd **out) {
    int ret;
    struct stat stat_buf;
    struct paio_error_bdb_fd *found;
    int len;
    int add;
    char *linkname;

    ret = plat_fstat(fd, &stat_buf) == -1 ? -errno : 0;

    for (found = TAILQ_FIRST(&api->fd_list); found->fd != fd;
         found = TAILQ_NEXT(found, fd_list_entry)) {
    }

    if (!ret && found && (found->device != stat_buf.st_dev ||
                          found->inode != stat_buf.st_ino)) {
         TAILQ_REMOVE(&api->fd_list, found, fd_list_entry);
         plat_free(found);
         found = NULL;
    }

    add = (!ret && !found);
    linkname = NULL;

    if (!add) {
    } else if (!plat_calloc_struct(&found)) {
        ret = -ENOMEM;
    } else if (plat_asprintf(&linkname, "/proc/self/fd/%d", fd) == -1) {
        ret = -ENOMEM;
    } else if (-1 == (len = plat_readlink(linkname, found->filename,
                                          sizeof(found->filename)))) {
               ret = -errno;
    } else if (len >= sizeof (found->filename)) {
        ret = -ENAMETOOLONG;
    } else {
        found->api = api;
        found->fd = fd;
        found->device = stat_buf.st_dev;
        found->inode = stat_buf.st_ino;
        found->filename[len] = 0;
        TAILQ_INSERT_TAIL(&api->fd_list, found, fd_list_entry);
    }

    if (ret && add) {
        plat_free(found);
    }
    if (linkname) {
        plat_free(linkname);
    }

    plat_assert_imply(!ret, found);
    if (!ret) {
        *out = found;
    }

    return (ret);
}

static int
paio_error_bdb_setup(struct paio_api *api_base, int maxevents,
                     struct paio_context **ctxp) {
    struct paio_error_bdb_api *api = (struct paio_error_bdb_api *)api_base;
    struct paio_error_bdb_context *context;
    int ret;
    fthWaitEl_t *unlock;

    if (!plat_calloc_struct(&context)) {
        errno = ENOMEM;
        ret = -1;
    } else {
        __sync_fetch_and_add(&api->ref_count, 1);

        context->base.api = api_base;

        ret = paio_setup(api->wrapped_api, maxevents,
                         &context->wrapped.context);
        if (!ret) {
            context->wrapped.initialized = 1;
        }

        unlock = fthLock(&api->lock, 1 /* write lock */, NULL);
        TAILQ_INSERT_TAIL(&api->context_list, context, context_list_entry);
        fthUnlock(unlock);
    }

    if (ret && context) {
        paio_error_bdb_destroy(&context->base);
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
paio_error_bdb_destroy(struct paio_context *ctx_base) {
    struct paio_error_bdb_context *context =
        PLAT_DEPROJECT(struct paio_error_bdb_context, base, ctx_base);
    struct paio_error_bdb_api *api;
    int ret;
    fthWaitEl_t *unlock;

    if (context) {
        if (context->wrapped.initialized) {
            ret = paio_destroy(context->wrapped.context);
            context->wrapped.initialized = 0;
        } else {
            ret = 0;
        }
        api = (struct paio_error_bdb_api *)context->base.api;

        /* Ref count is zero so no lock is needed */
        unlock = fthLock(&api->lock, 1 /* write lock */, NULL);
        TAILQ_REMOVE(&api->context_list, context, context_list_entry);
        fthUnlock(unlock);


        plat_free(context);
        paio_error_bdb_api_ref_count_dec(api);
    } else {
        ret = EINVAL;
    }

    return (ret);
}

static int
paio_error_bdb_submit(struct paio_context *ctx_base, long nr, struct iocb *ios[]) {
    struct paio_error_bdb_context *context =
        PLAT_DEPROJECT(struct paio_error_bdb_context, base, ctx_base);
    int ret;

    /*
     * We allow IOs to run to completion to get the timing variations from
     * the wrapped IOs and then inject the faults when they complete.
     *
     * XXX: drew 2010-05-20 We need to change this to simulate torn
     * writes but will punt until the first release.
     */
    ret = paio_submit(context->wrapped.context, nr, ios);

    return (ret);
}

static int
paio_error_bdb_cancel(struct paio_context *ctx_base, struct iocb *iocb,
                      struct io_event *evt) {
    struct paio_error_bdb_context *context =
        PLAT_DEPROJECT(struct paio_error_bdb_context, base, ctx_base);
    int ret;

    ret = paio_cancel(context->wrapped.context, iocb, evt);

    return (ret);
}

static long
paio_error_bdb_getevents(struct paio_context *ctx_base, long min_nr, long nr,
                         struct io_event *events, struct timespec *timeout) {
    struct paio_error_bdb_context *context =
        PLAT_DEPROJECT(struct paio_error_bdb_context, base, ctx_base);
    long ret;
    long i;

    ret = paio_getevents(context->wrapped.context, min_nr, nr, events, timeout);
    for (i = 0; i < ret; ++i) {
        paio_error_bdb_process_event(context, events + i);
    }

    return (ret);
}

static int
paio_error_bdb_set_error(struct paio_error_control *ec_base,
                         enum paio_ec_type error_type, const char *filename,
                         off_t start, off_t len) {

    struct paio_error_bdb_api *api =
        PLAT_DEPROJECT(struct paio_error_bdb_api, error_control, ec_base);
    struct paio_error_bdb_error *error;
    int ret;
    fthWaitEl_t *unlock;
    int namelen;

    /*
     * FIXME: drew 2010 This is missing functionality from the final version
     *
     * 1.  Extents are not correctly merged
     * 2.  Filename specific errors are not implemented
     */

    namelen = strlen(filename);
    if (len >= sizeof (error->filename)) {
        ret = -ENAMETOOLONG;
    } else if (!plat_calloc_struct(&error)) {
        ret = -ENOMEM;
    } else {
        error->api = api;
        error->error_type = error_type;
        memcpy(error->filename, filename, namelen + 1);
        error->start = start;
        error->len = len;

        unlock = fthLock(&api->lock, 1 /* write lock */, NULL);
        TAILQ_INSERT_TAIL(&api->error_list, error, error_list_entry);
        fthUnlock(unlock);

        ret = 0;
    }

    return (ret);
}

/**
 * @brief Process a single completed event
 * @param context <IN> context
 * @param event <INOUT> event for completed wrapped IO
 */
static void
paio_error_bdb_process_event(struct paio_error_bdb_context *context,
                             struct io_event *event) {
    struct paio_error_bdb_api *api = 
        PLAT_DEPROJECT(struct paio_error_bdb_api, base, context->base.api);
    struct paio_error_bdb_error *error;
    fthWaitEl_t *unlock;

    unlock = fthLock(&api->lock, 1 /* write lock */, NULL);
    for (error = TAILQ_FIRST(&api->error_list);
         error && !paio_error_bdb_error_process_locked(error, event);
         error = TAILQ_NEXT(error, error_list_entry)) {
    }

    fthUnlock(unlock);
}

/**
 * @brief Process error
 *
 * @param error <IN> error attached to parent context structure with
 * error->context->lock held
 * @param event <IN> event for completed wrapped IO
 * @return non-zero if event was applied
 */
static int
paio_error_bdb_error_process_locked(struct paio_error_bdb_error *error,
                                    struct io_event *event) {
    int match;
    int ret;
    int status;
    struct paio_error_bdb_fd *fd;

    fd = NULL;
    status = paio_error_bdb_api_get_fd_locked(error->api,
                                              event->obj->aio_fildes, &fd);
    plat_assert_imply(!status, fd != NULL);
    if (status < 0 || strcmp(error->filename, fd->filename)) {
        match = 0;
    } else {
        switch (error->error_type) {
        case PAIO_ECT_READ_ONCE:
        case PAIO_ECT_READ_STICKY:
            match = event->obj->aio_lio_opcode == IO_CMD_PREAD;
            break;
        case PAIO_ECT_READ_WRITE_ONCE:
        case PAIO_ECT_READ_WRITE_PERMANENT:
            match = event->obj->aio_lio_opcode == IO_CMD_PREAD ||
                event->obj->aio_lio_opcode == IO_CMD_PWRITE;
            break;
        }
    }

    ret = (match &&
           /* Successful IO with full transfer */
           !event->res2 && event->res == event->obj->u.c.nbytes &&
           /* Range intersect */
           error->start < event->obj->u.c.offset +
           event->obj->u.c.nbytes &&
           error->start + error->len >= event->obj->u.c.offset);

    if (ret) {
        paio_error_bdb_error_apply_locked(error, event);
    }

    return (ret);
}



/**
 * @brief Unconditionally apply error to event
 *
 * @param error <IN> error attached to parent context structure with
 * error->context->lock held
 * @param event <IN> event for completed wrapped IO
 */
static void
paio_error_bdb_error_apply_locked(struct paio_error_bdb_error *error,
                                  struct io_event *event) {
    int remove_error;

    plat_assert(event->obj->aio_lio_opcode == IO_CMD_PREAD ||
                event->obj->aio_lio_opcode == IO_CMD_PWRITE);


    /* FIXME: drew 2010-05-20 Need to split extents */

    /* FIXME: drew 2010-05-20 Is this correct ? */
    event->res2 = EIO;
    if (event->obj->aio_lio_opcode == IO_CMD_PREAD) {
        memset(event->obj->u.c.buf, 0, event->obj->u.c.nbytes);
    }
    event->res = 0;

    switch (error->error_type) {
    case PAIO_ECT_READ_ONCE:
    case PAIO_ECT_READ_WRITE_ONCE:
        remove_error = 1;
        break;

    case PAIO_ECT_READ_STICKY:
        remove_error = event->obj->aio_lio_opcode == IO_CMD_PWRITE;
        break;

    case PAIO_ECT_READ_WRITE_PERMANENT:
        remove_error = 0;
    }

    if (remove_error) {
        paio_error_bdb_error_free_locked(error);
    }
}

/**
 * @brief Free #paio_error_bdb_error
 *
 * @param error <IN> error with error->context->lock held
 */
static void
paio_error_bdb_error_free_locked(struct paio_error_bdb_error *error) {
    plat_free(error->filename);
#ifndef notyet
    /* XXX: drew 2010-05-20 Goes away with bdb backing */
    TAILQ_REMOVE(&error->api->error_list, error, error_list_entry);
#endif
    plat_free(error);
}



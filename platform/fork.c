/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

/*
 * File:   sdf/platform/fork_execve.c
 * Author: drew
 *
 * Created on February 26, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: fork.c 11222 2010-01-13 05:06:07Z drew $
 */

/*
 * Provide a vaguely reasonable child process creation model that handles the
 * common stdin/stdout/stderr redirection cases, reports errors
 * in the parent context, is easy to implement in a simulated environment, and
 * accomodates software (including third party library) writers who aren't
 * paranoid about setting their close-on-exec bit.
 */

#define PLATFORM_INTERNAL 1
/* Logging category */
#define LOG_CAT PLAT_LOG_CAT_PLATFORM_FORK

#include <sys/queue.h>

#include "platform/assert.h"
#include "platform/errno.h"
#include "platform/fcntl.h"
#include "platform/logging.h"
#include "platform.h"
#include "platform/select.h"
#include "platform/stdlib.h"
#include "platform/string.h"
#include "platform/unistd.h"
#include "platform/wait.h"

struct plat_at_fork_entry {
    void (*fn)(void *extra, pid_t pid);
    void *extra;
    TAILQ_ENTRY(plat_at_fork_entry) tailq_entry;
};

struct plat_at_fork {
    int initialized;
    TAILQ_HEAD(, plat_at_fork_entry) entries;
};

static struct plat_at_fork plat_at_fork_state;

/*
 * Use a pipe for the child to write failure status back to the parent
 * process.
 */
enum {
    PIPE_PARENT = 0,
    PIPE_CHILD = 1
};

/*
 * When fds is null, pass defaults in.
 */
static struct plat_fork_fd default_fds[] = {
    { PLAT_FORK_DUP, 0, 0 },
    { PLAT_FORK_DUP, 1, 1 },
    { PLAT_FORK_DUP, 2, 2 }
};
static int default_nfds = sizeof (default_fds) / sizeof(default_fds[0]);

/*
 * Do child part of fork, closing parent end of pipe and writing errno
 * to the child end when exec has failed.
 */
static void do_child(const char *filename, char * const argv[],
                     char * const envp[], int nfds,
                     const struct plat_fork_fd *fds,
                     int pipe_ends[2]) __attribute__((noreturn));

/*
 * Do parent part of fork, watching for error (in which case we wait
 * on the child) or close on exec.
 */
static pid_t do_parent(int nfds, const struct plat_fork_fd *fds,
                       int pipe_ends[2], pid_t pid);

static void plat_at_fork_do(pid_t pid);

pid_t
plat_fork_execve(const char *filename, char *const argv[],
                 char * const envp[], int nfds,
                 const struct plat_fork_fd *fds) {
    int error = 0;
    int pipe_ends[2] = { -1, -1 };
    char *args;
    pid_t pid;

    if (sys_pipe(pipe_ends) == -1) {
        pid = -1;
        error = errno;
    } else {
        /*
         * FIXME: If any file descriptors being used internally
         * (pipe_ends) we need to dup() them elsewhere.  PLAT_FORK_DUP
         * parent_fd collisions could be handled at the same time
         * or deferred until later.
         */
        pid = sys_fork();
        if (!pid) {
            do_child(filename, argv, envp, fds ? nfds : default_nfds,
                     fds ? fds : default_fds, pipe_ends);
        } else if (pid == -1) {
            error = errno;
        }
    }
    /* Child should have called execve() or _exit() before getting here */
    plat_assert(pid);
    pid = do_parent(fds ?  nfds : default_nfds, fds ? fds : default_fds,
                    pipe_ends, pid);

    /*
     * Restore original error from pipe or fork if descriptor closing
     * clobbered it.
     */
    if (pid == -1 && error) {
        plat_errno = error;
    }

    if (pid == -1) {
        args = plat_strarray_alloc(-1, (const char * const *)argv, " ");
        plat_log_msg(20941, LOG_CAT, PLAT_LOG_LEVEL_ERROR,
                     "plat_fork_execve(filename = %s, args = %s) failed: %d",
                     filename, args, plat_errno);
        plat_free(args);
    } else if (plat_log_enabled(LOG_CAT, PLAT_LOG_LEVEL_TRACE)) {
        args = plat_strarray_alloc(-1, (const char * const *)argv, " ");
        plat_log_msg(20942, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                     "plat_fork_execve(filename = %s, args = %s)",
                     filename, args);
        plat_free(args);
    }

    return (pid);
}

static void
do_child(const char *filename, char * const argv[], char * const envp[],
         int nfds, const struct plat_fork_fd *fds, int pipe_ends[2]) {
    int limit;
    int status;
    int tmp;
    int i;
    int max = -1;
    fd_set preserve;

    FD_ZERO(&preserve);
    FD_SET(pipe_ends[PIPE_CHILD], &preserve);
    FD_SET(pipe_ends[PIPE_PARENT], &preserve);

    status = fcntl(pipe_ends[PIPE_CHILD], F_SETFD, FD_CLOEXEC);

    /*
     * FIXME: Add a pass to duplicate file descriptors which
     * are being replaced in the child process but used elsewhere.
     */
    for (i = 0; !status && i < nfds; ++i) {
        if (fds[i].child_fd > max) {
            max = fds[i].child_fd;
        }
        /* FIXME: There's a limit on the length of the fd set */
        FD_SET(fds[i].child_fd, &preserve);
        if (fds[i].parent_fd != fds[i].child_fd) {
            status = sys_dup2(fds[i].parent_fd, fds[i].child_fd);
            plat_assert(!status || errno != EBADF);
        }
    }

    limit = getdtablesize();
    for (i = 0; !status && i < limit; ++i) {
        /*
         * The check against max is needed because the dtablesize
         * may be substantially bigger than an fdset.
         */
        if (i > max || !FD_ISSET(i, &preserve)) {
            do {
                tmp = sys_close(i);
            } while (tmp == -1 && errno == EINTR);
            if (tmp == -1 && errno != EBADF) {
                status = -1;
            }
        }
    }

    if (!status) {
        sys_execve(filename, argv, envp);
    }

    /*
     * FIXME: Write a string_t with something for logging if it wasn't
     * an exec failure.
     */
    if (sys_write(pipe_ends[PIPE_CHILD], &errno, sizeof(errno))) {}
    sys__exit(1);
}

static pid_t
do_parent(int nfds,  const struct plat_fork_fd *fds, int pipe_ends[2],
          pid_t pid) {
    int error = 0;
    int i;
    ssize_t got;
    pid_t wait_pid;
    int status;

    for (i = 0; i < nfds; ++i) {
        if (fds[i].type == PLAT_FORK_INHERIT &&
            sys_close(fds[i].parent_fd) == -1) {
            plat_assert(errno != EBADF);
        }
    }

    if (pipe_ends[PIPE_CHILD] != -1) {
        sys_close(pipe_ends[PIPE_CHILD]);
    }

    if (pid != -1) {
        do {
            got = sys_read(pipe_ends[PIPE_PARENT], &error, sizeof(error));
        } while (got == -1 && errno == EINTR);
        plat_assert(got == 0 || got == sizeof(error));

        if (got == sizeof(error)) {
            plat_assert(error);

            do {
                wait_pid = waitpid(pid, &status, 0 /* flags */);
            } while (wait_pid == -1 && errno == EINTR);

            /*
             * Caller could be catching SIGCHLD and waiting in another thread
             * or have SIGCHLD set to SIG_IGN
             */
            plat_assert(wait_pid == pid || (wait_pid == -1 && errno == ECHILD));
        }
    }

    if (pipe_ends[PIPE_PARENT] != -1) {
        sys_close(pipe_ends[PIPE_PARENT]);
    }

    if (error) {
        plat_errno = error;
    }

    return (error ? -1 : pid);
}

pid_t
plat_fork() {
    pid_t pid;

    pid = fork();
    if (!pid) {
        plat_at_fork_do(pid);
    }

    return (pid);
}

struct plat_at_fork_entry *
plat_at_fork_add(void (*fn)(void *extra, pid_t pid), void *extra) {
    struct plat_at_fork_entry *ret;

    if (!plat_at_fork_state.initialized) {
        TAILQ_INIT(&plat_at_fork_state.entries);
        plat_at_fork_state.initialized = 1;
    }

    ret = sys_malloc(sizeof (*ret));
    if (ret) {
        ret->fn = fn;
        ret->extra = extra;
        TAILQ_INSERT_TAIL(&plat_at_fork_state.entries, ret, tailq_entry);
    }

    return (ret);
}

void
plat_at_fork_remove(struct plat_at_fork_entry *entry) {
    if (entry) {
        TAILQ_REMOVE(&plat_at_fork_state.entries, entry, tailq_entry);
        sys_free(entry);
    }
}

static void
plat_at_fork_do(pid_t pid) {
    struct plat_at_fork_entry *entry;
    struct plat_at_fork_entry *next;

    if (plat_at_fork_state.initialized) {
        TAILQ_FOREACH_SAFE(entry, &plat_at_fork_state.entries, tailq_entry,
                           next) {
            (*entry->fn)(entry->extra, pid);
        }
    }
}

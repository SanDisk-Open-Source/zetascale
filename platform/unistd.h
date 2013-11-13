#ifndef PLATFORM_UNISTD_H
#define PLATFORM_UNISTD_H 1

/*
 * File:   $HeadURL: svn://svn.schoonerinfotech.net/schooner-trunk/trunk/sdf/platform/unistd.h $
 * Author: drew
 *
 * Created on January 25, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: unistd.h 11222 2010-01-13 05:06:07Z drew $
 */

/*
 * Thin wrappers for unix functions to accomodate interception for
 * 1) Blocking behavior in user-scheduled threads
 * 2) The simulated cluster environment
 *
 * readlink can return int or ssize_t so it's not handled normally
 */

#include <sys/cdefs.h>

#include <unistd.h>

#include "platform/types.h"
#include "platform/wrap.h"

#define PLAT_UNISTD_WRAP_FD_ITEMS()                                            \
    item(int, close, (int fd), (fd), /**/, /**/)                               \
    item(int, dup, (int fd), (fd), __THROW, __wur)                             \
    item(int, dup2, (int oldfd, int newfd), (oldfd, newfd), __THROW, /**/)     \
    item(int, pipe, (int filedes[2]), (filedes), __THROW, __wur)               \
    item(ssize_t, read, (int fd, void *buf, size_t nbytes), (fd, buf, nbytes), \
        /**/, __wur)                                                           \
    item(ssize_t, write, (int fd, __const void *buf, size_t nbytes),           \
        (fd, buf, nbytes), /**/, __wur)

#define PLAT_UNISTD_WRAP_FILE_ITEMS()                                          \
    item(int, ftruncate, (int fd, off_t length), (fd, length), __THROW, __wur) \
    item(int, truncate, (__const char *path, off_t length), (path, length),    \
        __THROW, __nonnull((1)) __wur)                                         \
    item(int, unlink, (__const char *pathname), (pathname),                    \
        __THROW, __nonnull((1)))

#ifdef notyet
    item(int, readlink, (__const char * __restrict path,                       \
        char * __restrict buf, size_t bufsize), (path, buf, bufsize),          \
        __THROW, __nonnull((1, 2)) __wur)
#endif

#define PLAT_UNISTD_WRAP_PROCESS_ITEMS()                                       \
    item(pid_t, getpid, (void), (), __THROW, /**/)                             \
    item(pid_t, getppid, (void), (), __THROW, /**/)

#define PLAT_UNISTD_WRAP_ITEMS()                                               \
    PLAT_UNISTD_WRAP_FD_ITEMS()                                                \
    PLAT_UNISTD_WRAP_FILE_ITEMS()                                              \
    PLAT_UNISTD_WRAP_PROCESS_ITEMS()

/* We don't wrap these because people should be using plat_fork_execve */
#define PLAT_UNISTD_WRAP_NO_PLAT_ITEMS()                                       \
    item(int, execv,  (__const char *path, char *__const argv[]),              \
        (path, argv), __THROW, __nonnull ((1)))                                \
    item(int, execve,  (__const char *path, char *__const argv[],              \
           char *__const envp[]), (path, argv, envp), __THROW,                 \
           __nonnull ((1)))                                                    \
    item(int, execvp,  (__const char *path, char *__const argv[]),             \
        (path, argv), __THROW, __nonnull ((1)))                                \

#ifdef PLATFORM_INTERNAL
#define sys__exit _exit
#define sys_readlink readlink
#define sys_fork fork
#endif

#if 0 // Sorry, Drew, I need these - Rico
/* We don't have wrapping code for varatic functions */
PLAT_WRAP_CPP_POISON(execl execlp execv execvp)
/* We don't have wrapping code for noreturn() functions */
PLAT_WRAP_CPP_POISON(_exit)
#endif
/* And readlink can return int or ssize_t */
PLAT_WRAP_CPP_POISON(readline)

__BEGIN_DECLS

#define __leaf__

#define item(ret, sym, declare, call, cppthrow, attributes) \
    PLAT_WRAP(ret, sym, declare, call, cppthrow, attributes)
PLAT_UNISTD_WRAP_ITEMS()
#undef item

#define item(ret, sym, declare, call, cppthrow, attributes) \
    PLAT_WRAP_NO_PLAT(ret, sym, declare, call, cppthrow, attributes)
PLAT_UNISTD_WRAP_NO_PLAT_ITEMS()
#undef item

/* Manual wrapper because of ((noreturn)) */
void plat__exit(int status) __attribute__((noreturn));

/* Manual wrapper because system version can return ssize_t or int */
ssize_t plat_readlink(const char *path, void *buf, size_t bufsize);

/* Manual wrapper because we're adding plat_at_fork */
pid_t plat_fork(void) __THROW;

__END_DECLS

#endif /* ndef PLATFORM_UNISTD_H */

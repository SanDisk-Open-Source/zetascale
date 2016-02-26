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

#ifndef PLATFORM_PLATFORM_H
#define PLATFORM_PLATFORM_H 1

/*
 * File:   sdf/platform.h
 * Author: drew
 *
 * Created on February 27, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: platform.h 11222 2010-01-13 05:06:07Z drew $
 */

/**
 * Miscellaneous platform functions which aren't exact API replacements
 * (ex. plat_fork_execve which isn't just execve) and don't belong to
 * a broader platform category (like logging).
 */

#include "platform/closure.h"
#include "platform/types.h"

enum plat_fork_fd_type {
    PLAT_FORK_DUP,
    PLAT_FORK_INHERIT
};

struct plat_fork_fd {
    enum plat_fork_fd_type type;
    int parent_fd;
    int child_fd;
};

/**
 * @brief Call void (*fn)(void *extra, pid_t pid) with 0 and pid in forked child
 * 
 * @param fn <IN> function
 * @param extra <IN> extra which has been initialized
 */
#define PLAT_AT_FORK(name, fn, extra) \
    static struct plat_at_fork_entry  * plat_at_fork_ ## name ## _entry;       \
    static void plat_at_fork_ ## name ## _constructor (void)                   \
    __attribute__((constructor));                                              \
    static void plat_at_fork_ ## name ## _constructor () {                     \
        plat_at_fork_ ## name ## _entry = plat_at_fork_add(fn, extra);         \
    }                                                                          \
    static void plat_at_fork_ ## name ## _destructor (void)                    \
    __attribute__((destructor));                                               \
    static void plat_at_fork_ ## name ## _destructor () {                      \
        plat_at_fork_remove(plat_at_fork_ ## name ## _entry);                  \
    }

__BEGIN_DECLS

/**
 * Perform the fork, exec sequence with a reasonable child process creation
 * model that handles the common stdin/stdout/stderr redirection cases,
 * reports errors in the parent context, is easy to implement in a simulated
 * environment, and accomodates software (including third party library)
 * writers who aren't * paranoid about setting their close-on-exec bit.
 *
 * @param <IN> filename  Path (absolute or relative) to executable.  PATH
 * is not searched.
 * @param <IN> argc null terminated argument array
 * @param <IN> envp null terminated environment array
 * @param <IN> nfds size of *fds.  Ignored when fds is null.
 * @param <IN> fds array of {type, parent_fd, child_fd} tuples for
 * duplicating (type == PLAT_FORK_DUP) or transfering (PLAT_FORK_TRANSFER)
 * file descriptors to child processes.  NULL means reasonable default
 * which is dup stdin/stdout/stderr.
 *
 * Transfered file descriptors are consumed regardless of whether the
 * fork and exec are successful.
 */
pid_t plat_fork_execve(const char *filename, char *const argv[],
                       char *const envp[], int nfds,
                       const struct plat_fork_fd *fds);

/**
 * @brief Specify a function used on #plat_fork
 *
 * Can only be called from the main thread, as in a constructor
 * 
 * @param <IN> fn called at #plat_fork with extra and 0 in child process
 * @param <IN> extra first argument to function
 * @return A handle for #plat_at_fork_remove, NULL on failure
 */
struct plat_at_fork_entry *plat_at_fork_add(void (*fn)(void *extra, pid_t pid),
                                            void *extra);

/**
 * @brief Remove a function used on #plat_fork
 *
 * @param entry <IN> return by #plat_at_fork_add
 */
void plat_at_fork_remove(struct plat_at_fork_entry *entry);


/**
 * @brief Get program name
 *
 * @return Program name, NULL on failure
 */
const char *plat_get_exe_name();

/**
 * @brief Get program path
 *
 * @return Program path, NULL on failure
 */
const char *plat_get_exe_path();

/**
 * @brief Return program identifier
 *
 * @return Good identification string, NULL on failure
 */
const char *plat_get_prog_ident();

/**
 * Allocate string from array of strings useful for printing arrays like
 * argv.
 *
 * @code
 * int main(int argc, char **argv) {
 *     char *args = plat_strarray_alloc(-1, (const char * const *)argv, " ");
 *     plat_assert(args);
 *     fprintf(stderr, "started with: %s\n", args);
 *     plat_free(args);
 *     return (0);
 *  }
 * @endcode
 *
 * @param <IN> nstring size of string array, -1 for NULL terminated
 * @param <IN> string array of strings
 * @param <IN> sep separator between strings.  May be NULL for concatenation
 */
char * plat_strarray_alloc(int nstring, const char * const *string,
                           const char *sep);


/**
 * Allocate string from array of strings useful for printing arrays like
 * argv using system malloc.
 *
 * @code
 * int main(int argc, char **argv) {
 *     char *args = plat_strarray_alloc(-1, (const char * const *)argv, " ");
 *     plat_assert(args);
 *     fprintf(stderr, "started with: %s\n", args);
 *     free(args);
 *     return (0);
 *  }
 * @endcode
 *
 * @param <IN> nstring size of string array, -1 for NULL terminated
 * @param <IN> string array of strings
 * @param <IN> sep separator between strings.  May be NULL for concatenation
 */
char * plat_strarray_sysalloc(int nstring, const char * const *string,
                              const char *sep);

/**
 * @brief strchr with length limit
 */
const char *plat_strnchr(const char *string, int n, int c);

/**
 * @brief count instances of a character in a string
 */
int plat_strncount(const char *string, int n, int c);

/**
 * @brief indirect string compare for qsort
 */
int plat_indirect_strcmp(const void *lhs, const void *rhs);

/**
 * @brief get temporary directory
 *
 * Create if it does not exist.
 *
 * @returns Temporary directory, NULL on failure.
 */
const char *plat_get_tmp_path();

/**
 * @brief Set temporary directory.  Hook for command line/property processor.
 *
 * Create if it does not exist.
 *
 * Returns 0 on success, -errno on failure.
 */
int plat_set_tmp_path(const char *path);

/**
 * @brief Probe memory for read/write permissions
 *
 * Every page in the memory buffer is probed.  This is useful to determine
 * if a specified mapping is in-use by other user code so that a fixed
 * mmap address can be safely used regardless of when the call is made.
 *
 * The memory buffer will be written when prot includes PROT_WRITE
 * and not_prot includes PROT_READ.  An erroneous failure will be
 * reported when prot includes PROT_WRITE, PROT_READ is in neither prot
 * nor not_prot, and a page is not readable.
 *
 * @param prot required permissions; fails when these don't exist
 * @param not_prot disallowed permissions; fails when these exist
 * @return  0 when the permissions are as requested, -EPERM on incorrect
 * permissions.
 */
int plat_mprobe(void *addr, size_t len, int prot, int not_prot);

/**
 * @brief Touch each page of memory to guarantee its pages are present.
 */
int plat_read_fault(void *addr, size_t len);

/**
 * @brief Touch each page of memory to guarantee its pages are present.
 */
int plat_write_fault(void *addr, size_t len);

/**
 * @brief Return memory size, -errno on failure
 */
ssize_t plat_get_address_space_size();

/**
 * @brief Ensure that the given memory mapped file is dumped
 *
 * @param filename <IN> Filename
 * @param shared <IN> Non-zero for shared mappings, zero for private
 * @return 0 on success, -errno on error
 */
int plat_ensure_dumped(const char *filename, int shared);

/**
 * @brief Get number of CPUs
 *
 * @return number of CPUs on success, -errno on error
 */
int plat_get_cpu_count();

/**
 * @brief Get CPUs sharing highest level cache
 *
 * @param peers <OUT> Set of CPUs sharing same cache.
 * @param cpu <IN> This cpu
 * @return 0 on success, -errno on error
 */

int plat_get_cpu_cache_peers(uint32_t *peers, int cpu);

__END_DECLS

#endif /* ndef PLATFORM_PLATFORM_H */

/*
 * File:   sdf/platform/tests/shmemtest_multi.c
 * Author: drew
 *
 * Created on February 2, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: shmemtest_multi.c 10648 2009-12-17 21:52:37Z drew $
 */

/*
 * Less trivial test for shared memory.
 *
 * - Initialize shared memory
 *
 * - Create shared memory list in parent process and detach shared memory
 *
 * - Start n worker processes which
 *    - Randomly shrink, grow, or check list
 *
 * - Wait for worker termination
 *
 * - Check shared list, free, and verify object usage counts are the same
 *   as before test execution.
 */

#define PLAT_SP_DEPRECATE

/* for getopt_long */
#undef _GNU_SOURCE
#define _GNU_SOURCE

#include <limits.h>
#include <getopt.h>

#define PLATFORM_INTERNAL 1

#include "platform/assert.h"
#include "platform/errno.h"
#include "platform/fcntl.h"
#include "platform/logging.h"
#include "platform/mutex.h"
#define PLAT_OPTS_NAME(name) name ## _shmemtest_multi
#include "platform/opts.h"
#include "platform/platform.h"
#include "platform/shmem.h"
#include "platform/stdio.h"
#include "platform/stdlib.h"
#include "platform/string.h"
#include "platform/time.h"
#include "platform/types.h"
#include "platform/unistd.h"
#include "platform/wait.h"

#include "misc/misc.h"

/* Logging category */
#define LOG_CAT PLAT_LOG_CAT_PLATFORM_TEST_SHMEM

/*
 * Command line arguments
 *     #define item(opt, desc, caps, parse, required)
 * where
 * opt is a 'C' string describing the option
 * arg is a  description of the argument or its argument
 * caps is an all-caps version of the option for enum definition
 * parse is a lambda function to parse it (using config and optarg
 * as provided to OPT_ITEMS())
 * required is one of reqiured, no, or optional indicating whether
 *     this option takes an argu,ent.
 */
#define PLAT_OPTS_ITEMS_shmemtest_multi()                                      \
    PLAT_OPTS_SHMEM(shmem)                                                     \
    item("exec", "executable name", EXEC,                                      \
         parse_string_alloc(&config->exe, optarg, PATH_MAX),                   \
         PLAT_OPTS_ARG_REQUIRED)                                               \
    item("runtime", "runtime in seconds", RUNTIME,                             \
         parse_int(&config->runtime, optarg, NULL),                            \
         PLAT_OPTS_ARG_REQUIRED)                                               \
    item("nproc", "number of processes", NPROC,                                \
         parse_int(&config->nproc, optarg, NULL),                              \
         PLAT_OPTS_ARG_REQUIRED)                                               \
    item("pthread", "use pthreads not forks", PTHREAD,                         \
         ({ config->test_mode = MODE_PTHREAD; 0; }), PLAT_OPTS_ARG_NO)         \
    item("len", "list length", LIST_LEN,                                       \
         parse_int(&config->list_len, optarg, NULL),                           \
         PLAT_OPTS_ARG_REQUIRED)                                               \
    item("list", "list head", LIST,                                            \
         head_sp_parse(&config->list_head, optarg, NULL),                      \
         PLAT_OPTS_ARG_REQUIRED)                                               \
    item("child", "child", CHILD,                                              \
         parse_int(&config->child, optarg, NULL),                              \
         PLAT_OPTS_ARG_REQUIRED)

struct plat_opts_config_shmemtest_multi;

struct list_head;
PLAT_SP(head_sp, struct list_head);

struct list_entry;
PLAT_SP(entry_sp, struct list_entry);

struct test_state {
    int argc;
    char **argv;
    char **envp;
    const struct plat_opts_config_shmemtest_multi *config;

    int attached;
    head_sp_t shared_head;
    struct plat_shmem_alloc_stats init_stats;
    struct plat_shmem_alloc_stats end_stats;

    int npthreads;
    pthread_t *pthreads;
};

enum test_mode {
    MODE_FORK = 0,
    MODE_PTHREAD
};

struct plat_opts_config_shmemtest_multi {
    struct plat_shmem_config shmem;

    /*
     * Executable name for child process so we don't need an emulated
     * /proc/self/exe.
     */
    char *exe;

    /* Run limit (in seconds) */
    int runtime;
    /* Number of processes */
    int nproc;
    /* Target list length */
    int list_len;

    /* Minimum allocation size in bytes */
    int64_t min_size;

    /* Maximum allocation size in bytes */
    int64_t max_size;

    /* Child nuber */
    int child;
    /* List head passed in from command line */
    head_sp_t list_head;

    enum test_mode test_mode;
};

struct list_head {
    plat_mutex_t lock;
    int checksum;
    int count;

    entry_sp_t head;
};

PLAT_SP_IMPL(head_sp, struct list_head);

struct list_entry {
    int value;
    entry_sp_t next;

/*
 * FIXME: Add variable sized char[] allocated in shared memory with random
 * size so we get better coverage of debug mode allocations which are
 * getting joined with their map/unmap neighbors.
 */
};

PLAT_SP_IMPL(entry_sp, struct list_entry);

int shmemtest_multi_main(int argc, char **argv, char **envp);
static int init(struct test_state *test_state);
static int do_parent(struct test_state *test_state);
static int create_thread(struct test_state *test_state);
static int fork_child(struct test_state *state);
static int wait_for_threads(struct test_state *test_state);
static int wait_for_children(struct test_state *state, const char *stage);
static void *pthread_do_child(void *arg);
static int do_child(const struct test_state *test_state);
static head_sp_t list_alloc();
static void list_free(head_sp_t shared_head);
/* Precondition: lock is held or not needed */
static void list_check(head_sp_t list_shared);
static int list_add(head_sp_t list_shared, int value);
static void list_remove(head_sp_t list_shared, int n);

int
main(int argc, char **argv, char **envp) {
    return (shmemtest_multi_main(argc, argv, envp));
}

int
shmemtest_multi_main(int argc, char **argv, char **envp) {
    struct plat_opts_config_shmemtest_multi config;
    struct test_state test_state;
    int ret;

    /* Initialize */
    memset(&config, 0, sizeof(config));
    plat_shmem_config_init(&config.shmem);
    config.exe = NULL;
    config.runtime = 2;
    config.nproc = 10;
    config.list_len = 100;
    config.min_size = 4;
    config.max_size = 512;
    config.list_head = head_sp_null;
    config.child = -1;

    memset(&test_state, 0, sizeof(test_state));
    test_state.argc = argc;
    test_state.argv = argv;
    test_state.envp = envp;
    test_state.config = &config;
    test_state.attached = 0;
    test_state.shared_head = head_sp_null;

    ret = plat_opts_parse_shmemtest_multi(&config, argc, argv);

    if (!ret) {
        ret = init(&test_state);
        if (ret) {
            plat_log_msg(21041, LOG_CAT, PLAT_LOG_LEVEL_ERROR,
                         "init failed");
        }
    }

    /* Run test */
    if (ret) {
    } else if (!head_sp_is_null(config.list_head)) {
        ret = do_child(&test_state);
    } else {
        ret = do_parent(&test_state);
    }

    if (!ret && test_state.attached) {
        plat_shmem_detach();
    }

    plat_shmem_config_destroy(&config.shmem);
    if (config.exe) {
        plat_free(config.exe);
    }

    return (ret);
}

/*
 * Initialize shared memory subsystems, including base functionality and
 * allocator.  Returns 0 on success and 1 on failure.
 */
static int
init(struct test_state *test_state) {
    const struct plat_opts_config_shmemtest_multi *config = test_state->config;
    const char *path;
    int ret = 0;
    int status;

    if (head_sp_is_null(config->list_head)) {
        status = plat_shmem_prototype_init(&config->shmem);
        if (status) {
            plat_log_msg(20876, LOG_CAT, PLAT_LOG_LEVEL_FATAL,
                         "shmem init failure: %s", plat_strerror(-status));
            ret = 1;
        }
    }

    if (!ret) {
        path = plat_shmem_config_get_path(&config->shmem);
        status = plat_shmem_attach(path);
        if (status) {
            plat_log_msg(20073, LOG_CAT, PLAT_LOG_LEVEL_FATAL,
                         "shmem_attach(%s) failed: %s", path,
                         plat_strerror(-status));
            ret = 1;
        } else {
            test_state->attached = 1;
        }
    }

    if (!ret && !head_sp_is_null(config->list_head)) {
        test_state->shared_head = config->list_head;
    }

    return (ret);
}

static int
do_parent(struct test_state *test_state) {
    int ret = 0;
    int i;
    int tmp;

    // Guarantee local arena was allocated before stats grabbed so
    // it doesn't show up as a leak
    plat_shmem_pthread_started();

    tmp = plat_shmem_alloc_get_stats(&test_state->init_stats);
    plat_assert_always(!tmp);

    test_state->shared_head = list_alloc();
    plat_assert_always(!head_sp_is_null(test_state->shared_head));

    if (test_state->config->nproc > 0) {
        if (test_state->config->test_mode == MODE_PTHREAD) {
            test_state->pthreads = calloc(test_state->config->nproc,
                                          sizeof (test_state->pthreads[0]));
            if (!test_state->pthreads) {
                ret = 1;
            }
        }

        for (i = 0; !ret && i < test_state->config->nproc; ++i) {
            if (test_state->config->test_mode == MODE_PTHREAD) {
                ret = create_thread(test_state);
            } else {
                ret = fork_child(test_state);
            }
        }

        if (test_state->config->test_mode == MODE_PTHREAD) {
            tmp = wait_for_threads(test_state);
        } else  {
            tmp = wait_for_children(test_state, "proc_worker");
        }
        if (tmp) {
            ret = tmp;
        }

        if (!ret && test_state->pthreads) {
            sys_free(test_state->pthreads);
        }


    } else {
        ret = do_child(test_state);
    }

    if (!ret) {
        list_check(test_state->shared_head);
    }
    if (!ret) {
        list_free(test_state->shared_head);
    }

    if (!ret) {
        tmp = plat_shmem_alloc_get_stats(&test_state->end_stats);
        plat_assert_always(!tmp);
        plat_assert_always(test_state->init_stats.allocated_bytes ==
                           test_state->end_stats.allocated_bytes);
        plat_assert_always(test_state->init_stats.allocated_count ==
                           test_state->end_stats.allocated_count);
    }


    return (ret);
}

static int
create_thread(struct test_state *test_state) {
    int ret;

    plat_assert_always(test_state->npthreads < test_state->config->nproc);

    ret = pthread_create(&test_state->pthreads[test_state->npthreads],
                         NULL /* attr */, pthread_do_child, test_state);

    if (!ret) {
        ++test_state->npthreads;
    }

    return (ret);
}

/*
 * Fork a single child which will execute *fn.
 * Returns 0 on success and 1 on error.
 */
static int
fork_child(struct test_state *test_state) {
    int ret = 0;
    static int count = 0;
    const struct plat_opts_config_shmemtest_multi *config = test_state->config;
    char *child_arg = NULL;
    char *list_arg = NULL;
    const char *exe_path = NULL;

    int argc;
    pid_t pid;
    char **argv = NULL;

    plat_asprintf(&child_arg, "%d", count);
    plat_assert_always(child_arg);
    plat_assert_always(strlen(child_arg));
    if (!child_arg) {
        ret = 1;
    }

    plat_asprintf(&list_arg, PLAT_SP_FMT,
                  PLAT_SP_FMT_ARG(test_state->shared_head));
    if (!list_arg) {
        ret = 1;
    }

    const char *more_args[] = {
        "--child", child_arg,
        "--list", list_arg,
        NULL
    };

    argc = test_state->argc + sizeof (more_args) / sizeof (more_args[0]);
    argv = plat_alloc(argc * sizeof (argv[0]));
    if (!argv) {
        ret = 1;
    } else {
        memcpy(argv, test_state->argv, test_state->argc * sizeof(argv[0]));
        memcpy(argv + test_state->argc, more_args, sizeof(more_args));
    }

    if (!ret) {
        exe_path = config->exe ? config->exe : plat_get_exe_path();
        if (!exe_path) {
            plat_log_msg(21773, LOG_CAT, PLAT_LOG_LEVEL_ERROR,
                         "can't determine exe path for starting children");
            ret = 1;
        }
    }

    if (!ret) {
        pid = plat_fork_execve(exe_path, argv, test_state->envp, 0, NULL);
        if (pid == -1) {
            plat_log_msg(21774, LOG_CAT, PLAT_LOG_LEVEL_ERROR,
                         "fork, execve failed: %s", plat_strerror(plat_errno));
            ret = 1;
        } else {
            plat_log_msg(21775, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                         "Child %d started", (int)pid);
        }
    }

    plat_free(argv);
    plat_free(child_arg);
    plat_free(list_arg);

    return (ret);
}

/*
 * Wait for all child processes.  Considers termination with non-zero
 * _exit(2) status or by uncaught signal to be an error.  Returns 0
 * on success and 1 on failure.
 */
static int
wait_for_children(struct test_state *test_state, const char *stage) {
    int ret = 0;
    int terminated = 0;
    int status;
    int exit_status;
    pid_t pid;

    do {
        pid = plat_waitpid(-1, &status, 0);
        if (pid != -1) {
            if (WIFEXITED(status)) {
                ++terminated;
                exit_status = WEXITSTATUS(status);
                if (exit_status) {
                    plat_log_msg(21776, LOG_CAT,
                                 PLAT_LOG_LEVEL_ERROR,
                                 "Child %d exited with status %d",
                                 (int)pid, WEXITSTATUS(status));
                    ret = 1;
                } else {
                    plat_log_msg(21776, LOG_CAT,
                                 PLAT_LOG_LEVEL_TRACE,
                                 "Child %d exited with status %d",
                                 (int)pid, WEXITSTATUS(status));
                }
            } else if (WIFSIGNALED(status)) {
                ++terminated;
                plat_log_msg(21777, LOG_CAT, PLAT_LOG_LEVEL_ERROR,
                             "Child %d terminated with signal %s%s", (int)pid,
                             strsignal(WTERMSIG(status)),
#ifdef WCOREDUMP
                             WCOREDUMP(status) ? " (core dumped)" :
#endif
                             "");
                ret = 1;
            }
        }
    } while (pid != -1 || plat_errno != ECHILD);

    plat_log_msg(21778, LOG_CAT, PLAT_LOG_LEVEL_DIAGNOSTIC,
                 "%d children terminated", terminated);

    return (ret);
}

static int
wait_for_threads(struct test_state *test_state) {
    int ret = 0;
    int i;
    int tmp;
    void *thread_ret;

    for (i = 0; i < test_state->npthreads; ++i) {
        tmp = pthread_join(test_state->pthreads[i], &thread_ret);
        if (tmp) {
            ret = 1;
        } else if (thread_ret) {
            ret = 1;
        }
    }


    return (ret);
}

enum {
    OP_ADD,
    OP_REMOVE,
    OP_CHECK,
    OP_COUNT
};

static void *
pthread_do_child(void *arg) {
    const struct test_state *test_state = (const struct test_state *)arg;
    int ret;

    plat_shmem_pthread_started();

    ret = do_child(test_state);

    plat_shmem_pthread_done();

    return (!ret ? NULL : (void *)test_state);
}

/*
 * Worker logic intended to run in parallel in child processes.  Randomly
 * executes remove/add/check actions, attempting to keep target list size.
 */
static int
do_child(const struct test_state *test_state) {
    const struct plat_opts_config_shmemtest_multi *config = test_state->config;
    head_sp_t shared_head = test_state->shared_head;
    struct list_head *local_head = NULL;
    int ret = 0;

    struct timeval now;
    struct timeval end;
    int count;
    int len;
    int i;

    plat_gettimeofday(&now, NULL);
    end = now;
    end.tv_sec += config->runtime;

    /* Do something different than the other processes but be repeatable */
    if (config->child != -1) {
        plat_srandom(get_seed_arg() + config->child);
    }

    head_sp_rwref(&local_head, shared_head);

    while (!ret && (now.tv_sec < end.tv_sec ||
                    (now.tv_sec == end.tv_sec && now.tv_usec < end.tv_usec))) {
        plat_mutex_lock(&local_head->lock);
        len = local_head->count;
        plat_mutex_unlock(&local_head->lock);

        switch (plat_random() % OP_COUNT) {
        case OP_ADD:
            if (len < config->list_len) {
                count = len + (plat_random() % (config->list_len - len));

                for (i = 0; i < count; ++i) {
                    /* May return ENOMEM */
                    list_add(shared_head, plat_random());
                }
            }
            break;

        case OP_REMOVE:
            if (len > 0) {
                count = plat_random() % (len + 1);
                for (i = 0; i < count; ++i) {
                    list_remove(shared_head, plat_random() % len);
                }
            }
            break;

        case OP_CHECK:
            plat_mutex_lock(&local_head->lock);
            list_check(shared_head);
            plat_mutex_unlock(&local_head->lock);
            break;

        default:
            plat_assert_always(0);
        }

        plat_gettimeofday(&now, NULL);
    }

    head_sp_rwrelease(&local_head);

    return (ret);
}

/* Allocate list in shared memory */
static head_sp_t
list_alloc() {
    head_sp_t shared_head;
    struct list_head *local_head = NULL;

    shared_head = plat_shmem_alloc(head_sp);
    head_sp_rwref(&local_head, shared_head);
    if (local_head) {
        memset(local_head, 0, sizeof(*local_head));
        plat_mutex_init(&local_head->lock);
        local_head->checksum = 0;
        local_head->count = 0;
        local_head->head = entry_sp_null;
    }

    head_sp_rwrelease(&local_head);

    return (shared_head);
}

/* Free shared_head and contents */
static void
list_free(head_sp_t shared_head) {
    struct list_head *local_head  = NULL;
    entry_sp_t shared_current;
    struct list_entry *local_current = NULL;

    head_sp_rwref(&local_head, shared_head);

    if (local_head) {
        list_check(shared_head);

        while (local_head->count > 0) {
            shared_current = local_head->head;
            entry_sp_rwref(&local_current, shared_current);
            plat_assert_always(local_current);

            local_head->checksum -= local_current->value;
            local_head->count--;
            local_head->head = local_current->next;

            plat_shmem_free(entry_sp, shared_current);
        }

        plat_assert_always(entry_sp_is_null(local_head->head));

        head_sp_rwrelease(&local_head);
        plat_shmem_free(head_sp, shared_head);
    }
}

/* Assert list_shared is valid.  Precondition: lock is held or not needed */
static void
list_check(head_sp_t list_shared) {
    const struct list_head *local_head = NULL;
    entry_sp_t shared_current;
    const struct list_entry *local_current = NULL;
    int checksum;
    int count;
    int prev;

    head_sp_rref(&local_head, list_shared);
    plat_assert_always(local_head);
    for (checksum = 0, count = 0, shared_current = local_head->head,
         prev = INT_MIN; !entry_sp_is_null(shared_current);
         prev = local_current->value, shared_current = local_current->next) {

        entry_sp_rref(&local_current, shared_current);
        plat_assert_always(local_current);

        plat_assert_always(prev <= local_current->value);

        checksum += local_current->value;
        count++;
    }

    plat_assert_always(checksum == local_head->checksum);
    plat_assert_always(count == local_head->count);

    head_sp_rrelease(&local_head);
    entry_sp_rrelease(&local_current);
}

/* Add entry with given value to list_shared */
static int
list_add(head_sp_t list_shared, int value) {
    int ret;
    struct list_head *local_head = NULL;
    entry_sp_t shared_current;
    struct list_entry *local_current = NULL;
    entry_sp_t *local_prev = NULL;
    entry_sp_t shared_add;
    struct list_entry *local_add = NULL;

    head_sp_rwref(&local_head, list_shared);
    plat_assert_always(local_head);

    plat_mutex_lock(&local_head->lock);

    local_prev = &local_head->head;
    shared_current = local_head->head;
    entry_sp_rwref(&local_current, shared_current);
    while (local_current && value >= local_current->value) {
        local_prev = &local_current->next;
        shared_current = local_current->next;
        entry_sp_rwref(&local_current, shared_current);
    }

    shared_add = plat_shmem_alloc(entry_sp);
    entry_sp_rwref(&local_add, shared_add);
    plat_assert_imply(!entry_sp_is_null(shared_add), local_add);

    ret = local_add ? 0 : -ENOMEM;
    if (local_add) {
        local_add->value = value;
        local_add->next = shared_current;
        *local_prev = shared_add;

        local_head->checksum += value;
        local_head->count++;
    }
    plat_mutex_unlock(&local_head->lock);

    head_sp_rwrelease(&local_head);
    entry_sp_rwrelease(&local_current);
    entry_sp_rwrelease(&local_add);

    return (ret);
}

/* Remove nth entry from list_shared */
static void
list_remove(head_sp_t list_shared, int n) {
    int i;
    struct list_head *local_head = NULL;
    entry_sp_t shared_current;
    struct list_entry *local_current;
    entry_sp_t *local_prev = NULL;

    head_sp_rwref(&local_head, list_shared);
    plat_assert_always(local_head);

    plat_mutex_lock(&local_head->lock);

    for (local_prev = &local_head->head, shared_current = local_head->head,
         entry_sp_rwref(&local_current, shared_current), i = 0;
         local_current && i < n;
         local_prev = &local_current->next,
         shared_current = local_current->next,
         entry_sp_rwref(&local_current, shared_current), ++i) {
    }

    if (local_current) {
        *local_prev = local_current->next;
        local_head->checksum -= local_current->value;
        local_head->count--;
        entry_sp_rwrelease(&local_current);
        plat_shmem_free(entry_sp, shared_current);
    }

    plat_mutex_unlock(&local_head->lock);

    head_sp_rwrelease(&local_head);
}

#include "platform/opts_c.h"

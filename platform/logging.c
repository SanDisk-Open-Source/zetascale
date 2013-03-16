/*
 * File:   sdf/platform/logging.c
 * Author: drew
 *
 * Created on February 3, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: logging.c 13381 2010-05-03 06:47:01Z drew $
 */

/**
 * Logging subsystem intended to provide high-performance logging with
 * binary encoding for space and time efficiency with storage of fine-grained
 * logs in a circular buffer that gets flushed when an error is encountered.
 */

/*
 * FIXME: This is a stub implementation that dumps to stderr that only
 * serves to define the API and provide a stop-gap measure
 *
 * sys_assert() is used because plat_assert() is built on top of the logging
 * code.
 */

#define PLATFORM_INTERNAL 1
#define PLATFORM_LOGGING_C 1

#include "sys/syslog.h"

#include <pthread.h>
#include <stdarg.h>

#include "platform/assert.h"
#include "platform/errno.h"
#include "platform/logging.h"
#include "platform/rwlock.h"
#include "platform/platform.h"
/* For asprintf */
#undef _GNU_SOURCE
#define _GNU_SOURCE
#include "platform/stdio.h"
#include "platform/stdlib.h"
#include "platform/string.h"
#include "platform/time.h"
#include "platform/unistd.h"
#include "sdftcp/trace.h"

/**
 * Default log threshold for immediate output when not specified in level
 * threshold for specified category.
 */
#define DEFAULT_LOG_LEVEL PLAT_LOG_LEVEL_TRACE

/*
 * XXX: Debugging problems may be simpler when this is  split into simulated
 * cluster wide (level definition) and per-process (active level) pieces.
 */
struct category {
    /** @brief sys_malloc allocated category name. */
    const char *name;

    /**
     * @brief Threshold for immediate output at time of log call.
     *
     * May be an actual level or PLAT_LOG_LEVEL_DEFAULT in which case
     * the next hierchical category is examined.
     */
    enum plat_log_level immediate_threshold;

    /**
     * categories.category index which should be examined for this
     * log message immediate_threshold when immediate_threshold is
     * set to PLAT_LOG_LEVEL_DEFAULT.
     */
    int redirect_index;
};

struct categories {
    /** @brief lock; read for get, write for updating category */
    plat_rwlock_t lock;

    /**
     * @brief set of categories in order of addition
     * (allocated with sys_malloc)
     */
    struct category category[PLAT_LOG_MAX_CATEGORY];

    /** @brief size of category */
    int ncategory;

    /** @brief maximum depth for redirections */
    int redirect_limit;
};

/*
 * XXX this will become a program level attribute if the simualtion code is
 * revived.
 */
static struct categories categories = {
    .lock = PLAT_RWLOCK_INITIALIZER,
    .category = {},
    .ncategory = 0,
    .redirect_limit = 0
};

enum plat_log_level plat_log_level_immediate_cache[PLAT_LOG_MAX_CATEGORY];

static const char *levels[] = {
#define item(caps, lower, value, syslog_prio) #lower,
PLAT_LOG_LEVEL_ITEMS()
#undef item
};
static const int nlevels = sizeof (levels)/sizeof (levels[0]);

/** @brief ref count by other subsystems */
static int ref_count;

enum output_mode {
    OUTPUT_FILE,
    OUTPUT_STDERR,
    OUTPUT_SYSLOG
};

struct output {
    /** @brief lock; Needs to be held when updating filename */
    plat_rwlock_t lock;

    /** @brief Output mode */
    enum output_mode mode;

    /**
     * @brief What file descriptors should be included in files
     *
     * XXX: drew 2009-03-27 We don't currently support standard
     * error redirection to syslog
     */
    enum plat_log_redirect redirect;

    /** @brief Filename.  Empty string for stderr */
    char filename[PATH_MAX + 1];

    /** @brief output file */
    FILE *file;
};

static struct output output = {
    .lock = PLAT_RWLOCK_INITIALIZER,
    .mode = OUTPUT_STDERR,
    .redirect = PLAT_LOG_REDIRECT_STDERR,
    .filename = "",
    .file = NULL
};


/** @brief strftime format string matching /bin/date (%+ doesn't work) */
static const char log_time_bin_date_format[] = "%b %d %T %G";

static const char log_time_hms_format[] = "%r";

/** @brief strftime format string allocated via sys_malloc */
static char *log_time_format = NULL;

enum log_relative {
    LOG_RELATIVE_NONE,
    LOG_RELATIVE_SECS,
    LOG_RELATIVE_SECS_FLOAT,
    LOG_RELATIVE_HMS,
    LOG_RELATIVE_HMS_FLOAT
};


/** @brief Log relative to #log_relative_start */
enum log_relative log_relative = LOG_RELATIVE_NONE;

struct timeval log_relative_start;

static void gettimeofday_wrapper(void *env, struct timeval *tv);

/** @brief where time stamps come from when log_time_format is non null */
void (*log_gettime_fn)(void *env, struct timeval *tv) = &gettimeofday_wrapper;
void *log_gettime_extra = NULL;

static void
fake_fthGetTimeOfDay(struct timeval *tv) {
    plat_gettimeofday(tv, NULL);
}

void fthGetTimeOfDay(struct timeval *tv) __attribute__((weak, alias("fake_fthGetTimeOfDay")));

static void
gettimeofday_wrapper(void *env, struct timeval *tv) {
    fthGetTimeOfDay(tv);
}

/*
 * Log pthread and fth thread associated with each log message for debugging
 * purposes without introducing link time dependencies.
 */
static struct fthThread *fake_fthSelf() {
    return (NULL);
}

struct fthThread *fthSelf() __attribute__((weak, alias("fake_fthSelf")));

#ifdef notyet
/* XXX: The real pthread_self never seems to get instantiated */
static pthread_t fake_pthread_self() {
    return (0);
}

pthread_t pthread_self() __attribute__((weak, alias("fake_pthread_self")));
#endif /* def notyet */

static void do_printf(enum plat_log_level level, const char *fmt, va_list ap);
static int get_syslog_prio(enum plat_log_level level);
static int parse_categories(const char *text, int len_arg,
                            enum plat_log_level level);
static int parse_category(const char *category, int len);
static enum plat_log_level parse_level(const char *level);
static void init_categories_locking();
static void destroy_categories();
static int init_categories();
static int init_category(const char *category, int len);
static int indirect_strcmp_null_last(const void *lhs, const void *rhs);
static void log_refresh_cache();
static enum plat_log_level log_immediate_threshold(int category);

/* Locally we have an implicit reference */
__attribute__((constructor)) void
plat_log_reference() {
    char *log_filename;
    int before;

    before = __sync_fetch_and_add(&ref_count, 1);

    if (!before) {
        init_categories_locking();

        log_filename = getenv("PLAT_LOG_FILENAME");
        if (log_filename && !output.filename[0]) {
            plat_log_set_file(log_filename, PLAT_LOG_REDIRECT_STDERR);
        }

        if (!output.file && output.mode == OUTPUT_STDERR) {
            output.file = stderr;
            output.filename[0] = 0;
        }

        sys_assert(log_gettime_fn);
        (*log_gettime_fn)(log_gettime_extra, &log_relative_start);

        plat_log_set_time_format_bin_date();
    }
}

/* Locally we have an implicit reference */
__attribute__((destructor)) void
plat_log_release() {
    int after = __sync_sub_and_fetch(&ref_count, 1);
    sys_assert(after >= 0);
    if (!after) {
        if (output.file && output.file != stderr) {
            fclose(output.file);
        }
        output.file = NULL;
        output.filename[0] = 0;
        destroy_categories();

        if (log_time_format) {
            sys_free(log_time_format);
        }
    }
}

int
plat_log_set_file(const char *filename, enum plat_log_redirect redirect) {
    int ret = 0;

    plat_rwlock_wrlock(&output.lock);

    if (strlen(filename) >= sizeof (output.filename)) {
        ret = -ENAMETOOLONG;
    }

    if (!ret) {
        if (!output.file || output.file == stdout || output.file == stderr) {
            output.file = fopen(filename, "a");
            if (!output.file) {
                ret = -errno;
            }
        } else if (!freopen(filename, "a", output.file)) {
            ret = -errno;
        }
    }

    if (!ret && (redirect & PLAT_LOG_REDIRECT_STDERR)) {
        fflush(stderr);
        if (dup2(fileno(output.file), 2) == -1) {
            ret = -errno;
        }
    }

    if (!ret && (redirect & PLAT_LOG_REDIRECT_STDOUT)) {
        fflush(stdout);
        if (dup2(fileno(output.file), 1) == -1) {
            ret = -errno;
        }
    }

    if (!ret) {
        output.mode = OUTPUT_FILE;
        output.redirect = redirect;
        strncpy(output.filename, filename, sizeof (output.filename) - 1);
    } else {
        fprintf(stderr, "Failed to open %s for log output: %s\n", filename,
                sys_strerror(-ret));
    }

    plat_rwlock_unlock(&output.lock);

    return (ret);
}

int
plat_log_reopen() {
    int ret = 0;

    plat_rwlock_wrlock(&output.lock);

    if (output.mode == OUTPUT_FILE) {
        if (!freopen(output.filename, "a", output.file)) {
            ret = -errno;
            fprintf(stderr, "Failed to reopen %s for log output: %s\n",
                    output.filename, sys_strerror(-ret));
        } else if (output.file == stderr && fileno(stderr) != 2) {
            sys_dup2(fileno(stderr), 2);
        }
    }

    plat_rwlock_unlock(&output.lock);

    return (ret);
}

int
plat_log_set_time_format(const char *format_arg) {
    int ret = 0;
    char *format;

    if (format_arg) {
        format = sys_strdup(format_arg);
        if (!format) {
            ret = -ENOMEM;
        }
    } else {
        format = NULL;
    }

    if (!ret) {
        if (log_time_format) {
            sys_free(log_time_format);
        }
        log_time_format = format;
    }

    return (ret);
}

int
plat_log_set_time_format_secs() {
    log_relative = LOG_RELATIVE_NONE;
    return (plat_log_set_time_format("%s"));
}

int
plat_log_set_time_format_relative_secs() {
    log_relative = LOG_RELATIVE_SECS;
    return (0);
}

int
plat_log_set_time_format_relative_secs_float() {
    log_relative = LOG_RELATIVE_SECS_FLOAT;
    return (0);
}

int
plat_log_set_time_format_relative_hms() {
    log_relative = LOG_RELATIVE_HMS;
    return (0);
}

int
plat_log_set_time_format_relative_hms_float() {
    log_relative = LOG_RELATIVE_HMS_FLOAT;
    return (0);
}

int
plat_log_set_time_format_bin_date() {
    log_relative = LOG_RELATIVE_NONE;
    return (plat_log_set_time_format(log_time_bin_date_format));
}

void
plat_log_set_gettime(void (*fn)(void *extra, struct timeval *tv), void *extra,
                     void (**old_fn)(void *extra, struct timeval *tv),
                     void **old_extra) {
    plat_log_msg(20950, PLAT_LOG_CAT_PLATFORM_LOGGING,
                 PLAT_LOG_LEVEL_DEBUG, "log gettime function changed");

    if (old_fn) {
        *old_fn = log_gettime_fn;
    }
    if (old_extra) {
        *old_extra = log_gettime_extra;
    }

    log_gettime_fn = fn;
    log_gettime_extra = extra;

    sys_assert(log_gettime_fn);
    (*log_gettime_fn)(log_gettime_extra, &log_relative_start);
}

void
plat_log_gettimeofday(struct timeval *tv) {
    (*log_gettime_fn)(log_gettime_extra, tv);
}

/**
 * @brief Format time in logging format
 *
 * @return number of bytes written on success, 0 on failure like
 * strftime.
 */
static size_t
plat_log_format_time(char *buf, size_t len, const struct timeval *tv_arg) {
    time_t time_time_t;
    struct tm time_tm;
    struct timeval tv;
    int ret;

    if (log_relative == LOG_RELATIVE_NONE) {
        time_time_t = tv_arg->tv_sec;
        localtime_r(&time_time_t, &time_tm);
        ret = strftime(buf, len, log_time_format ? log_time_format :
                       log_time_hms_format, &time_tm);
    } else {
        ret = 0; /* supress GCC intiialization warning */
        timersub(tv_arg, &log_relative_start, &tv);
        switch (log_relative) {
        case LOG_RELATIVE_NONE:
            assert(0);
            ret = 0;
            break;
        case LOG_RELATIVE_SECS:
            ret = snprintf(buf, len, "%ld", tv.tv_sec);
            break;
        case LOG_RELATIVE_SECS_FLOAT:
            ret = snprintf(buf, len, "%.6f",
                           tv.tv_sec + tv.tv_usec / 1000000.0);
            break;
        case LOG_RELATIVE_HMS:
            ret = snprintf(buf, len, "%ld:%02ld:%02ld",
                           (long)tv.tv_sec / (60*60),
                           (long)(tv.tv_sec / 60) % 60,
                           (long)tv.tv_sec % 60);
            break;
        case LOG_RELATIVE_HMS_FLOAT:
            ret = snprintf(buf, len, "%ld:%02ld:%s%02.6f",
                           (long)tv.tv_sec / (60*60),
                           (long)(tv.tv_sec / 60) % 60,
                           tv.tv_sec < 10 ? "0" : "",
                           (tv.tv_sec % 60) + tv.tv_usec / 1000000.0);
            break;
        }
        if (ret >= len) {
            ret = 0;
        }
    }

    return (ret);
}

char *
plat_log_timeval_to_string(const struct timeval *tv) {
    static __thread char buf[80];

    if (!plat_log_format_time(buf, sizeof(buf), tv)) {
        buf[0] = 0;
    }

    return (buf);
}

static
void plat_log_time(char *buf, int buflen) {
    struct timeval now;
    int offset;

    if (log_time_format || log_relative != LOG_RELATIVE_NONE) {
        sys_assert(log_gettime_fn);

        (*log_gettime_fn)(log_gettime_extra, &now);

        offset = plat_log_format_time(buf, buflen - 1, &now);
        if (offset > 0) {
            buf[offset] = ' ';
            buf[offset + 1] = 0;
        }
    } else {
        buf[0] = 0;
    }
    buf[buflen - 1]  = 0;
}

void
plat_log_op_msg(plat_op_label_t op, int logid,
                enum plat_log_category category, enum plat_log_level level,
                const char *format, ...) {
    char *real_format = NULL;
    const char *prog_ident;
    char time_buf[80];
    va_list ap;

    if (!categories.ncategory) {
        init_categories_locking();
    }

    plat_rwlock_rdlock(&categories.lock);

    sys_assert(0 <= category && category < categories.ncategory);
    sys_assert(0 <= level && level < nlevels);
    sys_assert(categories.category[category].name);

    if (plat_log_enabled(category, level)) {
        va_start(ap, format);

        prog_ident = plat_get_prog_ident();

        plat_log_time(time_buf, sizeof (time_buf));

        if (sys_asprintf(&real_format, "%s%s %lx %p %s:%s op=%llx,%lld %d-%d %s\n",
                     time_buf, prog_ident, (unsigned long)pthread_self(),
                     fthSelf(), categories.category[category].name,
                     levels[level], (long long)op.node_id, (long long)op.op_id,
                     category, logid, format)) {}
        if (real_format) {
            do_printf(level, real_format, ap);
        }
        /* From stdlib asprintf */
        sys_free(real_format);

        va_end(ap);
    }

    plat_rwlock_unlock(&categories.lock);
}

void
plat_log_msg_helper(const char *file, unsigned line, const char *function,
                    int logid, int category, enum plat_log_level level,
                    const char *format, ...)
{
    va_list ap;
    char time_buf[80];
    char *colbeg = "";
    char *colend = "";
    char *real_format = NULL;

    if (!categories.ncategory) {
        init_categories_locking();
    }

    plat_rwlock_rdlock(&categories.lock);

    sys_assert(categories.category[category].name);
    sys_assert(0 <= level && level < nlevels);
    sys_assert(0 <= category && category < categories.ncategory);

    va_start(ap, format);

    plat_log_time(time_buf, sizeof (time_buf));

    if (level >= PLAT_LOG_LEVEL_WARN && isatty(2)) {
        colbeg = (level == PLAT_LOG_LEVEL_WARN) ? "\033[35m" : "\033[31m";
        colend = "\033[39m";
    }

    if (file) {
        ignore(sys_asprintf(&real_format, "%s%s%x %s %s:%u %s%s %s\n",
                     colbeg, time_buf, (unsigned int)pthread_self(),
                     levels[level], file, line, function, colend, format));
    } else {
        ignore(sys_asprintf(&real_format, "%s%s%x %s%s %s\n",
                            colbeg, time_buf, (unsigned int)pthread_self(),
                            levels[level], colend, format));
    }

    if (real_format) {
        do_printf(level, real_format, ap);
    }
    sys_free(real_format);

    va_end(ap);

    plat_rwlock_unlock(&categories.lock);
}

/** @brief Do printf style output */
static void
do_printf(enum plat_log_level level, const char *fmt, va_list ap) {
    plat_rwlock_rdlock(&output.lock);

    switch (output.mode) {
    case OUTPUT_FILE:
    case OUTPUT_STDERR:
        if (output.file) {
            vfprintf(output.file, fmt, ap);
            fflush(output.file);
        } else {
            vfprintf(stderr, fmt, ap);
        }
        break;
    case OUTPUT_SYSLOG:
        vsyslog(get_syslog_prio(level), fmt, ap);
        break;
    }

    plat_rwlock_unlock(&output.lock);
}

/** @brief Convert plat_log_level to syslog priority */
static int
get_syslog_prio(enum plat_log_level level) {
    switch (level) {
#define item(caps, lower, value, syslog_prio) \
    case PLAT_LOG_LEVEL_ ## caps: return (syslog_prio);
    PLAT_LOG_LEVEL_ITEMS();
#undef item
    case PLAT_LOG_LEVEL_DEFAULT:
    case PLAT_LOG_LEVEL_INVALID:
        break;
    }

    return (LOG_INFO);
}

const char *
plat_log_cat_to_string(int category) {
    const char *ret;

    if (!categories.ncategory) {
        init_categories_locking();
    }

    plat_rwlock_wrlock(&categories.lock);
    if (0 <= category && category < categories.ncategory) {
        ret = categories.category[category].name;
    } else {
        ret = NULL;
    }
    plat_rwlock_unlock(&categories.lock);

    return (ret);
}

int
plat_log_parse_arg(const char *text) {
    int ret = 0;
    const char *equals;
    enum plat_log_level level;

    if (!categories.ncategory) {
        init_categories_locking();
    }

    equals = strchr(text, '=');

    if (!equals) {
        fprintf(stderr, "log argument %s not of the form category=value\n",
                text);
        ret = -EINVAL;
    } else {
        plat_rwlock_wrlock(&categories.lock);

        level = parse_level(equals + 1);
        if (PLAT_LOG_LEVEL_INVALID == level) {
            fprintf(stderr, "log argument %s invalid level\n", text);
            ret = -EINVAL;
        } else {
            ret = parse_categories(text, equals - text, level);
            log_refresh_cache();
        }

        plat_rwlock_unlock(&categories.lock);
    }

    return (ret);
}

int
plat_log_add_category(const char *category) {
    int ret;

    plat_rwlock_wrlock(&categories.lock);

    /*
     * This can be called from a constructor before the categories have been
     * initialized.
     */
    if (!categories.ncategory) {
        init_categories();
    }

    ret = init_category(category, strlen(category));
    plat_rwlock_unlock(&categories.lock);

    return (ret);
}

int
plat_log_add_subcategory(int super, const char *subpath) {
    int ret;
    const char *existing;
    char *path;

    existing = plat_log_cat_to_string(super);
    plat_assert(existing);
    path = NULL;
    if (sys_asprintf(&path, "%s/%s", existing, subpath)) {}
    plat_assert(path);
    ret = plat_log_add_category(path);
    plat_assert(ret > 0);
    sys_free(path);

    return (ret);
}

/**
 * Dump usage for the logging subsystem to stderr.
 */
void
plat_log_usage() {
    int i;
    int count;
    const char **category;
    char *categories_usage;

    plat_rwlock_wrlock(&categories.lock);

    count = categories.ncategory;
    category = sys_malloc(count * sizeof (category[0]));
    sys_assert(category);
    for (i = 0; i < count; ++i) {
        category[i] = categories.category[i].name;
    }

    plat_rwlock_unlock(&categories.lock);

    qsort(category, count, sizeof (category[0]), &indirect_strcmp_null_last);
    for (; count > 0 && !category[count - 1]; --count) {
    }
    categories_usage = plat_strarray_sysalloc(count, category, "\n\t");
    sys_assert(categories_usage);
    sys_free(category);

    fprintf(stderr,
            "log level is one of:\n"
#define item(upper, lower, value, syslog_prio) "\t" #lower "\n"
            PLAT_LOG_LEVEL_ITEMS()
#undef item
            "log category is one of:\n\t%s\n", categories_usage);

    sys_free(categories_usage);
}

/**
 * @brief Parse multiple log categories with {} expansion
 *
 * @param text <IN> text being parsed
 * @param text_len <IN> valid length of text (with text + text_len being
 * a potential delimiter)
 * @param level <IN> assigned to all expanded categories
 */
static int
parse_categories(const char *text, int text_len, enum plat_log_level level) {
    const char *left_brace;
    const char *right_brace;
    const char *next_left_brace;
    const char *sub;
    const char *sub_end;
    char *buffer;
    char *rest;
    char *ptr;
    int len;
    int category;
    int ret;

    left_brace = strchr(text, '{');
    right_brace = strchr(text, '}');

    if (!left_brace && !right_brace) {
        category = parse_category(text, text_len);
        if (PLAT_LOG_CAT_INVALID == category) {
            fprintf(stderr, "log argument %*.*s invalid category\n",
                    text_len, text_len, text);
            ret = -EINVAL;
        } else {
            categories.category[category].immediate_threshold = level;
            ret = 0;
        }
    } else if (left_brace && right_brace) {
        next_left_brace = strchr(left_brace + 1, '{');
        if (next_left_brace && next_left_brace < right_brace) {
            fprintf(stderr, "log argument %*.*s nested braces",
                    text_len, text_len, text);
            ret = -EINVAL;
        } else {
            buffer = sys_malloc(text_len + 1);
            sys_assert(buffer);

            len = left_brace - text;
            rest = buffer + len;
            memcpy(buffer, text, len);
            for (ret = 0, sub = left_brace + 1;
                 !ret && sub < right_brace; sub = sub_end + 1) {
                sub_end = strchr(sub, ',');
                if (!sub_end || sub_end > right_brace) {
                    sub_end = right_brace;
                }
                ptr = rest;
                len = sub_end - sub;
                memcpy(ptr, sub, len);
                ptr += len;

                len = text + text_len - right_brace - 1;
                memcpy(ptr, right_brace + 1, len);
                ptr += len;

                *ptr = 0;
                len = ptr - buffer;
                ret = parse_categories(buffer, len, level);
            }
            sys_free(buffer);
        }
    } else {
        fprintf(stderr, "log argument %*.*s unbalanced braces",
                text_len, text_len, text);
        ret = -EINVAL;
    }

    return (ret);
}

/**
 * Parse single category
 *
 * Precondition: categories.lock is held for reading
 *
 * @param category <IN> string starting with category name
 * @param len <IN> valid length
 *
 * @return categories.category index on success, < 0 on failure
 */
static int
parse_category(const char *category, int len) {
    int i;

    for (i = 0;
         i < categories.ncategory &&
         (!categories.category[i].name ||
          strncmp(categories.category[i].name, category, len) ||
          strlen(categories.category[i].name) != len);
         ++i) {
    }

    return (i != categories.ncategory ? i : PLAT_LOG_CAT_INVALID);
}

/**
 * Parse level
 *
 * Precondition: categories.lock is held for reading
 *
 * @param level <IN> name
 *
 * @return log level, < 0 on failure
 */
static enum plat_log_level
parse_level(const char *level) {
    int i;

    for (i = 0; i < nlevels && strcmp(levels[i], level); ++i) {
    }

    return (i != nlevels ? (enum plat_log_level)i : PLAT_LOG_LEVEL_INVALID);
}

/**
 * Locking idempotent flavor of init_categories
 *
 * @return 0 on success, -errno on failure
 */
static void
init_categories_locking() {
    plat_rwlock_wrlock(&categories.lock);
    if (!categories.ncategory) {
        init_categories();
    }
    plat_rwlock_unlock(&categories.lock);
}

static void
destroy_categories() {
    int i;

    if (categories.ncategory) {
        for (i = 0; i < categories.ncategory; ++i) {
            sys_free((void *)categories.category[i].name);
            categories.category[i].name = NULL;
        }
        categories.ncategory = 0;
    }
}

/**
 * Add all well-known categories described by compile-time constants with the
 * caller already holding categories.lock for writing.
 *
 * @return 0 on success, -errno on failure
 */

/* XXX this would be a lot cleaner with an initialize-once expression */
static int
init_categories() {
    int max = -1;
    int ret;
    int tmp;
    int i;

#define item(caps, quoted, value) \
    if (value > max) { \
        max = value; \
    }
    PLAT_LOG_CATEGORY_ITEMS();
#undef item

    /* Must initialize deprecated categories with NULL */
    ret = 0;
    categories.ncategory = max + 1;
#define item(caps, quoted, value)                                              \
    /* Sanity check that the number isn't already in use */                    \
    sys_assert(!categories.category[value].name);                              \
    categories.category[value].name = sys_strdup(quoted);                      \
    if (!categories.category[value].name) {                                    \
        ret = -errno;                                                          \
    } else {                                                                   \
        categories.category[value].immediate_threshold =                       \
            PLAT_LOG_LEVEL_DEFAULT;                                            \
        categories.category[value].redirect_index = PLAT_LOG_CAT_DEFAULT;      \
    }
    PLAT_LOG_CATEGORY_ITEMS();
#undef item

    sys_assert(0 <= PLAT_LOG_CAT_DEFAULT &&
               PLAT_LOG_CAT_DEFAULT < categories.ncategory);
    categories.category[PLAT_LOG_CAT_DEFAULT].immediate_threshold =
        DEFAULT_LOG_LEVEL;

    /* Add unspecified super categories */
    for (ret = 0, i = 0; !ret && i <= max; ++i) {
        if (categories.category[i].name) {
            tmp = init_category(categories.category[i].name,
                                strlen(categories.category[i].name));
            if (tmp < 0) {
                ret = tmp;
            }
        }
    }

    return (ret);
}

/**
 * @brief Initialize one category.
 *
 * Precondition: categories.lock is held for writing
 *
 * Invoking plat_log_init_category on one of the well-known categories
 * will initialize its super categories.
 *
 * @param category <IN> category being initialized.  where category
 * includes '/' characters all super-categories which do not exist
 * are added.  redirection is set so that when a category is set to
 * immediate_threshold PLAT_LOG_LEVEL_DEFAULT it uses its super-category's
 * log level.
 *
 * @return category index >= 0 on success, -errno on error.
 */
static int
init_category(const char *category, int len) {
    int failed = 0;
    int extend;
    const char *sub_end;
    int super_category;
    int index;
#ifdef PLAT_LOG_RUNTIME_CATEGORIES
    char *add;
#endif /* def PLAT_LOG_RUNTIME_CATEGORIES */
    int i;
    int ret;

    /* The user requested category plus sub categories up to each '/' */
    extend = 1 + plat_strncount(category, len, '/');

    plat_assert_always(categories.ncategory + extend <= PLAT_LOG_MAX_CATEGORY);

    if (extend > categories.redirect_limit) {
        categories.redirect_limit = extend + 1;
    }

    index = PLAT_LOG_CAT_DEFAULT;
    i = categories.ncategory;
    sub_end = category - 1;
    do {
        super_category = index;
        sub_end = plat_strnchr(sub_end + 1,
                               category + len - sub_end - 1, '/');
        if (!sub_end) {
            sub_end = category + len;
        }

        index = parse_category(category, sub_end - category);
        if (index == PLAT_LOG_CAT_INVALID) {
            fprintf(stderr, "unspecified log category %*.*s\n",
                    (int)(sub_end - category),
                    (int)(sub_end - category), category);
#ifdef PLAT_LOG_RUNTIME_CATEGORIES
            add = sys_malloc(sub_end - category + 1);
            if (!add) {
                failed = -plat_errno;
            } else {
                memcpy(add, category, sub_end - category);
                add[sub_end - category] = 0;
                categories.category[i].name = add;
                categories.category[i].immediate_threshold =
                    PLAT_LOG_LEVEL_DEFAULT;
                categories.category[i].redirect_index = super_category;
                index = i;
                ++i;
            }
#else /* def PLAT_LOG_RUNTIME_CATEGORIES */
            failed = -EINVAL;
#endif /* else def PLAT_LOG_RUNTIME_CATEGORIES */
        } else {
            if (categories.category[index].redirect_index ==
                PLAT_LOG_CAT_DEFAULT) {
                categories.category[index].redirect_index = super_category;
            }
        }
    } while (
#ifdef PLAT_LOG_RUNTIME_CATEGORIES
             !failed &&
#endif /* def PLAT_LOG_RUNTIME_CATEGORIES */
             *sub_end);

    if (!failed) {
        categories.ncategory = i;
        log_refresh_cache();
        ret = index;
    } else {
        ret = failed;
#ifndef PLAT_LOG_RUNTIME_CATEGORIES
        sys_abort();
#endif
    }

    return (ret);
}

static int
indirect_strcmp_null_last(const void *lhs, const void *rhs) {
    int ret;
    const char **lhs_strp = (const char **) lhs;
    const char **rhs_strp = (const char **) rhs;

    if (*lhs_strp && !*rhs_strp) {
        ret = -1;
    } else  if (!*lhs_strp && !*rhs_strp) {
        ret = 0;
    } else if (!*lhs_strp && *rhs_strp) {
        ret = 1;
    } else {
        ret = strcmp(*lhs_strp, *rhs_strp);
    }

    return (ret);
}

/**
 * @brief Update the entire cache because something changed
 *
 * Precondition: categories.lock is held for writing
 */
static void
log_refresh_cache() {
    int i;
    for (i = 0; i < categories.ncategory; ++i) {
        plat_log_level_immediate_cache[i] = log_immediate_threshold(i);
    }
    for (; i < PLAT_LOG_MAX_CATEGORY; ++i) {
        plat_log_level_immediate_cache[i] =
            plat_log_level_immediate_cache[PLAT_LOG_CAT_DEFAULT];
    }
}

/**
 * @brief Calculate the level for a given category
 *
 * Precondition: categories.lock is held for writing
 */
static enum plat_log_level
log_immediate_threshold(int category) {
    int redirect_count;
    int redirect_category;
    enum plat_log_level threshold;

    sys_assert(0 <= category && category < categories.ncategory);
    sys_assert(categories.category[category].name);

    redirect_count = 0;
    redirect_category = category;
    do {
        threshold = categories.category[redirect_category].immediate_threshold;
        redirect_category = categories.category[redirect_category].
                redirect_index;
        if (threshold != PLAT_LOG_LEVEL_DEFAULT) {
            ++redirect_count;
            sys_assert(redirect_count <= categories.redirect_limit);
        }
    } while (threshold == PLAT_LOG_LEVEL_DEFAULT);

    return (threshold);
}

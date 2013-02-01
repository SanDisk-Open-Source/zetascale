/*
 * An easier interface to FDF: configuration.
 * Author: Johann George
 *
 * Copyright (c) 2012-2013, Sandisk Corporation.  All rights reserved.
 */
#include <ctype.h>
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdarg.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>
#include "fdf_easy.h"
#include "fdf_easy_int.h"


/*
 * Configurable parameters.
 */
#define BUF_SIZE 256


/*
 * Error macros.
 */
#define mkerri(p, ...) \
    (fdf_aperr_(p, __FUNCTION__, 0, 0, __VA_ARGS__), 0)
#define mkerrp(p, ...) \
    (fdf_aperr_(p, __FUNCTION__, 0, 0, __VA_ARGS__), NULL)
#define mkerrep(p, ...) \
    (fdf_aperr_(p, __FUNCTION__, 0, errno, __VA_ARGS__), NULL)


/*
 * Macros for parsing.
 */
#define skip_space(p) do { while (*p == ' ' || *p == '\t') p++; } while (0)
#define skip_alnum(p) do { while (isalnum(*p) || *p == '_') p++; } while (0)
#define skip_toeol(p) do { while (*p) if (*p++ == '\n') break; } while (0)
#define skip_toeow(p) do { while (*p != '\0' && !isspace(*p)) p++; } while (0)


/*
 * Print out an error message in a buffer.
 */
void
fdf_perr(char *buf, int len, const char *func,
         FDF_status_t fdf_err, int sys_err, const char *fmt, va_list alist)
{
    int n = 0;

    if (func)
        n += snprintf(&buf[n], len-n, "%s", func);
    if (n > len - 1)
        n = len - 1;

    if (fmt) {
        if (n)
            n += snprintf(&buf[n], len-n, ": ");
        n += vsnprintf(&buf[n], len-n, fmt, alist);
        if (n > len - 1)
            n = len - 1;
    }

    if (fdf_err) {
        char *errstr = fdf_errmsg_(fdf_err);

        if (n)
            n += snprintf(&buf[n], len-n, ": ");
        if (errstr)
            n += snprintf(&buf[n], len-n, "%s (%d)", errstr, fdf_err);
        else
            n += snprintf(&buf[n], len-n, "(%d)", fdf_err);
    } else if (sys_err) {
        char errbuf[BUF_SIZE];
        strerror_r(sys_err, errbuf, sizeof(errbuf));

        if (n)
            n += snprintf(&buf[n], len-n, ": ");
        n += snprintf(&buf[n], len-n, "%s (%d)", errbuf, sys_err);
    }

    if (n > len - 1) {
        n = len - 5;
        n += snprintf(&buf[n], len-n, " ...");
    }
}


/*
 * Make an error message and allocate space for it.
 */
void
fdf_aperr_(char **errp, const char *func,
          FDF_status_t fdf_err, int sys_err, const char *fmt, ...)
{
    char buf[BUF_SIZE];
    va_list alist;

    if (!errp)
        return;
    va_start(alist, fmt);
    fdf_perr(buf, sizeof(buf), func, fdf_err, sys_err, fmt, alist);
    va_end(alist);
    *errp = strdup(buf);
}


/*
 * Read a file into memory.
 */
static char *
file_mem(char *path, char **errp)
{
    struct stat stat;

    int fd = open(path, O_RDONLY);
    if (fd < 0)
        return mkerrep(errp, "cannot open %s", path);

    if (fstat(fd, &stat) < 0) {
        close(fd);
        return mkerrep(errp, "fstat failed on %s", path);
    }

    size_t size = stat.st_size;
    if (!size) {
        close(fd);
        return NULL;
    }

    char *data = malloc(size + 1);
    if (!data) {
        close(fd);
        return mkerrp(errp, "malloc failed");
    }

    size_t s = read(fd, data, size);
    close(fd);
    if (s != size) {
        free(data);
        return mkerrep(errp, "read from %s failed", path);
    }

    data[size] = '\0';
    return data;
}


/*
 * Finish configuration.
 */
void
fdf_conf_done_(fdf_t *fdf)
{
    if (fdf->prop_data)
        free(fdf->prop_data);

    fdf_prop_t *prop = fdf->prop;
    while (prop) {
        fdf_prop_t *next = prop->next;
        free(prop);
        prop = next;
    }
}


/*
 * Initialize configuration with a FDF property file.
 */
int
fdf_conf(fdf_t *fdf, char *path, char **errp)
{
    fdf->prop_data = file_mem(path, errp);
    if (!fdf->prop_data)
        return 0;

    char *p = fdf->prop_data;
    while (*p) {
        skip_space(p);
        if (*p == '#') {
            skip_toeol(p);
            continue;
        }

        char *s1 = p;
        skip_alnum(p);
        char *s2 = p;
        skip_space(p);

        if (*p != '=')
            skip_toeol(p);
        else {
            p++;
            skip_space(p);
            char *s3 = p;
            skip_toeow(p);
            char *s4 = p;
            skip_toeol(p);

            fdf_prop_t *prop = malloc(sizeof(*prop));
            if (!prop) {
                fdf_conf_done_(fdf);
                return mkerri(errp, "malloc failed");
            }

            *s2 = '\0';
            *s4 = '\0';
            prop->name = s1;
            prop->value = s3;
            prop->next = fdf->prop;
            fdf->prop = prop;
        }
    }
    return 1;
}


/*
 * Get a FDF property.
 */
const char *
fdf_get_prop(fdf_t *fdf, char *name)
{
    fdf_prop_t *prop;

    for (prop = fdf->prop; prop; prop = prop->next)
        if (strcmp(prop->name, name) == 0)
            return prop->value;
    return NULL;
}


/*
 * Get a FDF property with a prefix.
 */
const char *
fdf_get_prop2(fdf_t *fdf, char *prefix, char *name)
{
    fdf_prop_t *prop;
    int plen = strlen(prefix);

    for (prop = fdf->prop; prop; prop = prop->next) {
        const char *p = prop->name;

        if (strncmp(p, prefix, plen) != 0)
            continue;
        if (p[plen] != '_')
            continue;
        if (strcmp(&p[plen+1], name) != 0)
            continue;
        return prop->value;
    }
    return NULL;
}

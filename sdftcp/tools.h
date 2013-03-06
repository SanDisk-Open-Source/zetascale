/*
 * File: tools.h
 * Author: Johann George
 * (c) Copyright 2009, Schooner Information Technology, Inc.
 */

#ifndef TOOLS_H
#define TOOLS_H

#include <stdarg.h>
#include <sys/types.h>


/*
 * Parameters
 * NANO - Nanoseconds in a second.
 * USEC - us in a second.
 * MSEC - ms in a second.
 */
#define MSEC     ((ntime_t)1000)
#define USEC     ((ntime_t)1000*1000)
#define NANO     ((ntime_t)1000*1000*1000)


/*
 * Macro functions.
 */
#define nel(a)      (sizeof(a)/sizeof(*(a)))
#define clear(v)    memset(&v, 0, sizeof(v))
#define streq(a, b) (strcmp(a, b) == 0)


/*
 * Type definitions.
 */
typedef int64_t ntime_t;


/*
 * Expandable structures.
 *
 *  c - Set if we want new space cleared.
 *  i - Amount used (only for strings).
 *  n - Number allocated.
 *  p - Pointer to data.
 *  s - Size.
 */
typedef struct xstr {
    void *p;
    int   s;
    int   c;
    int   i;
    int   n;
} xstr_t;


/*
 * Function prototypes.
 */
int   enabled_cpus(void);
int   xsvprint(xstr_t *xp, char *fmt, va_list alist);
void  m_free(void *ptr);
void  use_all_cpus(void);
void  xsfree(xstr_t *xp);
void  xsinit(xstr_t *xp);
void  xafree(xstr_t *xp);
void  xsprint(xstr_t *xp, char *fmt, ...);
void  strzcpy(char *dst, char *src, int len);
void  strncopyfill(char *dptr, size_t dlen, const char *sptr, size_t slen);
void  strncopynull(char *dptr, size_t dlen, const char *sptr, size_t slen);
void *strdup_q(char *str);
void *malloc_q(size_t size);
void *xasubs(xstr_t *xp, int i);
void *m_strdup(char *str, char *msg);
void *m_malloc(size_t size, char *msg);
void *malloc_z(size_t size, char *msg);
void *realloc_q(void *ptr, size_t size);
void *m_realloc(void *ptr, size_t size, char *msg);
void *xainit(xstr_t *xp, int esize, int asize, int clear);
char *semi_item(char **pp);
char *m_asprintf(char *msg, char *fmt, ...)
                 __attribute__((format(printf, 2, 3)));

#endif /* TOOLS_H */

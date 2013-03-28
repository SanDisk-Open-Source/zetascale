/*
 * Author: Johann George
 */
#include <stdio.h>
#include <stdarg.h>
#include <stdlib.h>
#include <string.h>
#include "tlib.h"
#include "xstr.h"


/*
 * Initialize an expandable string.
 */
void
xsinit(xstr_t *xp)
{
    xainit(xp, sizeof(char), 256, 0);
}


/*
 * Print to an expandable string in the manner of printf.
 */
void
xsprint(xstr_t *xp, char *fmt, ...)
{
    int s;
    va_list alist;
    
    do {
        va_start(alist, fmt);
        s = xsvprint(xp, fmt, alist);
        va_end(alist);
    } while (!s);
}


/*
 * Print to an expandable string in the manner of vsprintf.
 */
int
xsvprint(xstr_t *xp, char *fmt, va_list alist)
{
    int size;
    int left = xp->n - xp->i;
    int need = 1 + vsnprintf(&((char *)xp->p)[xp->i], left, fmt, alist);

    if (need <= left) {
        xp->i += need - 1;
        return 1;
    }

    size = xp->n * 2;
    if (size < need)
        size = need;
    xp->p = realloc_q(xp->p, size);
    xp->n = size;
    return 0;
}


/*
 * Free an expandable string.
 */
void
xsfree(xstr_t *xp)
{
    xafree(xp);
}


/*
 * Initialize an expandable array.
 *  xp    - The expandable array.
 *  esize - Size of each element.
 *  nelem - Number of elements.
 *  clear - If set, initialize new elements to 0.
 */
void *
xainit(xstr_t *xp, int esize, int nelem, int clear)
{
    int n = esize * nelem;

    xp->i = 0;
    xp->n = nelem;
    xp->s = esize;
    xp->c = clear;
    xp->p = malloc_q(n);

    if (clear)
        memset(xp->p, 0, n);
    return xp->p;
}


/*
 * Ensure that an expandable array has room to hold the given index and return
 * a pointer to that element.
 */
void *
xasubs(xstr_t *xp, int i)
{
    if (i >= xp->n) {
        int n = xp->n * 2;
        if (n <= i)
            n = i+1;
        xp->p = realloc_q(xp->p, n*xp->s);
        if (xp->c)
            memset(xp->p + (xp->n * xp->s), 0, (n-xp->n) * xp->s);
        xp->n = n;
    }
    return xp->p + i*xp->s;
}


/*
 * Free an expandable array.
 */
void
xafree(xstr_t *xp)
{
    free(xp->p);
    xp->s = 0;
    xp->i = 0;
    xp->n = 0;
    xp->p = NULL;
}

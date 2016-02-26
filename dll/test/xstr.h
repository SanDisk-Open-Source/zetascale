/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

/*
 * Author: Johann George.
 */
#ifndef XSTR_H
#define XSTR_H

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


int   xsvprint(xstr_t *xp, char *fmt, va_list alist);
void  xsfree(xstr_t *xp);
void  xsinit(xstr_t *xp);
void  xsprint(xstr_t *xp, char *fmt, ...);
void  xafree(xstr_t *xp);
void *xasubs(xstr_t *xp, int i);
void *xainit(xstr_t *xp, int esize, int asize, int clear);

#endif /* XSTR_H */

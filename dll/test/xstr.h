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

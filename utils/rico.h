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



#define	uchar	unsigned char
#define	ushort	unsigned short
#define	uint	unsigned
#define	ulong	unsigned long

#ifndef bool
#define	bool		char
#endif
#define	TRUE		(0 == 0)
#define	FALSE		(not TRUE)
#define	not		!
#define	and		&&
#define	or		||
#define	loop		while (TRUE)
#define	until(expr)	while (not (expr))
#define	unless(expr)	if (not (expr))

#ifndef nel
#define	nel( a)		(sizeof( a) / sizeof( (a)[0]))
#endif
#define	endof( a)	((a) + nel( a))
#define	bitsof( a)	(8 * sizeof( a))
#define	ctrl( c)	((c) - 0100)
#define	tab( col)	(((col)|7) + 1)
#define	trunc( n, r)	((n) - (n)%(r))
#define	roundup( n, r)	((n) - 1 - ((n)+(r)-1)%(r) + (r))
#define	divideup( a, b)	(roundup( a, b) / (b))
#define max( a, b)	((a)<(b)? (b): (a))
#define min( a, b)	((a)<(b)? (a): (b))

#ifndef streq
#define	streq( s0, s1)	(strcmp( s0, s1) == 0)
#endif

/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */



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

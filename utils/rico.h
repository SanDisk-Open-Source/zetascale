

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

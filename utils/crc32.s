

/
/ Copyright 2013 SanDisk Corporation.  All rights reserved.
/

////////
/
/ fastcrc32 - high speed CRC routine
/
/ Synposis
/
/	uint64_t fastcrc32( char bytes[], uint64_t nbyte, uint64_t level)
/
/ Description
/
/	Return a 64-bit checksum using the Intel SSE4 CRC32 instruction
/	available on all recent desktop and server CPUs.
/

	.globl	fastcrc32
	.type	fastcrc32, @function
fastcrc32:
	mov	$1, %eax
	mov	$2, %edx
	jmp	3f
2:
	crc32q	0*8(%rdi), %rax
	crc32q	1*8(%rdi), %rdx
	crc32q	2*8(%rdi), %rax
	crc32q	3*8(%rdi), %rdx
	crc32q	4*8(%rdi), %rax
	crc32q	5*8(%rdi), %rdx
	crc32q	6*8(%rdi), %rax
	crc32q	7*8(%rdi), %rdx
	add	$64, %rdi
	sub	$64, %rsi
3:	cmp	$64, %rsi
	jae	2b
	jmp	3f
2:
	crc32q	(%rdi), %rdx
	rol	$8, %rax
	xor	%rdx, %rax
	add	$8, %rdi
	sub	$8, %rsi
3:	cmp	$8, %rsi
	jae	2b
	shl	$32, %rdx
	xor	%rdx, %rax

	cmp	$4, %rsi
	jb	3f
	crc32l	(%rdi), %edx
	rol	$8, %rax
	xor	%rdx, %rax
	add	$4, %rdi
	sub	$4, %rsi
3:
	cmp	$2, %rsi
	jb	3f
	crc32w	(%rdi), %edx
	rol	$8, %rax
	xor	%rdx, %rax
	add	$2, %rdi
	sub	$2, %rsi
3:
	cmp	$1, %rsi
	jb	3f
	crc32b	(%rdi), %edx
	rol	$8, %rax
	xor	%rdx, %rax
3:
	ret


////////
/
/ check_x86_sse42 - check for SSE4.2 support
/
/ Synposis
/
/	int check_x86_sse42( void)
/
/ Description
/
/	Return non-zero if SSE4.2 is supported on this x86 processor.
/

	.globl	check_x86_sse42
	.type   check_x86_sse42, @function
check_x86_sse42:
	push	%rcx
	mov	$1, %rax
	cpuid
	mov	%rcx, %rax
	and	$1<<20, %rax	# Intel feature SSE4.2
	pop	%rcx
	ret

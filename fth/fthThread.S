#
#  File:   fthThread.S
#  Author: Jim
# 
#  Created on February 29, 2008
# 
#  (c) Copyright 2008, Schooner Information Technology, Inc.
#  http: //www.schoonerinfotech.com/
# 
#  $Id: fthThread.S,v 1.1 2009/03/23 00:35:41 jbertoni Exp jbertoni $
#
	
#
# Threading assembler
#
#

#include "fthSchedType.h"	

// Defines for assembler code
#define FTH_PC 0*8
#define FTH_RSP 1*8
#define FTH_RBX 2*8
#define FTH_RBP 3*8
#define FTH_R12 4*8
#define FTH_R13 5*8
#define FTH_R14 6*8
#define FTH_R15 7*8
#define FTH_SCHED 8*8

	.text
	
#define ENTRY(name) .globl name		     ; \
	.type name, @function		     ; \
name:
	
#define END(name)

#ifdef fthSetjmpLongjmp	

ENTRY (fthStackSwitch)
	push %rbx
	movq %rsp, %rbx
	movq %rsi, %rsp
	call fthDummy
	movq %rbx, %rsp
	pop %rbx
	ret

#endif

#ifdef fthAsmDispatch		

	.extern floatingQueue
	.extern floatingThread

ENTRY (fthToScheduler)
	movq %rbp, (FTH_RBP)(%rdi)      # save non-volatile registers
	movq %rbx, (FTH_RBX)(%rdi)
	movq %r12, (FTH_R12)(%rdi)
	movq %r13, (FTH_R13)(%rdi)
	movq %r14, (FTH_R14)(%rdi)
	movq %r15, (FTH_R15)(%rdi)
	pop  %r12			# pop the return address
	movq %rsp, (FTH_RSP)(%rdi)      # save the return stack pointer
	movq %r12, (FTH_PC)(%rdi)       # also save the return address

	movq (FTH_SCHED)(%rdi), %r12    # load the next context pointer

#
#  Once the floatingQueue is set, the thread can be redispatched, potentially,
#  so no references to the thread's data structures should be done after this
#  point.
#

	cmpq $1, %rsi                   # check whether to restore the floating
	jne  .L1                        #   thread queue
	cmpq floatingThread, %rdi       # done only if this is the floating
	jne  .L1                        #   thread
	movq %rdi, floatingQueue
.L1:
	movq %r12, %rdi                 # set the next context pointer

#
# Restore the scheduler context
#

	movq (FTH_RBX)(%rdi), %rbx
	movq (FTH_RSP)(%rdi), %rsp
	movq (FTH_RBP)(%rdi), %rbp
	movq (FTH_R12)(%rdi), %r12
	movq (FTH_R13)(%rdi), %r13
	movq (FTH_R14)(%rdi), %r14
	movq (FTH_R15)(%rdi), %r15
	jmp *(FTH_PC)(%rdi)

END (fthToScheduler)

ENTRY (fthDispatch)
	movq (FTH_RBX)(%rdi), %rbx
	movq (FTH_RSP)(%rdi), %rsp
	movq (FTH_RBP)(%rdi), %rbp
	movq (FTH_R12)(%rdi), %r12
	movq (FTH_R13)(%rdi), %r13
	movq (FTH_R14)(%rdi), %r14
	movq (FTH_R15)(%rdi), %r15
	jmp *(FTH_PC)(%rdi)
END (fthDispatch)


ENTRY (fthSaveSchedulerContext)
	pop  %rsi			# Temp pop the stack
	movq %rbx, (FTH_RBX)(%rdi)
	movq %rsp, (FTH_RSP)(%rdi)
	push %rsi			# Restore for return
	movq %rbp, (FTH_RBP)(%rdi)
	movq %r12, (FTH_R12)(%rdi)
	movq %r13, (FTH_R13)(%rdi)
	movq %r14, (FTH_R14)(%rdi)
	movq %r15, (FTH_R15)(%rdi)
	movq %rsi, (FTH_PC)(%rdi)
	retq
END (fthSaveSchedulerContext)

#endif	

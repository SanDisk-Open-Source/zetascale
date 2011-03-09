#ifndef PLATFORM_ALLOC_STACK_H
#define PLATFORM_ALLOC_STACK_H 1

/*
 * File:   sdf/platform/alloc_stack.h
 * Author: drew
 *
 * Created on March 27, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: alloc_stack.h 761 2008-03-28 02:31:04Z drew $
 */

#include "platform/defs.h"

__BEGIN_DECLS

/**
 * @brief Allocate stack with red-zone
 *
 * Malloc is used to obtain memory so that leak checkers like dmalloc do 
 * there thing.
 *
 * @param len <IN> length in bytes
 * @return bottom of stack (user probably initializes sp to ret + len)
 */
void *plat_alloc_stack(int len);

/**
 * @brief Free stack allocated with plat_alloc_stack
 *
 * @param stack <IN> stack created with plat_alloc_stack
 */
void plat_free_stack(void *stack);

__END_DECLS

#endif /* ndef PLATFORM_ALLOC_STACK */

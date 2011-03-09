/*
 * File:   fthThreadQDefines.h
 * Author: drew
 *
 * Created on August 11, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http: //www.schoonerinfotech.com/
 *
 * $Id: fthThreadQDefines.h 2791 2008-08-11 23:30:33Z drew $
 */

/*
 * The way the intrusive lists work, we need to 
 * - Define the list types
 * - Define the structure which inturn has list entry elements
 * - Define the inlines 
 *
 * This mess is easiest when an idempotent #define part of the lll
 * goo exists.
 */

// Linked list definitions for threads
#undef LLL_NAME
#undef LLL_EL_TYPE
#undef LLL_EL_FIELD
#undef LLL_INLINE

#define LLL_NAME(suffix) fthThreadQ ## suffix
#define LLL_EL_TYPE struct fthThread
#define LLL_EL_FIELD threadQ



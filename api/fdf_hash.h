/*
 * File:   utils/hash.h
 * Author: Jim
 *
 * Created on March 3, 2008, 1:38 PM
 *
 * SanDisk Proprietary Material, © Copyright 2012 SanDisk, all rights reserved.
 * http://www.sandisk.com
 *
 * $Id: hash.c 399 2008-02-28 23:46:15Z darpan $
 */

#ifndef __HASH_H
#define __HASH_H

#include <stdint.h>
#include <inttypes.h>

//
// Header file for Bob Tuttles lookup8 hash algorithm
//
// This is fast on 64-bit machines.  Just mask off the bits you need for shorter keys.
//

// Level is an arbitrary salt for the hash.
uint64_t zs_hash(const unsigned char *key, uint64_t keyLength, uint64_t level);

#endif

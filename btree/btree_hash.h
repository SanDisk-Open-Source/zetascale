/*
 * File:   utils/hash.h
 * Author: Jim
 *
 * Created on March 3, 2008, 1:38 PM
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: hash.c 399 2008-02-28 23:46:15Z darpan $
 */

#ifndef __BTREE_HASH_H
#define __BTREE_HASH_H

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <inttypes.h>

//
// Header file for Bob Tuttles lookup8 hash algorithm
//
// This is fast on 64-bit machines.  Just mask off the bits you need for shorter keys.
//

// Level is an arbitrary salt for the hash.
uint64_t btree_hash_int(const unsigned char *key, uint64_t keyLength, uint64_t level);
uint64_t btree_hash(const unsigned char *key, uint64_t keyLength, uint64_t level, uint64_t cguid);

#endif

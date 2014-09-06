/*
 * (c) Copyright 2008-2013, SanDisk Corporation.  All rights reserved.
 */
#ifndef __HASH_H
#define __HASH_H

uint64_t hashb(const unsigned char *key, uint64_t keyLength, uint64_t level);
uint64_t fastcrc32(const unsigned char *key, uint64_t keyLength, uint64_t level);
uint32_t checksum(char* buf, uint64_t length, uint64_t seed);

#endif

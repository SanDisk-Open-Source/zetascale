/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

/*
 * (c) Copyright 2008-2013, SanDisk Corporation.  All rights reserved.
 */
#ifndef __HASH_H
#define __HASH_H

uint64_t hashb(const unsigned char *key, uint64_t keyLength, uint64_t level);
uint64_t fastcrc32(const unsigned char *key, uint64_t keyLength, uint64_t level);
uint32_t checksum(char* buf, uint64_t length, uint64_t seed);

#endif

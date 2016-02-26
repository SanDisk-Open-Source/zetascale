/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

/*
 * File:   ssd/fifo/mcd_hash.h
 * Author: Bob Jenkins/Xiaonan Ma/Brian O'Krafka
 *
 * Created on Sept 1, 2010
 *
 * (c) Copyright 2010, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: mcd_hash.h 13657 2010-05-16 03:01:47Z briano $
 */

#ifndef __MCD_HASH_H__
#define __MCD_HASH_H__

extern uint32_t mcd_hash( const void     *key,       /* the key to hash */
			  size_t          length,    /* length of the key */
		          const uint32_t  initval);  /* initval */
#endif  /* __MCD_HASH_H__ */

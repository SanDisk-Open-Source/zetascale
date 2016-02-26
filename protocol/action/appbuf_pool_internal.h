/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

/*
 * File:   appbuf_pool_internal.h
 * Author: Brian O'Krafka
 *
 * Created on March 3, 2009
 *
 * (c) Copyright 2009, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: appbuf_pool_internal.h 802 2008-03-29 00:44:48Z darpan $
 */

#ifndef _APPBUF_POOL_INTERNAL_H
#define _APPBUF_POOL_INTERNAL_H

#ifdef __cplusplus
extern "C" {
#endif

typedef struct SDF_appBufState {
   SDF_appBufProps_t    props;
   uint64_t             nget;
   uint64_t             nfree;
} SDF_appBufState_t;

#ifdef	__cplusplus
}
#endif

#endif /* _APPBUF_POOL_INTERNAL_H */

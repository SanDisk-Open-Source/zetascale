/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

/*
 * File:   appbuf_pool.h
 * Author: Brian O'Krafka
 *
 * Created on March 3, 2009
 *
 * (c) Copyright 2009, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: appbuf_pool.h 802 2008-03-29 00:44:48Z darpan $
 */

#ifndef _APPBUF_POOL_H
#define _APPBUF_POOL_H

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
    int     dummy;  /* placeholder */
} SDF_appBufProps_t;

struct SDF_appBufState;

extern int init_app_buf_pool(struct SDF_appBufState **ppabs, SDF_appBufProps_t *pprops);
extern void destroy_app_buf_pool(struct SDF_appBufState *pabs);
extern void *get_app_buf(struct SDF_appBufState *pabs, uint64_t size);
extern int free_app_buf(struct SDF_appBufState *pabs, void *pbuf);
extern void get_app_buf_pool_stats(struct SDF_appBufState *pabs, uint64_t *pnget, uint64_t *pnfree);

#ifdef	__cplusplus
}
#endif

#endif /* _APPBUF_POOL_H */

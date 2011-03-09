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

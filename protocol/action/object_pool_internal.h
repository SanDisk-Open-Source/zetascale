/*
 * File:   object_pool_internal.h
 * Author: Brian O'Krafka
 *
 * Created on March 3, 2009
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: object_pool_internal.h 308 2008-02-20 22:34:58Z tomr $
 */

#ifndef _OBJECT_POOL_INTERNAL_H
#define _OBJECT_POOL_INTERNAL_H

#ifdef __cplusplus
extern "C" {
#endif

struct SDF_bufpool {
    int                   dummy;
    SDF_bufpool_props_t   props;
};
typedef struct SDF_bufpool SDF_bufpool_t;

#ifdef	__cplusplus
}
#endif

#endif /* _OBJECT_POOL_INTERNAL_H */

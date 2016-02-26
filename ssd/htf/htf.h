/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

/*
 * File:   htf.h
 * Author: Brian O'Krafka
 *
 * Created on January 8, 2009
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: htf.h 308 2008-02-20 22:34:58Z briano $
 */

#ifndef _HTF_H
#define _HTF_H

#ifdef __cplusplus
extern "C" {
#endif

/* put htf-specific stuff here */

extern struct flashDev *htf_flashOpen(char *name, int flags);
extern struct shard *htf_shardCreate(struct flashDev *dev, uint64_t shardID, 
	  int flags, uint64_t quota, unsigned maxObjs);
extern int htf_flashGet(struct ssdaio_ctxt *pctxt, struct shard *shard, struct objMetaData *metaData, 
	  char *key, char **dataPtr, int flags);
extern int htf_flashPut(struct ssdaio_ctxt *pctxt, struct shard *shard, struct objMetaData *metaData, 
	  char *key, char *data, int flags);

#ifdef	__cplusplus
}
#endif

#endif /* _HTF_H */

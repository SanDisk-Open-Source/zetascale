/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

/************************************************************************
 *
 * File:   clipper_aio.h
 * Author: Brian O'Krafka
 *
 * Created on January 8, 2009
 *
 * (c) Copyright 2009, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: clipper_aio.h 308 2008-02-20 22:34:58Z briano $
 ************************************************************************/

#ifndef _CLIPPER_AIO_H
#define _CLIPPER_AIO_H

#ifdef __cplusplus
extern "C" {
#endif

typedef struct clipper_aio_state {
    uint64_t       size;
    int            fildes;
} clipper_aio_state_t;

struct flashDev;

extern int clipper_aio_init(clipper_aio_state_t *psas, char *devName);
extern int clipper_aio_read_flash(struct flashDev *dev, char *pbuf, uint64_t offset, uint64_t size);
extern int clipper_aio_write_flash(struct flashDev *dev, char *pbuf, uint64_t offset, uint64_t size);

#ifdef	__cplusplus
}
#endif

#endif /* _CLIPPER_AIO_H */


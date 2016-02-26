/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

/************************************************************************
 *
 * File:   ssd_aio_local.h
 * Author: Brian O'Krafka
 *
 * Created on January 8, 2009
 *
 * (c) Copyright 2009, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: ssd_aio.h 308 2008-02-20 22:34:58Z briano $
 ************************************************************************/

#ifndef _SSD_AIO_LOCAL_H
#define _SSD_AIO_LOCAL_H

#ifdef __cplusplus
extern "C" {
#endif

// #define SSD_AIO_ALIGNMENT      512
#define SSD_AIO_ALIGNMENT      4096

struct ssdaio_state {
    uint64_t       size;
    int            fildes;
};

struct ssdaio_ctxt {
    uint64_t    dummy;
    // xxxzzz Xiaonan:  add your stuff here 
};

#ifdef	__cplusplus
}
#endif

#endif /* _SSD_AIO_LOCAL_H */


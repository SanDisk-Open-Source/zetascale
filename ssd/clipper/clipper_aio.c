/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

/************************************************************************
 *
 * File:   clipper_aio.c    Functions to Read/Write SSD Flash Drives
 * Author: Brian O'Krafka
 *
 * Created on January 8, 2009
 *
 * (c) Copyright 2009, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: clipper_aio.c 308 2008-02-20 22:34:58Z briano $
 ************************************************************************/

#define _CLIPPER_AIO_C

#include <fcntl.h>
#include <stdint.h>
#include <pthread.h>
#include <sched.h>
#include <stdio.h>
#include <stdarg.h>
#include <aio.h>
#include "platform/logging.h"
#include "fth/fth.h"
#include "ssd/ssd.h"
#include "ssd/ssd_aio.h"
#include "ssd/ssd_aio_local.h"
#include "ssd/ssd_local.h"
#include "utils/hash.h"
#include "flash/flash.h"
#include "platform/logging.h"
#include "clipper_aio.h"
#include "utils/hash.h"

int clipper_aio_init(clipper_aio_state_t *psas, char *devName)
{
    // open the file for the SSD
    psas->fildes = open(devName, O_RDWR | O_DIRECT | O_CREAT, S_IRUSR | S_IWUSR);
    if (psas->fildes == -1) {
	plat_log_msg(21658, PLAT_LOG_CAT_FLASH, PLAT_LOG_LEVEL_FATAL,
		     "Could not open SSD file: %s - %s", devName, plat_strerror(errno));
	return(-1);
    }
    // psas->size = 16ULL*1024*1024*1024;  // xxxzzz fix this!
    psas->size = 1ULL*1024*1024*1024;  // xxxzzz fix this!
    return(0);
}

int clipper_aio_write_flash(struct flashDev *pdev, char *pbuf, uint64_t offset, uint64_t size)
{
    struct aiocb           acb;
    int                    errstatus;
    ssize_t                ret;
    clipper_aio_state_t   *pcas;

    pcas = (clipper_aio_state_t *) pdev->paio_state;

    (void) memset((void *) &acb, 0, (size_t) sizeof(acb));
    acb.aio_fildes     = pcas->fildes;
    acb.aio_lio_opcode = LIO_WRITE; // ignored
    acb.aio_reqprio    = 0; // no priority change
    acb.aio_buf        = pbuf;
    acb.aio_nbytes     = size;
    acb.aio_sigevent.sigev_notify = SIGEV_NONE;
    acb.aio_offset     = offset;

    if (aio_write(&acb) != 0) {
	fprintf(stderr, "aio_write failed\n");
	return(FLASH_EAGAIN);
    }

    while (1) {
	/* yield then check if write is complete */

	// fthNanoSleep(10000); /* nanosec */
	fthYield(1);  // xxxzzz try different values here

	errstatus = aio_error(&acb);
	if (errstatus == 0) {
	    break;
	} else if (errstatus == EINPROGRESS) {
	    continue;
	} else {
	    fprintf(stderr, "aio_write failed with errstatus=%d ('%s')\n", errstatus, plat_strerror(errstatus));
	    return(FLASH_EAGAIN);
	}
    }
    if ((ret = aio_return(&acb)) != size) {
	fprintf(stderr, "aio_write failed with return status %"PRIu64", errno: '%s' (%d)\n", ret, plat_strerror(errno), errno);
	return(FLASH_EAGAIN);
    }

    // fprintf(stderr, "flashPut succeeded: fd=%d, offset=%d, size=%d\n", acb.aio_fildes, acb.aio_offset, acb.aio_nbytes);

    return(FLASH_EOK);
}

int clipper_aio_read_flash(struct flashDev *pdev, char *pbuf, uint64_t offset, uint64_t size)
{
    struct aiocb           acb;
    int                    errstatus;
    ssize_t                ret;
    clipper_aio_state_t   *pcas;

    pcas = (clipper_aio_state_t *) pdev->paio_state;

    (void) memset((void *) &acb, 0, (size_t) sizeof(acb));
    acb.aio_fildes     = pcas->fildes;
    acb.aio_lio_opcode = LIO_READ; // ignored
    acb.aio_reqprio    = 0; // no priority change
    acb.aio_buf        = pbuf; // xxxzzz what are alignment requirements here?
    acb.aio_nbytes     = size;
    acb.aio_sigevent.sigev_notify = SIGEV_NONE;
    acb.aio_offset     = offset;

    if (aio_read(&acb) != 0) {
	return(1);
    }

    while (1) {
	/* yield then check if read is complete */

	// fthNanoSleep(10000); /* nanosec */
	fthYield(1);  // xxxzzz try different values here

	errstatus = aio_error(&acb);
	if (errstatus == 0) {
	    break;
	} else if (errstatus == EINPROGRESS) {
	    continue;
	} else {
	    // fprintf(stderr, "aio_read failed with errstatus=%d ('%s')\n", errstatus, plat_strerror(errstatus));
	    return(errstatus);
	}
    }
    if ((ret = aio_return(&acb)) != size) {
	// fprintf(stderr, "aio_read failed with return status %d, errno: '%s' (%d)\n", (int)ret, plat_strerror(errno), errno);
	return(ret);
    }

    return(0);
}







/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

/*
 * File:   XMbox.h
 * Author: Jim
 *
 * Created on February 29, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http:                                     //www.schoonerinfotech.com/
 *
 * $Id: XMbox.h 396 2008-02-29 22:55:43Z jim $
 */

/**
 * @brief Cross-thread mailbox.  This is the FTH-callable version.
 */

#include <pthread.h>

#include "platform/shmem.h"
#include "sdfappcommon/XMbox.h"

shmem_void_t ftopMboxWait(ftopMbox_t *xmbox);
shmem_void_t ftopMboxTry(ftopMbox_t *xmbox);
void ptofMboxPost(ptofMbox_sp_t xmboxShmem, shmem_void_t mailShmem);




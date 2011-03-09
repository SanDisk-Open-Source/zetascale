/*
 * File:   fthXMbox.h
 * Author: Jim
 *
 * Created on February 29, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http:                                     //www.schoonerinfotech.com/
 *
 * $Id: fthXMbox.h 396 2008-02-29 22:55:43Z jim $
 */

/**
 * @brief Cross-thread mailbox.  This is the FTH-callable version.
 */

// Include the common portion
#include "sdfappcommon/XMbox.h"

// Routine definitions
void ftopMboxPost(ftopMbox_t *xmbox, shmem_void_t mailShmem);
shmem_void_t ptofMboxWait(ptofMbox_t *mb);
shmem_void_t ptofMboxTry(ptofMbox_t *mb);

/*
 * File:   XResume.h
 * Author: Jim
 *
 * Created on February 29, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http:                                     //www.schoonerinfotech.com/
 *
 * $Id: XMbox.h 396 2008-02-29 22:55:43Z jim $
 */

#ifndef _SDFAPPCOMMON_X_RESUME
#define _SDFAPPCOMMON_X_RESUME

/**
 * @brief Cross-thread mailbox.  This is the common portion (both threads)
 */

#include "fth/fth.h"
#include "fth/fthThread.h"

#include "XList.h"


struct ptofThreadPtrs;

// Use a non-shmem version for now since shmem-based fthThread pointers do not exist
typedef struct ptofThreadPtrs {
    struct fthThread *head;
    struct fthThread *tail;
} ptofThreadPtrs_t;

XLIST_H(XResume, fthThread, resumeNext);

void XResume(struct fthThread *thread, uint64_t arg);
struct fthThread *XSpawn(void (*startRoutine)(uint64_t), long minStackSize);

#endif

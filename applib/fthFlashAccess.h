//----------------------------------------------------------------------------
// ZetaScale
// Copyright (c) 2016, SanDisk Corp. and/or all its affiliates.
//
// This program is free software; you can redistribute it and/or modify it under
// the terms of the GNU Lesser General Public License version 2.1 as published by the Free
// Software Foundation;
//
// This program is distributed in the hope that it will be useful, but WITHOUT
// ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
// FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License v2.1 for more details.
//
// A copy of the GNU Lesser General Public License v2.1 is provided with this package and
// can also be found at: http://opensource.org/licenses/LGPL-2.1
// You should have received a copy of the GNU Lesser General Public License along with
// this program; if not, write to the Free Software Foundation, Inc., 59 Temple
// Place, Suite 330, Boston, MA 02111-1307 USA.
//----------------------------------------------------------------------------

/*
 * File:   fthFlashAccess.c
 * Author: Darpan Dinker
 *
 * Created on October 3, 2008, 3:40 AM
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id$
 */

#ifndef _FTHFLASHACCESS_H
#define	_FTHFLASHACCESS_H

#ifdef	__cplusplus
extern "C" {
#endif

#include "platform/shmem.h"

/**
 * Mailbox for queue shared between Pthreads and Fth (workers).
 */
typedef struct mbox_q {
    ptofMbox_sp_t mbox_shmem;
} FIO_Q;

/** Flash IO scoreboard entry : interface between an application Pthread and an
Fth */
struct FIOEntry {
    char op;
    shard_t *shard;
    char *key;
    int keyLen;
    char *dataIn;
    char **dataOut;
    int dataLen; // in or out
    int retStatus;

    ftopSimpleSignal_t sb_wait_mbox;
};

PLAT_SP(FIOEntry_sp, struct FIOEntry);

////////////////////////////////////////////////////////////////////////////////

/**
 * Initialize the data-structures, shmem, Fth runtime, etc. needed to access 
 * Flash. Need to call once only.
 * @param fthSchedNum <IN>, number of Fth schedulers
 * @param fthNum <IN>, number of Fth per Fth scheduler
 */
int initFthFlashAccess(unsigned fthSchedNum, unsigned fthNum);

int pThreadGetObject(char *key, int keyLen, char **data, int *dataLen);
int pThreadPutObject(char *key, int keyLen, char *data, int *dataLen);        

#ifdef	__cplusplus
}
#endif

#endif	/* _FTHFLASHACCESS_H */


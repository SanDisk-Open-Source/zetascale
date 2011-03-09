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

#ifndef _SDFAPPCOMMON_X_MBOX
#define _SDFAPPCOMMON_X_MBOX

/**
 * @brief Cross-thread mailbox.  This is the common portion (both threads)
 */

#include <pthread.h>
#include <semaphore.h>

#include "platform/shmem.h"

#include "XList.h"

//
// SHEMEM ssetup

struct XMboxEl;
PLAT_SP(XMboxEl_sp, struct XMboxEl);

struct ptofMboxPtrs;
PLAT_SP(ptofMboxPtrs_sp, struct ptofMboxPtrs);

struct ftopMbox;
PLAT_SP(ftopMbox_sp, struct ftopMbox);

struct ptofMbox;
PLAT_SP(ptofMbox_sp, struct ptofMbox);

//
// Structure for FtoP mailbox elements
typedef struct XMboxEl {
    shmem_void_t mailShmem;                  // Arbitrary mail entity (generic SHMEM type)
    XMboxEl_sp_t nextShmem;                  // Next element
    union {
        ftopMbox_sp_t ftopMboxShmem;         // Owning mailbox
        ptofMbox_sp_t ptofMboxShmem;
    };
} XMboxEl_t;

XLIST_SHMEM_H(XMboxEl, XMboxEl, nextShmem);

//
// Structure to hold the head/tail shmem pointers for the PTOF global queue
typedef struct ptofMboxPtrs {
    XMboxEl_sp_t headShmem;
    XMboxEl_sp_t tailShmem;
} ptofMboxPtrs_t;

#include "fth/fth.h"
#include "fth/fthSpinLock.h"
#include "fth/fthThreadQ.h"

// Base structure for cross-thread mailbox (Pthread consumer, FTH thread producer)
typedef struct ftopMbox {
    pthread_mutex_t pthreadOrder;            // For ordering of Pthreads (consumers)
    sem_t lock[3];                           // For Pthread/Fthread handshake

    XMboxEl_sp_t headShmem;                  // Head of MBOX list (consumer pointer)
    XMboxEl_sp_t tailShmem;                  // Tail of MBOX list (producer pointer)

    int fthState;                            // State of the FTH locking
    fthSpinLock_t fthSpin;                   // Spinlock for FTH (producer) lock
} ftopMbox_t;

typedef struct ptofMbox {
    fthSpinLock_t *spin;
    int pending;                             // Mail for this MB queued to global list
    fthThreadQ_lll_t threadQ;
    XMboxEl_sp_t mailHeadShmem;              // Head of MBOX list (consumer pointer)
    XMboxEl_sp_t mailTailShmem;              // Tail of MBOX list (producer pointer)
} ptofMbox_t;


// Routines
void ftopMboxInit(ftopMbox_t *xmbox);
int ftopMboxDestroy(ftopMbox_t *xmbox);
void ptofMboxInit(ptofMbox_t *xmbox);
int ptofMboxDestroy(ptofMbox_t *xmbox);
#endif

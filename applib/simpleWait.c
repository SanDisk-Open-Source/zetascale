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

#include <errno.h>

#include "simpleWait.h"

void
ftopSimpleWait(ftopSimpleSignal_t * sig)
{
    int ret;

    do {
        ret = sem_wait(&sig->sem);
    } while (ret && (errno == EINTR || errno == EAGAIN));

    plat_assert(ret==0);
}

void
ftopSimplePost(ftopSimpleSignal_t * sig)
{
    sem_post(&sig->sem);
}

void
ftopSimpleSignalInit(ftopSimpleSignal_t * sig)
{
    plat_assert(sig);
    /* pshared =1 => good for IPC, value = 0  =>locked state) */
    int ret = sem_init(&sig->sem, 1, 0);
    plat_assert(ret==0);
}

void
ftopSimpleSignalDestroy(ftopSimpleSignal_t * sig)
{
    plat_assert(sig);
    int ret = sem_destroy(&sig->sem);
    plat_assert(ret==0);
}

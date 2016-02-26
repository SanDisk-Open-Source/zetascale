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


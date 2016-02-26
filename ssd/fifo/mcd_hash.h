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
 * File:   ssd/fifo/mcd_hash.h
 * Author: Bob Jenkins/Xiaonan Ma/Brian O'Krafka
 *
 * Created on Sept 1, 2010
 *
 * (c) Copyright 2010, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: mcd_hash.h 13657 2010-05-16 03:01:47Z briano $
 */

#ifndef __MCD_HASH_H__
#define __MCD_HASH_H__

extern uint32_t mcd_hash( const void     *key,       /* the key to hash */
			  size_t          length,    /* length of the key */
		          const uint32_t  initval);  /* initval */
#endif  /* __MCD_HASH_H__ */

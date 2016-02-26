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

#ifndef PLATFORM_ERROR_CONTROL_INTERNAL_H
#define PLATFORM_ERROR_CONTROL_INTERNAL_H 1

/*
 * File:   $URL: svn://svn.schoonerinfotech.net/schooner-trunk/trunk/sdf/platform/aio_error_control_internal.h $
 * Author: drew
 *
 * Created on March 8, 2010
 *
 * (c) Copyright 2010, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: aio_error_control_internal.h 13831 2010-05-25 02:41:49Z drew $
 */

/**
 * @brief Entry points for error control apis
 *
 * Instances may put a #paio_error_control structure in their state
 * and translate using #PLAT_DEPROJECT to type-pun.
 */
struct paio_error_control {
    /** @brief Set an error */
    int (*set_error)(struct paio_error_control *error_control,
                     enum paio_ec_type error_type, const char *filename,
                     off_t start, off_t len);
};

#endif /* ndef PLATFORM_ERROR_CONTROL_INTERNAL_H */

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

#ifndef OPTS_ENUM_H
#define OPTS_ENUM_H 1

/*
 * File:   sdf/platform/opts.h
 * Author: drew
 *
 * Created on February 2, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: opts_enum.h 12469 2010-03-24 04:29:31Z drew $
 */

/**
 * Enum values for sdf/platform/opts.h which are being extracted
 * for separate use in memcached.c
 */

/** @brief option options */
enum plat_opts_opt_options {
    /** @brief Option takes no argument (default) */
    PLAT_OPTS_ARG_NO = 0,
    /** @brief Option requires an argument */
    PLAT_OPTS_ARG_REQUIRED = 1,
    /** @brief Option has optional argument (default) */
    PLAT_OPTS_ARG_OPTIONAL = 2,

    PLAT_OPTS_ARG_BITS = 0x3,

    /** @brief Option is optional (default) */
    PLAT_OPTS_OPT_OPTIONAL = 0,
    /** @brief Option is required */
    PLAT_OPTS_OPT_REQUIRED = 4,

    PLAT_OPTS_OPT_BITS = 0x4
};

#endif /* ndef OPTS_ENUM_H */

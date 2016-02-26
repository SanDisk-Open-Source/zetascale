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

#ifndef PLATFORM_PLATEXITS_H
#define PLATFORM_PLATEXITS_H 1

/*
 * File:   sdf/platform/platexits.h
 * Author: drew
 *
 * Created on March 9, 2008, 8:00 PM
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: platexits.h 3566 2008-09-25 08:39:07Z drew $
 */

/**
 * Standard exit codes.
 */
enum plat_exits {
    /** Normal termination */
    PLAT_EXIT_OK = 0,
    /** Failed, but can be restarted */
    PLAT_EXIT_FAILED_RESTART = 1,
    /**
     * Failed, restart all processes including those not started by executord
     * but which are clients of the system.
     */
    PLAT_EXIT_FAILED_RESTART_ALL = 2,
    /**
     * Failed, reboot required for recovery (device may be in inconsistent
     * state).
     */
    PLAT_EXIT_FAILED_REBOOT = 3,
    /**
     * Failed, operator intervention required (config/command line arg
     * problem, etc.)
     */
    PLAT_EXIT_FAILED_OPERATOR = 4
};

#endif /* ndef PLATFORM_PLATEXITS_H */

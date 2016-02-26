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
 * File:   platform_check.h
 * Author: darryl
 *
 * Created on  February 17, 2010, 10:35 AM
 *
 * Copyright (c) 2010 Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 */


/* <PREFACE>
 * Check for a valid hardware platform.
 * Intended for use by applications that must only run on
 * authorized hardware.
 * 
 * Returns 1 if ok to run, 0 if not ok.
 */
#include <stdio.h>

int is_valid_schooner_platform();
int test_schooner_platform_check();

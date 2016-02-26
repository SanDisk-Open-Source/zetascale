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

#ifndef REPLICATION_TEST_API_H
#define REPLICATION_TEST_API_H 1

/*
 * File: sdr/protocol/replication/tests/test_api.h
 *
 * Author: drew
 *
 * Created on October 31, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: test_api.h 5283 2008-12-24 02:45:30Z xwang $
 */

/**
 * Test infrastructure api
 */
#include "protocol/replication/replicator.h"

/**
 * @brief Callback interface for async flash API.
 *
 * send_msg, timer_dispatcher, and gettime() may be used for scheduling.
 *
 * XXX: This is expedient
 */
typedef struct replicator_api replication_test_flash_api_t;

#endif /* ndef REPLICATION_TEST_API_H */

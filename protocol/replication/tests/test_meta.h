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

#ifndef REPLICATION_TEST_META_H
#define REPLICATION_TEST_META_H 1

/*
 * File: sdr/protocol/replication/tests/test.h
 *
 * Author: drew
 *
 * Created on October 31, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: test_meta.h 5283 2008-12-24 02:45:30Z xwang $
 */

/**
 * Replication test operation meta-data
 */

#include "platform/string.h"
#include "platform/time.h"


struct replication_test_meta {
    /** @brief Expiration time since the epoch in seconds */
    SDF_time_t exptime;

    /**
     * @brief Object creation time since the epoch in seconds
     *
     * This must be <= flushtime.
     */
    SDF_time_t createtime;

    /** @brief Object expiration time from the epoch, 0 for none */
    struct timeval exptime_val;
};

static void 
replication_test_meta_init(struct replication_test_meta *meta) {
    memset(meta, 0, sizeof (*meta));
}


#endif /* ndef REPLICATION_TEST_META_H */

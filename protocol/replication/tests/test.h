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

#ifndef REPLICATION_TEST_H
#define REPLICATION_TEST_H 1

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
 * $Id: test.h 5283 2008-12-24 02:45:30Z xwang $
 */

/**
 * Test definition.
 *
 * XXX: drew 2008-11-4 In hindsight, I don't think the test needs this
 * to integrate with the frame-work.  Ad-hoc use of the sleep functions
 * should be entirely sufficient.
 */

#include "platform/closure.h"
#include "platform/defs.h"
#include "platform/time.h"

struct replication_test;


enum replication_test_event_type {
    /** @brief Event returned */
    RT_EVENT_EVENT,

    /** @brief No more events */
    RT_EVENT_TEST_DONE,

    /** @brief Wait for next event */
    RT_EVENT_WAIT
}

/** @brief Replication test event callback */
PLAT_CLOSURE1(replication_test_event_cb,
              struct replication_test *, test);

struct replication_test_event {
    /** @brief When the event should happen */
    struct timeval when;

    /** @brief What to invoke */
    replication_test_event_cb_t what;
};

/**
 * @brief Replication test state
 *
 * Users may include this structure at the front of their test state.
 */
struct replication_test {
    /**
     * @brief  Get asynchronous event from test.
     *
     * Simple tests may have a single event which wakes up their
     * test thread at T=0 and change the state to return when the
     * test should terminate.
     *
     * Events must be scheduled for a point in time after the
     * previous.
     *
     * @return Whether an event exists; *event is only set if there is a
     * next event at this point in time.
     */
    enum replication_test_event_type
        (*get_next_fn)(struct replication_test *test,
                       struct replication_test_event *event);

    /** @brief Test framework associated with this test */
    struct replication_test_framework *framework;
};

#endif /* ndef REPLICATION_TEST_H */

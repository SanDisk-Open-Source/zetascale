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

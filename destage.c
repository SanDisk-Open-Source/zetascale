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


write()
{
    - states of nvram buffers:
	- free
        - batch_queued
        - queued
	- processing
	- crash-safe
    - copy data to statically allocated (optionally nvram) buffer
    if (batch_commit) {
	- set to state "batch_queued"
	- put mail in batch request queue
    } else {
	- set to state "queued"
	- put mail in non-batch request queue
    }
    - wait for response
    - set state to "free"
}

read()
{
    - hack for now: just do a ZS read
        - may return stale data if a queued write is in progress
}

nonbatch_write_queue_handler() [there is a pool of these]
{
    while (1) {
        - retrieve write request
	- set state to "processing"
	- send ack to requester
	- do ZS write
	- set state to "free"
    }
}

batch_write_queue_handler() [there is one of these]
{
    while (1) {
        - sleep for batch window
        - retrieve all pending write requests
	- set to state "queued"
	- pass all requests to nonbatch request queue
	- wait for acks from all requests
	- send ack to requester
    }
}

========================
read lookaside buffer:
========================

- just use existing lat table?

========================
ordering issues:
========================

- on writes:
    - at enqueue: create_start (must-not-exist), create_end 
        - write lock is held until create_end
    - at dequeue (write is complete and durable):
        - write_start, release buffer, write_end_and_delete
        - write lock is held until end_and_delete 
    - should not have to hold locks because app must enforce isolation
    - "must-not-exist" check will detect isolation violation

- transactions:
    - requests from a particular therad must all go to the same
      write handler thread (ZS assumes this for transaction processing)
        - this also enforces proper write ordering per client thread

- on reads:
    - read_start
    - if hit:
        - get data from buffer
	- read_end
	- read lock is held until read_end
    - if miss:
        - get data from ZS
	- no read lock held after call to read_start with a miss

- with write destaging, can lower-level ZS logging be eliminated?
    - requires txn start/commit entries in destaging buffer
    - how about collecting entire txn in destage buffer before
      proceeding?
        - still need lower level txn's for btree

- multi-instance for performance:
    - problem: txn's across instances!

- range query:
    - xxxzzz TBD




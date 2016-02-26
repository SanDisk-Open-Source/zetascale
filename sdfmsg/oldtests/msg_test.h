/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

#ifndef MSG_TEST_H_
#define MSG_TEST_H_
#include "sdfmsg/sdf_msg_types.h"

enum TRACE_LEVEL {concisetrace = 0, generaltrace = 1, detailtrace = 2};


/*
 * protocols and its buffer size
 * MSG_TRANSPORT=0 binsize=128
 * SDF_SYSTEM=1 binsize=256
 * SDF_CONSISTENCY=2 binsize=4096
 * SDF_MANAGEMENT=3 binsize=256
 * SDF_MEMBERSHIP=4 binsize=256
 * SDF_FLSH=5 binsize=256
 * SDF_METADATA=6 binsize=256
 * SDF_REPLICATION=7 binsize=256
 * SDF_SHMEM=8 binsize=256
 * SDF_RESPONSES=9 binsize=4096
 * SDF_3RDPARTY=10 binsize=4096
 * SDF_FINALACK=11 binsize=256
 * SDF_UNEXPECT=12 binsize=128
*/

void * ConsistencyPthreadRoutine(void *arg);
void * ManagementPthreadRoutine(void *arg);
void * SystemPthreadRoutine(void *arg);
void * MembershipPthreadRoutine(void *arg);
void * FlushPthreadRoutine(void *arg);
void * MetadataPthreadRoutine(void *arg);
void * ReplicationPthreadRoutine(void *arg);
void * OrderTestFthPthreadRoutine(void *arg);
void * OrderTestPthreadRoutine(void *arg);
void * BigSizePthreadRoutine(void *arg);
void * BigSizeFthPthreadRoutine(void *arg);
void * ReceiveQueuePthreadRoutine(void *arg);
void * SendQueuePthreadRoutine(void *arg);
void * SimplexSendReceiveRoutine(void *arg);
void * ThroughputThreadRoutine(void *arg);

struct sdf_queue_pair * local_create_myqpairs(service_t protocol,
        uint32_t myid, uint32_t pnode);

void TestTrace(int tracelevel, int selflevel, const char *format, ... );
#endif /*MSG_TEST_H_*/

/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

/*
* File:   fast_logtest.c
* Author: Wei,Li
*
* Created on Nov 25, 2008
*
* (c) Copyright 2008, Schooner Information Technology, Inc.
* http://www.schoonerinfotech.com/
*
*/

/*
* Trivial test for fast log.
*/
#include <pthread.h>
#include <sys/time.h>
#include "platform/rwlock.h"

#include "platform/stdio.h"
#include "platform/string.h"
#include "platform/stdlib.h"
#include "platform/spin_rw.h"
#include "platform/util_trace.h"
#include "platform/types.h"

#define NUM_PTHREADS 7 
#define NUM_LOGS 100
static unsigned long long time_so_far()
{ 
	struct timeval tp;
	if (gettimeofday(&tp, (struct timezone *) NULL) == -1)
		perror("gettimeofday");
	return (tp.tv_sec*1000000 + tp.tv_usec);
}
void *pthreadLogger(void *arg) {
	for(int i=0;i<NUM_LOGS;i++)
	{
		int size=64;
		trace_content_t trace={0,1,2,9,1,i,3,24};
                trace.bytes = size;
		printf_fast(&trace);
	}
	printf(".");
	return 0;
}
int main(int argc, char **argv) {
	pthread_t pthread_loggers[NUM_PTHREADS];
	unsigned long long start=time_so_far();
	for (uint64_t i = 0; i < NUM_PTHREADS; i++) {
		pthread_create(&pthread_loggers[i], NULL, &pthreadLogger, NULL);
	}

	for (uint64_t i = 0; i < NUM_PTHREADS; i++) {
		pthread_join(pthread_loggers[i], NULL);
	}
	printf("%.3f us per log\n",((double)(time_so_far()-start))/(NUM_LOGS)/(NUM_PTHREADS));
	return (0);
}

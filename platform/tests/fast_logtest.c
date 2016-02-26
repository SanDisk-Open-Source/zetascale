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

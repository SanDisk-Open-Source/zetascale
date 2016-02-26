/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

#include <unistd.h>
#include <assert.h>
#include <fcntl.h>
#include "zs.h"

int main() 
{
	char fname[1024];
	struct ZS_state* zs_state;
    // Trx are incompatible with GC, so switching them off
    ZSSetProperty("ZS_TRX", "0");

    ZS_status_t ret = ZSInit(&zs_state);
	assert(ret);
	sprintf(fname, "/tmp/zs_listen_pid.%d", getpid());
	int r = open(fname, O_CREAT, 0660);
	assert(r > 0);
	sleep(60);
    (void)ZSShutdown(zs_state);
}


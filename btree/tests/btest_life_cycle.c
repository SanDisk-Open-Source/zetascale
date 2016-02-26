/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

/************************************************************************
 * 
 *  btest_life_cycle.c  Jan. 21, 2013   Harihara Kadayam
 * 
 *  Built-in self-Test program for btree package.
 * 
 * NOTES: xxxzzz
 * 
 ************************************************************************/

#include <stdio.h>
#include <stdlib.h>
#include "btest_common.h"

int main(int argc, char *argv[])
{
	btest_cfg_t *cfg;

	cfg = btest_init(argc, argv, "btest_lifecycle_test", NULL);
	if (cfg == NULL) {
		return 0;
	}
	
	btest_rand_data_gen(cfg);
	btest_life_cycle(cfg);

	return 0;
}

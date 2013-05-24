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

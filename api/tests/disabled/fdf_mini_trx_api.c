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

#include <stdlib.h>
#include <assert.h>
#include <unistd.h>
#include <pthread.h>
#include <string.h>
#include "zs.h"
#include "test.h"

static ZS_cguid_t  cguid_shared;
static char *base = "container";
static int iterations = 1000;
static int threads = 1;
static long size = 1024 * 1024 * 1024;

void* worker(void *arg)
{
    int i;

    struct ZS_iterator*		_zs_iterator;

    ZS_cguid_t  				 cguid;
    char 						 cname[32] 			= "cntr0";
    char        				*data;
    uint64_t     				 datalen;
    char        				*key;
    uint32_t     				 keylen;
    char 						 key_str[24] 		= "key00";
    char 						 key_data[24] 		= "key00_data";

    t(zs_init_thread(), ZS_SUCCESS);

    sprintf(cname, "%s-%x-mini-trx", base, (int)pthread_self());

    t(fdf.create_container(cname, size, &cguid), ZS_SUCCESS);

    t(zs_enumerate(cguid, &_zs_iterator), ZS_SUCCESS);

    while (zs_next_enumeration(cguid, _zs_iterator, &key, &keylen, &data, &datalen) == ZS_SUCCESS) {
		fprintf(stderr, "%x sdf_enum: key=%s, keylen=%d, data=%s, datalen=%ld\n", (int)pthread_self(), key, keylen, data, datalen);
		//advance_spinner();
    }

    fprintf(stderr, "\n");

    t(zs_finish_enumeration(cguid, _zs_iterator), ZS_SUCCESS);

	t(zs_transaction_start(), ZS_SUCCESS);

    for(i = 0; i < iterations; i++)
    {
		sprintf(key_str, "key%04ld-%08d", (long) arg, i);
		sprintf(key_data, "key%04ld-%08d_data", (long) arg, i);

		t(zs_set(cguid, key_str, strlen(key_str) + 1, key_data, strlen(key_data) + 1), ZS_SUCCESS);

		advance_spinner();
    }

	t(zs_transaction_commit(), ZS_SUCCESS);

    for(i = 0; i < iterations; i++)
    {
		sprintf(key_str, "key%04ld-%08d", (long) arg, i);
		sprintf(key_data, "key%04ld-%08d_data", (long) arg, i);

		t(zs_get(cguid, key_str, strlen(key_str) + 1, &data, &datalen), ZS_SUCCESS);

		assert(!memcmp(data, key_data, 11));	

		advance_spinner();
    }

    t(zs_enumerate(cguid, &_zs_iterator), ZS_SUCCESS);

    while(zs_next_enumeration(cguid, _zs_iterator, &key, &keylen, &data, &datalen) == ZS_SUCCESS) {
		fprintf(stderr, "%x sdf_enum: key=%s, keylen=%d, data=%s, datalen=%ld\n", (int)pthread_self(), key, keylen, data, datalen);
		//advance_spinner();
    }

    fprintf(stderr, "\n");

    t(zs_finish_enumeration(cguid, _zs_iterator), ZS_SUCCESS);

    return 0;
}

void mini_trx_api_test()
{
	t(zs_transaction_commit(), ZS_FAILURE_NO_TRANS);

	t(zs_transaction_start(), ZS_SUCCESS);
	t(zs_transaction_commit(), ZS_SUCCESS);

	t(zs_transaction_start(), ZS_SUCCESS);
	t(zs_transaction_start(), ZS_FAILURE_ALREADY_IN_TRANS);

	t(zs_transaction_commit(), ZS_SUCCESS);
	t(zs_transaction_commit(), ZS_FAILURE_NO_TRANS);
}

int main(int argc, char *argv[])
{
    pthread_t thread_id[threads];
    int i;
    char 						 name[32];

	if ( argc < 4 ) {
		fprintf( stderr, "Usage: %s <size in gb> <threads> <iterations>\n", argv[0] );
		return 0;
	} else {
		size = atol( argv[1] ) * 1024 * 1024 * 1024;
		threads = atoi( argv[2] );
		iterations = atoi( argv[3] );
		fprintf(stderr, "size=%lu, hreads=%d, iterations=%d\n", size, threads, iterations);
	}

    t(zs_init(), ZS_SUCCESS);

    t(zs_init_thread(), ZS_SUCCESS);

    sprintf(name, "%s-mini-trx", base);

    t(fdf.create_container(name, size, &cguid_shared), ZS_SUCCESS);

	fprintf(stderr, "Mini transaction API tests\n");
	mini_trx_api_test();

	fprintf(stderr, "Multi threaded mini transaction API tests\n");

    for(i = 0; i < threads; i++)
		pthread_create(&thread_id[i], NULL, worker, (void*)(long)i);

    for(i = 0; i < threads; i++)
		pthread_join(thread_id[i], NULL);

    fprintf(stderr, "All tests passed\n");

	zs_shutdown();

    return(0);
}

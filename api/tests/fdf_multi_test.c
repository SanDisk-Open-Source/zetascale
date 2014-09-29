#include <stdlib.h>
#include <assert.h>
#include <unistd.h>
#include <pthread.h>
#include <string.h>
#include <getopt.h>
#include "zs.h"
#include "test.h"

static char *base = "container";
static int iterations = 1000;
static int threads = 1;
static long size = 1024 * 1024 * 1024;
static int recover = 0;
static int delete_container = 0;

static struct option long_options[] = {
    {"delete_container",     no_argument,       0, 'd'},
    {"recover",              no_argument,       0, 'r'},
    {"help",                 no_argument,       0, 'h'},
    {"size",                 required_argument, 0, 's'},
    {"iterations",           required_argument, 0, 'i'},
    {"threads",              required_argument, 0, 't'},
    {0, 0, 0, 0}
};

void print_help(char *pname)
{
    fprintf(stderr, "%s --size=<size gb> --iterations=<iterations> --threads=<threads> --delete_container --recover --help\n\n", pname);
}

int get_options(int argc, char *argv[])
{
    int option_index = 0;
    int c;

    while (1) {
        c = getopt_long (argc, argv, "ds:i:t:rh", long_options, &option_index);

        if (c == -1)
            break;

        switch (c) {

        case 's':
            size = atol(optarg) * 1024 * 1024 * 1024;
            break;

        case 'i':
            iterations = atoi(optarg);
            break;

        case 't':
            threads = atoi(optarg);
            break;

        case 'd':
            delete_container = 1;
            break;

        case 'r':
            recover = 1;
            break;

        case 'h':
            print_help(argv[0]);
            return -1;

        default:
            print_help(argv[0]);
            return -1;
        }
    }

    return 0;
}

void* worker(void *arg)
{
    int i;

    struct ZS_iterator 		*_zs_iterator;

    ZS_cguid_t  				 cguid;
    char 						 cname[32] 			= "cntr0";
    char        				*data;
    uint64_t     				 datalen;
    char        				*key;
    uint32_t     				 keylen;
    uint32_t                     self               = (uint32_t) pthread_self();
    char 						 key_str[64] 		= "key00";
    char 						 key_data[1024] 	= "key00_data";

    t(zs_init_thread(), ZS_SUCCESS);

    sprintf(cname, "%s-%lu", base, (long) arg);

    if (recover) 
        t(zs_open_container(cname, size, &cguid), ZS_SUCCESS);
    else 
        t(zs_create_container_dur(cname, size, ZS_DURABILITY_SW_CRASH_SAFE, &cguid), ZS_SUCCESS);

    for(i = 0; i < iterations; i++)
    {
		sprintf(key_str, "key%08d-%08u", i, self);
		sprintf(key_data, "key%04ld-%01015d", (long) arg, i);

		t(zs_set(cguid, key_str, strlen(key_str) + 1, key_data, strlen(key_data) + 1), ZS_SUCCESS);

		advance_spinner();
    }

    for(i = 0; i < iterations; i++)
    {
		sprintf(key_str, "key%08d-%08u", i, self);
		sprintf(key_data, "key%04ld-%01015d", (long) arg, i);

    	t(zs_get(cguid, key_str, strlen(key_str) + 1, &data, &datalen), ZS_SUCCESS);

		assert(!memcmp(data, key_data, datalen));	
		advance_spinner();
    }

    t(zs_enumerate(cguid, &_zs_iterator), ZS_SUCCESS);

    while (zs_next_enumeration(cguid, _zs_iterator, &key, &keylen, &data, &datalen) == ZS_SUCCESS) {
		fprintf(stderr, "%x zs_enum: key=%s, keylen=%d, data=%s, datalen=%lu\n", (int)pthread_self(), key, keylen, data, datalen);
		//advance_spinner();
    }

    fprintf(stderr, "\n");

    t(zs_finish_enumeration(cguid, _zs_iterator), ZS_SUCCESS);

    if (delete_container)
        t(zs_delete_container(cguid), ZS_SUCCESS);

    t(zs_release_thread(), ZS_SUCCESS);

    return 0;
}

void set_prop(int recover)
{
    ZSLoadProperties(getenv("ZS_PROPERTY_FILE"));
    if (recover)
        ZSSetProperty("ZS_REFORMAT", "0");
    else
        ZSSetProperty("ZS_REFORMAT", "1");
    unsetenv("ZS_PROPERTY_FILE");
}

int main(int argc, char *argv[])
{
    int i;
    char 						 name[32];

    if (get_options(argc, argv) == -1)
        return(-1);

    sprintf(name, "%s-foo", base);

    set_prop(recover);

    t(zs_init(),  ZS_SUCCESS);

    t(zs_init_thread(), ZS_SUCCESS);

    pthread_t thread_id[threads];

    for(i = 0; i < threads; i++)
		pthread_create(&thread_id[i], NULL, worker, (void*)(long)i);

    for(i = 0; i < threads; i++)
		pthread_join(thread_id[i], NULL);

    fprintf(stderr, "DONE\n");

    t(zs_release_thread(), ZS_SUCCESS);

	zs_shutdown();

    return(0);
}

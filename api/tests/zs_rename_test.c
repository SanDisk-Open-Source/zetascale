#include <stdlib.h>
#include <assert.h>
#include <unistd.h>
#include <pthread.h>
#include <string.h>
#include <getopt.h>
#include "zs.h"
#include "test.h"

static char *cbase = NULL;
static char *rbase = NULL;
static int iterations = 1000;
static int threads = 1;
static int recover = 0;
static int delay = 0;

static struct option long_options[] = {
    {"container_name",       required_argument, 0, 'c'},
    {"rename_container",     required_argument, 0, 'n'},
    {"threads",              required_argument, 0, 't'},
    {"delay",                required_argument, 0, 'd'},
    {"recover",              no_argument,       0, 'r'},
    {"help",                 no_argument,       0, 'h'},
    {0, 0, 0, 0}
};

void print_help(char *pname)
{
    fprintf(stderr, "%s --container_name=<cname> --rename_container=<new cname> --threads=<threads> --delay=<seconds> --recover --help\n\n", pname);
    fprintf(stderr, "Container names will have the logical thread id appended: container.0\n");
}

int get_options(int argc, char *argv[])
{
    int option_index = 0;
    int c;

    while (1) {
        c = getopt_long (argc, argv, "c:n:rd:t:h", long_options, &option_index);

        if (c == -1)
            break;

        switch (c) {

        case 'c':
            cbase = optarg;
            break;

        case 'n':
	    rbase = optarg;
	    break;

        case 't':
	    threads = atoi(optarg);
	    break;

        case 'd':
	    delay = atoi(optarg);
	    break;

        case 'r':
            recover = 1;
            break;

        case 'h':
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
    ZS_cguid_t      cguid;
    char           *data;
    uint64_t        datalen;
    char            key_str[64]	= "key00";
    char            key_data[1024] = "key00_data";
    char            cname[256];
    char            rname[256];

    t(zs_init_thread(), ZS_SUCCESS);

    sprintf(cname, "%s.%lu", cbase, (long) arg);

    if (rbase) 
        sprintf(rname, "%s.%lu", rbase, (long) arg);

    if (!recover) {
        // create container, set objects and rename container
        t(zs_create_container_dur(cname, 0, ZS_DURABILITY_SW_CRASH_SAFE, &cguid), ZS_SUCCESS);

        for(i = 0; i < iterations; i++)
        {
            sprintf(key_str, "key%08d-%08ld", i, (long) arg);
            sprintf(key_data, "key%04ld-%01015d", (long) arg, i); 
            t(zs_set(cguid, key_str, strlen(key_str) + 1, key_data, strlen(key_data) + 1), ZS_SUCCESS);
        }

        if (rbase) {
            t(zs_rename_container(cguid, rname), ZS_SUCCESS);
        }

    } else {
        // open existing container and get objects
        if (rbase) {
            t(zs_open_container(rname, 0, &cguid), ZS_SUCCESS);
        } else {
            t(zs_open_container(cname, 0, &cguid), ZS_SUCCESS);
        }

        for(i = 0; i < iterations; i++)
        { 
            sprintf(key_str, "key%08d-%08ld", i, (long) arg);
            sprintf(key_data, "key%04ld-%01015d", (long) arg, i);
            t(zs_get(cguid, key_str, strlen(key_str) + 1, &data, &datalen), ZS_SUCCESS);
            assert(!memcmp(data, key_data, datalen));	
        }
    }

    t(zs_close_container(cguid), ZS_SUCCESS);

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

    if (get_options(argc, argv) == -1)
        return(-1);

    if (!cbase) {
        print_help(argv[0]);
        return(-1);
    }

    set_prop(recover);

    t(zs_init(),  ZS_SUCCESS);

    t(zs_init_thread(), ZS_SUCCESS);

    if (rbase && delay) {
        fprintf(stderr,"Sleeping for %d seconds while you execute a \"flip set rename_pre_meta_error or\nflip set rename_post_meta_error\" from the admin command line processor\n", delay);
        sleep(delay);
    }

    pthread_t thread_id[threads];

    for(i = 0; i < threads; i++) 
        pthread_create(&thread_id[i], NULL, worker, (void*)(long)i);

    for(i = 0; i < threads; i++) 
        pthread_join(thread_id[i], NULL);

    fprintf(stderr, "Test succeeded\n");

    t(zs_release_thread(), ZS_SUCCESS);

    zs_shutdown();

    return(0);
}

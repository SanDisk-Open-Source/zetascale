#include <stdlib.h>
#include <assert.h>
#include <unistd.h>
#include <pthread.h>
#include <string.h>
#include <getopt.h>
#include <zs.h>

static int btree_opt = 0;    
static int flog_opt = 1;    // must always be run
static int pot_opt = 1;    
static char *logfile = NULL;

static struct ZS_state* zs_state;
static __thread struct ZS_thread_state *_zs_thd_state;

static struct option long_options[] = { 
    {"help",    no_argument,       0, 'h'}, 
    {"logfile", required_argument, 0, 'l'}, 
    {0, 0, 0, 0} 
};

void print_help(char *pname) 
{
    fprintf(stderr, "\nExecute validation of ZetaScale persistent metadata, btree strucutres and recovery logs.\n\n");
    fprintf(stderr, "%s --btree --logfile=file --help\n\n", pname);
}

int get_options(int argc, char *argv[])
{
    int option_index = 0;
    int c;

    while (1) { 
        c = getopt_long (argc, argv, "bl:h", long_options, &option_index); 

        if (c == -1) 
            break;
     
        switch (c) { 
     
        case 'b': 
            btree_opt = 1; 
            break;
     
        case 'l': 
            logfile = optarg; 
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

int init_zs() {
    ZS_status_t status = ZS_FAILURE;

    if ( ZS_SUCCESS != ( status = ZSInit( &zs_state ) ) ) {
        fprintf(stderr, "Failed to initialize ZS API!\n");
        return status;
    }

    if ( ZS_SUCCESS != ( status = ZSInitPerThreadState(zs_state, &_zs_thd_state ) ) ) {
        fprintf(stderr, "Failed to initialize ZS API!\n");
        return status;
    }

    return status;
}

int close_zs()
{
    ZS_status_t status = ZS_FAILURE;

    ZSCheckClose();

    if ( ZS_SUCCESS != ( status = ZSReleasePerThreadState(&_zs_thd_state ) ) ) {
        fprintf(stderr, "Failed to release ZS thread state!\n");
        return status;
    }

    if (btree_opt) {
        if ( ZS_SUCCESS != (status = ZSShutdown( zs_state ) ) ) {
            fprintf(stderr, "Failed to shutdown ZS API!\n");
            return status;
        }
    }

    return status;
}

ZS_status_t check_meta()
{
    return ZSCheckMeta();
}

ZS_status_t check_btree()
{
    return ZSCheck( _zs_thd_state );
}

ZS_status_t check_flog()
{
    return ZSCheckFlog( );
}

ZS_status_t check_pot()
{
    return ZSCheckPOT( );
}

void set_props()
{
    ZSLoadProperties(getenv("ZS_PROPERTY_FILE"));
    ZSSetProperty("ZS_CHECK_MODE", "1");
    ZSSetProperty("ZS_REFORMAT", "0");
    unsetenv("ZS_PROPERTY_FILE");
}

int main(int argc, char *argv[])
{
    ZS_status_t status = ZS_FAILURE;

    if ( get_options( argc, argv) ) {
        return -1;
    }

    set_props();

    ZSCheckInit(logfile);

    // always check superblock and shard metadata
    if (ZS_SUCCESS != check_meta()) {
        fprintf(stderr, "meta check failed, unable to continue\n");
        return -1;
    } else {
        fprintf(stderr, "meta check succeeded\n");
    }

    if (init_zs() < 0) {
        fprintf(stderr, "ZS init failed\n");
        return -1;
    }

    // must always be run
    if (flog_opt) {
        if (ZS_SUCCESS != (status = check_flog())) {
            fprintf(stderr, "flog check failed: %s\n", ZSStrError(status));
        } else {
            fprintf(stderr, "flog check succeeded\n");
        }
    }

    if (pot_opt) {
        if (ZS_SUCCESS != (status = check_pot())) {
            fprintf(stderr, "pot check failed: %s\n", ZSStrError(status));
        } else {
            fprintf(stderr, "pot check succeeded\n");
        }
    }

    if (btree_opt) {
        if (ZS_SUCCESS != (status = check_btree())) {
            fprintf(stderr, "btree check failed: %s\n", ZSStrError(status));
        } else {
            fprintf(stderr, "btree check succeeded\n");
        }
    }

    if (close_zs() < 0) {
        fprintf(stderr, "ZS close failed\n");
        return -1;
    }

    return 0;
}

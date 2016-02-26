/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

#include <stdlib.h>
#include <assert.h>
#include <unistd.h>
#include <pthread.h>
#include <string.h>
#include <getopt.h>
#include <zs.h>

static struct ZS_state* zs_state;

static struct option long_options[] = { 
    {"help",    no_argument,       0, 'h'}, 
    {0, 0, 0, 0} 
};

void print_help(char *pname) 
{
    fprintf(stderr, "Formats the ZetaScale Storage\n");
    fprintf(stderr, "Set the enviornmental variable: ZS_PROPERTY_FILE with the ZetaScale configuration file path\n");
    fprintf(stderr, "Set the enviornmental variable: ZS_LIB with the Zetascale dynamic library filepath\n");
    
}

int get_options(int argc, char *argv[])
{
    int option_index = 0;
    int c;

    while (1) { 
        c = getopt_long (argc, argv, "h", long_options, &option_index); 
        if (c == -1) 
            break;
     
        switch (c) { 
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
        fprintf(stderr, "Failed to initialize ZetaScale\n");
        return status;
    }
    return status;
}

int close_zs()
{
    ZS_status_t status = ZS_FAILURE;
    if ( ZS_SUCCESS != (status = ZSShutdown( zs_state ) ) ) {
        fprintf(stderr, "Failed to shutdown ZetaScale!\n");
        return status;
    }
    return status;
}

int main(int argc, char *argv[])
{
    if ( get_options( argc, argv) ) {
        return -1;
    }

    if( getenv("ZS_PROPERTY_FILE") == NULL ) {
        fprintf(stderr,"Formating failed:The enviornmental variable ZS_PROPERTY_FILE is not set\n");
        exit(1);
    }
    if( getenv("ZS_LIB") == NULL ) {
        fprintf(stderr,"Formating failed:The enviornmental variable ZS_LIB is not set\n");
        exit(1);
    }
    printf("Formating the ZetaScale storage based on the configuration:%s\n", getenv("ZS_PROPERTY_FILE"));
    printf("**********************************************\n");
    ZSSetProperty("ZS_REFORMAT", "1");
    if( init_zs() != ZS_SUCCESS ) {
        printf("**************************\n");
        fprintf(stderr,"\n\nFormating failed: Intilization of ZetaScale failed. Please check the configuration\n");
    }
    close_zs();
    printf("****************\n");
    printf("Formating completed\n");
    return 0;
}

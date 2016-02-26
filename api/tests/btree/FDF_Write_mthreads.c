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


#include <unistd.h>
#include <string.h>
#include "zs.h"
#include <stdlib.h>
#include <assert.h>

static struct ZS_state     *zs_state;
#define MAX_KEY_LEN 2000
#define MAX_DATA_LEN 2048000
#define MAX_KEYS 1000000

char cname[64]="Container1";
int key_len, data_len, num_objs;
ZS_cguid_t cguid;

typedef struct thd_cfg {
    int id;
    int num_iter;
    int key_len;
    int data_len;
}thd_cfg_t;


typedef struct key {
    char key[MAX_KEY_LEN];
    int key_len;
}keys_t;

keys_t keys[MAX_KEYS];

int preEnvironment(bool reformat)
{
    if (!reformat) {
        ZSSetProperty("ZS_REFORMAT", "0");
    }

    if(ZSInit( &zs_state) != ZS_SUCCESS ) {
        fprintf( stderr, "ZS initialization failed!\n" );
        return 0 ;
    }
    return 1;
}

void CleanEnvironment(struct ZS_thread_state *thd_state)
{
    ZSReleasePerThreadState(&thd_state);
    ZSShutdown(zs_state);
}

void rand_str(char *dest, size_t length) {
    char charset[] = "0123456789"
                     "abcdefghijklmnopqrstuvwxyz"
                     "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    uint64_t t = time(NULL);

    while (length-- > 8) {
        //size_t index = (double) rand() / RAND_MAX * (sizeof charset - 1);
        size_t index = rand() % 62;
        *dest++ = charset[index];
    }
    memcpy((uint64_t *)dest, &t, sizeof(uint64_t));
    dest += 8;
    *dest = '\0';
}


void *worker(void *arg) {
    //ZS_container_props_t p;
    uint32_t i;
    struct ZS_thread_state *zs_thd_state;
    char key[MAX_KEY_LEN], value[MAX_DATA_LEN];
    ZS_status_t ret;
    thd_cfg_t *cfg = (thd_cfg_t*)arg;
    char *outval;
    uint64_t outlen;
    int data_len, j, key_len;

    fprintf(stderr,"Starting thread: %d  key_len:%d datalen:%d\n",cfg->id, cfg->key_len,cfg->data_len);
    if(ZS_SUCCESS != ZSInitPerThreadState( zs_state, &zs_thd_state ) ) {
        fprintf( stderr, "ZS thread initialization failed!\n" );
        return 0;
    }

    for( j = 0; j < cfg->num_iter; j++ ) {
        fprintf(stderr,"Iteration:%d\nWRITE %d objects\n",j+1,num_objs);
        for ( i = 0; i < num_objs; i++ ) {
//            key_len = rand()%cfg->key_len;
            key_len = cfg->key_len;
            if( key_len == 0 ) {
                key_len  = 1;
            }
            rand_str(key,key_len);
            //rand_str(value, cfg->data_len);
            data_len = rand()%cfg->data_len;
            if( data_len == 0 ) {
                data_len = 10;
            }
            memcpy(keys[i].key,key,cfg->key_len);
            keys[i].key_len = key_len;
            //fprintf(stderr,"datalen:%d key_len:%d\n",data_len,key_len);
            ret = ZSWriteObject(zs_thd_state, cguid, key, key_len, value, data_len, 1);
            if( ret != ZS_SUCCESS ) {
                //fprintf( stderr, "Write failed %s\n",ZSStrError(ret));
                //return NULL;
            }
        }
#if 0
        fprintf(stderr,"UPDATE %d objects\n",num_objs);
        for ( i = 0; i < num_objs; i++ ) { 
            rand_str(key,cfg->key_len);
            //rand_str(value, cfg->data_len);
            data_len = rand()%cfg->data_len;
            memcpy(keys[i].key,key,cfg->key_len);
            //fprintf(stderr,"datalen:%d\n",data_len);
            ret = ZSWriteObject(zs_thd_state, cguid, key, cfg->key_len, value, 300, 0); 
            if( ret != ZS_SUCCESS ) { 
                fprintf( stderr, "Write failed\n");
                return NULL;
            }   
        }  
#endif


#if 0
        fprintf(stderr,"READ %d objects\n",num_objs);
        for ( i = 0; i < num_objs; i++ ) {
            ret = ZSReadObject(zs_thd_state, cguid, keys[i].key, cfg->key_len, &outval, &outlen);
            if (ret != ZS_SUCCESS) {
                fprintf( stderr, "Read failed\n");
                assert(0);
                return NULL;
            }
	    free(outval);
        }
#endif

        outval = NULL;
        outlen = 0; 
        fprintf(stderr,"DELETE %d objects\n",num_objs/2);
        for ( i = 0; i < num_objs/2; i++ ) {
            ret = ZSDeleteObject(zs_thd_state, cguid, keys[i].key, keys[i].key_len);
            if (ret != ZS_SUCCESS) {
                //fprintf( stderr, "delete failed\n");
                //assert(0);
        //        return NULL;
            }
        }
    }
    ZSReleasePerThreadState(&zs_thd_state);
    fprintf(stderr, "Released per thread\n");

    return NULL;
}

int main(int argc, char *argv[])
{
    int num_threads;
    ZS_container_props_t p;
    uint32_t flag;
    struct ZS_thread_state *zs_thd_state;
    ZS_status_t ret;
    int i, iter;
    bool reformat;

    if( argc < 4 ) {
        printf("Usage: %s <num_objs> <num_thds> <key_len> <data_len> <num_iter_per_thd> <reformat 0|1>\n", argv[0]);
        exit(1);
    }

    num_objs = atoi(argv[1]);
    num_threads = atoi(argv[2]);
    key_len = atoi(argv[3]);
    data_len = atoi(argv[4]);
    iter = atoi(argv[5]);
    reformat = atoi(argv[6]);

    flag = reformat ? 1 : 0;
    if( 1 != preEnvironment(reformat)) {
        return 1;
    }

    if(ZS_SUCCESS != ZSInitPerThreadState( zs_state, &zs_thd_state ) ) {
        fprintf( stderr, "ZS thread initialization failed!\n" );
        return 0;
    }

    ZSLoadCntrPropDefaults(&p);
    //p.size_kb = 102400;
    p.size_kb = 0;
    fprintf( stderr, "Creating/Opening container: %s\n",cname);
    ret = ZSOpenContainer(zs_thd_state,cname,&p,flag,&cguid);
    if( ret != ZS_SUCCESS ) {
        fprintf( stderr, "Container open failed\n");
        return ZS_FAILURE;
    } 

    pthread_t thread_id[1024];
    thd_cfg_t tcfg[1024];
    fprintf( stderr, "Creating %d Worker threads\n", num_threads);
    for(i = 0; i < num_threads; i++) {
        tcfg[i].id = i;
        tcfg[i].num_iter = iter;
        tcfg[i].key_len = key_len;
        tcfg[i].data_len = data_len;
        pthread_create(&thread_id[i], NULL, worker, (void*) (&tcfg[i]));
        //key_len = key_len + 2;
        //data_len = data_len + 200;
    }

    for(i = 0; i < num_threads; i++) {
        fprintf(stderr, "Joining thread_id[%d]=%u\n", i, (uint32_t)thread_id[i]);
        pthread_join(thread_id[i], NULL);
    }

    fprintf(stderr, "Threads joined\n");
    ZSCloseContainer(zs_thd_state, cguid);
    fprintf(stderr, "Containers closed\n");

    CleanEnvironment(zs_thd_state);

    fprintf( stderr, "Test completed\n");
}

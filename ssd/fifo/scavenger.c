/** @file scavenger.c
 *  @brief FDF scavenger implementation.
 *
 *  This contains the functions for FDF to 
 *  scavenge expired objects from FDF
 *
 *  @author Tomy Cheru (tcheru)
 *  SanDisk Proprietary Material, © Copyright 20124 SanDisk, all rights reserved.
 *  http://www.sandisk.com
 */

#include <fth/fth.h>
#include <fdf.h>
#include <time.h>
#include <ctype.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdint.h>
#include <unistd.h>
#include <string.h>
#include <fdf_internal.h>
#include <protocol/action/action_internal_ctxt.h>
#include <protocol/action/recovery.h>
#include <utils/properties.h>

uint64_t num_objs_expired = 0;
uint64_t num_scav_scan_yld = 0;
uint64_t num_scav_scan_complete = 0;

static __thread struct FDF_thread_state *scav_ts;
static bool stop_scavenger = false;
static void *scavenger_handler(void *);
static pthread_t scav_thd = (pthread_t)NULL;

pthread_cond_t scav_cv = PTHREAD_COND_INITIALIZER;
pthread_mutex_t scav_mutex = PTHREAD_MUTEX_INITIALIZER;

extern FDF_status_t fdf_get_ctnr_status(FDF_cguid_t cguid, int delete_ok);
extern unsigned long int getProperty_uLongInt(const char *key, 
                const unsigned long int defaultVal);
extern FDF_status_t  cguid_to_shard(SDF_action_init_t *pai, FDF_cguid_t cguid, 
                shard_t **shard_ptr, int delete_ok);

typedef SDF_action_init_t pai_t;
typedef struct FDF_iterator e_state_t;
extern FDF_status_t scavenger_scan_next(pai_t *pai, e_state_t *es, 
                uint64_t *cguid, char **key, uint64_t *keylen);
extern FDF_status_t scavenger_scan_done(pai_t *pai, e_state_t *es);
extern FDF_status_t scavenger_scan_init(pai_t *pai, shard_t *sshard, 
                e_state_t **esp);

#define LOG_CAT PLAT_LOG_CAT_SDF_NAMING
#define LOG_ERR PLAT_LOG_LEVEL_ERROR
#define LOG_TRACE PLAT_LOG_LEVEL_TRACE
#define LOG_DBG PLAT_LOG_LEVEL_TRACE
#define LOG_INFO PLAT_LOG_LEVEL_INFO

/*
 * start/resume scavenger thread. 
 * On FDFInit, a scavneger thread is spawned and stays for process lifetime
 * also called from admin console to resume scavneger scan.
 */
FDF_status_t fdf_start_scavenger_thread(struct FDF_state *fdf_state )
{
    stop_scavenger = false;

    if(getProperty_Int("FDF_TRX", 0)) {
        plat_log_msg(180210, PLAT_LOG_CAT_PRINT_ARGS, PLAT_LOG_LEVEL_WARN,
                "FDF_EXPIRY_SCAVENGER_ENABLE is incompatible with FDF_TRX. Disabling FDF_EXPIRY_SCAVENGER_ENABLE.");
        return FDF_FAILURE;
    }

    if( fdf_state == NULL ) {
        plat_log_msg(160236, LOG_CAT, LOG_ERR, 
                "invalid fdf state.");
        return FDF_FAILURE; 
    }

    if((pthread_t)NULL == scav_thd) { //from FDFInit()

        if( 0 == pthread_create(&scav_thd,NULL, 
                    scavenger_handler,(void *)fdf_state)){
            plat_log_msg(160237, LOG_CAT, LOG_DBG, 
                    "scavenger thread create success.");
            return FDF_SUCCESS;
        }

        plat_log_msg(160238, LOG_CAT, LOG_ERR, 
                "scavenger thread create failed.");
        return FDF_FAILURE;

    } else { //from admin thread

        if(0 == pthread_cond_broadcast(&scav_cv)) {
            plat_log_msg( 160239, LOG_CAT, LOG_DBG, 
                    "woke up scavenger.");
            return FDF_SUCCESS;
        } else {
            plat_log_msg( 160240, LOG_CAT, LOG_ERR, 
                    "scavenger wakeup failed.");
            return FDF_FAILURE;
        }
    }
}

/*
 * stop/pause scavenger thread.
 * FDFShutdown() and admin thread uses it
 * will mark for stopping, will enter an long yield(365days) on
 * very next iteration of scan.
 * to be resumed by fdf_start_scavenger_thread()
 */
FDF_status_t fdf_stop_scavenger_thread()
{
    plat_log_msg( 160241, LOG_CAT, LOG_DBG, 
            "stoping scavenger thread.");
    stop_scavenger = true;
    return FDF_SUCCESS;
}

/*
 * scavenger worker
 */
static void *scavenger_handler(void *arg)
{
    FDF_status_t s;
    SDF_action_init_t *pai;
    char *key;
    struct timespec abstime;
    struct FDF_iterator *iter;
    struct shard *sshard = NULL;
    uint32_t num_objs_scanned=0;
    uint32_t keylen;
    uint64_t cguid;
    uint64_t keylen_64;
    uint64_t scan_complete_sleep_sec;
    uint64_t scan_rate;
    uint64_t sec = 0;
    uint64_t nsec = 0;
    uint64_t delta;
    void *retval = NULL;

    scan_complete_sleep_sec = getProperty_uLongInt(
            "FDF_EXPIRY_SCAVENGER_SCAN_INTERVAL", 1); // 1 sec
    scan_rate = getProperty_uLongInt(
            "FDF_EXPIRY_SCAVENGER_SCAN_RATE", 1024); // 1k objects
    delta = getProperty_uLongInt(
            "FDF_EXPIRY_SCAVENGER_INTERVAL", 1000); // 1 milli sec

    delta *= 1000; //property in microseconds, convery to nano
    sec = delta / NANO;
    nsec = (delta - (sec * NANO));

    if(FDF_SUCCESS != FDFInitPerThreadState(( struct FDF_state * ) arg, 
                ( struct FDF_thread_state ** )&scav_ts)) {
        plat_log_msg(160242, LOG_CAT, LOG_ERR, 
                "scavenger thread state init failed.");
        pthread_exit(retval);
    }

    pai = (SDF_action_init_t *)scav_ts;
    s = cguid_to_shard(pai, VDC_CGUID, &sshard, 0); // scan VDC

    if (s != FDF_SUCCESS) {
        plat_log_msg(160243, LOG_CAT, LOG_ERR, 
                "scavenger bad shard.");
        pthread_exit(retval);
    }

    while(1){   

        s = scavenger_scan_init(pai, sshard, &iter);

        if( s != FDF_SUCCESS) {
            plat_log_msg(160244, LOG_CAT, LOG_ERR, 
                    "scavenger scan init failed.");
            pthread_exit(retval);
        }

        plat_log_msg(160245, LOG_CAT, LOG_DBG, 
                "scavenger scan init successful.\n");

        while(1){

            //respond to stop/pause command
            if (stop_scavenger == true){
                plat_log_msg(160246, LOG_CAT, LOG_DBG,
                        "scavenger paused.");
                pthread_mutex_lock(&scav_mutex);
                clock_gettime(CLOCK_REALTIME, &abstime);
                abstime.tv_sec += 31536000; //yield for an year
                pthread_cond_timedwait(&scav_cv, &scav_mutex, &abstime);
                pthread_mutex_unlock(&scav_mutex);
                plat_log_msg(160247, LOG_CAT, LOG_DBG,
                        "scavenger resuming.");
            }

            //handle scan rate expiry
            if (num_objs_scanned >= scan_rate){
                plat_log_msg(160248, LOG_CAT, LOG_DBG, 
                        "scavenger yielding, scan rate reached.");
                num_scav_scan_yld++;
                pthread_mutex_lock(&scav_mutex);
                clock_gettime(CLOCK_REALTIME, &abstime);
                abstime.tv_sec += sec;
                abstime.tv_nsec += nsec;
                pthread_cond_timedwait(&scav_cv, &scav_mutex, &abstime);
                pthread_mutex_unlock(&scav_mutex);
                plat_log_msg(160249, LOG_CAT, LOG_DBG, 
                        "scavenger resuming from scan rate yield.");
                num_objs_scanned = 0;
            }

            s = scavenger_scan_next( pai, iter, &cguid, 
                    (char **)&key, &keylen_64);

            num_objs_scanned++;
            if (s == FDF_SUCCESS){
                continue;
            }

            if(s == FDF_EXPIRED){
                keylen = keylen_64;
                if(FDF_SUCCESS != FDFDeleteObject(scav_ts, cguid, key, keylen)){
                    plat_log_msg(160250, LOG_CAT, LOG_DBG, 
                            "scavenger expiry delete failed.");
                    continue;
                }
                plat_log_msg(160251, LOG_CAT, LOG_DBG, 
                        "scavenger expiry delete success.");
                plat_free(key);
                num_objs_expired++;
                continue;
            }

            if(s == FDF_SCAN_DONE) break;
        }

        plat_log_msg(160252, LOG_CAT, LOG_DBG, 
                "scavenger scan completed.");
        s = scavenger_scan_done(pai, iter);
        plat_log_msg(160253, LOG_CAT, LOG_DBG, 
                "scavenger yielding, scan complete.");
        num_scav_scan_complete++;
        pthread_mutex_lock(&scav_mutex);
        clock_gettime(CLOCK_REALTIME, &abstime);
        abstime.tv_sec += scan_complete_sleep_sec;
        pthread_cond_timedwait(&scav_cv, &scav_mutex, &abstime);
        pthread_mutex_unlock(&scav_mutex);
        plat_log_msg(160254, LOG_CAT, LOG_DBG, 
                "scavenger scan resuming.");
        num_objs_scanned = 0;
    }     
}

/*
 * File:   fcnl_multinode_multiptl_test2.c
 * Author: mac
 *
 * Created on Aug 26, 2008, 11:53 PM
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 */

#define LOCAL_SHMEM 1

#ifdef MPI_BUILD
#include <mpi.h>
#endif

#include <stdlib.h>
#include <stdint.h>
#include <pthread.h>
#include <sched.h>
#include <stdio.h>

#include "platform/logging.h"
#define PLAT_OPTS_NAME(name) name ## _mpilogme
#include "platform/opts.h"
#include "platform/shmem.h"
#include "platform/fcntl.h"
#include "fth/fth.h"
#include "fth/fthMbox.h"
#include "platform/errno.h"
#include "utils/properties.h"

#include "sdfmsg/sdf_msg_types.h"
#include "fcnl_test.h"
int myid = -1, pnodeid = -1;

#define LOG_CAT PLAT_LOG_CAT_SDF_SDFMSG

pthread_t fthPthread;

int
main(int argc, char *argv[]) {
    struct plat_opts_config_mpilogme config;
    SDF_boolean_t success = SDF_TRUE;
    uint32_t numprocs;
    int tmp, namelen, mpiv = 0, mpisubv = 0, i;
    char processor_name[MPI_MAX_PROCESSOR_NAME];
    int msg_init_flags = SDF_MSG_MPI_INIT;
    config.inputarg = 0;
    config.msgtstnum = 500;

    /* We may not need to gather anything from here but what the heck */
    loadProperties("/opt/schooner/config/schooner-med.properties"); // TODO get filename from command line

    /* make sure this is first in order to get the the mpi init args */
    success = plat_opts_parse_mpilogme(&config, argc, argv) ? SDF_FALSE : SDF_TRUE;

    printf("input arg %d msgnum %d success %d\n", config.inputarg, config.msgtstnum, success);
    fflush(stdout);
    myid = sdf_msg_init_mpi(argc, argv, &numprocs, &success, msg_init_flags);

    if ((!success) || (myid < 0)) {
        printf("Node %d: MPI Init failure... exiting - errornum %d\n", myid, success);
        fflush(stdout);
        MPI_Finalize();
        return (EXIT_FAILURE);
    }

    int debug = 0;
    while(debug);

    tmp = init_msgtest_sm((uint32_t)myid);

    /* Enable this process to run threads across 2 cpus, MPI will default to running all threads
     * on only one core which is not what we really want as it forces the msg thread to time slice
     * with the fth threads that send and receive messsages
     * first arg is the number of the processor you want to start off on and arg #2 is the sequential
     * number of processors from there
     */
    lock_processor(0, 7);
    sleep(1);
    msg_init_flags =  msg_init_flags | SDF_MSG_RTF_DISABLE_MNGMT;

    /* Startup SDF Messaging Engine FIXME - dual node mode still - pnodeid is passed and determined
     * from the number of processes mpirun sees.
     */
    sdf_msg_init(myid, &pnodeid, msg_init_flags);

    MPI_Get_version(&mpiv, &mpisubv);
    MPI_Get_processor_name(processor_name, &namelen);

    printf("Node %d: MPI Version: %d.%d Name %s \n", myid, mpiv, mpisubv, processor_name);
    fflush(stdout);

    plat_log_msg(
            PLAT_LOG_ID_INITIAL,
            LOG_CAT,
            PLAT_LOG_LEVEL_TRACE,
            "\nNode %d: Completed Msg Init.. numprocs %d pnodeid %d Starting Test\n",
            myid, numprocs, pnodeid);

    for (i = 0; i < 2; i++) {
        sleep(2);
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                "\nNode %d: Number of sleeps %d\n", myid, i);
    }

    fthInit();
    sdf_msg_startmsg(myid, 0, NULL); 


    /* SAVE THIS may need to play with the priority later */
#if 0
    struct sched_param param;
    int newprio = 60;
    pthread_attr_t hi_prior_attr;

    pthread_attr_init(&hi_prior_attr);
    pthread_attr_setschedpolicy(&hi_prior_attr, SCHED_FIFO);
    pthread_attr_getschedparam(&hi_prior_attr, &param);
    param.sched_priority = newprio;
    pthread_attr_setschedparam(&hi_prior_attr, &param);
    pthread_create(&fthPthread, &hi_prior_attr, &fthPthreadRoutine, NULL);
#endif

    pthread_attr_t attr;
    pthread_attr_init(&attr);
    pthread_create(&fthPthread, &attr, &MultiNodeMultiPtlMstosrPthreadRoutine, &numprocs);

    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "\nNode %d: Created pthread for FTH %d\n", myid, i);

    pthread_join(fthPthread, NULL);

    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "\nNode %d: SDF Messaging Test Complete - i %d\n", myid, i);

    /* Lets stop the messaging engine this will block until they complete */
    /* FIXME arg is the threadlvl */
#if 0
    if (numprocs > 1) {
        if (!myid) {
            for (int index = 1; index < numprocs; index ++)
                sdf_msg_nsync(myid, index);
        }
        else {
            sdf_msg_nsync(myid, 0);
        }

    }
#endif
    sdf_msg_stopmsg(myid, SYS_SHUTDOWN_SELF);

    plat_shmem_detach();

    if (myid == 0) {
        sched_yield();
        printf("Node %d: Exiting message test after yielding... Calling MPI_Finalize\n", myid);
        fflush(stdout);
        sched_yield();
        MPI_Finalize();
    }
    else {
        printf("Node %d: Exiting message test... Calling MPI_Finalize\n", myid);
        fflush(stdout);
        sched_yield();
        MPI_Finalize();
    }
    printf("Successfully ends\n");
    return (EXIT_SUCCESS);

}

#include "platform/opts_c.h"

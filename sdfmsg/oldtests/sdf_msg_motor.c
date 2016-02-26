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

/*
 * File:   sdf_msg_motor.c
 * Author: Tom Riddle
 *
 * Created on February 20, 2008, 7:45 AM
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: sdf_msg_motor.c 308 2008-02-20 22:34:58Z tomr $
 */

#ifdef MPI_BUILD
#include <mpi.h>
#endif

#include <stdlib.h>
#include <stdint.h>
#include <pthread.h>
#include <sched.h>
#include <stdio.h>
#include <signal.h>

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
#include "sdfmsg/tests/fcnl_test.h"

#include "sdftcp/msg_msg.h"
#include "sdftcp/msg_err.h"

#include "Utilfuncs.h"

#define WRAPTEST 0
#define LOCAL_PROPS 0
#define NEW_MSG 0

int myid = -1, pnodeid = -1;

#define LOG_CAT PLAT_LOG_CAT_SDF_SDFMSG
#define MSGTST_FTHSTKSZE 131072

extern void fthThreadRoutine1(uint64_t arg);
extern void fthThreadRoutine2(uint64_t arg);
extern void fthThreadRoutine(uint64_t arg);
extern void sdf_msg_resp_gc(uint64_t arg);
extern void *sdf_msg_tester_start(void * arg);

extern void fthThreadRoutineWt1(uint64_t arg);
extern void fthThreadRoutineWt2(uint64_t arg);
extern void fthThreadRoutineWt(uint64_t arg);


pthread_t fthPthread;
int msgCount;
char mynname[SDF_MSG_MAX_NODENAME];      /* this nodes unix hostname */

#if NEW_MSG
/*
 * Drop alarm routine. It will be called when user type Ctrl+C
 */

static int TimeToQuit = 0;

static void
drop_alarm(int signo) {
    TimeToQuit = 1;
}

/* Initialize the socket message interface */

static msg_init_t *
init_new_msg(msg_init_t *initmsg) {
/* Test the new message init call*/

    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_DEBUG,
                 "\nNEW_MSG_INIT... reg interrupt \n");

    /* register signal process function */
    if (SIG_ERR == signal(SIGINT, drop_alarm)) {
        error(SYS, "Register signal function failed");
        return (NULL);
    }

    /* Fill in some stuff */
    initmsg->name = mynname;

    int ret = gethostname(initmsg->name, sizeof(mynname));

    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_DEBUG,
                 "\nNEW_MSG_INIT... starting on %s\n", initmsg->name);

//    initmsg->ifaces = NULL;
//    initmsg->port = 0;
//    initmsg->class = 0;
    initmsg->debug = 1;

    /* initiate msg system */
    msg_init(initmsg);
    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_DEBUG,
                 "\nNEW_MSG_INIT... initmsg %p ret %d name %s ifaces %s debug %d\n",
                 initmsg, ret, initmsg->name, initmsg->ifaces, initmsg->debug);
    return(initmsg);
}
#endif

void *
fthPthreadRoutine(void *arg)
{

    /* use the std tstconfig struct from now on */
    struct plat_opts_config_mpilogme *tstconfig = (struct plat_opts_config_mpilogme *)arg;

    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE, 
                 "\nNode %d FTH threads firing up\n", tstconfig->myid);

    // Start a thread
    if (WRAPTEST) {
        fthResume(fthSpawn(&fthThreadRoutineWt1, MSGTST_FTHSTKSZE), 1);
        fthResume(fthSpawn(&fthThreadRoutineWt, MSGTST_FTHSTKSZE), (uint64_t)msgCount);
    }
    else {
        fthResume(fthSpawn(&fthThreadRoutine1, MSGTST_FTHSTKSZE), 1);
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_DEBUG,
                     "\nNode %d Finished Creation and Spawned #1 with %d\n", tstconfig->myid, MSGTST_FTHSTKSZE);
        fthResume(fthSpawn(&fthThreadRoutine2, MSGTST_FTHSTKSZE), 2); // Start a thread
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_DEBUG,
                     "\nNode %d Finished Creation and Spawned #2 with %d\n", tstconfig->myid, MSGTST_FTHSTKSZE);
        fthResume(fthSpawn(&fthThreadRoutine, MSGTST_FTHSTKSZE), (uint64_t)tstconfig->msgtstnum);
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_DEBUG,
                     "\nNode %d Finished Creation and Spawned #3 with %d\n", tstconfig->myid, MSGTST_FTHSTKSZE);
    }
    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_DEBUG,
                 "\nNode %d Finished Creation and Spawned -- Now starting sched\n", tstconfig->myid);

    fthSchedulerPthread(0);

    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE, 
                 "\nFTH scheduler halted\n");
    return (0);

}


int
main(int argc, char *argv[]) {
    msgCount = 0;
    struct plat_opts_config_mpilogme config;
    struct plat_opts_config_mpilogme *tstconfig = &config;
    SDF_boolean_t success = SDF_TRUE;
    uint32_t numprocs;
    int tmp, namelen, mpiv = 0, mpisubv = 0;
    char processor_name[MPI_MAX_PROCESSOR_NAME];

    /* grab the defaults for the msg testing config struct, msgflags are set to SDF_MSG_NO_MPI_INIT */
    msgtst_setconfig(tstconfig);

    /* grab the command line args */
    success = plat_opts_parse_mpilogme(&config, argc, argv) ? SDF_FALSE : SDF_TRUE;

    /* We grab the schooner default node id's from the props file 
     * and you can hardcode your favorite here to your local path if you choose */
#if LOCAL_PROPS
    char *lp = "/export/sdf_dev/schooner-trunk/trunk/config/schooner-med.properties";
    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_DEBUG,
                 "\nUsing Local Prop file %s", lp);
    loadProperties("/export/sdf_dev/schooner-trunk/trunk/config/schooner-med.properties");
#else
    loadProperties(config.propertyFileName);
#endif

    /* after reading the props file set the initial flags */
    msgtst_setpreflags(tstconfig);

    /* Check to see if a command line option for msg_mpi has been set if not its SDF_MSG_NO_MPI_INIT */
    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_DEBUG,
                 "\ninput arg nnum %d msg_mpi %d msgnum %d msg_init_flags 0x%x propfile %s success %d\n",
                 config.nnum, config.sdf_msg_init_state, config.msgtstnum, tstconfig->msg_init_flags,
                 config.propertyFileName, success);

#if NEW_MSG
    /* NEW_MSG setting to no init to start a single process, will override the command line */
    int msg_init_flags = SDF_MSG_NO_MPI_INIT;

    /* NEW_MSG start test in single thread messaging mode */
    msg_init_flags =  msg_init_flags | SDF_MSG_SINGLE_NODE_MSG;
    msg_init_flags =  msg_init_flags | SDF_MSG_NEW_MSG;

    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_DEBUG,
                 "\nWARNING RUNNING NEW_MSG No MPI msg_init_flags 0x%x\n", msg_init_flags);

    /* NEW_MSG call to init the socket based messaging */
    msg_init_t *initmsg = plat_alloc(sizeof(msg_init_t));
    memset(initmsg, 0, sizeof(msg_init_t));
    init_new_msg(initmsg);
#else
    int msg_init_flags = SDF_MSG_MPI_INIT;
#endif

    myid = sdf_msg_init_mpi(argc, argv, &numprocs, &success, msg_init_flags);
    tstconfig->myid = myid;
    tstconfig->numprocs = numprocs;
    tstconfig->startMessagingThreads = getProperty_Int("SDF_MSG_ENGINE_START", 1);
    msgCount = tstconfig->msgtstnum;

    msgtst_setpostflags(tstconfig);

    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_DEBUG,
                 "\nNode %d: num requested msgs %d with numprocs %d flags 0x%x\n", myid,
                 msgCount, numprocs, tstconfig->msg_init_flags);

    if ((!success) || (myid < 0)) {
	printf("Node %d: MPI Init failure... exiting - errornum %d\n", myid, success);
	fflush(stdout);
        MPI_Finalize();
        return (EXIT_FAILURE);
    }

    tmp = init_msgtest_sm((uint32_t)myid);

    /* 
     * Enable this process to run threads across at least 2 cpus, MPI will default to running all threads 
     * on only one core which is not what we really want as it forces the msg thread to time slice
     * with the fth threads that send and receive messsages
     * first arg is the number of the processor you want to start off on and arg #2 is the sequential
     * number of processors from there.
     */
    lock_processor(0, tstconfig->cores);

    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_DEBUG,
                 "\nNode %d: with numprocs %d msg start %d\n", myid, numprocs, tstconfig->startMessagingThreads);

    msg_init_flags =  msg_init_flags | SDF_MSG_RTF_DISABLE_MNGMT;

    sdf_msg_init(myid, &pnodeid, msg_init_flags);

    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_DEBUG,
                 "\nNode %d: msg_init_flags = 0x%x\n", myid, msg_init_flags);

    if (tstconfig->msg_init_flags & SDF_MSG_MPI_INIT) {
        MPI_Get_version(&mpiv, &mpisubv);
        MPI_Get_processor_name(processor_name, &namelen);

        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                     "\nNode %d: %s MPI Version: %d.%d Name %s \n", 
                     myid, __func__, mpiv, mpisubv, processor_name);

        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                     "\nNode %d: Completed Msg Init procs %d active nodes 0x%x Starting Test\n", 
                     myid, numprocs, pnodeid);
    }
    /* let the msg thread do it's thing, flag SDF_MSG_RTF_DISABLE_MNGMT will disable message managment pthread */

    sdf_msg_startmsg(myid, SDF_MSG_RTF_DISABLE_MNGMT, NULL);

/* XXX SAVE THIS may need to play with the priority later */
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

    /* create the fth test threads */

    fthInit();

    pthread_attr_t attr;
    pthread_attr_init(&attr);
    pthread_attr_setdetachstate( &attr, PTHREAD_CREATE_JOINABLE );
    size_t stack_size;
    int stat = pthread_attr_getstacksize (&attr, &stack_size);
    pthread_create(&fthPthread, &attr, &fthPthreadRoutine, tstconfig);

    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_DEBUG,
                 "\nNode %d: Created pthread for FTH %d with stacksize %lu stat %d\n", myid, 
                 msgCount, stack_size, stat);

    pthread_join(fthPthread, NULL);

    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_DEBUG,
                 "\nNode %d: SDF Messaging Test Complete, start shutdown msgCount %d\n", myid, msgCount);

    /* Lets stop the messaging engine this will block until they complete */
    /* FIXME arg is the threadlvl, sending  */
    sdf_msg_stopmsg(myid, SYS_SHUTDOWN_SELF);

    plat_shmem_detach();

    if (myid == 0) {
        sched_yield();
        printf("Node %d: Exiting message test after yielding... Calling MPI_Finalize\n", myid);
        fflush(stdout);
        sched_yield();
        //if (tstconfig->sdf_msg_init_state) {
            MPI_Finalize();
        //}
    }
    else {
        printf("Node %d: Exiting message test... Calling MPI_Finalize\n", myid);
        fflush(stdout);
        sched_yield();
        MPI_Finalize();
    }
    return (EXIT_SUCCESS);
    /* FIXME dummy call here such that log warnings do not fail build */
    if (0) {
        plat_opts_usage_one("", "", 1);
    }
}
#include "platform/opts_c.h"

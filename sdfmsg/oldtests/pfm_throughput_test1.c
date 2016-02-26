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
 * File:   pfm_throughput_test1.c
 * Author: Norman Xu
 *
 * Created on June 23, 2008, 7:45AM
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: pfm_throughput_test1.c 308 2008-06-23 22:34:58Z normanxu $
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

#include "sdfmsg/sdf_msg_types.h"
#include "pfm_test.h"
#define PLAT_OPTS_ITEMS_mpilogme() \
    PLAT_OPTS_SHMEM(shmem)

struct plat_opts_config_mpilogme {
    struct plat_shmem_config shmem;
};

int myid = -1, pnodeid = -1;

PLAT_LOG_CAT_LOCAL(LOG_CAT, "sdf/sdfmsg/tests/mpilog");

pthread_t fthPthread;
int msgCount;

/* Lock this process to the lowest number cpu available and enable 2 cores to run threads */
// static void lock_processor(uint32_t numcpus) {
//     cpu_set_t cpus;
//     int i;
//
// /* FIXME need to be a bit more intelligent about recognizing the number of cores in a system
//     * but for now we assume we have at least 2 cores available on any machine we do this on
//  */
//     CPU_ZERO(&cpus);
//
//     for (i = 0; i < numcpus; ++i) {
//         CPU_SET(i, &cpus);
//         printf("Node %d i %d\n", myid, i);
//     }
//     (void) sched_setaffinity(0, sizeof(cpus), &cpus);
//     plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
//                  "Completed locking processor...\n");
// }

#endif

int main(int argc, char *argv[]) {
    msgCount = 50;
    struct plat_opts_config_mpilogme config;
    SDF_boolean_t success = SDF_TRUE;
    uint32_t numprocs;

    myid = sdf_msg_init_mpi(argc, argv, &numprocs, &success);

    if ((!success) || (myid < 0)) {
        printf("Node %d: MPI Init failure... exiting - errornum %d\n", myid,
                success);
        fflush(stdout);
        MPI_Finalize();
        return (EXIT_FAILURE);
    }

    sleep(1);

    memset(&config, 0, sizeof(config));
    plat_shmem_config_init(&config.shmem);
    // Totally arbitrary default
    config.shmem.size = 1024 * 1024;

    plat_opts_parse_mpilogme(&config, argc, argv);

    /*
     * Must be initialzed after MPI because #init_sdfmsg_tst_sm
     * uses node id to select backing file.
     */
#ifdef LOCAL_SHMEM
    int tmp = init_sdfmsgtst_sm(myid);
    if (tmp) {
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                "Node %d: SHEMEM Init failure... exiting\n", myid);
        MPI_Finalize();
        return (EXIT_FAILURE);
    }
#endif

    /* Enable this process to run threads across 2 cpus, MPI will default to running all threads
     * on only one core which is not what we really want as it forces the msg thread to time slice
     * with the fth threads that send and receive messsages
     */
    //     lock_processor(2);
    // get number of cpus
    int cpunum = sysconf(_SC_NPROCESSORS_ONLN);
    printf("This machine has %d cpus\n", cpunum);
    lock_processor(0, cpunum);
    /* Startup SDF Messaging Engine FIXME - dual node mode still - pnodeid is passed and determined
     * from the number of processes mpirun sees.
     */
    sdf_msg_init(myid, &pnodeid, 0);

    plat_log_msg(
            PLAT_LOG_ID_INITIAL,
            LOG_CAT,
            PLAT_LOG_LEVEL_TRACE,
            "\nNode %d: Completed Msg Init.. numprocs %d pnodeid %d Starting Test\n",
            myid, numprocs, pnodeid);

    for (msgCount = 0; msgCount < 2; msgCount++) {
        sleep(2);
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                "\nNode %d: Number of sleeps %d\n", myid, msgCount);
    }

    /* create the fth test threads */

    //     fthInit();                               // Init

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
    pthread_create(&fthPthread, &attr, &ThroughputThreadRoutine, &myid);

    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
            "\nNode %d: Created pthread for FTH %d\n", myid, msgCount);

    pthread_join(fthPthread, NULL);

    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
            "\nNode %d: SDF Messaging Test Complete - msgCount %d\n", myid,
            msgCount);

    /* Lets stop the messaging engine this will block until they complete */
    /* FIXME arg is the threadlvl */
    sdf_msg_stopmsg(myid, SYS_SHUTDOWN_SELF);

    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
            "Messaging Engine Stopped - process %d... \n", myid);
    sleep(2);
    printf("Node %d: Messaging Test Complete\n", myid);
    fflush(stdout);

    sdf_msg_nsync(myid, (myid == 0 ? 1 : 0));

    MPI_Finalize();
    return (EXIT_SUCCESS);

}

#include "platform/opts_c.h"

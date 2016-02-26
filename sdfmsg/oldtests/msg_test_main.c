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
 * File:   msg_test_main.c
 * Author: Norman Xu
 *
 * Created on April 30, 2008, 9:42 AM
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: msg_test_main.c 308 2008-04-30 9:42:58Z norman $
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
#define PLAT_OPTS_NAME(name) name ## _testitem
//#define PLAT_OPTS_NAME(name) name ## _mpilogme
#include "platform/opts.h"
#include "platform/shmem.h"
#include "platform/fcntl.h"
#include "fth/fth.h"
#include "fth/fthMbox.h"
#include "platform/errno.h"
#include "sdfmsg/sdf_fth_mbx.h"
#include "sdfmsg/sdf_msg_types.h"
#include "msg_test.h"
#include "slinkedlist.h"
/*#define PLAT_OPTS_ITEMS_mpilogme() \
    PLAT_OPTS_SHMEM(shmem)

 struct plat_opts_config_mpilogme {
 struct plat_shmem_config shmem;
 };*/

#define PLAT_OPTS_ITEMS_testitem() \
    PLAT_OPTS_SHMEM(shmem) \
    item("testitems", "test protocol names", TESTITEMS, \
            parse_string_alloc(&config->protocol_names, optarg, 1024), \
            PLAT_OPTS_ARG_REQUIRED) \
    item("nitems", "number of testitems", NITEMS, \
            parse_int(&config->nproto, optarg, NULL), \
            PLAT_OPTS_ARG_REQUIRED) \
    item("tracelevel", "trace level", TRACELEVEL, \
            parse_string_alloc(&psztracelevel, optarg, 256), \
             PLAT_OPTS_ARG_REQUIRED | PLAT_OPTS_ARG_NO) \
            
struct plat_opts_config_testitem {
    struct plat_shmem_config shmem;
    int nproto;
    char *protocol_names;
};

typedef struct traceinput {
    char sztrace[256];
    enum TRACE_LEVEL tracelevel;
} TraceInput;

//<Norman add>
//typedef void* (*TestThreadFunc)(uint64_t);
typedef void* (*TestThreadFunc)(void *);

typedef struct testItemEntry {
    char test_names[256];
    TestThreadFunc testfunc;
} TestItemEntry;

TestItemEntry testitementries[] = {
        { "fthordertest", OrderTestFthPthreadRoutine },
        { "throughputtest", ThroughputThreadRoutine},
        { "pthreadordertest", OrderTestFthPthreadRoutine },
        { "bigsizetest", BigSizePthreadRoutine},
        { "bigsizefthtest", BigSizeFthPthreadRoutine},
        { "receivequeuetest", ReceiveQueuePthreadRoutine},
        { "sendqueuetest", SendQueuePthreadRoutine},
        { "simplexsendreceive", SimplexSendReceiveRoutine},
        { "consistency", ConsistencyPthreadRoutine },
        { "management", ManagementPthreadRoutine },
        { "membership", MembershipPthreadRoutine },
        { "metadata", MetadataPthreadRoutine },
        { "replicaton", ReplicationPthreadRoutine },
        { "system", SystemPthreadRoutine },
        { "flush", FlushPthreadRoutine },
        { "", NULL } };

char *psztracelevel;
TraceInput traceinputs[] = { { "concise", concisetrace }, { "general",
        generaltrace }, { "detail", detailtrace }, 
};
//</Norman add>  

int myid, pnodeid;

PLAT_LOG_CAT_LOCAL(LOG_CAT, "sdf/sdfmsg/tests/mpilog")
;
/* used to sync the fth threads in sdf_msg_protocol.c for the unit tests upon start */
int outtahere;

//pthread_t fthConsistencythread;
pthread_t fthPthread;
int msgCount;
enum TRACE_LEVEL tracelevel;
static char * backing_file = "/tmp/shmem";
static char * backing_file1 = "/tmp/shmem1";
/* Lock this process to the lowest number cpu available */
static void lock_processor(void) {
    cpu_set_t cpus;
    uint32_t cpu;

    return; /* keep the compiler happy */

    (void) sched_getaffinity(0, CPU_SETSIZE, &cpus);
    for (cpu = 0; cpu < CPU_SETSIZE; ++cpu) {
        if (CPU_ISSET(cpu, &cpus)) {
            break;
        }
    }
    CPU_ZERO(&cpus);
    CPU_SET(cpu, &cpus);
    (void) sched_setaffinity(0, CPU_SETSIZE, &cpus);
    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
            "Completed locking processor...\n");
}

#ifdef LOCAL_SHMEM
int init_sdfmsgtst_sm() {
    int ret = 0;
    int fd = -1;
    int tmp;

    if (myid == 0) {
        tmp = plat_unlink(backing_file);
    } else if (myid == 1) {
        tmp = plat_unlink(backing_file1);
    }
    if (tmp && plat_errno != ENOENT) {
        plat_log_msg(PLAT_LOG_ID_INITIAL, PLAT_LOG_CAT_PLATFORM_TEST_SHMEM,
                PLAT_LOG_LEVEL_FATAL, "ulink(%s) failed: %s", backing_file,
                plat_strerror(plat_errno));
        ret = 1;
    }

    if (!ret) {
        if (myid == 0)
            fd = plat_creat(backing_file, 0644);
        else if (myid == 1)
            fd = plat_creat(backing_file1, 0644);
        if (fd == -1) {
            plat_log_msg(PLAT_LOG_ID_INITIAL, PLAT_LOG_CAT_PLATFORM_TEST_SHMEM,
                    PLAT_LOG_LEVEL_FATAL, "creat(%s) failed: %s", backing_file,
                    plat_strerror(plat_errno));
            ret = 1;
        }
    }

    if (!ret && plat_ftruncate(fd, (off_t) 40960) == -1) {
        plat_log_msg(PLAT_LOG_ID_INITIAL, PLAT_LOG_CAT_PLATFORM_TEST_SHMEM,
                PLAT_LOG_LEVEL_FATAL, "truncate(%s, %lld) failed: %s",
                backing_file, (long long) 40960, plat_strerror(plat_errno));
        ret = 1;
    }

    if (fd != -1) {
        plat_close(fd);
    }

    if (!ret) {
        if (myid == 0)
            tmp = plat_shmem_attach(backing_file);
        else if (myid == 1)
            tmp = plat_shmem_attach(backing_file1);
        if (tmp) {
            plat_log_msg(PLAT_LOG_ID_INITIAL, PLAT_LOG_CAT_PLATFORM_TEST_SHMEM,
                    PLAT_LOG_LEVEL_FATAL, "shmem_attach(%s) failed: %s",
                    backing_file, plat_strerror(-tmp));
            ret = 1;
        }
    }

    return (ret);
}

#endif

int gettracelevel(char* sztracelevel, enum TRACE_LEVEL *tracelevel) {
    int i;
    for (i=0; i<sizeof(traceinputs); i++) {
        if (strcmp(traceinputs[i].sztrace, sztracelevel)==0) {
            *tracelevel = traceinputs[i].tracelevel;
            return 1;
        }
    }
    return 0;
}
//count testitems from the input
int numberoftestitems(char * psztestitems) {
    char * ptemp = psztestitems;
    char * pwatch = 0;
    char szitem[256];
    int count = 0;
    int copylength = 0;
    if (!psztestitems || strlen(psztestitems)==0)
        return 0;
    while (*ptemp!=0) {
        /*printf("the current character is %c\n", *ptemp);*/
        if (*ptemp==' ' || *ptemp=='\t') {
            if (pwatch!=0) {
                copylength = (ptemp - pwatch)<255 ? (ptemp - pwatch) : 255;
                strncpy(szitem, pwatch, copylength);
                count++;
                pwatch = 0;
            }
            ptemp++;
            continue;
        } else if (pwatch==0) {
            pwatch = ptemp;
            ptemp++;
        } else
            ptemp++;
    }
    if (pwatch!=0) {
        copylength = (ptemp - pwatch)<255 ? (ptemp - pwatch) : 255;
        strncpy(szitem, pwatch, copylength);
        count++;
    }
    return count;
}

sLinkPtr createLLEntryfromitem(char * pszitem) {
    int i=0;
    sLinkPtr pentry = 0;
    while (testitementries[i].testfunc!=NULL) {
        if (strcmp(pszitem, testitementries[i].test_names)==0) {
            pentry = (sLinkPtr) plat_malloc(sizeof(struct sLinkEntry));
            pentry->userdef_ptr = testitementries[i].testfunc;
            strcpy(pentry->str, testitementries[i].test_names);
            pentry->next = 0;
            pentry->prev = 0;
            return pentry;
        }
        i++;
    }
    return 0;
}

int appendLLfromtestitems(sLinkListPtr plist, char * psztestitems) {
    char * ptemp = psztestitems;
    char * pwatch = 0;
    char szitem[256];
    int count = 0;
    int copylength = 0;
    sLinkPtr pentry = 0;
    numberoftestitems(psztestitems);
    if (!plist || !psztestitems || strlen(psztestitems)==0)
        return 0;
    while (*ptemp!=0) {
        if (*ptemp==' ' || *ptemp=='\t') {
            if (pwatch!=0) {
                copylength = (ptemp - pwatch)<255 ? (ptemp - pwatch) : 255;
                strncpy(szitem, pwatch, copylength);
                szitem[copylength]='\0';
                count++;
                if ((pentry = createLLEntryfromitem(szitem))!=0) {
                    if (appendLinkEntry(plist, pentry))
                        TestTrace(tracelevel, generaltrace,
                                "item %s appended\n", szitem);
                }
                TestTrace(tracelevel, generaltrace,
                        "Number %d testitem is %s\n", count, szitem);
                pwatch = 0;
            }
            ptemp++;
            continue;
        } else if (pwatch==0) {
            pwatch = ptemp;
            ptemp++;
        } else
            ptemp++;
    }
    if (pwatch!=0) {
        copylength = (ptemp - pwatch)<255 ? (ptemp - pwatch) : 255;
        strncpy(szitem, pwatch, copylength);
        szitem[copylength]='\0';
        count++;
        if ((pentry = createLLEntryfromitem(szitem))!=0) {
            if (appendLinkEntry(plist, pentry))
                TestTrace(tracelevel, generaltrace,
                        "item %s appended to test list\n", szitem);
        } else
            TestTrace(tracelevel, generaltrace, "szitem %s can't be found\n",
                    szitem);
    }
    return count;
}

int main(int argc, char *argv[]) {
    msgCount = 50;
    struct plat_opts_config_testitem config;
    tracelevel = generaltrace;
    SDF_boolean_t success = SDF_TRUE;
    uint32_t numprocs;

    sLinkListPtr linkedtestlist[2];
    
    myid = sdf_msg_init_mpi(argc, argv, &numprocs, &success);

    if (!success) {
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                     "Node %d: MPI Init failure... exiting\n", myid);
        MPI_Finalize();
        return (EXIT_SUCCESS);
    }
    memset(&config, 0, sizeof(config));
    plat_shmem_config_init(&config.shmem);

    // Totally arbitrary default
    config.shmem.size = 1024*1024;
    config.protocol_names = 0;
    config.nproto = 0;
    psztracelevel = 0;
    if (plat_opts_parse_testitem(&config, argc, argv)) {
        printf("Error parse input argument\n");
        return 2;
    } else {
        if (config.nproto==0 || config.protocol_names==0) {
            TestTrace(concisetrace, concisetrace,
                    "--testitems and --nitems must be specified!\n");
            return 2;
        }
        if (numberoftestitems(config.protocol_names)!=config.nproto) {
            TestTrace(concisetrace, concisetrace,
                    "number of testitems does not match!\n");
            return 2;
        }
        if (psztracelevel!=0) {
            if (!gettracelevel(psztracelevel, &tracelevel)) {
                TestTrace(concisetrace, concisetrace,
                        "set tracelevel to defaul one: general\n");
            }
        } else {
            TestTrace(concisetrace, concisetrace,
                    "set tracelevel to defaul one: general\n");
        }

        TestTrace(tracelevel, generaltrace, "%d %s\n", config.nproto,
                config.protocol_names);
    }



#ifdef LOCAL_SHMEM
    int tmp;
    printf("init_sdfmsgtst_sm called\n");
    tmp = init_sdfmsgtst_sm();
#endif

    lock_processor(); /* FIXME just a placeholder for now */

    /* FIXME we have to get off this dual node crap and soon */
    pnodeid = myid == 0 ? 1 : 0;

    /* Startup SDF Messaging Engine FIXME again another hardcoded value */
    printf("before init\n");   
    sdf_msg_init(myid, &pnodeid, 0);
    printf("after init\n");     
    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
            "\nNode %d: Completed Msg Init.. Starting Test\n", myid);

    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
            "\nSchooner Node ID %d pnode %d total procs %d\n", myid, pnodeid,
            numprocs);

    for (msgCount = 0; msgCount < 2; msgCount++) {
        sleep(2);
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                "\nNode %d: Number of sleeps %d\n", myid, msgCount);
    }
    // </Initialization>
    sdf_msg_startmsg(myid, 0, NULL);
    // Run the test one by one
    /* create the fth test threads */
    linkedtestlist[myid] = createsLinkList();
    appendLLfromtestitems(linkedtestlist[myid], config.protocol_names);
    TestTrace(tracelevel, detailtrace, "linked test list length %d\n",
            linkedtestlist[myid]->length);

    linkedtestlist[myid]->current = linkedtestlist[myid]->head;
    while (linkedtestlist[myid]->current) {
        sLinkPtr linkentry = linkedtestlist[myid]->current;
        TestTrace(tracelevel, detailtrace, "id %d test item %s begins\n", myid,
                linkentry->str);

        //memset(&fthPthread, 0, sizeof(pthread_t));
        TestTrace(tracelevel, generaltrace, "before pthread create\n");
        
        // set the priority
        struct sched_param param;
        int newprio = 60;
        pthread_attr_t hi_prior_attr;

        pthread_attr_init(&hi_prior_attr);
        pthread_attr_setschedpolicy(&hi_prior_attr, SCHED_RR);
        pthread_attr_getschedparam(&hi_prior_attr, &param);
        param.sched_priority = newprio;
        pthread_attr_setschedparam(&hi_prior_attr, &param);

        pthread_create(&fthPthread, &hi_prior_attr,
                (TestThreadFunc)linkentry->userdef_ptr, &myid);
        TestTrace(tracelevel, generaltrace, "after pthread create\n");
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                "\nNode %d: Created pthread%d\n", myid, msgCount);

        pthread_join(fthPthread, NULL);

        TestTrace(
                tracelevel,
                generaltrace,
                "########################################\n\
                node %d thread %s has runs out\n\
                ########################################\n",
                myid, linkedtestlist[myid]->current->str);

        linkedtestlist[myid]->current = linkedtestlist[myid]->current->next;
    }

    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
            "\nNode %d: Do we get here %d\n", myid, msgCount);

    TestTrace(tracelevel, detailtrace, "node %d before stopmsgng\n", myid);
    /* Lets stop the messaging engine this will block until they complete */
    /* FIXME arg is the threadlvl */
    sdf_msg_stopmsg(myid, 2);

    TestTrace(tracelevel, generaltrace, "node %d after stopmsgng\n", myid);

    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
            "\nNode %d: Do we get here %d\n", myid, msgCount);

    destorysLinkList(linkedtestlist[myid]);
    TestTrace(tracelevel, detailtrace, "LinkList myid is %d is destroyed!\n",
            myid);

    MPI_Finalize();

    return (EXIT_SUCCESS);
}
#include "platform/opts_c.h"


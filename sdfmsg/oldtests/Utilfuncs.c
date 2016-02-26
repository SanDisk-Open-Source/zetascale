/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

#ifdef MPI_BUILD
#include <mpi.h>
#endif

#include <stdint.h>
#include <pthread.h>
#include <sched.h>
#include <stdio.h>
#include "platform/assert.h"
#include "platform/errno.h"
#include "platform/fcntl.h"
#include "platform/logging.h"
#include "platform/shmem.h"
#include "platform/stdlib.h"
#include "platform/stdio.h"
#include "platform/string.h"
#include "platform/opts.h"
#include "platform/types.h"
#include "utils/properties.h"

#include "sdfmsg/sdf_msg_types.h"
#include "sdfmsg/sdf_msg.h"
#include "sdfmsg/sdf_fth_mbx.h"
#include "fcnl_test.h"

#define LOG_CAT PLAT_LOG_CAT_SDF_SDFMSG 

struct msgtst_config {
    /** Filename for backing store */
    char *mmap;
    /** Size of file */
    int64_t size;
};

static char processret[8][128];

/** Support up to 4 sdf_msg daemons on a node at the same time */
struct msgtst_config msgconfig[] = {{ "", 16 * 1024 * 1024 }, 
                                    { "", 16 * 1024 * 1024 }, 
                                    { "", 16 * 1024 * 1024 }, 
                                    { "", 16 * 1024 * 1024 }};

struct sdf_queue_pair * local_create_myqpairs(service_t protocol,
        uint32_t myid, uint32_t pnode) {
    struct sdf_queue_pair *q_pair;

    if (myid == pnode) {
        return (q_pair = (sdf_create_queue_pair(myid, pnode, protocol,
                protocol, SDF_WAIT_FTH)));
    } else {
        return (q_pair = (sdf_create_queue_pair(myid, myid == 0 ? 1 : 0,
                protocol, protocol, SDF_WAIT_FTH)));
    }
}

struct sdf_queue_pair * local_create_myqpairs_with_pthread(service_t protocol,
        uint32_t myid, uint32_t pnode) {
    struct sdf_queue_pair *q_pair;

    if (myid == pnode) {
        return (q_pair = (sdf_create_queue_pair(myid, pnode, protocol,
                protocol, SDF_WAIT_CONDVAR)));
    } else {
        return (q_pair = (sdf_create_queue_pair(myid, myid == 0 ? 1 : 0,
                protocol, protocol, SDF_WAIT_CONDVAR)));
    }
}

void TestTrace(int tracelevel, int selflevel, const char *format, ...) {
    char buf[2048];
    //if program trace level is higher than selflevel, print the trace information
    if (tracelevel >= selflevel) {
        va_list args;
        va_start(args, format);
        sprintf(buf, "MSG_TEST_TRACE: ");
        vsnprintf(buf + strlen(buf), sizeof(buf) - strlen(buf), format, args);
        va_end(args);
        puts(buf);
    }
}

/*
 * fth thread sender say goodbye to receiver
 * or receiver tells its brothers to finalize
 */
int sdf_msg_say_bye(vnode_t dest_node, service_t dest_service,
        vnode_t src_node, service_t src_service, msg_type_t msg_type,
        sdf_fth_mbx_t *ar_mbx, uint64_t length) {
#define BYE_LENGTH 256
    struct sdf_msg* msg_bye;
    int ret, i;
    //only support consistency now
    if (dest_service != SDF_CONSISTENCY || msg_type != GOODBYE) {
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_FATAL,
                "Wrong parameters, check your protocol type and msg_type.\n");
        return -1;
    }

    //alloc 256bytes with 128byte payload, for it can carry some additinal information
    msg_bye = (struct sdf_msg *)sdf_msg_alloc(length);
    if (msg_bye == NULL) {
        /* FIXME should default to an error  */
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_FATAL,
                "sdf_msg_alloc(TSZE) failed\n");
        return -1;
    }

    for (i = 0; i < length; i++) {
        msg_bye->msg_payload[i] = (unsigned char) 0xFF;
    }
    printf("Before send bye\n");fflush(stdout);
    ret = sdf_msg_send((struct sdf_msg *) msg_bye, length, dest_node,
            dest_service, src_node, src_service, msg_type, ar_mbx, NULL);
    printf("After send bye\n");fflush(stdout);
    if (ret != 0) {
        plat_log_msg(
                PLAT_LOG_ID_INITIAL,
                LOG_CAT,
                PLAT_LOG_LEVEL_TRACE,
                "\nNode %d: Message Sent Error ret %d protocol %d type %d HALTING\n",
                src_node, ret, src_service, msg_type);
        return -1;
    }
    sdf_msg_free_buff(msg_bye);
#if 0
    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
            "After sending message, we should wait for ack\n");
    aresp = fthMboxWait(ar_mbx->abox);
    if (!ar_mbx->actlvl) {
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                "\nNode %d: actvl %d\n", src_node, ar_mbx->actlvl);
        plat_assert(ar_mbx->actlvl >= 1);
    } else {
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                "\nNode %d: Sleeping on RESP, fth mailbox %p\n", src_node,
                ar_mbx->rbox);
        /*
         * Sleep on the mailbox waiting to get a properly directed response message
         */
        if (ar_mbx->rbox) {
            sdf_msg_t* msg = (sdf_msg_t *) fthMboxWait(ar_mbx->rbox);

            plat_log_msg(
                    PLAT_LOG_ID_INITIAL,
                    LOG_CAT,
                    PLAT_LOG_LEVEL_TRACE,
                    "\nNode %d: RESP msg %p seq %lu sn %d dn %d proto %d type %d\n",
                    src_node, msg, msg->msg_conversation, msg->msg_src_vnode,
                    msg->msg_dest_vnode, msg->msg_dest_service, msg->msg_type);

            /* release the receive buffer back to the sdf messaging thread */
            ret = sdf_msg_free_buff(msg);
            plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                    "\nNode %d: freed buff, ret %d\n", src_node, ret);
        }
    }
#endif
    return 0;
#undef BYE_LENGTH
}

static int 
sdfmsg_shmem_createConfig(uint32_t index) {
    char *backing_dir = NULL;
    char *backing_file = NULL;
    char *user = getenv("USER");
    char *propName = plat_alloc(128);
    const char *basedir;
    int status;

    for (unsigned count = 0; count < 4; count++) {
        snprintf(propName, 128, "NODE[%u].SHMEM.BASEDIR", count);
        basedir = getProperty_String(propName, plat_get_tmp_path());

        // ensure basedir exists
        if (index == count) {
            int ret = mkdir(basedir, 0777);
            if (0 != ret && EEXIST != plat_errno) {
                plat_log_msg(PLAT_LOG_ID_INITIAL, PLAT_LOG_CAT_SDF_SDFMSG,
                        PLAT_LOG_LEVEL_FATAL,
                        "Error creating path (%s), returned (%d)", basedir, ret);
                return (ret);
            }
        }

        if (user != NULL) {
            status = plat_asprintf(&backing_dir, "%s/%s", basedir, user);
        } else {
            status = plat_asprintf(&backing_dir, "%s/%d", basedir, getuid());
        }
        plat_assert(status != -1);

        // create backing_file
        if (index == count) {
            int ret = mkdir(backing_dir, 0777);
            if (0 != ret && EEXIST != plat_errno) {
                plat_log_msg(PLAT_LOG_ID_INITIAL, PLAT_LOG_CAT_SDF_SDFMSG,
                        PLAT_LOG_LEVEL_FATAL,
                        "Error creating path (%s), returned (%d)",
                        backing_file, ret);
                return (ret);
            }
        }

        status = plat_asprintf(&msgconfig[count].mmap, "%s/sdf_shmem%u",
                               backing_dir, count);
        plat_assert(status != -1);
    
        plat_free(backing_file);
    }
    plat_free(propName);
    return (0);
}

static SDF_boolean_t 
init_msgtest_sm_internal(struct plat_shmem_config *shmem_config, uint32_t index) {
    SDF_boolean_t ret = SDF_TRUE;
    int tmp;
    const char *path;

    if (0 != sdfmsg_shmem_createConfig(index)) {
        return SDF_FALSE;
    }

    if (SDF_TRUE == ret && 
        plat_shmem_config_path_is_default(shmem_config)) {
        char *propName = NULL;
        plat_asprintf(&propName, "NODE[%u].SHMEM.SIZE", index);
        int64_t sm_size = getProperty_uLongLong(propName, msgconfig[index].size);
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_DEBUG,
                     "\nPROP: %s=%lu\n", propName, sm_size);
        if (plat_shmem_config_add_backing_file(shmem_config, msgconfig[index].mmap, sm_size)) {
            ret = SDF_FALSE;
        }
        plat_free(propName);
    }

    path = plat_shmem_config_get_path(shmem_config);

    if (SDF_TRUE == ret) {
        int fake = 0;
        if (fake == getProperty_Int("SHMEM_FAKE", 0)) {
            plat_shmem_config_set_flags(shmem_config, PLAT_SHMEM_CONFIG_DEBUG_LOCAL_ALLOC);
        }
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                     "\nPROP: SHMEM_FAKE = %d\n", fake);
        tmp = plat_shmem_prototype_init(shmem_config);
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_DEBUG,
                     "\nDone with plat_shmem_prototype_init: ret = %d\n", tmp);
        if (tmp) {
            plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_FATAL,
                         "\nshmem_init(%s) failed: %s\n", path, plat_strerror(-tmp));
            ret = SDF_FALSE;
        }
    }

    if (SDF_TRUE == ret) {
        tmp = plat_shmem_attach(plat_shmem_config_get_path(shmem_config));
        if (tmp) {
            plat_log_msg(PLAT_LOG_ID_INITIAL, PLAT_LOG_CAT_PLATFORM_TEST_SHMEM,
                    PLAT_LOG_LEVEL_FATAL, "shmem_attach(%s) failed: %s",
                    msgconfig[index].mmap, plat_strerror(-tmp));
            ret = SDF_FALSE;
        }
    }

    return (ret);
}

SDF_boolean_t init_msgtest_sm(uint32_t index) {
    SDF_boolean_t ret = SDF_TRUE;
    struct plat_shmem_config shmem_config;

    plat_shmem_config_init(&shmem_config);

    if (SDF_TRUE == ret) {
        ret = init_msgtest_sm_internal(&shmem_config, index);
    }

    plat_shmem_config_destroy(&shmem_config);

    return (ret);
}

/* setup configuration defaults for tests */

int
msgtst_setconfig(struct plat_opts_config_mpilogme *config) {

    config->inputarg = 0;
    config->msgtstnum = 50;
    strncpy(config->propertyFileName,
            "/opt/schooner/config/schooner-med.properties",
            sizeof(config->propertyFileName));
    config->sdf_msg_init_state = 0;
    config->msg_init_flags = SDF_MSG_NO_MPI_INIT;
    config->numprocs = 1;
    config->myid = 0;
    config->cores = 2;
    config->startMessagingThreads = 0;
    config->nnum = 0;
    config->port=101;
    return (0);
}

int
msgtst_setpreflags(struct plat_opts_config_mpilogme *config) {
    if (config->sdf_msg_init_state) {
        config->msg_init_flags = SDF_MSG_MPI_INIT;
        if (config->sdf_msg_init_state == 2) {
            config->msg_init_flags |= SDF_MSG_RTF_DISABLE_MNGMT;
        }
    }
    return (0);
}

int
msgtst_setpostflags(struct plat_opts_config_mpilogme *config) {
    if ((!config->sdf_msg_init_state) && (config->startMessagingThreads)) {
        config->msg_init_flags = ((config->msg_init_flags | SDF_MSG_RTF_DISABLE_MNGMT)
                                  | SDF_MSG_SINGLE_NODE_MSG);
    } else {

    }
    return (0);
}



vnode_t
local_get_pnode(int localrank, int localpn, uint32_t numprocs) {
    vnode_t node = 0;
    int tmp = 1, i;
    tmp = tmp << localrank;
    tmp = tmp ^ localpn;
    printf("Node %d: %s eligible node mask 0x%x\n", localrank, __func__, tmp);
    fflush(stdout);
    for (i = 0; i < numprocs; i ++) {
        if ((tmp >> i) & 1) {
            node = i;
            break;
        }
    }
    return node;
}
void
local_setmsg_payload(sdf_msg_t *msg, int size, uint32_t nodeid, int num) {
    int i;
    for (i = 0; i < size; ++i) {
    if (nodeid == 0)
        msg->msg_payload[i] = (char)(num + 65);//A B C ....
    else
        msg->msg_payload[i] = (char)(num + 97);//a b c ....
    }
    if (nodeid == 0)
        printf("node %d ,sender sends message %c\n", nodeid, num + 65);
    else
        printf("node %d, sender sends message %c\n", nodeid, num + 97);
}

void
local_setmsg_mc_payload(sdf_msg_t *msg, int size, uint32_t nodeid, int index, int maxcount, uint64_t ptl) {
    int i;
    for (i = 0; i < size; ++i)
        msg->msg_payload[i] = 0 * maxcount + index;
    printf("node %d ,sender sends message %d\n", nodeid, 0 * maxcount + index);

}


void
local_printmsg_payload(sdf_msg_t *msg, int size, uint32_t nodeid) {
    int i;
    char *m = (char *)(msg->msg_payload);
    for (i = 0; i < size; i ++) {
        printf("%d ", *m);
        m ++;
        if ((i % 16) == 15) {
            printf("  nodeid %d", nodeid);
            putchar('\n');
            fflush(stdout);
        }
    }
}

/* process the return value of the queue posts, alert if there is an error */

int
process_ret(int ret_err, int prt, int type, vnode_t myid) {

    strcpy(processret[1], "QUEUE_FULL");
    strcpy(processret[2], "QUEUE_EMPTY");

    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_ERROR,
                 "\nNode %d: Message Post Failed - %s returned val %d protocol %d type %d WAITING 2 SECs\n", 
                 myid, processret[ret_err], ret_err, prt, type);
    sleep(2);
    return(ret_err);
}

/* this is deprecated */

#if 0
uint64_t
get_timestamp()
{
    struct timespec curtime;
    
    (void) clock_gettime(CLOCK_REALTIME, &curtime);
    return ((uint64_t)curtime.tv_nsec);
}   

uint64_t
show_howlong(uint64_t oldtm, int n, uint64_t *arr, vnode_t myid, int DBGP)
{
    struct timespec curtime;
    uint64_t difftm;
    
    (void) clock_gettime(CLOCK_REALTIME, &curtime);
    if (curtime.tv_nsec < oldtm) {
        difftm = (1000000000 + curtime.tv_nsec) - oldtm;
    } else {
        difftm = curtime.tv_nsec - oldtm;
    }
    
    arr[n] = difftm;
    if (DBGP) {
        printf("\nNode %d: Seq %d - ELAPSED TIME new %li old %li diff %li\n",
               myid, n, curtime.tv_nsec, oldtm, difftm);
        fflush(stdout); 
    }
    return ((uint64_t)curtime.tv_nsec);
}   
#endif

uint64_t 
get_passtime(struct timespec* start, struct timespec* end)
{
        uint64_t sec, nsec, ret;
        if(start->tv_nsec > end->tv_nsec)
        {
                sec = end->tv_sec - 1 - start->tv_sec;
                nsec = end->tv_nsec + 1000000000 - start->tv_nsec;
        }
        else
        {
                sec = end->tv_sec - start->tv_sec;
                nsec = end->tv_nsec - start->tv_nsec;
        }

        ret = sec * 1000000 + nsec / 1000;
        return ret;
}

/*
 * File:   agent_config.h
 * Author: Xiaonan Ma
 *
 * Created on Jul 11 17:47:24 PDT 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: agent_common.h 8575 2009-08-03 17:22:42Z briano $
 */

#ifndef __AGENT_CONFIG_H__
#define	__AGENT_CONFIG_H__

#include "sdftcp/trace.h"
#undef LOG_CAT

#ifdef SDFAPI
#include "api/sdf.h"
#else
#include "common/sdf_properties.h"
#endif /* SDFAPI */
#include "fth/fthOpt.h"

/*
 * XXX: drew 2008-11-14 This is growing out of control.  We want to generate
 * a common configuration which is passed into all the subsystems so changes
 * don't ripple through the system.
 */
struct plat_opts_config_sdf_agent {
    struct plat_shmem_config shmem;

    // Will be deprecated
    /** @brief no default logging output */

    int log_less;

    /** @brief use message-passing interface when not replicating */
    int flash_msg;

    /** @brief number of ways to replicate, 0 for direct flash, can be 1 */
    int always_replicate;

    /** @brief Default replication type */
    SDF_replication_t replication_type;

    /**
     * @brief Number of outstanding writes for for default replication
     *
     * Putting a cap on the number of outstanding commands which can be 
     * executed bounds the search space in which replicas can disagree
     * following a crash.
     */
    int replication_outstanding_window;

    /**
     * @brief Lease on home node
     *
     * This trades off switch-over time (and therefore unavailability
     * while a failure exists) for background flash traffic executing
     * Paxos NOPs.
     *
     * If it becomes impossible to issue a request this frequently
     * the system stalls.
     */
    int replication_lease_secs;

    /**
     * @brief Leases remain until liveness changes for test 
     *
     * When debugging recovery state problems lease timeouts are inconvienient
     * because they change the system state. When sdf_replicator_config,
     * lease_usecs is set to this value leases will never time out and
     * all attempts to acquire a lease will succeed as long as the old
     * home node is dead according to the liveness subsystem and the new
     * meta-data is causally related to the previous version with a correct
     * ltime.
     */
    unsigned replication_lease_liveness : 1;

    /**
     * @brief Switch-back timeout
     *
     * The replication code will hold-off on regaining ownership of vips
     * it gave back until it sees a node dead event or the other node 
     * assume ownership.
     *
     * This timeout should be unecessary (we will never fail to generate
     * a node dead event) although as of 2009-12-16 drew feels anxiety 
     * about the coming release and decided to include this as a fail
     * safe.
     */
    int replication_switch_back_timeout_secs; 

    /**
     * @brief Attempt to land VIPs on preferred node at startup
     *
     * XXX: drew 2009-11-04 In the situation 
     *
     * node 1 start, gets preferred vip 10.0.0.1
     * client writes to node 1
     * node 2 start, gets preferred vip 10.0.0.2
     *
     * sdf/action/simple_replication.c would never copy the client writes
     * to node 2.
     *
     * VIPs are handled with a preference list
     *
     * vip 10.0.0.1 prefered nodes { 1, 2 }
     * vip 10.0.0.2 preferred nodes { 2, 1 }
     *
     * So that a simultaneous restart puts VIPs on their preferred nodes
     * and to minimize chances of two nodes grabbing the same VIP and
     * colliding on retry, the acquisition delay is a constant 
     * (to receive updates on what nodes currently host what VIPs which
     * were triggered by the liveness system) plus an offset according
     * to the nodes instance.
     *
     * For instance, node 1 may wait the fixed 1 second before acquiring
     * 10.0.0.1 and 2 seconds before getting 10.0.0.2 while node 2 
     * does the opposite.
     *
     * The simplest work around for the simple_replication.c bug
     * is to make the initial back-off entirely dependent on 
     * node number, which leads to the situation
     *
     * node 1 start, gets both vips 10.0.0.1 and 10.0.0.2
     * client writes to node 1
     * node 2 start, gets no vips
     * recovery
     * noed 1 drops 10.0.0.2
     * node 2 gets 10.0.0.2
     *
     * This is now the default behavior.  
     * 
     * #replication_initial_preference causes the historic 
     * behavior.
     */
    unsigned replication_initial_preference : 1;

    /** @brief disable fast path from action to home code */
    int disable_fast_path;

    /** @brief system recovery type */
    int system_recovery;

    /** @brief system started in authoritive mode or not*/
    int auth_mode;

    /** @brief restart after a node goes down */
    int system_restart;

    /** @brief Number of nodes.  0 implies that this will come from MPI */
    int nnodes;
    unsigned numHomeThreads;
    unsigned numActionThreads;
    unsigned numFlashProtocolThreads;
    unsigned numAsyncPutThreads;
    unsigned numReplicationThreads;
    unsigned defaultShardCount;

    char flashDevName[PATH_MAX];

    unsigned numFlashDevs;

    char propertyFileName[PATH_MAX];

    int numObjs;

    /* how many mboxes should the agent create to listen  on */
    unsigned numAgentMboxes;

    /** @brief FFDC buffer len in bytes */
    int64_t ffdc_buffer_len;

    /** @brief disable FFDC logging */
    int ffdc_disable;
};


/*
 * XXX: drew 2009-06-26 We should include a replicator config structure 
 * and give that its own argument parsing code like shmem to keep all of 
 * its argument processing local to the code
 */
#define PLAT_OPTS_ITEMS_sdf_agent() \
    PLAT_OPTS_SHMEM(shmem)                                                     \
    PLAT_OPTS_FTH()                                                            \
    item("log_less", "don't use Darpan's defaults", LOG_LESS,                  \
         ({ config->log_less = 1; 0; }), PLAT_OPTS_ARG_NO)                     \
    item("flash_msg", "use message passing between home & flash threads",      \
         FLASH_MSG, ({ config->flash_msg = 1; 0; }), PLAT_OPTS_ARG_NO)         \
    item("replicate", "replication ways",                                      \
         REPLICATE, parse_int(&config->always_replicate, optarg, NULL),        \
         PLAT_OPTS_ARG_REQUIRED)                                               \
    item("disable_fast_path", "disable action home fast path ",                \
         DISABLE_FAST_PATH, ({ config->disable_fast_path = 1; 0; }),           \
         PLAT_OPTS_ARG_NO)                                                     \
    item("num_objs", "number of objects to create in flash shards",            \
         NUM_OBJS, parse_int(&config->numObjs, optarg, NULL),                  \
         PLAT_OPTS_ARG_REQUIRED)                                               \
    item("flash_dev", "flash device, embed %d for node number",                \
         FLASH_DEV, parse_string(config->flashDevName, optarg),                \
         PLAT_OPTS_ARG_REQUIRED)                                               \
    item("property_file", "property file name",                                \
         PROPERTY_FILE, parse_string(config->propertyFileName, optarg),        \
         PLAT_OPTS_ARG_REQUIRED)                                               \
    item("recover", "when you want to recover the flash device(s)",            \
         RECOVER, ({ config->system_recovery = SYS_FLASH_RECOVERY; 0; }),      \
         PLAT_OPTS_ARG_OPTIONAL)                                               \
    item("reformat", "when you want to reformat the flash device(s)",          \
         REFORMAT, ({ config->system_recovery = SYS_FLASH_REFORMAT; 0; }),     \
         PLAT_OPTS_ARG_NO)                                                     \
    item("auth_mode", "when you want to start the instance in authoritative mode",          \
         AUTHMODE, ({ config->auth_mode = 1; 0; }),     \
         PLAT_OPTS_ARG_NO)                                                     \
    item("restart", "for restarting after a node goes down",                   \
         RESTART, ({ config->system_restart = 1; 0; }),                        \
         PLAT_OPTS_ARG_NO)                                                     \
    item("nnodes", "number of nodes (override mpi for testing, etc.)",         \
         NNODES, parse_int(&config->nnodes, optarg, NULL),                     \
         PLAT_OPTS_ARG_REQUIRED)                                               \
    item("replication_outstanding_window",                                     \
         "per container parallel IOs for default replication",                 \
         REPLICATION_OUTSTANDING_WINDOW,                                       \
         parse_int(&config->replication_outstanding_window, optarg, NULL),     \
         PLAT_OPTS_ARG_REQUIRED)                                               \
    item("replication_lease_secs",                                             \
         "lease in seconds setting minimum switch over time",                  \
         REPLICATION_LEASE_SECS,                                               \
         parse_int(&config->replication_lease_secs, optarg, NULL),             \
         PLAT_OPTS_ARG_REQUIRED)                                               \
    item("replication_lease_liveness",                                         \
         "use liveness instead of leases for test",                            \
         REPLICATION_LEASE_LIVENESS,                                           \
         ({ config->replication_lease_liveness = 1; 0; }), PLAT_OPTS_ARG_NO)   \
    item("replication_no_lease_liveness",                                      \
         "do not use liveness instead of leases for test",                     \
         REPLICATION_NO_LEASE_LIVENESS,                                        \
         ({ config->replication_lease_liveness = 0; 0; }), PLAT_OPTS_ARG_NO)   \
    item("replication_switch_back_timeout_secs",                               \
         "timeout on switch-back",                                             \
         REPLICATION_SWITCH_BACK_TIMEOUT_SECS,                                 \
         parse_int(&config->replication_switch_back_timeout_secs, optarg,      \
                   NULL), PLAT_OPTS_ARG_REQUIRED)                              \
    item("replication_initial_preference",                                     \
         "land VIPs on preferred nodes instead of first",                      \
         REPLICATION_INITIAL_PREFERENCE,                                       \
         ({ config->replication_initial_preference = 1; 0; }),                 \
         PLAT_OPTS_ARG_NO)                                                     \
    item("replication_type", "replication type", REPLICATION_TYPE,             \
         ({                                                                    \
          config->replication_type = str_to_sdf_replication(optarg);           \
          if (config->replication_type == SDF_REPLICATION_INVALID) {           \
            sdf_replication_usage();                                           \
          }                                                                    \
          config->replication_type == SDF_REPLICATION_INVALID ? -EINVAL : 0;   \
          }), PLAT_OPTS_ARG_REQUIRED)                                          \
    item("msg_affinity", "messaging: thread affinity", MSG_AFFINITY,           \
         ({ sdf_msg_setinitn("msg_affinity", optarg), 0;}),                    \
         PLAT_OPTS_ARG_REQUIRED)                                               \
    item("msg_alias", "messaging: node alias", MSG_ALIAS,                      \
         ({ sdf_msg_setinitn("msg_alias", optarg), 0;}),                       \
         PLAT_OPTS_ARG_REQUIRED)                                               \
    item("msg_class", "messaging: class", MSG_CLASS,                           \
         ({ sdf_msg_setinitn("msg_class", optarg), 0;}),                       \
         PLAT_OPTS_ARG_REQUIRED)                                               \
    item("msg_dead", "messaging: node minimum dead time", MSG_DEAD,            \
         ({ sdf_msg_setinitn("msg_dead", optarg), 0;}),                        \
         PLAT_OPTS_ARG_REQUIRED)                                               \
    item("msg_debug", "messaging: debug parameters", MSG_DEBUG,                \
         ({ sdf_msg_setinitn("msg_debug", optarg), 0;}),                       \
         PLAT_OPTS_ARG_REQUIRED)                                               \
    item("msg_flags", "messaging: flags", MSG_FLAGS,                           \
         ({ sdf_msg_setinitn("msg_flags", optarg), 0;}),                       \
         PLAT_OPTS_ARG_REQUIRED)                                               \
    item("msg_iface", "messaging: interfaces", MSG_IFACE,                      \
         ({ sdf_msg_setinitn("msg_iface", optarg), 0;}),                       \
         PLAT_OPTS_ARG_REQUIRED)                                               \
    item("msg_linklive", "messaging: link liveness time", MSG_LINKLIVE,        \
         ({ sdf_msg_setinitn("msg_linklive", optarg), 0;}),                    \
         PLAT_OPTS_ARG_REQUIRED)                                               \
    item("msg_live", "messaging: node liveness time", MSG_LIVE,                \
         ({ sdf_msg_setinitn("msg_live", optarg), 0;}),                        \
         PLAT_OPTS_ARG_REQUIRED)                                               \
    item("msg_mustlive", "messaging: liveness gets own thread", MSG_MUSTLIVE,  \
         ({ sdf_msg_setinitn("msg_mustlive", optarg), 0;}),                    \
         PLAT_OPTS_ARG_REQUIRED)                                               \
    item("msg_nconn", "messaging: default number of connections", MSG_NCONN,   \
         ({ sdf_msg_setinitn("msg_nconn", optarg), 0;}),                       \
         PLAT_OPTS_ARG_REQUIRED)                                               \
    item("msg_nhold", "messaging: default number of hold buffers", MSG_NHOLD,  \
         ({ sdf_msg_setinitn("msg_nhold", optarg), 0;}),                       \
         PLAT_OPTS_ARG_REQUIRED)                                               \
    item("msg_nobcast", "messaging: turn of broadcasting", MSG_NOBCAST,        \
         ({ sdf_msg_setinitn("msg_nobcast", optarg), 1;}),                     \
         PLAT_OPTS_ARG_REQUIRED)                                               \
    item("msg_nodes", "messaging: expected node count", MSG_NODES,             \
         ({ sdf_msg_setinitn("msg_nodes", optarg), 0;}),                       \
         PLAT_OPTS_ARG_REQUIRED)                                               \
    item("msg_nthreads", "messaging: number of threads", MSG_NTHREADS,         \
         ({ sdf_msg_setinitn("msg_nthreads", optarg), 0;}),                    \
         PLAT_OPTS_ARG_REQUIRED)                                               \
    item("msg_ntunique", "messaging: unique affinity threads", MSG_NTUNIQUE,   \
         ({ sdf_msg_setinitn("msg_ntunique", optarg), 0;}),                    \
         PLAT_OPTS_ARG_REQUIRED)                                               \
    item("msg_ping", "messaging: ping interval for liveness", MSG_PING,        \
         ({ sdf_msg_setinitn("msg_ping", optarg), 0;}),                        \
         PLAT_OPTS_ARG_REQUIRED)                                               \
    item("msg_rank", "messaging: rank", MSG_RANK,                              \
         ({ sdf_msg_setinitn("msg_rank", optarg), 0;}),                        \
         PLAT_OPTS_ARG_REQUIRED)                                               \
    item("msg_statfreq", "messaging: stats frequency shown", MSG_STATFREQ,     \
         ({ sdf_msg_setinitn("msg_statfreq", optarg), 0;}),                    \
         PLAT_OPTS_ARG_REQUIRED)                                               \
    item("msg_stats", "messaging: statistic options", MSG_STATS,               \
         ({ sdf_msg_setinitn("msg_stats", optarg), 0;}),                       \
         PLAT_OPTS_ARG_REQUIRED)                                               \
    item("msg_tcpport", "messaging: TCP port", MSG_TCPPORT,                    \
         ({ sdf_msg_setinitn("msg_tcpport", optarg), 0;}),                     \
         PLAT_OPTS_ARG_REQUIRED)                                               \
    item("msg_timeout", "messaging: timeout for responses", MSG_TIMEOUT,       \
         ({ sdf_msg_setinitn("msg_timeout", optarg), 0;}),                     \
         PLAT_OPTS_ARG_REQUIRED)                                               \
    item("msg_udpport", "messaging: UDP port", MSG_UDPPORT,                    \
         ({ sdf_msg_setinitn("msg_udpport", optarg), 0;}),                     \
         PLAT_OPTS_ARG_REQUIRED)                                               \
    item("ffdc_buffer_len", "ffdc: per thread buffer size in bytes",           \
         FFDC_BUFFER_LEN, parse_size(&config->ffdc_buffer_len, optarg, NULL),  \
         PLAT_OPTS_ARG_REQUIRED)                                               \
    item("ffdc_disable", "disable FFDC logging", FFDC_DISABLE,                 \
        ({ config->ffdc_disable = 1; 0; }), PLAT_OPTS_ARG_NO)                                                     

#endif  /* __AGENT_CONFIG_H__ */

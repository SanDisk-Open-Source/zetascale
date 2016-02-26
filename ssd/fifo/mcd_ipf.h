/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

/*
 *  (c) 2009, Schooner Information Technology, Inc.
 */

/*
 *  Procedures:
 *
 *     ipf_start     - start the ipf process
 *     ipf_remember  - tell the ipf process to remember an interface
 *     ipf_forget    - tell the ipf process to forget an interface
 *     ipf_fail      - instruct the ipf process to start failover
 *     ipf_exit      - instruct the ipf process to terminate
 */

#include <stdint.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <netinet/in.h>
#include <event.h>
#include <limits.h>
#include <netdb.h>
#include <netinet/tcp.h>

#include "common/sdftypes.h"

#ifndef MCD_IPF_H
#define MCD_IPF_H
struct sdf_replicator;

/*
 * maximum number of containers supported by one instance of memcached
 */
#define MCD_MAX_NUM_SCHED       32
#define MCD_MAX_NUM_FTHREADS    1025

/* Type of network port */
typedef enum {
    PORT_TYPE_TCP,
    PORT_TYPE_UDP
} Network_Port_Type;

enum protocol {
    ascii_prot = 3, /* arbitrary value. */
    binary_prot,
    negotiating_prot /* Discovering the protocol */
};

enum mcd_bin_substates {  
    mcd_bin_no_state,
    mcd_bin_reading_set_header, 
    mcd_bin_reading_cas_header, 
    mcd_bin_read_set_value,
    mcd_bin_reading_get_key,
    mcd_bin_reading_stat,
    mcd_bin_reading_del_header,
    mcd_bin_reading_incr_header,
    mcd_bin_read_flush_exptime,
    mcd_bin_reading_sasl_auth,
    mcd_bin_reading_sasl_auth_data
};  


typedef struct mcd_conn mcd_conn;
struct mcd_conn {
    int    sfd;
    int    xfd;
    int    state;
    enum mcd_bin_substates substate;
    struct event event;
    short  ev_flags;
    short  which;   /** which events were just triggered */

    char   *rbuf;   /** buffer to read commands into */
    char   *rcurr;  /** but if we parsed some already, this is where we stopped */
    int    rsize;   /** total allocated size of rbuf */
    int    rbytes;  /** how much data, starting from rcur, do we have unparsed */

    struct request *reqList;

    char   *wbuf;
    char   *wcurr;
    int    wsize;
    int    wbytes;
    int    write_and_go; /** which state to go into after finishing current write */
    void   *write_and_free; /** free this memory after finishing writing */

    char   *ritem;  /** when we read in an item's value, it goes here */
    int    rlbytes;

    /* data for the swallow state */
    int    sbytes;    /* how many bytes to swallow */

    /* data for the mwrite state */
    struct iovec *iov;
    int    iovsize;   /* number of elements allocated in iov[] */
    int    iovused;   /* number of elements used in iov[] */

    struct msghdr *msglist;
    int    msgsize;   /* number of elements allocated in msglist[] */
    int    msgused;   /* number of elements used in msglist[] */
    int    msgcurr;   /* element in msglist[] being transmitted now */
    int    msgbytes;  /* number of bytes in current msg */

    /* data for UDP clients */
    bool   udp;       /* is this is a UDP "connection" */
    int    request_id; /* Incoming UDP request ID, if this is a UDP "connection" */
    struct sockaddr request_addr; /* Who sent the most recent request */
    socklen_t request_addr_size;
    unsigned char *hdrbuf; /* udp packet headers */
    int    hdrsize;   /* number of headers' worth of space is allocated */

    int    binary;    /* are we in binary mode */
    bool   noreply;   /* True if the reply should not be sent. */
    mcd_conn   *next;     /* Used for generating a list of conn structures */

    /* needed for fthread support */
    int                         fth_count;
    int                         fd_closed;
    int                         num_cmds;  /* for debugging */
    void                      * pai;       /* pointer to ActionInitState */
    struct mcd_fth_state      * fth_state;

    /* per connection lock-free request free list */
    int                         free_req_count;
    int                         free_req_curr;
    struct request           ** free_reqs;

    /* for per-fthread worker fth mbox support */
    void                      * mbox;

    /* for multi-container support */
    int                         port;
    SDFContainer              * sdf_container;
    struct bucket_stats       * bucket_stats;
    struct stats              * stats;

    /* for event updates */
    int                         new_flags;

    /* for backup */
    void                      * shard;
    struct mcd_container      * mcd_container;

    /* for hot key */
    struct in_addr              client_addr;

    /* for dynamic container stale conn handling */
    uint32_t                    cntr_gen;

    /* for ips_per_container support */
    uint16_t                    ref_count;
#if 0
    /* for binary protocol support */
    enum protocol               protocol;
    //enum network_transport      transport;  /* transport used by this conn */
    protocol_binary_request_header binary_header;
    uint64_t                    cas;          /* the cas to return */
    short                       cmd;          /* current cmd being processed */
    int                         opaque;
    int                         keylen;
    bool                        switch_fth;   /* yield current fth */
    bool                        sasl_pending; /* wait for sasl processing */
    // LIBEVENT_THREAD        * thread;       /* thread serving this conn */
      
    /* for SASL support */
    sasl_conn_t               * sasl_conn;
#endif
};

struct vip {
    char *    address;
    char      if_id[64];
    int       is_standby;
    int       standby_node;
    int       node;
    uint32_t  mask;
    uint32_t  tcp_port;
    uint32_t  udp_port;
    uint64_t  container_id;
    uint64_t  cguid;
};

struct settings {
    int maxconns;
    int adm_port;
    int num_ports;
    int tcp_ports[MCD_MAX_NUM_CNTRS];
    int udp_ports[MCD_MAX_NUM_CNTRS];
    struct vip vips[MCD_MAX_NUM_CNTRS];
    char *inter;
    int verbose;
    //rel_time_t oldest_live; /* ignore existing items older than this */
    int evict_to_free;
    char *socketpath;   /* path to unix socket if using local socket */
    int access;  /* access mask (a la chmod) for unix domain socket */
    double factor;          /* chunk size growth factor */
    int chunk_size;
    int num_threads;        /* number of libevent threads to run */
    int num_sdf_threads;    /* number of SDF threads to run */
    int num_cores;          /* number of fthread schedulers */
    int sdf_log_level;      /* log level for SDF */
    int sdf_persistence;    /* 1=persistent; 0=not persistent */
    char prefix_delimiter;  /* character that marks a key prefix (for stats) */
    int detail_enabled;     /* nonzero if we're collecting detailed stats */
    int assert_on_miss;     /* assert on a get miss, for hardware debugging */
    int flush_all_hack;     /* call flushInvalCon instead of InvalConObjs */
    int schooner_stats;     /* include schooner info in stats output */
    int fast_bin_trace;     /* enable fast binary tracing */
    size_t flash_size_total;    /* total flash size */
    char aio_base[PATH_MAX + 1];/* base directory for storage */
    int aio_create;             /* create files if they don't exist */
    int aio_first_file;         /* first index to %d of aio file */
    int aio_num_files;          /* number of files for the aio subsystem */
    int aio_sub_files;          /* number of aio sub-files */
    int aio_queue_len;          /* maximum aio queue length */
    int64_t aio_total_size;     /* total flash size for AIO (bytes) */
    int aio_sync_enabled;       /* 1=enable device sync; 0=disabled */
    float rec_log_size_factor;  /* scale factor for log size (can grow w/neg)*/
    int rec_log_verify;         /* 1=enable verification; 0=no verification */
    int rec_log_segsize;        /* log buffer segment size (in MB) */
    int rec_upd_segsize;        /* update buffer segment size (in MB) */
    int rec_upd_bufsize;        /* total size of segmented update buffer (MB)*/
    int rec_upd_max_chunks;     /* max #chunks for object table update */
    int rec_upd_yield;          /* obj table update yield (MB/rec_upd_yield) */
    int rec_upd_verify;         /* 1=enable verification; 0=no verification */
    int fake_miss_rate;         /* emulated get miss rate */
    int mq_ssd_balance;         /* ssd load balancing for multi-queue */
    int no_direct_io;           /* disable direct IO (for ramdisk setup) */
    int latency_stats_on;       /* enable detailed get/set latency stats */
    int udp_per_fth;            /* allocate per fth UDP reply socket */
    int enable_fifo;            /* enable the FIFO SSD support */
    int bypass_aio_check;       /* bypass aio file size check */
    int auto_delete;            /* automatically delete expired objects */
    int indep_clone;            /* start memcached for independent cloning */
    int debug_hang_mcd;         /* make memcached hang, for debugging on */
    int start_vips;             /* starts vips during initialization */
    int chksum_object;          /* store checksum for entire object */
    int chksum_data;            /* store checksum for object data */
    int chksum_metadata;        /* store checksum for object metadata */
    int sb_data_copies;         /* redundant copies of superblock on raid */
    int static_containers;      /* read container info from properties file */
    int max_aio_errors;         /* max # of EIOs b4 we abort in rep_mode */
    int req_buf_memcheck;       /* request buf memory usage checking */
    uint64_t max_req_memory;    /* max total size for req bufs, in MBs */
    int prefix_delete;          /* support for prefix-based deletion */
    int max_num_prefixes;       /* max number of prefixes per container */
    char prefix_del_delimiter;  /* default delimiter for prefix-deletion */
    int ips_per_cntr;           /* multiple containers on the same IP */
    int bin_prot_support;       /* support for binary protocol */
    enum protocol binding_protocol; /* ascii or binary protocol */
    int reqs_per_event;         /* max num of reqs processed per event */
    int mcd_compat_ver;         /* memcached compatible version */
    int item_size_max;          /* max object size */
    int backlog;
    bool use_cas;
    bool sasl;                  /* SASL support */
    bool mcd_req_stats;         /* tracking request structures in use */
    bool mcd_mem_stats;         /* tracking detailed memory usage */
    bool multi_fifo_writers;    /* support for multiple fifo writer */
    bool nonblocking_sasl;      /* sasl nonblocking authentication fix */
    bool aio_wc;                /* use write-combining aio */
    /* Configuration for platform/aio_api write combiner */
    struct paio_wc_config aio_wc_config;
    /* Configuration for platform/aio_api basic AIO */
    struct paio_libaio_config aio_libaio_config;

#ifdef MEMCACHED_DEBUG
    bool aio_error_injection;   /* enable error inejection */
    /* Configuration for platform/aio_api error injection */
    struct paio_error_bdb_config aio_error_bdb_config;
#endif /* def MEMCACHED_DEBUG */
};

#define MCD_KEY_MAX_LENGTH	250
#define MCD_MAX_SUFFIX_LENGTH 	 45
#define MCD_MAX_SUBCMD_LENGTH 	  9
#define MCD_MAX_SENDBUF_SIZE (256 * 1024 * 1024)
#define MCD_MAX_RECVBUF_SIZE (32 * 1024 * 1024)


typedef enum {
    MCD_CMD_GET = 1,
    MCD_CMD_GETS,
    MCD_CMD_SET,
    MCD_CMD_ADD,
    MCD_CMD_REPLACE,
    MCD_CMD_APPEND,
    MCD_CMD_PREPEND,
    MCD_CMD_CAS,
    MCD_CMD_INCR,
    MCD_CMD_DECR,
    MCD_CMD_DELETE,
    MCD_CMD_RAW_GET,
    MCD_CMD_RAW_SET,
    MCD_CMD_SYNC,
    MCD_CMD_SYNC_INVAL,
    MCD_CMD_SYNC_ALL,   // no key
    MCD_CMD_SYNC_INVAL_ALL,   // no key
    MCD_CMD_INVAL_ALL,  // no key
    MCD_CMD_FLUSH_ALL,  // no key
    MCD_CMD_STATS,      // no key
    MCD_CMD_VERSION,    // no key
    MCD_CMD_VERBOSITY,  // no key
    MCD_CMD_QUIT,       // no key
    MCD_CMD_SHUTDOWN,   // no key
    MCD_CMD_TRACE,      // no key
    MCD_CMD_RTDUMP,     // no key
    MCD_CMD_LOG,        // no key
    MCD_CMD_SCHOONER,   // no key
    MCD_CMD_BACKUP,     // no key
    MCD_CMD_START_REPLICATION, // no key
    MCD_CMD_STOP_REPLICATION, // no key
    MCD_CMD_ACTIVATE_VIP, // no key
    MCD_CMD_DEACTIVATE_VIP, // no key
    MCD_CMD_RESTORE,    // no key
    MCD_CMD_STATE_MACHINE_TEST,
    MCD_CMD_ATTACH_TEST,
    MCD_CMD_SASL,
#ifndef NO_JOHANN
    MCD_CMD_TEST,       // no key
#endif
    MCD_CMD_INVALID     // invalid command
} mcd_command_id;


typedef struct mcd_request {
    /* Common command variables */
    char            key[MCD_KEY_MAX_LENGTH + 1];       /* key (null terminated) */
    char            suffix[MCD_MAX_SUFFIX_LENGTH + 1]; /* suffix (null ended) */
    char            subcmd[MCD_MAX_SUBCMD_LENGTH + 1]; /* subcommand (for stats) */
    char           *data;          /* pointer to object_data struct */
    struct request *next;          /* for chained (multi-key) requests  */
    time_t          expTime;       /* expire time, stored as Unix time */
    time_t          curTime;       /* current time, stored as Unix time */
    uint32_t        dataLen;       /* data length (w/o object_data), max 1MB */
    uint8_t         suffixLen;     /* suffix length */
    uint8_t         keyLen;        /* key length, max 250 bytes */
    bool            multiKey;      /* false if 1st of batch or non-multi */
    mcd_command_id      cmd;           /* command */

    /* Command-specific variables */
    uint64_t        delta;         /* increment/decrement value */
    uint64_t        initial;       /* increment/decrement value */
    uint64_t        cas_id;        /* the CAS identifier */
    uint64_t        prev_seqno;    /* sequence number */
    uint64_t        curr_seqno;    /* sequence number */
    uint32_t        flags;         /* opaque flags from client */
    uint32_t        master_node;   /* master node to perform operation on */
    uint32_t        slave_node;    /* slave node to perform operation on */
    uint32_t        vip;           /* vip to perform operation on */
    bool            has_exptime;   /* exptime specified, for prefix_delete */

    /* SDF-specific variables */
    SDFContainer    sdf_container; /* container handle */
    SDF_context_t   sdf_context;   /* context for SDF APIs */
    bool            pinned;        /* true = request has a pinned buffer */

    /* Miscellaneous variables */
    uint64_t        hash;          /* hash value of key */
    int             lock_bucket;   /* index into lock table */
    struct conn    *conn;          /* pointer to connection structure */

    /* State machine test commands */
    int             stm_testcmd;
    int             stm_test_arg;
} mcd_request_t;

/**
 *  @brief Create a socket for a vip
 */
extern int mcd_vip_server_socket(struct vip *, Network_Port_Type);


extern void ipf_set_active(int);
extern int  ipf_start(struct sdf_replicator *);
extern int  ipf_start_simple(int);
extern void ipf_remember(uint32_t, uint32_t, char *, int16_t, struct timeval);
extern void ipf_forget(uint32_t);
extern void ipf_fail(int);
extern void ipf_exit(void);
extern SDF_boolean_t ipf_is_node_ready(void);
extern SDF_boolean_t ipf_is_node_dominant();
extern SDF_boolean_t ipf_is_node_started_first_time() ;
extern SDF_boolean_t ipf_is_node_independent();
extern SDF_boolean_t ipf_is_node_in_mirrored();
extern int ipf_delete_vip(char *node_name, char *ifname, char *ip, char *err_str);
extern int ipf_add_vip(char *node_name, char *ifname, char *ip, 
                                             char *mask, char *gw, char *err_str);
extern void ipf_handle_container_add(int vport, int rport, int node);
extern void ipf_handle_container_delete(int vport, int rport, int node);
#ifndef IPF_TEST
extern void process_vip_command(struct mcd_conn * c, mcd_request_t * req);
extern void process_state_machine_command( struct mcd_conn * c, mcd_request_t * req );
#endif
#endif /* MCD_IPF_H */

#ifndef PLATFORM_SHMEM_INTERNAL_H
#define PLATFORM_SHMEM_INTERNAL_H 1

/*
 * File: sdf/platform/shmem_internal.h
 * Author: drew
 *
 * Created on January 24, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: shmem_internal.h 4854 2008-12-05 22:26:01Z drew $
 */

/**
 * Shared memory subsystem internal structures, both shared and proces local.
 */

#include <sys/cdefs.h>
#include <sys/types.h>
#include <sys/un.h>
#include <netinet/in.h>

#include <limits.h>

#include "platform/msg.h"
#include "platform/mutex.h"

#include "platform/shmem.h"
#include "platform/shmem_arena.h"
#include "platform/shmem_ptrs.h"
#include "platform/shmem_global.h"


/*
 * Shared-memory structures which reside in shared memory
 */

/**  @brief Allocator configuration.  */
struct shmem_alloc_config {
    /** @brief Default arena */
    enum plat_shmem_arena default_arena;

    /** @brief Configuration for each arena class */
    struct plat_shmem_arena_config arena_config[PLAT_SHMEM_ARENA_COUNT];
};

/**
 * Socket address for shmemd<->client communication.
 */
union shmem_socket_addr {
    struct sockaddr_in inet_addr;
    struct sockaddr_un unix_addr;
};

/*
 * Describes a single mmap-backed segment, either device or memory backed
 * by tmpfs, filesystem, etc.
 */
struct shmem_descriptor_mmap {
    /** @brief backing store file */
    char backing_name[PATH_MAX + 1];

    /** @brief Offset into mmap'd file descriptor */
    off_t offset;

    /** @brief Serial of physmem device */
    uint64_t physmem_serial;
};

/* Describes a SYSV shm backed segment */
struct shmem_descriptor_sysv {
    int shmid;
};

enum shmem_descriptor_flags {
    /** @brief First segment in system */
    SHMEM_DESCRIPTOR_FIRST = 1 << 0,

    /** @brief The segment has no header on it */
    SHMEM_DESCRIPTOR_NO_HEADER = 1 << 1,

#ifdef notyet
    /**
     * @brief Using huge pages
     *
     * XXX: this currently isn't set by the code
     */
    SHMEM_DESCRIPTOR_HUGE_PAGES = 1 << 2,
#endif

    /** @brief The segment should be pre-faulted on attach */
    SHMEM_DESCRIPTOR_PREFAULT = 1 << 3
};

/* Complete user-visible descriptor structure */
struct shmem_descriptor {
    /** @brief Type indicating attachment mechanism (mmap, shmat, etc.) */
    enum shmem_type type;

    /** @brief shmem_descriptor_flags */
    int flags;

    /** @brief Total length including internal structures like header */
    size_t len;

    /**
     * @brief Physical address
     *
     * Within a segment, addresses are physically contiguous.  NULL is used
     * for no known physical mapping.
     */
    uint64_t paddr;

    union {
        struct shmem_descriptor_mmap mmap;
        struct shmem_descriptor_sysv sysv;
    } specific;
};


enum {
    /** @brief sh,  */
    SHMEM_HEADER_MAGIC = 0x00006873
};

enum shmem_header_flags {
    SHMEM_HEADER_HUGE_PAGES = 1 << 0
};

/**
 * Header at the front of every shared memory segment
 */
struct shmem_header {
    /* SHMEM_HEADER_MAGIC */
    int32_t magic;

    /* @brief This shared memory segment */
    struct shmem_descriptor self;

    /** @brief Next shmem segment */
    struct shmem_descriptor next;

    /** @brief Administrative area for shared space, valid in first segment */
    shmem_admin_sp_t admin;

    /*
     * XXX: This is expedient.  These things belong in a separate
     * structure and are only valid in the first segment.
     */

    /** @brief Options from #shmem_header_flags */
    int flags;

    /** @brief Fixed address which phys, virt, etc. reside within */

    void *map;

    /** @brief Size of map */
    uint64_t map_len;

    /** @brief Address space length. */
    uint64_t address_space;

    /**
     * @brief Fixed address at which the physmem pool gets mapped
     *
     * Within [map, map + map_len)
     */
    void *phys_map;

    /**
     * @brief Fixed address at which the virtual space is mapped.
     *
     * Once we strip the segment headers off
     * individual physmem segments we can string them together for
     * contiguous allocations > the 4 megabyte chunks returned by
     * the kernel.
     *
     * Within [map, map + map_len)
     */
    void *virt_map;
};

/*
 * FIXME: the admin structure should be renamed the well-known structure.
 */
enum {
    /* sad0 */
    SHMEM_ADMIN_MAGIC = 0x30646173,
    SHMEM_ADMIN_VERSION = 1
};

enum {
    SHMEM_FLAG_EMULATE_PHYS = 1 << 0
};


/**
 * The first segment has an additional piece of data starting immediately
 * after the header (it would be more appropriate to just have a pointer
 * to this in every segment) which points to administrative info for
 * shmemd and the allocator.
 */
struct shmem_admin {
    /* SHMEM_ADMIN_MAGIC */
    int32_t magic;

    /*
     * Version is separate since it changes everytime the well-known
     * shared structures list does.
     */
    int32_t version;

    /*
     * Pointer to shared memory allocation state which "should" typically
     * follow this but may may be page-aligned, be in its own segment, or
     * whatever.
     *
     * On initialization shmemd starts, calls set_attr_shmemd_attached(),
     * refreshes the list of attributes, and calls shmem_alloc_init().
     * shmem_alloc_init() checks the shmem_admin structure; and where
     * alloc_state is unset initializes.
     *
     * Once shmem_alloc returns successfully (allocations may block until
     * recovery) shmemd either assumes the existing shmemd plat_process
     * structure or allocates a new one when this is null.
     */
    shmem_alloc_sp_t alloc_state;

#ifdef notyet
    /*
     * The process structure associated with shmemd for action consistency
     * is referenced by a well-known location so that subsequent incarnations
     * can perform non-stop recovery
     */
    plat_process_sp_t shmemd;
#endif

    /*
     * Configuration flags.
     */
    int32_t config_flags;

    /**
     * @brief Global objects is kept here.
     *
     * The associated defines are in shemem_global.  On initialization,
     * shmem_globals[] are set to zero bytes like BSS.
     */
    uint64_t shmem_globals[SHMEM_GLOBAL_COUNT];

    /**
     * @brief Allocator configuration.
     *
     * XXX: drew 2008-12-03 This is just here for inclusion in the
     * admin structure as a side effect of how things currently
     * initialize.  It should be internal to shmem_alloc.c with an
     * API change in the initialization.
     */
    struct shmem_alloc_config alloc_config;

#ifdef notyet
    /*
     * A shared hash is required to implement the reference counted
     * id->shared structure mapping code.
     */
    plat_shmem_id_hash_sp_t shmem_id_hash;

    /*
     * FIXME: The shared memory IPC queues need to be bootstrapped some how.
     */
#endif
};

/*
 * Shared memory structures for user space
 */

/**
 * Client state except for the attachment table
 *
 * plat_shmem is externally visible as a typedef
 */
struct plat_shmem {
    /** @brief Process level attached state. */
    struct {
#ifdef notyet
        /* XXX: The initial implementation initializes once only */

        /** @brief The shmem system has staticly assigned segments */
        int immutable;
        /** @brief Only for attached when this is not immutable */
        plat_mutex_t lock;
#endif
        /** @brief Current set of attached segments */
        struct plat_shmem_attached *attached;
    } attached;

    /*
     * FIXME: Once we have an event loop, add one here which accepts new
     * socket connections and watches for EOF on the connection socket.
     */

    /* Re-connect listening state */
    struct {
        /* Address to listen on */
        union shmem_socket_addr addr;

        /*
         * Inbound socket on which this is listening for reconnections following
         * shmemd restart.
         */
        int fd;

    } listen;

    /* Connection state */
    struct {
        /*
         * Need to comminicate with shmemd should be rare; so a heavyweight
         * lock around the IPC mechanism should be OK, at least until we
         * get the event code done and everything impemented in terms of
         * it.
         */
        plat_mutex_t lock;

        /* Sequence number used for socket commands */
        uint64_t seqno;

        /*
         * Connection to shmemd used to connect to the first shared memory
         * segment and to receive inbound connections when shmemd has
         * terminated abnormally.
         */
        int fd;

    } connect;

    plat_process_sp_t process_ptr;
};

__BEGIN_DECLS

/**
 * @brief Initialize shared memory allocator
 *
 * Preconditions: Shared memory subsystem is initialized with first real
 * segment attached.
 *
 * @return 0 on success, -errno on error
 */
int shmem_alloc_attach(int create);

/** @brief Detach from shared memory */
void shmem_alloc_detach();

/** @brief Initialize allocator configuration */
void shmem_alloc_config_init(struct shmem_alloc_config *config);

/**
 * @brief parse --plat/shmem/arena arg
 *
 * @return 0 on success, -plat_errno on failure
 */
int shmem_alloc_config_parse_arena(struct shmem_alloc_config *config,
                                   const char *arg);

/** @brief set used limit (in bytes) for specified arena */
void shmem_alloc_config_set_arena_used_limit(struct shmem_alloc_config *config,
                                             enum plat_shmem_arena arena,
                                             int64_t limit);

/** @brief Destroy allocator configuration */
void shmem_alloc_config_destroy(struct shmem_alloc_config *config);

/** @brief Convert string to arena */
enum plat_shmem_arena shmem_str_to_arena(const char *string, size_t n);

__END_DECLS

#endif /* ndef PLATFORM_SHMEM_INTERNAL_H */

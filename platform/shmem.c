/**
 * Author: drew
 *
 * Created on January 24, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: shmem.c 16015 2011-01-26 00:55:38Z briano $
 */

/**
 * Shared memory attach/detach and initialization. The shared memory allocator
 * assumes complete ownership of the shared memory pool.
 *
 * Initialization is split into two parts #plat_shmem_prototype_init
 * which gets a set of shared memory segments that can be initialzed
 * by attaching the first one, with the remainder happening on
 * demand in #plat_shmem_attach.
 *
 * This allows the client side to share a minimal amount of configuration
 * information with the initialization code run out of sdfagent and makes
 * it impossible to run the two with different configuration and
 * potentially disasterous results.
 */

#define PLATFORM_INTERNAL 1
#define PLATFORM_SHMEM_C 1

/*  This enables code that manipulates /proc/self/coredump_filter to
 *  dump shmem memory regions.
 */
// #define ENABLE_DUMP_CORE

#include <sys/ioctl.h>

#include "valgrind/valgrind.h"

#include "platform/assert.h"
#include "platform/attr.h"
#include "platform/errno.h"
#include "platform/fcntl.h"
#include "platform/logging.h"
#include "platform/mman.h"
#include "platform/platform.h"
#include "platform/shm.h"
#include "platform/shmem.h"
#include "platform/stat.h"
#include "platform/stdio.h"
#include "platform/stdlib.h"
#include "platform/string.h"
#include "platform/unistd.h"

#include "misc/misc.h"

#include "physmem/physmem.h"

#include "private/shmem_internal.h"

#include "platform/shmem_c.h"


#undef min
#define min(a, b) ((a) <= (b) ? (a) : (b))

#ifndef PLAT_SHMEM_FAKE

#ifndef notyet
#define PLAT_ALLOC_INITIAL_VALUE PLAT_ALLOC_MALLOC

/* XXX: This doesn't work for non-shmem programs */
#else
#define PLAT_ALLOC_INITIAL_VALUE PLAT_ALLOC_UNINITIALIZED
#endif

enum plat_alloc_method plat_alloc_method = PLAT_ALLOC_INITIAL_VALUE;
/**
 * @brief Shared memory address space
 *
 * Linux likes to re-use address space pieces when things are unmapped,
 * so a separate initialize-attach sequence which unmaps often leaves us
 * with mmap putting pages where we want to so the attach fails (or
 * we get incorrect behavior).
 */
struct plat_shmem_map {
    void *base;
    size_t len;
};

static struct plat_shmem_map plat_shmem_map;

#endif /* ndef PLAT_SHMEM_FAKE */

const struct plat_shmem_attached *plat_shmem_attached = NULL;

struct shmem_config_opaque {
    struct shmem_descriptor *default_descriptor;

    int ndescriptors;

    /* Array of pointers implies that descriptors[n] retains same address */
    struct shmem_descriptor **descriptors;

    /** @brief Configuration for allocator */
    struct shmem_alloc_config alloc_config;
};

enum shmem_init_state_flags {
    /** @brief Initializing the first segment */
    SHMEM_INIT_STATE_FIRST_SEGMENT = 1 << 0,

    /**
     * @brief Use no headers on this segment.  Segments without headers
     * can be concatenated for virtual memory allocations.
     */

    SHMEM_INIT_STATE_NO_HEADER = 1 << 1
};

/**
 * @brief State used by #plat_shmem_prototype_init and sub-functions
 */
struct shmem_init_state {
    /** @brief Configuration */
    const struct plat_shmem_config *config;

    /** @brief #shmem_init_state_flags */
    int flags;

    /** @brief Serial number of current physmem device */
    uint64_t physmem_serial;

    /** @brief Current simulated physical address */
    uint64_t sim_phys;

    /** @brief Address space length (mapped sparsely) */
    uint64_t address_space;

    /** @brief Attached mapping */
    void *map;

    /** @brief Length of map */
    uint64_t map_len;

    /**
     * @brief Fixed address at which the physmem pool gets mapped
     *
     * (NULL for floating)
     */
    void *phys_map;

    /**
     * @brief Fixed address at which the virtual memory pool gets mapped.
     *
     * (NULL for floating).
     */
    void *virt_map;

};

/** @brief how argument to #shmem_map_zero */
enum shmem_map_zero {
    /** @brief Replace existing memory mapping */
    SHMEM_MAP_ZERO_REPLACE,
    /** @brief Fail if a mapping exists at the non-NULL address */
    SHMEM_MAP_ZERO_ALLOCATE,
    /** @brief Try for a given address but take what we get */
};

/** @brief context argument for #shmem_detach_segment */
enum shmem_detach_segment {
    /** @brief For normal operation */
    SHMEM_DETACH_SEGMENT_NORMAL,
    /**
     * @brief For initialization, where the segment isn't replacing a piece
     * of /dev/zero.
     */
    SHMEM_DETACH_SEGMENT_INIT
};

enum {
    /**
     * @brief address space padding
     *
     * Linux tries to fill things in at the top of memory.  Push where we
     * land down, so if too much allocation happens on re-attach we won't
     * find that our address space has been snagged.
     *
     * This is totally arbitrary
     */
    SHMEM_INIT_PREMAP_LEN = 1 << 30,

    /**
     * @brief Address space alignment in bytes
     *
     * XXX: This should come from huge page size, but I couldn't find a
     * define ooutside the kernel.  The biggest huge page size seems to be
     * 4M (Intel depending on PAE state, other platforms).  Do that.
     */
    SHMEM_INIT_MAP_ALIGN = 4 << 20
};

#ifndef PLAT_SHMEM_FAKE
extern int shmem_alloc_attach(int create);
extern void shmem_alloc_detach();
#endif /* def PLAT_SHMEM_FAKE */

static int
shmem_config_add_mmap_descriptor(struct plat_shmem_config *config,
                                 const char *path, enum shmem_type shmem_type,
                                 off_t len);

static int
shmem_mmap_descriptor_alloc(struct shmem_descriptor **descriptorp,
                            const char *path, enum shmem_type shmem_type,
                            off_t len);

#ifndef PLAT_SHMEM_FAKE
static int
shmem_map_zero(void **out, void *in, size_t len, enum shmem_map_zero how);

static int shmem_vm_in_use(const void *addr, size_t len);

static int
shmem_init_descriptor(struct shmem_init_state *init_state,
                      struct shmem_descriptor *first_added_segment,
                      struct shmem_descriptor *src,
                      struct shmem_descriptor *next_segment);
static int
shmem_init_backing_file(struct shmem_init_state *init_state,
                        struct shmem_descriptor *first_added_segment,
                        const char *path, size_t len,
                        const struct shmem_descriptor *next_segment);
static int __attribute__((unused))
shmem_init_backing_dev_physmem(struct shmem_init_state *init_state,
                               struct shmem_descriptor *first_added_segment,
                               const char *path,
                               const struct shmem_descriptor *next,
                               enum shmem_type shmem_type);
static int
shmem_init_backing_dev_regions(struct shmem_init_state *init_state,
                               int fd, const struct physmem_regions *regions,
                               struct shmem_descriptor *first_added_segment,
                               const char *path,
                               const struct shmem_descriptor *next,
                               enum shmem_type shmem_type);

static int
shmem_init_backing_mmap(struct shmem_init_state *init_state,
                        int fd, struct shmem_descriptor *first_added_segment,
                        const char *path, enum shmem_type shmem_type,
                        off_t offset, size_t len,
                        uint64_t paddr, const struct shmem_descriptor *next);

static void
shmem_init_complete_first_header(struct shmem_init_state *init_state,
                                 struct shmem_header *header);

static int
shmem_attached_refresh(struct plat_shmem_attached *attached);

static int
shmem_attach_add_segment(struct plat_shmem_attached *attached,
                         struct plat_shmem_attached_segment *attached_segment,
                         const struct shmem_descriptor *descriptor);

static int shmem_attach_segment(struct plat_shmem_attached_segment *attached,
                                const struct shmem_descriptor *descriptor,
                                void *phys_ptr, void *virt_ptr);

static int
shmem_attach_segment_mmap(struct plat_shmem_attached_segment *attached,
                          const struct shmem_descriptor *descriptor,
                          void *phys_ptr, void *virt_ptr);

static int
shmem_attach_segment_sysv(struct plat_shmem_attached_segment *attached,
                          const struct shmem_descriptor *descriptor,
                          void *phys_ptr, void *virt_ptr);
static int shmem_detach_segment(struct plat_shmem_attached_segment *segment,
                                enum shmem_detach_segment how);
static void shmem_attached_close(struct plat_shmem_attached *attached);

#endif /* def PLAT_SHMEM_FAKE */

/* Placate cstyle */
#define LPAREN '('
#define RPAREN ')'

int
plat_shmem_prototype_init(const struct plat_shmem_config *config) {
#ifdef PLAT_SHMEM_FAKE
    plat_log_msg(20980, PLAT_LOG_CAT_PLATFORM_SHMEM,
                 PLAT_LOG_LEVEL_DEBUG,
                 "Not initializing due to -DPLAT_SHMEM_FAKE");
    return (0);
#else
    int ret = 0;
    int status;
    struct shmem_descriptor first_segment = {};
    struct plat_shmem_attached_segment first_attached;
    struct shmem_header *header;
    struct shmem_admin *admin;
    struct shmem_init_state init_state;
    int attached = 0;
    int i;
    int limit;
    size_t map_len;
    void *map_request;
    void *premap = NULL;
    size_t align;
    ssize_t tmp;

    memset(&init_state, 0, sizeof (init_state));
    init_state.config = config;

#ifdef PLAT_SHMEM_ADDRESS_SPACE
    if (config->adress_space != 0 &&
        config->address_space != PLAT_SHMEM_ADDRESS_SPACE)  {
        plat_log_msg(20981, PLAT_LOG_CAT_PLATFORM_SHMEM,
                     PLAT_LOG_LEVEL_WARN,
                     "Ignoring address space request 0x%lx !="
                     " PLAT_SHMEM_ADDRESS_SPACE = 0x%lx",
                     (long)config->address_space,
                     (long)PLAT_SHMEM_ADDRESS_SPACE);
    }
    init_state.address_space = PLAT_SHMEM_ADDRESS_SPACE;
#else
    if (config->address_space != 0) {
        init_state.address_space = config->address_space;
    } else if (RUNNING_ON_VALGRIND) {
        init_state.address_space = PLAT_SHMEM_DEFAULT_VALGRIND_ADDRESS_SPACE;
    } else {
        tmp = plat_get_address_space_size();
        if (tmp < 0) {
            plat_log_msg(20982, PLAT_LOG_CAT_PLATFORM_SHMEM,
                         PLAT_LOG_LEVEL_ERROR,
                         "Can't determine address space size: %s",
                         plat_strerror(-tmp));
            ret = (int)tmp;
        } else {
            /*
             * XXX: drew 2008-12-01 It's easier to round up here to avoid
             * problems with huge pages than doing it elsewhere.
             */
            if (tmp & (SHMEM_INIT_MAP_ALIGN - 1)) {
                tmp += (SHMEM_INIT_MAP_ALIGN -
                        (tmp & (SHMEM_INIT_MAP_ALIGN - 1)));
            }
            init_state.address_space = tmp;
        }
    }
#endif

#ifdef PLAT_SHMEM_MAP
    if (!ret && config->base_adresss != 0 &&
        config->base_address != PLAT_SHMEM_MAP) {
        plat_log_msg(20983, PLAT_LOG_CAT_PLATFORM_SHMEM,
                     PLAT_LOG_LEVEL_WARN,
                     "Ignoring base address request 0x%lx !="
                     " PLAT_SHMEM_MAP = 0x%lx",
                     (long)config->base_address,
                     (long)PLAT_SHMEM_MAP);
    }
    map_request = PLAT_SHMEM_MAP;
#else
    map_request = (void *)config->base_address;
#endif

    init_state.sim_phys = config->sim_phys;
    if (!ret && init_state.sim_phys) {
        plat_log_msg(20984, PLAT_LOG_CAT_PLATFORM_SHMEM,
                     PLAT_LOG_LEVEL_DEBUG,
                     "Simulating physical memory starting at 0x%lx",
                     (long)init_state.sim_phys);
    }

    /*
     * When Linux address space randomization is off, it likes to
     * start mmap attachments at the top of stack and grow down.  If we
     * let the kernel place us, get a bit more during initialization which
     * won't be allocated when the client attaches so different startup
     * sequences don't run us out of address space.
     *
     * We have hooks to start off at a fixed address; we should point that
     * beyond "our" heap end but before where Linux wants to put randomized
     * mmap attaches as in /dev/zero.
     */
    if (!ret && !map_request) {
        ret = shmem_map_zero(&premap, NULL, SHMEM_INIT_PREMAP_LEN,
                             SHMEM_MAP_ZERO_ALLOCATE);
    }

    /*
     * Allocate address space.  This is twice the size of the address space
     * so we have room for the sparsely mapped physical address space
     * (address - phys_map = phys address) and contiguouos virtual address
     * space (it's a simplification).
     *
     * If we let the kernel provide, we get some extra and re-align so that
     * the constraints for huge pages will be satisfied.
     */
    map_len = init_state.address_space * 2;
    if (!ret) {
        if (!map_request) {
            ret = shmem_map_zero(&init_state.map, NULL,
                                 map_len + SHMEM_INIT_MAP_ALIGN,
                                 SHMEM_MAP_ZERO_ALLOCATE);
            /* Chop off front */
            if (!ret) {
                align = SHMEM_INIT_MAP_ALIGN -
                    (((size_t)init_state.map) & (SHMEM_INIT_MAP_ALIGN - 1));

                if (align != SHMEM_INIT_MAP_ALIGN) {
                    plat_log_msg(20985,
                                 PLAT_LOG_CAT_PLATFORM_SHMEM,
                                 PLAT_LOG_LEVEL_TRACE,
                                 "aligning address map by %lu bytes",
                                 (unsigned long)align);
                }

                status = plat_munmap(init_state.map, align);
                if (status) {
                    plat_log_msg(20986,
                                 PLAT_LOG_CAT_PLATFORM_SHMEM,
                                 PLAT_LOG_LEVEL_ERROR,
                                 "munmap of start alignment failed: %s",
                                 plat_strerror(plat_errno));
                    ret = -plat_errno;
                }


                if (align != SHMEM_INIT_MAP_ALIGN) {
                    status = plat_munmap((char *)init_state.map + map_len +
                                         align, SHMEM_INIT_MAP_ALIGN - align);
                    if (status) {
                        plat_log_msg(20987,
                                     PLAT_LOG_CAT_PLATFORM_SHMEM,
                                     PLAT_LOG_LEVEL_ERROR,
                                     "munmap of end alignment failed: %s",
                                     plat_strerror(plat_errno));
                        if (!ret) {
                            ret = -plat_errno;
                        }
                    }
                }

                init_state.map = (void *)((char *)init_state.map + align);
            }
        } else {
            ret = shmem_map_zero(&init_state.map, map_request,
                                 map_len, SHMEM_MAP_ZERO_ALLOCATE);
        }
    }

    if (!ret) {
        init_state.map_len = map_len;
        init_state.phys_map = init_state.map;
        init_state.virt_map = (void *)((char *)init_state.map +
                                       init_state.address_space);
    }

    /* Initialize shared state for each segment */
    if (!ret) {
        if (!config->opaque->ndescriptors) {
            init_state.flags |= SHMEM_INIT_STATE_FIRST_SEGMENT;

            ret = shmem_init_descriptor(&init_state, &first_segment,
                                        config->opaque->default_descriptor,
                                        NULL /* next segment */);
        } else {
            for (i = limit = config->opaque->ndescriptors - 1; !ret && i >= 0;
                 --i) {
                if (!i) {
                    init_state.flags |= SHMEM_INIT_STATE_FIRST_SEGMENT;
                }

                ret = shmem_init_descriptor(&init_state, &first_segment,
                                            config->opaque->descriptors[i],
                                            i == limit ? NULL : &first_segment);
            }
        }
    }

    /*
     * Pseudo-attach first segment so the separate admin structure can
     * be allocated.
     *
     * XXX: This is an implementation artifact.  With special casing
     * of the first segment and the shmem-pointers-are-local type
     * pun we could do it there.
     */
    if (!ret) {
        ret = shmem_attach_segment(&first_attached, &first_segment,
                                   NULL /* phys pointer */,
                                   init_state.virt_map);
        attached = !ret;
    }

    /* Initialize admin structure and point the header at it */
    if (!ret) {
        header = (struct shmem_header *)(first_attached.phys_ptr ?
                                         first_attached.phys_ptr :
                                         first_attached.virt_ptr);

        /* XXX: Should assert that segment is long enough for all headers */
        admin = (struct shmem_admin *)(header + 1);
        header->admin.base.ptr = admin;

        memset(admin, 0, sizeof (*admin));
        admin->magic = SHMEM_ADMIN_MAGIC;
        admin->config_flags = config->flags;
        admin->alloc_config = config->opaque->alloc_config;
    }

    /*
     * XXX: Should initialize shmem subsystems like alloc, but that currently
     * requires a completely setup shmem.
     */

    if (attached) {
        status = shmem_detach_segment(&first_attached,
                                      SHMEM_DETACH_SEGMENT_INIT);
        if (!ret) {
            ret = status;
        }
    }

    /* All done.  Report results */
    if (!ret) {
        /*
         * Split because gcc won't handle two definitions of item
         * within a macro expansion and plat_log_msg is a macro to
         * capture file and line information.
         */
        const char format_string[] = "shared memory initialized"
            " virt mapping=%p phys mapping=%p address space=0x%lx flags ="
#define item(caps, val) "%s"
            PLAT_SHMEM_CONFIG_FLAGS_ITEMS()
#undef item
            "";

#define item(caps, val) /* cstyle */, (config->flags & (val) ? " " #caps : "")
        plat_log_msg(PLAT_LOG_ID_INITIAL, PLAT_LOG_CAT_PLATFORM_SHMEM,
                     PLAT_LOG_LEVEL_DEBUG, format_string,
                     init_state.virt_map, init_state.phys_map,
                     (unsigned long)init_state.address_space
                     /* no comma because item includes one */
                     PLAT_SHMEM_CONFIG_FLAGS_ITEMS());
#undef item
    }

    if (init_state.map) {
        /* On failure release the virt and phys address space */
        if (ret) {
            status = plat_munmap(init_state.map, init_state.map_len);
            if (status) {
                plat_log_msg(20988, PLAT_LOG_CAT_PLATFORM_SHMEM,
                             PLAT_LOG_LEVEL_ERROR,
                             "munmap /dev/zero failed : %s",
                             plat_strerror(plat_errno));
                if (!ret) {
                    ret = -plat_errno;
                }
            }
        /*
         * Preserve address space for future generations because we might
         * not get it back.
         */
        } else if (!plat_shmem_map.base) {
            plat_shmem_map.base = init_state.map;
            plat_shmem_map.len = init_state.map_len;
        } else {
            plat_assert(plat_shmem_map.base == init_state.map);
            plat_assert(plat_shmem_map.len == init_state.map_len);
        }
    }

    /*
     * Release the padding which we used to guarantee we weren't last
     * and therefore likely to be unavailable in other processes.
     */
    if (premap) {
        plat_munmap(premap, SHMEM_INIT_PREMAP_LEN);
    }

    return (ret);
#endif /* else def PLAT_SHMEM_FAKE */
}

int
plat_shmem_attach(const char *space) {
#ifdef PLAT_SHMEM_FAKE
    plat_log_msg(20989, PLAT_LOG_CAT_PLATFORM_SHMEM,
                 PLAT_LOG_LEVEL_DEBUG,
                 "Not attaching due to -DPLAT_SHMEM_FAKE");
    return (0);
#else
    struct plat_shmem_attached *attached = NULL;
    int ret = 0;
    int fd = -1;
    ssize_t got;
    struct shmem_header bootstrap_header = {};
    shmem_header_sp_t shared_header;
    struct shmem_header *local_header;
    struct shmem_admin *local_admin = NULL;
    const char *human_method;

    if (plat_shmem_attached) {
        ret = -PLAT_EALREADYATTACHED;
    }

    if (!ret) {
        attached = sys_calloc(1, sizeof (*attached));
        if (!attached) {
            ret = -ENOMEM;
        }
    }

    if (!ret) {
        attached->segments = sys_calloc(2, sizeof (*attached->segments));
        if (!attached->segments) {
            ret = -ENOMEM;
        } else {
            attached->nsegments = 2;
        }
    }

    if (!ret) {
        fd = plat_open(space, O_RDWR);
        if (fd == -1) {
            plat_log_msg(20990, PLAT_LOG_CAT_PLATFORM_SHMEM,
                         PLAT_LOG_LEVEL_ERROR, "open(%s) failed: %s",
                         space, plat_strerror(plat_errno));
            ret = -plat_errno;
        }
    }

    if (!ret) {
        got = plat_read(fd, &bootstrap_header, sizeof (bootstrap_header));
        if (got == -1) {
            plat_log_msg(20991, PLAT_LOG_CAT_PLATFORM_SHMEM,
                         PLAT_LOG_LEVEL_ERROR, "read %s failed: %s",
                         space, plat_strerror(plat_errno));
            ret = -plat_errno;
        } else if (got < sizeof (bootstrap_header)) {
            plat_log_msg(20992, PLAT_LOG_CAT_PLATFORM_SHMEM,
                         PLAT_LOG_LEVEL_ERROR, "read %s too short", space);
            ret = -EIO;
        }
    }

    if (!ret && bootstrap_header.magic != SHMEM_HEADER_MAGIC) {
        plat_log_msg(20993, PLAT_LOG_CAT_PLATFORM_SHMEM,
                     PLAT_LOG_LEVEL_ERROR,
                     "plat_shmem_attach first segment bad magic %x",
                     bootstrap_header.magic);
        ret = -EINVAL;
    }

    if (!ret && !(bootstrap_header.self.flags & SHMEM_DESCRIPTOR_FIRST)) {
        plat_log_msg(20994, PLAT_LOG_CAT_PLATFORM_SHMEM,
                     PLAT_LOG_LEVEL_ERROR,
                     "plat_shmem_attach first segment no first flag");
        ret = -EINVAL;
    }

    /*
     * XXX: virt_map and phys_map have to be within the single map adress.
     * The fields are an implementation artifact that we may want to
     * eliminate.
     */
#ifdef PLAT_SHMEM_VIRT_MAP
    if (!ret && bootstrap_header.virt_map != (void *)PLAT_SHMEM_VIRT_MAP) {
        plat_log_msg(20995, PLAT_LOG_CAT_PLATFORM_SHMEM,
                     PLAT_LOG_LEVEL_ERROR,
                     "plat_shmem_attach virt_map at %p not compile time %p",
                     bootstrap_header.virt_map, (void *)PLAT_SHMEM_VIRT_MAP);
        ret = -EINVAL;
    }
#endif /* def PLAT_SHMEM_VIRT_MAP */

#ifdef PLAT_SHMEM_PHYS_MAP
    if (!ret && bootstrap_header.phys_map != (void *)PLAT_SHMEM_PHYS_MAP) {
        plat_log_msg(20996, PLAT_LOG_CAT_PLATFORM_SHMEM,
                     PLAT_LOG_LEVEL_ERROR,
                     "plat_shmem_attach phys_map at %p not compile time %p",
                     bootstrap_header.phys_map, (void *)PLAT_SHMEM_PHYS_MAP);
        ret = -EINVAL;
    }
#endif /* def PLAT_SHMEM_PHYS_MAP */

#ifdef PLAT_SHMEM_ADDRESS_SPACE
    if (!ret && bootstrap_header.address_space != PLAT_SHMEM_ADDRESS_SPACE) {
        plat_log_msg(20997, PLAT_LOG_CAT_PLATFORM_SHMEM,
                     PLAT_LOG_LEVEL_ERROR,
                     "plat_shmem_attach phys address space of %llx"
                     " not compile time %llx",
                     (long long)bootstrap_header.address_space,
                     (long long)PLAT_SHMEM_PHYS_ADDRESS_SPACE);
        ret = -EINVAL;
    }
#endif  /* def PLAT_SHMEM_ADDRESS_SPACE */

    if (!ret) {
        if (plat_shmem_map.base) {
            if (bootstrap_header.map != plat_shmem_map.base) {
                plat_log_msg(20998, PLAT_LOG_CAT_PLATFORM_SHMEM,
                             PLAT_LOG_LEVEL_ERROR,
                             "plat_shmem_attach header map %p not existing %p",
                             bootstrap_header.map, plat_shmem_map.base);
                ret = -EINVAL;
            } else if (plat_shmem_map.base && bootstrap_header.map_len !=
                       plat_shmem_map.len) {
                plat_log_msg(20999, PLAT_LOG_CAT_PLATFORM_SHMEM,
                             PLAT_LOG_LEVEL_ERROR,
                             "plat_shmem_attach header len 0x%lx"
                             " not existing 0x%lx",
                             (unsigned long)bootstrap_header.map_len,
                             (unsigned long)plat_shmem_map.len);
                ret = -EINVAL;
            }
        } else {
            ret = shmem_map_zero(&plat_shmem_map.base, bootstrap_header.map,
                                 bootstrap_header.map_len,
                                 SHMEM_MAP_ZERO_ALLOCATE);
            if (ret) {
                plat_log_msg(21000, PLAT_LOG_CAT_PLATFORM_SHMEM,
                             PLAT_LOG_LEVEL_ERROR,
                             "plat_shmem_attach failed to alloc addr space: %s",
                             plat_strerror(-ret));
            } else {
                plat_shmem_map.len = bootstrap_header.map_len;
            }
        }
    }

    if (!ret) {
        /*
         * XXX: Now that in-core and on-disk pieces are identical it would be
         * most reasonable to just indirect to the header or make a copy.
         */
        attached->map = bootstrap_header.map;
        attached->map_len = bootstrap_header.map_len;
        attached->address_space = bootstrap_header.address_space;
        attached->phys_map = bootstrap_header.phys_map;
        attached->phys_end = attached->phys_map;
        attached->virt_map = bootstrap_header.virt_map;
        attached->virt_end = attached->virt_map;
    }

    if (fd != -1) {
        plat_close(fd);
    }

    /*
     * Kludge around how we bootstrap so that for debugging people can copy
     * backing store files between machines.
     */
    if (!ret) {
        switch (bootstrap_header.self.type) {
        case SHMEM_TYPE_MMAP_FILE:
        case SHMEM_TYPE_MMAP_DEV:
        case SHMEM_TYPE_MMAP_DEV_PHYSMEM:
        case SHMEM_TYPE_MMAP_DEV_PHYSMEM_VIRT:
            if (strcmp(space,
                       bootstrap_header.self.specific.mmap.backing_name)) {
                plat_log_msg(21794, PLAT_LOG_CAT_PLATFORM_SHMEM,
                             PLAT_LOG_LEVEL_WARN,
                             "plat/shmem/file path %s does not match header %s",
                             space,
                             bootstrap_header.self.specific.mmap.backing_name);

                strncpy(bootstrap_header.self.specific.mmap.backing_name, space,
                        sizeof (bootstrap_header.self.specific.mmap.backing_name));
            }
            break;
        /* Ignore */
        case SHMEM_TYPE_NULL:
        case SHMEM_TYPE_SYSV:
            break;
        }
    }

    /* Attach first segment */
    if (!ret) {
        /* Skip segment 0 used for shared memory NULL */
        ret = shmem_attach_add_segment(attached, &attached->segments[1],
                                       &bootstrap_header.self);
    }

    /* Attach remaining segments */
    if (!ret) {
        ret = shmem_attached_refresh(attached);
    }

    /* XXX: Should theoretically move this stuff to init */
    if (!ret) {
        plat_attr_shmem_attached_set(attached);
        plat_shmem_attached = attached;

        shared_header.base = plat_shmem_first_segment();

        shmem_header_sp_rwref(&local_header, shared_header);
        shmem_admin_sp_rwref(&local_admin, local_header->admin);
        if (local_admin->magic != SHMEM_ADMIN_MAGIC) {
            plat_log_msg(21001, PLAT_LOG_CAT_PLATFORM_SHMEM,
                         PLAT_LOG_LEVEL_ERROR,
                         "plat_shmem_attach admin segment bad magic %x",
                         local_admin->magic);
            ret = -EINVAL;
        }

        /*
         * FIXME: drew 2008-12-17 we probably want to allow different flags
         * for the sdf code running in the agent (may want to replace malloc
         * for performance reasons) and client (which needs to attach and
         * detach.
         */
        if (!ret) {
            attached->config_flags = local_admin->config_flags;
        }

        if (!(local_admin->config_flags &
              PLAT_SHMEM_CONFIG_RETAIN_ADDRESS_SPACE) && 
            attached->phys_map &&
            (char *)attached->phys_map + attached->address_space !=
            attached->phys_end &&
            plat_munmap(attached->phys_end, (char *)attached->phys_map +
                        attached->address_space -
                        (char *)attached->phys_end) == -1) {
            plat_log_msg(21847, PLAT_LOG_CAT_PLATFORM_SHMEM,
                         PLAT_LOG_LEVEL_ERROR, "Failed to trim phys_map: %s",
                         plat_strerror(-errno));
            if (!ret) {
                ret = -errno;
            }
        }

        if (!(local_admin->config_flags &
              PLAT_SHMEM_CONFIG_RETAIN_ADDRESS_SPACE) &&
            attached->virt_map &&
            (char *)attached->virt_map + attached->address_space !=
            attached->virt_end &&
            plat_munmap(attached->virt_end, (char *)attached->virt_map +
                        attached->address_space -
                        (char *)attached->virt_end) == -1) {
            plat_log_msg(21848, PLAT_LOG_CAT_PLATFORM_SHMEM,
                         PLAT_LOG_LEVEL_ERROR, "Failed to trim virt_map: %s",
                         plat_strerror(-errno));
            if (!ret) {
                ret = -errno;
            }
        }

        shmem_header_sp_rwrelease(&local_header);
        shmem_admin_sp_rwrelease(&local_admin);

        if (!ret) {
            ret = shmem_alloc_attach(1 /* create */);
        }

        if (!ret) {
            if (attached->config_flags &
                PLAT_SHMEM_CONFIG_DEBUG_REPLACE_MALLOC) {
                plat_alloc_method = PLAT_ALLOC_PHYSMEM;
                human_method = "malloc except for physmem";
            } else if (attached->config_flags &
                       PLAT_SHMEM_CONFIG_DEBUG_LOCAL_ALLOC)  {
                plat_alloc_method = PLAT_ALLOC_MALLOC;
                human_method = "always use malloc";
            } else {
                plat_alloc_method = PLAT_ALLOC_BY_ARENA;
                human_method = "by arena (malloc as default)";
            }

            plat_log_msg(21002, PLAT_LOG_CAT_PLATFORM_SHMEM,
                         PLAT_LOG_LEVEL_DEBUG,
                         "plat_shmem_attach alloc method %s",
                         human_method);
        }

        if (ret) {
            plat_shmem_detach();
        }
    } else {
        shmem_attached_close(attached);
    }

    return (ret);
#endif /* else def PLAT_SHMEM_FAKE */
}

int
plat_shmem_detach() {
#ifdef PLAT_SHMEM_FAKE
    return (0);
#else
    /* GCC 4.2.3 chokes on the cast when it's included in the function argument list */
    struct plat_shmem_attached *not_const = (struct plat_shmem_attached *)plat_shmem_attached;

    if (plat_shmem_attached) {
        /*
         * FIXME: Shared memory consumers should register closures
         * which are automagically invoked at detach time.  This also
         * suggests state which is more complex than a simple pointer to
         * the shared allocation structure.
         */
        shmem_alloc_detach();
        shmem_attached_close(not_const);

        plat_attr_shmem_attached_set(NULL);
        plat_shmem_attached = NULL;

        plat_alloc_method = PLAT_ALLOC_INITIAL_VALUE;
    }

    return (0);
#endif /* else def PLAT_SHMEM_FAKE */
}

void
plat_shmem_pthread_started() {
#ifndef PLAT_SHMEM_FAKE
    shmem_alloc_pthread_started();
#endif /* ndef PLAT_SHMEM_FAKE */
}

void
plat_shmem_pthread_done() {
#ifndef PLAT_SHMEM_FAKE
    shmem_alloc_pthread_done();
#endif /* ndef PLAT_SHMEM_FAKE */
}

int
plat_shmem_ptr_base_parse(plat_shmem_ptr_base_t *outptr, const char *string,
                          const char **endptr) {
    int ret = 0;
    plat_shmem_ptr_base_t base = PLAT_SPB_INITIALIZER;
    char *next;
    unsigned long tmp;

    tmp = strtoul((char *)string, &next, 0);
    if (tmp == ULONG_MAX && errno == ERANGE) {
        ret = -ERANGE;
    } else {
        base.ptr = (void *)tmp;
    }

    if (!ret && outptr) {
        *outptr = base;
    }
    if (endptr) {
        *endptr = next;
    }

    return (ret);
}

#ifndef PLAT_SHMEM_FAKE

#define SHMEM_PROBE_PROC 1

/**
 * @brief Unconditionally replace address space with pages from /dev/null
 *
 * @param out <OUT> Address return.
 * @param in <IN> Requested address, NULL for any
 * @param how <IN> When how iS SHMEM_MAP_ZERO_ALLOCATE and in is non-NULL,
 * #shmem_map_zero fails when a mapping exists at the location.
 * @return 0 on success, -errno on failure.
 */
static int
shmem_map_zero(void **out, void *in, size_t len, enum shmem_map_zero how) {
    int tmp;
    int ret = 0;
    //int fd = -1;
    void *ptr;

#ifdef SHMEM_PROBE_PROC
    if (!ret && how == SHMEM_MAP_ZERO_ALLOCATE && in) {
        tmp = shmem_vm_in_use(in, len);
        if (tmp < 0) {
            ret = tmp;
        } else if (tmp > 0) {
            ret = -EBUSY;
        }

        if (ret) {
            plat_log_msg(21003, PLAT_LOG_CAT_PLATFORM_SHMEM,
                         PLAT_LOG_LEVEL_ERROR,
                         "Address space 0x%lx-0x%lx not available: %s",
                         (unsigned long)in, ((unsigned long)in) + len,
                         plat_strerror(-ret));
        }
    }
#endif

/*EF: Approach with /dev/zero was extremely slow, when multiple instance run at once
MAP_ANONYMOUS works much better */
#if 0
    if (!ret) {
        fd = plat_open("/dev/zero", O_RDWR);
        if (fd == -1) {
            plat_log_msg(21004, PLAT_LOG_CAT_PLATFORM_SHMEM,
                         PLAT_LOG_LEVEL_ERROR, "open(/dev/zero) failed: %s",
                         plat_strerror(plat_errno));
            ret = -plat_errno;
        }
    }
#endif

    if (!ret) {
        /*
         * Private read-only mappings of /dev/zero are considered to have
         * 0-cost my the Linux over-commit logic so they're a guaranteed
         * way to get the address space we want.
         *
         * SHMEM_PROBE_PROC approach doesn't work when address randomization
         * is turned on.
         */
        ptr = plat_mmap(in, len, PROT_NONE,
                        MAP_ANONYMOUS | MAP_PRIVATE|(in ?  MAP_FIXED : 0),
                        0, 0);
#if 0
        ptr = plat_mmap(in, len, PROT_NONE,
#ifdef SHMEM_PROBE_PROC
                        MAP_PRIVATE|(in ?  MAP_FIXED : 0),
#else /* SHMEM_PROBE_PROC */
                        MAP_PRIVATE|((in && how == SHMEM_MAP_ZERO_REPLACE) ?
                                     MAP_FIXED : 0),
#endif /* else def SHMEM_PROBE_PROC */
                        fd, 0);
#endif
        if (ptr == MAP_FAILED) {
            plat_log_msg(21005, PLAT_LOG_CAT_PLATFORM_SHMEM,
                         PLAT_LOG_LEVEL_ERROR,
                         "mmap /dev/zero hint %p len 0x%lx failed: %s",
                         in, (unsigned long)len, plat_strerror(plat_errno));
            ret = -plat_errno;
#ifndef SHMEM_PROBE_PROC
        /*
         * FIXME: This requires address space randomization to be off.  The
         * approach using plat_memprobe takes too long.  The right thing to
         * do is reimplement plat_memprobe in terms of opening
         * /proc/self/mmap instead of poking memory.
         */
        } else if (in && ptr != in) {
            plat_log_msg(21006, PLAT_LOG_CAT_PLATFORM_SHMEM,
                         PLAT_LOG_LEVEL_ERROR,
                         "mmap() placed 0x%lx bytes pages at %p not %p",
                         (long)len, ptr, in);
            plat_munmap(ptr, len);
            ret = -EIO;
#endif /* n def SHMEM_PROBE_PROC */
        } else if (out) {
            *out = ptr;
        }
    }
#if 0
    if (fd != -1) {
        plat_close(fd);
    }
#endif

    return (ret);
}

/**
 * @brief Return if the virtual memory is in use
 *
 * @param addr <IN> base address of interest
 * @param len <IN> length of interest
 *
 * @return negative on error, 0 on unmapped, positive on mapped.
 */

/*
 * Sanity check that the address space we want to adjust to is in-use.
 *
 * Checking for SIGBUS on every page takes too long and doesn't account for
 * mapped memory with no permissions; mmap with a hint and no FIXED flag
 * will map to an address other than hinted if address space randomization
 * is in effect as the default is for security; reading the kernel mmap
 * is a fast safe check but non-portable.  Live with it.
 */
static int
shmem_vm_in_use(const void *addr, size_t len) {
    /* XXX: Line should be big enough to accomodate entry with longest path */
    const int line_len = 80 /* data */ + PATH_MAX /* file name */ + 1 /* NUL */;
    int ret = 0;
    unsigned long start;
    unsigned long end;
    int done;

    char *line = NULL;
    char *file;
    FILE *proc_maps = NULL;

    line = sys_malloc(line_len);
    if (!line) {
        ret = -ENOMEM;
    }

    if (!ret) {
        proc_maps = fopen("/proc/self/maps", "r");
        if (!proc_maps) {
            ret = errno ? -errno : -EIO;
            plat_log_msg(21007, PLAT_LOG_CAT_PLATFORM_SHMEM,
                         PLAT_LOG_LEVEL_ERROR,
                         "can't open /proc/self/maps: %s",
                         plat_strerror(-ret));
        }
    }

    for (start = end = 0, done = ret;
         !done && proc_maps /* placate coverity */; /* cstyle space */) {
        errno  = 0;
        if (!fgets(line, line_len, proc_maps)) {
            if (errno) {
                ret = -errno;
            }
            done = 1;
        }
        if (!done) {
            /* An overly long line may not be NUL terminated */
            line[line_len - 1] = 0;
            if (sscanf(line, "%lx-%lx", &start, &end) != 2) {
                plat_log_msg(21008, PLAT_LOG_CAT_PLATFORM_SHMEM,
                             PLAT_LOG_LEVEL_ERROR,
                             "invalid line reading /proc/self/maps: %s",
                             line);
                ret = -EINVAL;
                done = 1;
            }
        }
        if (!done) {
            plat_log_msg(20819, PLAT_LOG_CAT_PLATFORM_SHMEM,
                         PLAT_LOG_LEVEL_TRACE, "%s", line);

            if (start >= ((unsigned long)addr) + len) {
#if 0
                /*
                 * Keep going for now because we want to trace what's happening
                 */
                done = 1;
#endif
            } else if (start >= (unsigned long)addr ||
                       end > ((unsigned long)addr) + len) {
                file = strchr(line, ' ');
                if (!file) {
                    file = "";
                } else {
                    ++file;
                }
                /*
                 * XXX: This will usually be an error, do we want to log it
                 * as such?
                 */
                plat_log_msg(21009, PLAT_LOG_CAT_PLATFORM_SHMEM,
                             PLAT_LOG_LEVEL_DEBUG,
                             "request 0x%lx-0x%lx existing map 0x%lx-0x%lx %s",
                             (unsigned long)addr, (unsigned long)addr + len,
                             start, end, file);
                ret = -EEXIST;
#if 0
                done = 1;
#endif
            }
        }
    }

    if (proc_maps) {
        fclose(proc_maps);
    }

    if (line) {
        sys_free(line);
    }

    return (ret);
}

/**
 * @brief Initialize the backing store associated with a given descriptor.
 *
 * Multiple segments may be added
 *
 * @param first_added_segment <OUT> The first of the added segments is stored
 * here when non-null
 * @param src <IN> Description of this segment.
 * @param next_segment <IN> Description of next segment when non-NULL
 * @return -plat_errno on failure.
 */
static int
shmem_init_descriptor(struct shmem_init_state *init_state,
                      struct shmem_descriptor *first_added_segment,
                      struct shmem_descriptor *src,
                      struct shmem_descriptor *next_segment) {
    int ret = -EINVAL; /* placate gcc */

    switch (src->type) {
    case SHMEM_TYPE_MMAP_FILE:
        ret = shmem_init_backing_file(init_state, first_added_segment,
                                      src->specific.mmap.backing_name, src->len,
                                      next_segment);
        break;
    case SHMEM_TYPE_MMAP_DEV_PHYSMEM:
    case SHMEM_TYPE_MMAP_DEV_PHYSMEM_VIRT:
        ret = shmem_init_backing_dev_physmem(init_state, first_added_segment,
                                             src->specific.mmap.backing_name,
                                             next_segment,
                                             src->type);
        break;
    case SHMEM_TYPE_MMAP_DEV:
    case SHMEM_TYPE_SYSV:
        /* FIXME: the above are unimplemented */
    case SHMEM_TYPE_NULL:
        plat_log_msg(21010, PLAT_LOG_CAT_PLATFORM_SHMEM,
                     PLAT_LOG_LEVEL_ERROR,
                     "cannot attach segment due to unsupported descriptor"
                     " type %d", src->type);
        ret = -EINVAL;
    }

    return (ret);
}

static int
shmem_init_backing_file(struct shmem_init_state *init_state,
                        struct shmem_descriptor *first_added_segment,
                        const char *path, size_t len,
                        const struct shmem_descriptor *next_segment) {
    int ret = 0;
    int fd = -1;

    plat_log_msg(21011, PLAT_LOG_CAT_PLATFORM_SHMEM,
                 PLAT_LOG_LEVEL_TRACE, "init file %s len %llu",
                 path, (unsigned long long)len);

    if (!ret && plat_unlink(path) == -1 && plat_errno != ENOENT) {
        plat_log_msg(21012, PLAT_LOG_CAT_PLATFORM_SHMEM,
                     PLAT_LOG_LEVEL_ERROR, "ulink(%s) failed: %s", path,
                     plat_strerror(plat_errno));
    }

    if (!ret) {
        fd = plat_open(path, O_RDWR|O_CREAT|O_EXCL, 0666);
        if (fd == -1) {
            plat_log_msg(20990, PLAT_LOG_CAT_PLATFORM_SHMEM,
                         PLAT_LOG_LEVEL_ERROR, "open(%s) failed: %s",
                         path, plat_strerror(plat_errno));
            ret = -plat_errno;
        }
    }

    if (!ret && plat_ftruncate(fd, (off_t)len) == -1) {
        plat_log_msg(21013, PLAT_LOG_CAT_PLATFORM_SHMEM,
                     PLAT_LOG_LEVEL_ERROR, "truncate(%s, %lld) failed: %s",
                     path, (long long) len, plat_strerror(plat_errno));
        ret = -plat_errno;
    }

    if (!ret) {
        ret = shmem_init_backing_mmap(init_state, fd, first_added_segment, path,
                                      SHMEM_TYPE_MMAP_FILE,
                                      0 /* offset */, len, 0 /* paddr */,
                                      next_segment);
    }

    if (fd != -1) {
        plat_close(fd);
    }

    if (!ret) {
        plat_log_msg(21014, PLAT_LOG_CAT_PLATFORM_SHMEM,
                     PLAT_LOG_LEVEL_DEBUG,
                     "shared memory file %s len %lu initialized",
                     path, (unsigned long)len);
    } else {
        plat_log_msg(21015, PLAT_LOG_CAT_PLATFORM_SHMEM,
                     PLAT_LOG_LEVEL_ERROR,
                     "shared memory file %s initialization"
                     " failed: %s", path, plat_strerror(-ret));
    }

    return (ret);
}

static int
shmem_init_backing_dev_physmem(struct shmem_init_state *init_state,
                               struct shmem_descriptor *first_added_segment,
                               const char *path,
                               const struct shmem_descriptor *next,
                               enum shmem_type shmem_type) {
    int ret = 0;
    int fd = -1;
    struct physmem_regions *regions = NULL;
    struct physmem_regions *new_regions;

    fd = plat_open(path, O_RDWR);
    if (fd == -1) {
        plat_log_msg(20990, PLAT_LOG_CAT_PLATFORM_SHMEM,
                     PLAT_LOG_LEVEL_ERROR, "open(%s) failed: %s",
                     path, plat_strerror(plat_errno));
        ret = -plat_errno;
    } else {
        regions = sys_calloc(1, sizeof (*regions));
        if (!regions) {
            ret = -ENOMEM;
        }
    }

    if (!ret && ioctl(fd, PHYSMEMGETREGIONS, regions) == -1) {
        plat_log_msg(21016, PLAT_LOG_CAT_PLATFORM_SHMEM,
                     PLAT_LOG_LEVEL_ERROR, "ioctl(%s) failed: %s",
                     path, plat_strerror(errno));
        ret = -errno;
    }

    if (!ret) {
        new_regions = sys_realloc(regions, sizeof (*regions) +
                                  regions->total_region *
                                  sizeof (regions->region[0]));
        if (!new_regions) {
            ret = -ENOMEM;
        } else {
            regions = new_regions;
            regions->nregion = regions->total_region;
        }
    }

    if (!ret && ioctl(fd, PHYSMEMGETREGIONS, regions) == -1) {
        plat_log_msg(21017, PLAT_LOG_CAT_PLATFORM_SHMEM,
                     PLAT_LOG_LEVEL_ERROR, "second ioctl(%s) failed: %s",
                     path, plat_strerror(errno));
        ret = -errno;
    }

    if (!ret) {
        init_state->physmem_serial = regions->serial;

        switch (shmem_type) {
        case SHMEM_TYPE_MMAP_DEV_PHYSMEM:
            ret = shmem_init_backing_dev_regions(init_state, fd, regions,
                                                 first_added_segment,
                                                 path, next, shmem_type);
            break;
        case SHMEM_TYPE_MMAP_DEV_PHYSMEM_VIRT:
            ret = shmem_init_backing_mmap(init_state, fd, first_added_segment,
                                          path, shmem_type, 0 /* offset */,
                                          regions->total_len, 0 /* paddr */,
                                          next);
            break;
        default:
            plat_assert(0);
            ret = -EINVAL;
        }

        init_state->physmem_serial = 0;
    }

    if (fd != -1) {
        plat_close(fd);
    }

    if (regions) {
        sys_free(regions);
    }

    if (!ret) {
        plat_log_msg(21018, PLAT_LOG_CAT_PLATFORM_SHMEM,
                     PLAT_LOG_LEVEL_DEBUG,
                     "shared memory physmem device %s len %llu initialized",
                     path, (unsigned long long)regions->total_len);
    } else {
        plat_log_msg(21019, PLAT_LOG_CAT_PLATFORM_SHMEM,
                     PLAT_LOG_LEVEL_ERROR,
                     "shared memory physmem device  %s initialization"
                     " failed: %s", path, plat_strerror(-ret));
    }

    return (ret);
}

static int
shmem_init_backing_dev_regions(struct shmem_init_state *init_state,
                               int fd, const struct physmem_regions *regions,
                               struct shmem_descriptor *first_added_segment,
                               const char *path,
                               const struct shmem_descriptor *next,
                               enum shmem_type shmem_type) {
    int ret = 0;
    const struct physmem_region *region;
    struct shmem_descriptor prev_added;
    struct shmem_descriptor added;
    int32_t i;
    int first_flag;
    int first_region;

    plat_log_msg(21020, PLAT_LOG_CAT_PLATFORM_SHMEM,
                 PLAT_LOG_LEVEL_TRACE,
                 "adding %u regions from device %s total size %llu",
                 regions->nregion, path,
                 (unsigned long long)regions->total_len);

    if (next) {
        prev_added = *next;
    } else {
        memset(&prev_added, 0, sizeof (added));
    }

    /*
     * With physical memory, one descriptor initializes multiple segments
     * but only one is "first".
     */

    /* Skip over any physmem regions too short to hold the initial header */
    for (first_region = 0; first_region < regions->nregion; ++first_region) {

        region = regions->region + first_region;

        if (region->len <= sizeof (struct shmem_header)) {
            plat_log_msg(21021, PLAT_LOG_CAT_PLATFORM_SHMEM,
                         PLAT_LOG_LEVEL_DEBUG,
                         "skipping region from device %s offset %llu"
                         " paddr %llu len %llu - too small", path,
                         (unsigned long long)region->offset,
                         (unsigned long long)region->paddr,
                         (unsigned long long)region->len);
        } else if ((size_t)region->paddr > init_state->address_space) {
            plat_log_msg(21022, PLAT_LOG_CAT_PLATFORM_SHMEM,
                         PLAT_LOG_LEVEL_DEBUG,
                         "skipping region from device %s offset %llu"
                         " paddr %llu len %llu - paddr too high", path,
                         (unsigned long long)region->offset,
                         (unsigned long long)region->paddr,
                         (unsigned long long)region->len);
        } else {
            break;
        }
    }

    first_flag = init_state->flags & SHMEM_INIT_STATE_FIRST_SEGMENT;
    if (first_flag) {
        init_state->flags &= ~SHMEM_INIT_STATE_FIRST_SEGMENT;
    }

    for (i = regions->nregion - 1; !ret && i >= first_region; --i) {
        if (i == first_region && first_flag) {
            init_state->flags |= SHMEM_INIT_STATE_FIRST_SEGMENT;
        }

        region = regions->region + i;

        if (region->len <= sizeof (struct shmem_header)) {
            plat_log_msg(21021, PLAT_LOG_CAT_PLATFORM_SHMEM,
                         PLAT_LOG_LEVEL_DEBUG,
                         "skipping region from device %s offset %llu"
                         " paddr %llu len %llu - too small", path,
                         (unsigned long long)region->offset,
                         (unsigned long long)region->paddr,
                         (unsigned long long)region->len);
        } else if ((size_t)region->paddr > init_state->address_space) {
            plat_log_msg(21022, PLAT_LOG_CAT_PLATFORM_SHMEM,
                         PLAT_LOG_LEVEL_DEBUG,
                         "skipping region from device %s offset %llu"
                         " paddr %llu len %llu - paddr too high", path,
                         (unsigned long long)region->offset,
                         (unsigned long long)region->paddr,
                         (unsigned long long)region->len);
        } else {
            plat_log_msg(21023, PLAT_LOG_CAT_PLATFORM_SHMEM,
                         PLAT_LOG_LEVEL_TRACE,
                         "initializing region from device %s offset %llu"
                         " paddr %llu len %llu", path,
                         (unsigned long long)region->offset,
                         (unsigned long long)region->paddr,
                         (unsigned long long)region->len);
            ret = shmem_init_backing_mmap(init_state, fd, &added, path,
                                          shmem_type,
                                          region->offset, region->len,
                                          region->paddr, &prev_added);
            if (!ret) {
                prev_added = added;
            }
        }
    }

    if (!ret && first_added_segment) {
        *first_added_segment = prev_added;
    }

    return (ret);
}

/**
 * @brief Initialize shared segment backed by mmap
 *
 * @param paddr <IN> Starting physical address of this segment; may be zero.
 * When zero and physmem address simulation is on init_state->sim_phys
 * is assigned and has the length added.
 *
 * XXX: Currently, the actual segment initialization code is stored here.
 * If we switch to getting memory segments via some other means the actual
 * code should move someplace else.
 */
static int
shmem_init_backing_mmap(struct shmem_init_state *init_state,
                        int fd, struct shmem_descriptor *first_added_segment,
                        const char *path, enum shmem_type shmem_type,
                        off_t offset, size_t len,
                        uint64_t paddr, const struct shmem_descriptor *next) {
    struct shmem_header *header;
    int ret = 0;
    int status;
    int pathlen;
    void *ptr = MAP_FAILED;

    plat_log_msg(21024, PLAT_LOG_CAT_PLATFORM_SHMEM,
                 PLAT_LOG_LEVEL_TRACE,
                 "init mmap %s offset %llu len %llu paddr %llx",
                 path, (unsigned long long)offset, (unsigned long long)len,
                 (unsigned long long)paddr);

    if (!ret) {
        ptr = plat_mmap(NULL, len, PROT_READ|PROT_WRITE, MAP_SHARED, fd,
                        offset);
        if (ptr == MAP_FAILED) {
            plat_log_msg(21025, PLAT_LOG_CAT_PLATFORM_SHMEM,
                         PLAT_LOG_LEVEL_ERROR, "mmap() failed: %s",
                         plat_strerror(plat_errno));
            ret = -plat_errno;
        }
    }

    if (!ret && (init_state->config->flags & PLAT_SHMEM_CONFIG_PREFAULT)) {
        /*
         * Touch every page so that we get unique pages created for backing
         * store, especially sources of virtual memory like tmpfs
         * or hugetlbfs.
         *
         * XXX: We could make this optional for /dev/physmem if this
         * is too painful.
         */
        ret = plat_write_fault(ptr, len);
        if (ret) {
            plat_log_msg(21026, PLAT_LOG_CAT_PLATFORM_SHMEM,
                         PLAT_LOG_LEVEL_ERROR,
                         "Error writing pages to %s, probably out of memory",
                         path);
            ret = -ENOMEM;
        }
    }

    if (!ret) {
        header = (struct shmem_header *)ptr;
        memset(header, 0, sizeof (*header));
        header->magic = SHMEM_HEADER_MAGIC;
        header->self.type = shmem_type;
        header->self.len = len;

        if (init_state->config->flags & PLAT_SHMEM_CONFIG_PREFAULT) {
            header->self.flags |= SHMEM_DESCRIPTOR_PREFAULT;
        }

        if (paddr) {
            header->self.paddr = paddr;
        } else if (init_state->sim_phys > 0) {
            header->self.paddr = init_state->sim_phys;
            init_state->sim_phys += len;
        } else {
            header->self.paddr = 0;
        }

        header->self.specific.mmap.offset = offset;
        header->self.specific.mmap.physmem_serial = init_state->physmem_serial;

        pathlen = strlen(path) + 1;
        plat_assert(pathlen <=
                    sizeof (header->self.specific.mmap.backing_name));
        memcpy(header->self.specific.mmap.backing_name, path, pathlen);

        if (next) {
            header->next = *next;
        }

        if (init_state->flags & SHMEM_INIT_STATE_FIRST_SEGMENT) {
            shmem_init_complete_first_header(init_state, header);
        }

        /* After shmem_init_complete_first_header so it picks up any flags */
        if (first_added_segment) {
            *first_added_segment = header->self;
        }
    }

    if (ptr != MAP_FAILED) {
        status = plat_munmap(ptr, len);
        if (status) {
            if (!ret) {
                ret = -plat_errno;
            }
            plat_log_msg(21027, PLAT_LOG_CAT_PLATFORM_SHMEM,
                         PLAT_LOG_LEVEL_ERROR, "munmap %s failed: %s",
                         path, plat_strerror(-plat_errno));
        }
    }

    return (ret);
}

/**
 * @brief Finish initializing first header.
 *
 * @param header <IN, OUT> First header which has otherwise been completely
 * initialized
 */
static void
shmem_init_complete_first_header(struct shmem_init_state *init_state,
                                 struct shmem_header *header) {

    header->self.flags |= SHMEM_DESCRIPTOR_FIRST;

    header->map = init_state->map;
    header->map_len = init_state->map_len;

    header->phys_map = init_state->phys_map;
    header->virt_map = init_state->virt_map;
    header->address_space = init_state->address_space;
}

/**
 * @brief Attach all unattached segments to shmem
 *
 * Precondition: The caller has exclusive access to shmem (ex: initialization,
 * which single threaded) or holds locks (not implemented as of 2008-07-09).
 *
 * Returns: 0 on success, errno on failure
 */
static int
shmem_attached_refresh(struct plat_shmem_attached *attached) {
    int ret = 0;
    struct shmem_header *header;
    struct plat_shmem_attached_segment *segments;
    int n;

    /* segment[0] is null segment */
    plat_assert(attached->nsegments >= 2);

    do {
        /* Follow last segment's next descriptor until end */
        n = attached->nsegments;
        header = (struct shmem_header *)(attached->segments[n - 1].virt_ptr);
        if (header->next.type != SHMEM_TYPE_NULL) {
            segments = sys_realloc(attached->segments,
                                   sizeof (*segments) * (n + 1));
            if (!segments) {
                ret = -ENOMEM;
            } else {
                ret = shmem_attach_add_segment(attached, &segments[n],
                                               &header->next);
            }
            if (!ret) {
                attached->segments = segments;
                ++attached->nsegments;
            } else if (segments != attached->segments) {
                sys_free(segments);
            }
        }
    } while (header->next.type != SHMEM_TYPE_NULL && !ret);

    return (ret);
}

/**
 * @brief Add a segment on the tail end of existing virtual address space.
 *
 * As a side effect, attached->virt_end is set to the end of the current
 * segment.
 */
static int
shmem_attach_add_segment(struct plat_shmem_attached *attached,
                         struct plat_shmem_attached_segment *attached_segment,
                         const struct shmem_descriptor *descriptor) {
    int ret;
    void *phys_ptr;

    plat_assert(attached->phys_map);
    plat_assert(attached->virt_map);
    plat_assert(attached->virt_end);

    plat_assert(descriptor->paddr < attached->address_space);

    if (descriptor->paddr) {
        phys_ptr = ((char *)attached->phys_map + descriptor->paddr);
    } else {
        phys_ptr = NULL;
    }

    ret = shmem_attach_segment(attached_segment, descriptor,
                               phys_ptr, attached->virt_end);
    if (!ret) {
        attached->virt_end = (void *)((char *)attached->virt_end +
                                      attached_segment->len);
        if (phys_ptr) {
            phys_ptr = (char *)phys_ptr + attached_segment->len;
            if (phys_ptr > attached->phys_end) {
                attached->phys_end = phys_ptr;
            }
        }
    }

    return (ret);
}

/**
 * @brief Attach a single shared memory segment
 *
 * This works whether or not shmem is currently attached, which makes it
 * useful for the initialization code.
 *
 * @param phys_ptr <IN> Desired mapping.  May be NULL for unspecified
 * mapping.
 *
 * @param virt_ptr <IN> Desired mapping.  May be NULL for unspecified
 * mapping.
 */
static int
shmem_attach_segment(struct plat_shmem_attached_segment *attached_segment,
                     const struct shmem_descriptor *descriptor,
                     void *phys_ptr, void *virt_ptr) {
    int ret;

    switch (descriptor->type) {
    case SHMEM_TYPE_MMAP_FILE:
    case SHMEM_TYPE_MMAP_DEV:
    case SHMEM_TYPE_MMAP_DEV_PHYSMEM:
        ret = shmem_attach_segment_mmap(attached_segment, descriptor,
                                        phys_ptr, virt_ptr);
        break;
    case SHMEM_TYPE_SYSV:
        ret = shmem_attach_segment_sysv(attached_segment, descriptor,
                                        phys_ptr, virt_ptr);
        break;
    default:
        plat_log_msg(21028, PLAT_LOG_CAT_PLATFORM_SHMEM,
                     PLAT_LOG_LEVEL_ERROR,
                     "cannot attach segment due to invalid descriptor type %d",
                     descriptor->type);
        ret = EINVAL;
    }

    if (!ret && (descriptor->flags & SHMEM_DESCRIPTOR_PREFAULT)) {
        if (attached_segment->phys_ptr) {
            plat_read_fault(attached_segment->phys_ptr, attached_segment->len);
        }
        /* Make minor write faults for all pages. */
        if (attached_segment->virt_ptr) {
            plat_read_fault(attached_segment->virt_ptr, attached_segment->len);
        }
    }

    return (ret);
}

static int
shmem_attach_segment_mmap(struct plat_shmem_attached_segment *attached_segment,
                          const struct shmem_descriptor *descriptor,
                          void *phys_ptr, void *virt_ptr) {
    int fd;
    void *phys_out = NULL;
    void *virt_out = NULL;
    int ret = 0;

    plat_assert(descriptor->type == SHMEM_TYPE_MMAP_DEV ||
                descriptor->type == SHMEM_TYPE_MMAP_FILE ||
                descriptor->type == SHMEM_TYPE_MMAP_DEV_PHYSMEM ||
                descriptor->type == SHMEM_TYPE_MMAP_DEV_PHYSMEM_VIRT);

    plat_log_msg(21029, PLAT_LOG_CAT_PLATFORM_SHMEM,
                 PLAT_LOG_LEVEL_TRACE,
                 "attach mmap %s offset %llu len %llu paddr %llx",
                 descriptor->specific.mmap.backing_name,
                 (unsigned long long)descriptor->specific.mmap.offset,
                 (unsigned long long)descriptor->len,
                 (unsigned long long)descriptor->paddr);

    /*
     * FIXME: Ideally we'll have a mode where we first map the segment
     * read-only and enable write-access as needed.  Using the extra space
     * in the SHMEM_PTR as a page count could be interesting as long as
     * users weren't relying on pointer tricks.
     */
    fd = plat_open(descriptor->specific.mmap.backing_name, O_RDWR);
    if (fd == -1) {
        plat_log_msg(20990, PLAT_LOG_CAT_PLATFORM_SHMEM,
                     PLAT_LOG_LEVEL_ERROR, "open(%s) failed: %s",
                     descriptor->specific.mmap.backing_name,
                     plat_strerror(plat_errno));
        ret = -plat_errno;
    }

    if (!ret && descriptor->type == SHMEM_TYPE_MMAP_DEV_PHYSMEM) {
        struct physmem_regions regions = {};
        if (ioctl(fd, PHYSMEMGETREGIONS, &regions) == -1) {
            plat_log_msg(21016, PLAT_LOG_CAT_PLATFORM_SHMEM,
                         PLAT_LOG_LEVEL_ERROR, "ioctl(%s) failed: %s",
                         descriptor->specific.mmap.backing_name,
                         plat_strerror(errno));
            ret = -errno;
        } else if (regions.serial != descriptor->specific.mmap.physmem_serial) {
            plat_log_msg(21030, PLAT_LOG_CAT_PLATFORM_SHMEM,
                         PLAT_LOG_LEVEL_ERROR,
                         "device %s serial %lx does not match %lx",
                         descriptor->specific.mmap.backing_name,
                         regions.serial,
                         descriptor->specific.mmap.physmem_serial);
            ret = -EINVAL;
        }
    }

    if (!ret && descriptor->paddr && phys_ptr) {
        phys_out = plat_mmap(phys_ptr, descriptor->len, PROT_READ|PROT_WRITE,
                             MAP_SHARED|MAP_FIXED, fd,
                             descriptor->specific.mmap.offset);
        if (phys_out == MAP_FAILED) {
            plat_log_msg(21031, PLAT_LOG_CAT_PLATFORM_SHMEM,
                         PLAT_LOG_LEVEL_ERROR, "mmap(%s) failed for phys: %s",
                         descriptor->specific.mmap.backing_name,
                         plat_strerror(plat_errno));
            ret = -plat_errno;
        } else {
            /* Implied by MAP_FIXED */
            plat_assert(!virt_ptr || virt_out == virt_ptr);
        }
    }

    if (!ret) {
        virt_out = plat_mmap(virt_ptr, descriptor->len, PROT_READ|PROT_WRITE,
                             MAP_SHARED|(virt_ptr ? MAP_FIXED : 0), fd,
                             descriptor->specific.mmap.offset);
        if (virt_out == MAP_FAILED) {
            plat_log_msg(21032, PLAT_LOG_CAT_PLATFORM_SHMEM,
                         PLAT_LOG_LEVEL_ERROR, "mmap(%s) failed for virt: %s",
                         descriptor->specific.mmap.backing_name,
                         plat_strerror(plat_errno));
            ret = -plat_errno;
        } else {
            /* Implied by MAP_FIXED */
            plat_assert(!virt_ptr || virt_out == virt_ptr);
        }
    }

    if (!ret) {
        attached_segment->type = descriptor->type;
        attached_segment->phys_ptr = phys_out;
        attached_segment->virt_ptr = virt_out;
        attached_segment->header_len = sizeof(struct shmem_header);
        attached_segment->len = descriptor->len;
        attached_segment->paddr = descriptor->paddr;
    } else {
        if (virt_out) {
            if (virt_ptr && virt_ptr == virt_out) {
                shmem_map_zero(NULL, virt_out, descriptor->len,
                               SHMEM_MAP_ZERO_REPLACE);
            } else {
                plat_munmap(virt_out, descriptor->len);
            }
        }

        if (phys_out) {
            if (phys_ptr && phys_ptr == phys_out) {
                shmem_map_zero(NULL, phys_out, descriptor->len,
                               SHMEM_MAP_ZERO_REPLACE);
            } else {
                plat_munmap(phys_out, descriptor->len);
            }
        }
    }

    #ifdef ENABLE_DUMP_CORE
    if (!ret && plat_ensure_dumped(descriptor->specific.mmap.backing_name,
                                   1 /* shared */)) {
        plat_log_msg(21033, PLAT_LOG_CAT_PLATFORM_SHMEM,
                     PLAT_LOG_LEVEL_WARN,
                     "May be unable to core dump contents of %s",
                     descriptor->specific.mmap.backing_name);
    }
    #endif

    if (fd != -1) {
        plat_close(fd);
    }

    return (ret);
}

static int
shmem_attach_segment_sysv(struct plat_shmem_attached_segment *attached,
                          const struct shmem_descriptor *descriptor,
                          void *phys_ptr, void *virt_ptr) {
    void *ptr;
    int ret;

    plat_assert(descriptor->type == SHMEM_TYPE_SYSV);

    ptr = plat_shmat(descriptor->specific.sysv.shmid, virt_ptr,
                     SHM_REMAP /* flags */);
    if (!ptr) {
        plat_log_msg(21034, PLAT_LOG_CAT_PLATFORM_SHMEM,
                     PLAT_LOG_LEVEL_FATAL, "shmat(%d) failed: %s",
                     descriptor->specific.sysv.shmid,
                     plat_strerror(plat_errno));
        ret = -plat_errno;
    } else {
        attached->type = SHMEM_TYPE_SYSV;
        attached->virt_ptr = ptr;
        attached->phys_ptr = NULL;
        attached->header_len = sizeof(struct shmem_header);
        attached->len = descriptor->len;
        ret = 0;
    }

    return (ret);
}

static void
shmem_attached_close(struct plat_shmem_attached *attached) {
    int i;

    if (attached) {
        for (i = 0; i < attached->nsegments; ++i) {
            shmem_detach_segment(&attached->segments[i],
                                 SHMEM_DETACH_SEGMENT_NORMAL);
        }

        if (attached->phys_map) {
            plat_munmap(attached->phys_map,
                        (attached->config_flags &
                         PLAT_SHMEM_CONFIG_RETAIN_ADDRESS_SPACE) ?
                        attached->address_space : (char *)attached->phys_end -
                        (char *)attached->phys_map);
        }

        if (attached->virt_map) {
            plat_munmap(attached->virt_map,
                        (attached->config_flags &
                         PLAT_SHMEM_CONFIG_RETAIN_ADDRESS_SPACE) ?
                        attached->address_space : (char *)attached->virt_end -
                        (char *)attached->virt_map);
        }

        sys_free(attached->segments);
        sys_free(attached);
    }
}

static int
shmem_detach_segment(struct plat_shmem_attached_segment *segment,
                     enum shmem_detach_segment how) {
    int ret = 0;
    int status;

    switch (segment->type) {
    case SHMEM_TYPE_NULL:
        break;
    case SHMEM_TYPE_MMAP_FILE:
    case SHMEM_TYPE_MMAP_DEV:
    case SHMEM_TYPE_MMAP_DEV_PHYSMEM:
    case SHMEM_TYPE_MMAP_DEV_PHYSMEM_VIRT:
        /* Q: What happens with stacked mmaps? */
#if 0
        ret = (plat_munmap(segment->ptr, segment->len) == -1) ? -plat_errno : 0; */
#endif
        break;
    case SHMEM_TYPE_SYSV:
        /* Q: What happens if we just unmap or remap all the pages ? */
        ret = (shmdt(segment->virt_ptr) == -1) ? -plat_errno : 0;
        break;
    }

    if (how != SHMEM_DETACH_SEGMENT_INIT) {
        if (segment->virt_ptr) {
            status = shmem_map_zero(NULL, segment->virt_ptr, segment->len,
                                    SHMEM_MAP_ZERO_REPLACE);
            if (!ret && status) {
                ret = status;
            }
        }

        if (segment->phys_ptr) {
            status = shmem_map_zero(NULL, segment->phys_ptr, segment->len,
                                    SHMEM_MAP_ZERO_REPLACE);
            if (!ret && status) {
                ret = status;
            }
        }
    }

    return (ret);
}
#endif /* ndef PLAT_SHMEM_FAKE */

void
plat_shmem_config_init(struct plat_shmem_config *config) {
    int status;
    const char *tmp_path;
    char *backing_file = NULL;

    memset(config, 0, sizeof (*config));

    plat_sys_calloc_struct(&config->opaque);
    plat_assert_always(config->opaque);

    tmp_path = plat_get_tmp_path();
    plat_assert_always(tmp_path);

    status = sys_asprintf(&backing_file, "%s/shmem", tmp_path);
    plat_assert_always(status != -1);

    status = shmem_mmap_descriptor_alloc(&config->opaque->default_descriptor,
                                         backing_file, SHMEM_TYPE_MMAP_FILE,
                                         PLAT_SHMEM_DEFAULT_SIZE);

    sys_free(backing_file);
    plat_assert_always(!status);

#ifndef DEPRECATED
    /** XXX: Kludge to make plat_shmem_attach(config.mmap) work */
    config->mmap = plat_shmem_config_get_path(config);
#endif

    shmem_alloc_config_init(&config->opaque->alloc_config);
}

int
plat_shmem_config_parse_file(struct plat_shmem_config *config,
                             const char *arg) {
    int ret;
    char path[PATH_MAX + 1];
    const char *separator;
    int64_t len;
    int pathlen;

    separator = strchr(arg, ':');
    if (separator) {
        ret = parse_size(&len, separator + 1, NULL);
        if (!ret) {
            pathlen = separator - arg;
            memcpy(path, arg, pathlen);
            path[pathlen] = 0;
            ret = plat_shmem_config_add_backing_file(config, path, len);
        }
    } else {
        ret = plat_shmem_config_add_backing_file(config, arg,
                                                 PLAT_SHMEM_DEFAULT_SIZE);
    }

    return (ret);
}

int
plat_shmem_config_parse_arena(struct plat_shmem_config *config,
                              const char *arg) {
    return (shmem_alloc_config_parse_arena(&config->opaque->alloc_config,
                                           arg));
}


// Currently a NOP since there's nothing in there dynamically allocated
void
plat_shmem_config_destroy(struct plat_shmem_config *config) {
    struct shmem_config_opaque *opaque = config->opaque;
    int i;

    if (opaque) {
        if (opaque->default_descriptor) {
            sys_free(opaque->default_descriptor);
        }
        if (opaque->descriptors) {
            for (i = 0; i < opaque->ndescriptors; ++i) {
                sys_free(opaque->descriptors[i]);
            }
            sys_free(opaque->descriptors);
        }
        shmem_alloc_config_destroy(&opaque->alloc_config);
        sys_free(opaque);
        config->opaque = NULL;
    }
}

int
plat_shmem_config_add_backing_file(struct plat_shmem_config *config,
                                   const char *file, off_t size) {
    return (shmem_config_add_mmap_descriptor(config, file,
                                             SHMEM_TYPE_MMAP_FILE, size));
}

int
plat_shmem_config_add_backing_physmem(struct plat_shmem_config *config,
                                      const char *device) {
    return (shmem_config_add_mmap_descriptor(config, device,
                                             SHMEM_TYPE_MMAP_DEV_PHYSMEM,
                                             0 /* size */));
}

int
plat_shmem_config_add_backing_physmem_virt(struct plat_shmem_config *config,
                                           const char *device) {
    return (shmem_config_add_mmap_descriptor(config, device,
                                             SHMEM_TYPE_MMAP_DEV_PHYSMEM_VIRT,
                                             0 /* size */));
}

const char *
plat_shmem_config_get_path(const struct plat_shmem_config *config) {
    struct shmem_config_opaque *opaque = config->opaque;
    const char *ret = NULL; /* placate gcc */
    struct shmem_descriptor *descriptor;

    descriptor = (opaque->ndescriptors > 0) ? opaque->descriptors[0] :
        opaque->default_descriptor;

    switch (descriptor->type) {
    case SHMEM_TYPE_MMAP_FILE:
    case SHMEM_TYPE_MMAP_DEV:
    case SHMEM_TYPE_MMAP_DEV_PHYSMEM:
    case SHMEM_TYPE_MMAP_DEV_PHYSMEM_VIRT:
        ret = descriptor->specific.mmap.backing_name;
        break;
    case SHMEM_TYPE_SYSV:
    case SHMEM_TYPE_NULL:
        plat_assert_always(0);
    }

    return (ret);
}

int
plat_shmem_config_path_is_default(const struct plat_shmem_config *config) {
    return (!config->opaque->ndescriptors);
}

void
plat_shmem_config_set_flags(struct plat_shmem_config *config, int flags) {
    config->flags |= flags;
}

void
plat_shmem_config_clear_flags(struct plat_shmem_config *config, int flags) {
    config->flags &= ~flags;
}

/** @brief Set limit for given arena */
void
plat_shmem_config_set_arena_used_limit(struct plat_shmem_config *config,
                                       enum plat_shmem_arena arena,
                                       int64_t limit) {
    shmem_alloc_config_set_arena_used_limit(&config->opaque->alloc_config,
                                            arena, limit);
}

/* XXX: Not really shmem, move to alloc.c */
void *
plat_memdup_alloc(const void *src, size_t len) {
    void *ret;

    /* Incase plat_malloc returns physical memory from shmem */
    ret = sys_malloc(len);
    if (ret) {
        memcpy(ret, src, len);
    }

    return (ret);
}

/* XXX: Not really shmem, move to alloc.c */
void
plat_memdup_free(void *ptr) {
    sys_free(ptr);
}

static int
shmem_config_add_mmap_descriptor(struct plat_shmem_config *config,
                                 const char *path, enum shmem_type shmem_type,
                                 off_t len) {
    int ret;
    int i;
    struct shmem_descriptor *descriptor = NULL;
    struct shmem_descriptor **new_descriptors;
    struct shmem_config_opaque *opaque = config->opaque;

    ret = shmem_mmap_descriptor_alloc(&descriptor, path, shmem_type, len);

    if (!ret) {
        if (opaque->descriptors) {
            i = opaque->ndescriptors;
            new_descriptors = sys_realloc(opaque->descriptors, (i + 1) *
                                          sizeof (*opaque->descriptors));
        } else {
            i = 0;
            new_descriptors = sys_malloc(sizeof (*opaque->descriptors));
        }

        if (!new_descriptors) {
            ret = -ENOMEM;
        } else {
            opaque->descriptors = new_descriptors;
            opaque->descriptors[i] = descriptor;
            opaque->ndescriptors = i + 1;
        }
    }

    if (ret && descriptor) {
        sys_free(descriptor);
    }

#ifndef DEPRECATED
    /* XXX: Kludge to make plat_shmem_attach(config.mmap) work */
    config->mmap = plat_shmem_config_get_path(config);
#endif

    return (ret);
}

static int
shmem_mmap_descriptor_alloc(struct shmem_descriptor **descriptorp,
                            const char *path, enum shmem_type shmem_type,
                            off_t len)  {
    int ret;
    struct shmem_descriptor *descriptor = NULL;
    int pathlen = strlen(path) + 1;

    ret = plat_sys_calloc_struct(&descriptor) ? 0 : -ENOMEM;

    if (!ret && pathlen > sizeof (descriptor->specific.mmap.backing_name)) {
        ret = -ENAMETOOLONG;
    }

    if (!ret) {
        descriptor->type = shmem_type;
        descriptor->len = len;
        memcpy(descriptor->specific.mmap.backing_name, path, pathlen);
    }

    if (ret && descriptor) {
        sys_free(descriptor);
    }

    if (!ret) {
        *descriptorp = descriptor;
    }

    return (ret);
}

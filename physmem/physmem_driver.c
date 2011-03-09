/*
 * File:   physmem_driver
 *
 * Author: drew
 *
 * Created on July 16, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: physmem_driver.c 3568 2008-09-25 08:46:17Z drew $
 */

/**
 * physmem provides access to PCI consistent memory from user and
 * kernel space, exposing the physical mappings so that hardware
 * can be instructed to do DMAs without crossing the user-kernel
 * boundary.
 * 
 * XXX: This allocates DMAable chunks until it runs out, the problems
 * being that 1) there aren't many (I've run out at 15M/1G) and that
 * the left over bits can be quite small.
 *
 * alloc_bootmem pages "could" be used as a workaround although that
 * requires a statically linked kernel and could get interesting with
 * corporate IT or Qlogic requirements to run a specific kernel version.
 *
 * some form of vm-abandon (all free memory becomes buffer cache) call 
 * might help
 *
 * XXX: It's unclear what the implications are of allocating dma coherent
 * memory without a device pointer.  Maybe that helps on NUMA boxes to
 * get memory closer to the bus?  If that's a good thing we should refactor
 * so that Christian's driver calls in here.
 *
 * The default also appears to be that we can only get memory < 4G more
 * requires setting a mask on a specific device.
 */

#include <linux/device.h>
#include <linux/fs.h>
#include <linux/init.h>
#include <linux/jiffies.h>
#include <linux/kernel.h>
/*
 * XXX: we're debugging on a paleolithic Centos install which lacks
 * lacks rounddown_pow_of_two
 */
#ifdef notyet
#include <linux/log2.h>
#else
#include <linux/bitops.h>
#endif
#include <linux/module.h>
#include <linux/moduleparam.h>
#include <linux/pci.h>
#include <linux/slab.h>
#include <linux/uaccess.h>

#include "physmem.h"

#define PHYSMEM_DEVICE_NAME "physmem_driver"

/* XXX: The hooks for udev are GPL only according to the build tools */
#ifdef GPL
MODULE_LICENSE("GPL");
#else
MODULE_LICENSE("Proprietary");
#endif
MODULE_AUTHOR("Drew Eckhardt");
MODULE_DESCRIPTION("Userspace access to PCI consistent physical memory");
MODULE_SUPPORTED_DEVICE(PHYSMEM_DEVICE_NAME);

enum {
    /** @brief Maximum number of devices */
    PHYSMEM_MAX_DEVICE = 16,

    /** @brief Minimum segment size. */
    /*
     * XXX: This is not usable for the current shmem implementation with its
     * standard header per segment.  shmem will ignore segments that are
     * too small.
     */
    PHYSMEM_MIN_SEGMENT = 4096
};

enum physmem_read_write {
    PHYSMEM_READ,
    PHYSMEM_WRITE
};

/** @brief Size of each device in megabytes */
static int physmem_size_mb[PHYSMEM_MAX_DEVICE] = { };

/** @brief Number of comma separated entries passed as physmem_size_mb */
static int physmem_size_mb_count = 0;

module_param_array(physmem_size_mb, int, &physmem_size_mb_count,
                   0000 /* perm */);
MODULE_PARM_DESC(physmem_size_mb, "Array of physmem device sizes in MiB");

static struct physmem_state {
    int major;
    int minor_offset;

#ifdef GPL
    struct class *class;
#endif

    int nminor;
    struct physmem_minor **minor;
} physmem_state = { };

struct physmem_minor {
    struct pci_dev *dev;
    uint64_t serial;
    size_t total_len;
    int nsegment;
    struct physmem_segment *segment;
};

struct physmem_segment {
    /** @brief Kernel virtual address */
    void *virt;
    /** @brief Physical address for DMA */
    dma_addr_t dma;
    /** @brief Offset from start of memory */
    /* XXX: should be loff_t or dma_addr_t for x86 32 bit with big paddrs */
    size_t offset;
    /** @brief Length of semgnet */
    size_t len;
} physmen_segment;

static int __init physmem_init(void);
static int physmem_minor_alloc(struct physmem_minor **retp,
                               struct pci_dev *dev, 
                               uint64_t serial,
                               size_t mb);
static void __exit physmem_cleanup(void);
static void physmem_minor_free(struct physmem_minor *minor);
static int physmem_open(struct inode *inode, struct file *file);
static int physmem_release(struct inode *inode, struct file *file);
static int physmem_ioctl(struct inode *inode, struct file *file,
                         unsigned int cmd, unsigned long arg);
static int physmem_mmap(struct file *file, struct vm_area_struct *vma);
static ssize_t physmem_read(struct file *file, char __user *buf, size_t len,
                            loff_t *ppos);
static ssize_t physmem_write(struct file *file, const char __user *buf,
                             size_t len, loff_t *ppos);
static int physmem_get_minor(struct physmem_minor **minorp, struct file *file);
static ssize_t physmem_read_write(enum physmem_read_write rw,
                                  struct file *file,
                                  char __user *buf, size_t length,
                                  loff_t *ppos);
static int physmem_copy(enum physmem_read_write rw, void __user *user,
                        void *kernel, size_t len);
#ifdef PHYSMEM_DEBUG_FILL
static void physmem_debug_fill(void *dest, int *val, size_t len);
#endif

static struct file_operations physmem_fops = {
    .owner = THIS_MODULE,
    .open = physmem_open,
    .release = physmem_release,
    .ioctl = physmem_ioctl,
    .mmap = physmem_mmap,
    .read = physmem_read,
    .write = physmem_write
};

static int __init
physmem_init(void)
{
    int ret = 0;
    int i;
    uint64_t serial_base;

    printk(KERN_INFO "physmem start\n");
    printk(KERN_INFO "physmem %d devices\n",
           physmem_size_mb_count);
    for (i = 0; i < physmem_size_mb_count; ++i) {
        printk(KERN_INFO "physmem size[%d] = %d MiB\n", i,
               physmem_size_mb[i]);
    }


    physmem_state.minor_offset = 0;

    if (!physmem_size_mb_count) {
        printk(KERN_ERR "physmem no size specified\n");
        ret = -EINVAL;
    }

    if (!ret) {
        physmem_state.nminor = physmem_size_mb_count;
        physmem_state.minor = kcalloc(physmem_state.nminor,
                                      sizeof (*physmem_state.minor[0]),
                                      GFP_KERNEL);
        if (!physmem_state.minor) {
            ret = -ENOMEM;
        }
    }

    serial_base = get_jiffies_64();
    for (i = 0; !ret && i < physmem_size_mb_count; ++i) {
        ret = physmem_minor_alloc(&physmem_state.minor[
                                  physmem_state.minor_offset + i], NULL,
                                  serial_base + i, physmem_size_mb[i]);
    }

    if (!ret) {
        physmem_state.major = register_chrdev(0 /* major */,
                                              PHYSMEM_DEVICE_NAME,
                                              &physmem_fops);
        if (physmem_state.major < 0) {
            printk(KERN_ERR "physmem failed to register device %d\n",
                   physmem_state.major);
            ret = physmem_state.major;
        }
    }

#ifdef GPL
    if (!ret) {
        physmem_state.class = class_create(THIS_MODULE, PHYSMEM_DEVICE_NAME);
        if (!physmem_state.class) {
            ret = -EIO;
        }
    }

    if (!ret) {
        for (i = 0; i < physmem_state.nminor; ++i) {
            device_create(physmem_state.class, NULL,
                          MKDEV(physmem_state.major, i),
                          PHYSMEM_DEVICE_NAME "%d", i);
        }
    }
#endif

    if (ret) {
        physmem_cleanup();
    }

    return (ret);
}

/* XXX: Can this move to after the static definition */
module_init(physmem_init);

/** @brief Allocate all state associated with minor device */
static int
physmem_minor_alloc(struct physmem_minor **retp, 
                    struct pci_dev *dev,
                    uint64_t serial,
                    size_t mb) {
    int ret = 0;
    int i;
    size_t want;
    size_t remain;
    size_t alloc_size;
    struct physmem_minor *minor;
    struct physmem_segment *segment;
#ifdef PHYSMEM_DEBUG_FILL
    int debug_fill_val = 0;
#endif

    minor = kcalloc(1, sizeof (*minor), GFP_KERNEL);
    if (minor) {
        minor->serial = serial;
        minor->dev = dev;
        minor->nsegment = 0;
        minor->total_len = 0;
    } else {
        ret = -ENOMEM;
    }

    want = mb * 1024 * 1024;
    /*
     * FIXME: We should use the min of this and some form of MAX_ORDER
     * macro so we supress the spurious kernel messages about failed
     * memory allocations.
     */
#ifdef notyet
    alloc_size = rounddown_pow_of_two(want);
#else
    alloc_size = 1UL << (fls_long(want) - 1);
#endif
    remain = want;

    printk(KERN_DEBUG "physmem want 0x%lx try segment 0x%lx bytes\n",
           (unsigned long)remain, (unsigned long)alloc_size);
    for (i = 0;
         !ret && remain > 0 && alloc_size >= PHYSMEM_MIN_SEGMENT; /* none */) {

        if (alloc_size >= remain) {
#ifdef notyet
            alloc_size = rounddown_pow_of_two(remain);
#else
            alloc_size = 1UL << (fls_long(remain) - 1);
#endif
        }

        /*
         * XXX: we're debugging on a paleolithic Centos install which
         * lacks krealloc so it gets simulated.
         */
#ifdef USE_KREALLOC
        segment = krealloc(minor->segment,
                           (minor->nsegment + 1) * sizeof (*minor->segment),
                           GFP_KERNEL);
#else
        segment = kmalloc((minor->nsegment + 1) * sizeof (*minor->segment),
                          GFP_KERNEL);
        if (segment) {
            memcpy(segment, minor->segment,
                   minor->nsegment * sizeof (*minor->segment));
        }
#endif
        if (segment) {
            minor->segment = segment;
        } else {
            ret = -ENOMEM;
        }

        segment = &minor->segment[minor->nsegment];
        if (!ret) {
            memset(segment, 0, sizeof (*segment));
        }
        while (!ret && alloc_size >= PHYSMEM_MIN_SEGMENT && !segment->virt) {
            segment->virt = pci_alloc_consistent(minor->dev, alloc_size,
                                                 &segment->dma);
            if (segment->virt) {
                segment->len = alloc_size;
                segment->offset = minor->total_len;
                remain -= segment->len;
                minor->total_len += segment->len;
                printk(KERN_DEBUG "physmem attached virt %p phys %llx"
                       " len 0x%lx\n", segment->virt,
                       (unsigned long long)segment->dma,
                       (unsigned long)segment->len);
#ifdef PHYSMEM_DEBUG_FILL
                physmem_debug_fill(segment->virt, &debug_fill_val,
                                   segment->len);
#endif
            } else {
                alloc_size >>= 1;
            }
        }
        if (!ret && !segment->virt) {
            ret = -ENOMEM;
        }
        if (!ret) {
            ++minor->nsegment;
        }
    }

    if (ret) {
        printk(KERN_ERR "physmem failed after attaching %d segments"
               " total len 0x%lx of 0x%lx\n", minor->nsegment,
               (unsigned long)minor->total_len, (unsigned long)want);
        physmem_minor_free(minor);
    } else {
        printk(KERN_DEBUG "physmem successfully attached %d segments"
               " total len 0x%lx\n", minor->nsegment,
               (unsigned long)minor->total_len);
        *retp = minor;
    }

    return (ret);
}

/* Can be called on partially initialized device */
static void __exit
physmem_cleanup()
{
    int i;

#ifdef GPL
    if (physmem_state.class) {
        class_destroy(physmem_state.class);
    }
#endif

    if (physmem_state.major > 0) {
        unregister_chrdev(physmem_state.major, PHYSMEM_DEVICE_NAME);
    }

    if (physmem_state.minor) {
        for (i = 0; i < physmem_state.nminor; ++i) {
            physmem_minor_free(physmem_state.minor[i]);
        }
        kfree(physmem_state.minor);
    }

    printk(KERN_INFO "physmem done\n");
}
module_exit(physmem_cleanup);

/**
 * @brief Free minor device state
 *
 * Can be called on partially initialized structure.
 */
static void
physmem_minor_free(struct physmem_minor *minor) {
    struct physmem_segment *segment;
    struct physmem_segment *end_segment;
    size_t total_free;

    if (minor) {
        if (minor->segment) {

            for (total_free = 0, segment = minor->segment, end_segment =
                 minor->segment + minor->nsegment; segment < end_segment;
                 ++segment) {
                if (segment->virt) {
                    pci_free_consistent(minor->dev, segment->len,
                                        segment->virt, segment->dma);
                    total_free += segment->len;
                }
            }
            kfree(minor->segment);

            printk(KERN_DEBUG "physmem freed 0x%lx bytes\n",
                   (long)total_free);

        }
        kfree(minor);
    }
}

static int
physmem_open(struct inode *inode, struct file *file) {
    int ret = 0;

    if (!try_module_get(THIS_MODULE)) {
        ret = -EFAULT;
    }

    if (!ret) {
        ret = physmem_get_minor(NULL, file);
    }

    return (ret);
}

static int
physmem_release(struct inode *inode, struct file *file) {
    module_put(THIS_MODULE);
    return (0);
}

static int
physmem_ioctl(struct inode *inode, struct file *file, unsigned int cmd,
              unsigned long arg) {
    int ret = 0;
    int i;
    int limit;
    struct physmem_minor *minor = NULL;
    struct physmem_segment *segment;
    struct physmem_regions regions;
    struct physmem_regions __user *uregions =
        (struct physmem_regions __user *)arg;
    struct physmem_region region;
    struct physmem_region __user *uregion;

    if (cmd != PHYSMEMGETREGIONS) {
        printk(KERN_ERR "physmem command %x not %x\n",
               cmd, (unsigned)PHYSMEMGETREGIONS);
        ret = -EINVAL;
    }
    if (!ret) {
        ret = physmem_get_minor(&minor, file);
    }
    if (!ret && copy_from_user(&regions, uregions, sizeof (regions))) {
        ret = -EFAULT;
    }
    if (!ret) {
        regions.serial = minor->serial;
        regions.total_region = minor->nsegment;
        regions.total_len = minor->total_len;
        if (copy_to_user(uregions, &regions, sizeof (regions))) {
            ret = -EFAULT;
        }
    }
    if (!ret) {
        limit = min_t(int, regions.nregion, minor->nsegment);
        uregion = (struct physmem_region __user *) uregions->region;
        for (i = 0; !ret && i < limit; ++i) {
            segment = &minor->segment[i];
            region.offset = segment->offset;
            region.paddr = (uint64_t)segment->dma;
            region.len = segment->len;

            uregion = &uregions->region[i];
            if (copy_to_user(uregion, &region, sizeof (region))) {
                ret = -EFAULT;
            }
        }
    }

    return (ret);
}

static int
physmem_mmap(struct file *file, struct vm_area_struct *vma) {
    int ret = 0;
    struct physmem_minor *minor = NULL;
    struct physmem_segment *segment;
    struct physmem_segment *end_segment;
    pgprot_t prot;
    loff_t offset_start = 0;
    loff_t offset_end = 0;
    loff_t offset;

    size_t copy_len;
    unsigned long copy_uptr;
    dma_addr_t copy_dma;

#if 0
    /*
     * XXX: Since this is actual memory (where cache entries get invalidated),
     * there doesn't seem to be a good reason to make it uncached.
     */
    prot = pgprot_noncached(vma->vm_page_prot);
#else
    prot = vma->vm_page_prot;
#endif

    ret = physmem_get_minor(&minor, file);
    /* Check start separately to prevent wrap */
    if (!ret) {
        offset_start = (loff_t)vma->vm_pgoff << PAGE_SHIFT;
        if (offset_start >= minor->total_len) {
            printk(KERN_ERR "physmem attempt to mmap start offset 0x%llx"
                   " past len 0x%lx", (unsigned long long)offset_start,
                   (unsigned long)minor->total_len);
            ret = -EINVAL;
        }
    }
    /* FIXME: This could overflow on a big VMA */
    if (!ret) {
        offset_end = offset_start + vma->vm_end - vma->vm_start;
        if (offset_start >= minor->total_len) {
            printk(KERN_ERR "physmem attempt to mmap end offset 0x%llx"
                   " past len 0x%lx", (unsigned long long)offset_start,
                   (unsigned long)minor->total_len);
            ret = -EINVAL;
        }
    }

    /*
     * XXX Logically the main part of this is identical to #physmem_read_write
     * except for when we check EOF (fail up-front for mmap(2), return a short
     * IO for read(2) or write(2)).
     */
    if (!ret) {
        /* Find segment. */
        for (segment = minor->segment, end_segment =
             minor->segment + minor->nsegment;
             segment < end_segment && offset_start >= segment->offset +
             segment->len; ++segment) {
        }

        /* XXX: Should not happen */
        if (segment == end_segment) {
            printk(KERN_ERR "physmem mmap unexpectedly ran into last segment");
            ret = -EIO;
        }

        /*
         * XXX: Should only run into last segment at EOF; we're just being
         * paranoid.
         */
        for (offset = offset_start;
             !ret && segment < end_segment && segment->offset <= offset_end &&
             offset < offset_end; ++segment) {
            copy_len = min_t(size_t, (size_t)(offset_end - offset),
                             (size_t)(segment->offset + segment->len - offset));
            /*
             * XXX: This works for the cases it's been tested in.  It seems
             * that vma->vm_pgoff is the page offset into the file and needs
             * to add into vma->vma_start which is only relative to the
             * surrounding vm structure.
             */
            copy_uptr = vma->vm_start + (offset - offset_start);
            copy_dma = segment->dma + (offset - segment->offset);

            /*
             * XXX: This may need to change if we accomodate red zones.  It's
             * unclear what we should do when providing a sparse map or where
             * we want certain pages to fault.
             */
            if (offset != offset_start && offset != segment->offset) {
                printk(KERN_ERR "physmem discontiguous read\n");
                ret = -EIO;
            } else {
                ret = remap_pfn_range(vma, copy_uptr,
                                      /* XXX: use pfn xlat fn? */
                                      copy_dma >> PAGE_SHIFT, copy_len, prot);
                if (ret) {
                    printk(KERN_ERR "physmem mmap remap_pfn_range failed"
                           " user 0x%lx dma 0x%llx len 0x%lx: %d", copy_uptr,
                           (unsigned long long)copy_dma,
                           (unsigned long)copy_len, ret);
                } else {
                    offset += copy_len;
                }
            }
        }
    }

    return (ret);
}

static ssize_t
physmem_read(struct file *file, char __user *buf, size_t len, loff_t *ppos) {
    return (physmem_read_write(PHYSMEM_READ, file, buf, len, ppos));
}

static ssize_t
physmem_write(struct file *file, const char __user *buf, size_t len,
              loff_t *ppos) {
    return (physmem_read_write(PHYSMEM_WRITE, file, (char __user *)buf, len,
                               ppos));
}

/** @brief Translate open file into minor state */
static int
physmem_get_minor(struct physmem_minor **minorp, struct file *file) {
    int ret;
    int minor;

    minor = MINOR(file->f_dentry->d_inode->i_rdev);
    if (physmem_state.minor_offset <= minor && minor < physmem_state.nminor) {
        if (minorp) {
            *minorp = physmem_state.minor[minor - physmem_state.minor_offset];
        }
        ret = 0;
    } else {
        ret = -ENODEV;
    }

    return (ret);
}

/** @brief Read or write same logic otherwise */
static ssize_t
physmem_read_write(enum physmem_read_write rw, struct file *file,
                   char __user *buf, size_t length, loff_t *ppos) {
    struct physmem_minor *minor = NULL;
    struct physmem_segment *segment;
    struct physmem_segment *end_segment;
    loff_t offset_start;
    loff_t offset_end;
    loff_t offset;
    ssize_t ret;

    size_t copy_len;
    char __user *copy_uptr;
    void *copy_kptr;

    offset = 0;                              // Suppress GCC complaints
    offset_start = 0;

    ret = physmem_get_minor(&minor, file);
    if (!ret) {
        /* Find segment */
        for (segment = minor->segment, end_segment =
             minor->segment + minor->nsegment, offset_start = *ppos;
             segment < end_segment && offset_start >= segment->offset +
             segment->len; ++segment) {
        }

        /* Copy data */
        for (offset_end = offset_start + length, offset = offset_start;
             !ret && segment < end_segment && segment->offset <= offset_end &&
             offset < offset_end; ++segment) {
            copy_len = min_t(size_t, (size_t)(offset_end - offset),
                             (size_t)(segment->offset + segment->len - offset));
            copy_uptr = (char __user *)buf + offset - offset_start;
            copy_kptr = (char *)segment->virt + offset - segment->offset;

            /*
             * XXX: This should change if we ever want to include red-zones
             * in the kernel mapping
             */
            if (offset != offset_start && offset != segment->offset) {
                printk(KERN_ERR "physmem discontiguous read\n");
                ret = -EIO;
            } else if (physmem_copy(rw, copy_uptr, copy_kptr, copy_len)) {
                printk(KERN_ERR "physmem copy %s buf %p uptr %p vaddr %p"
                       " len %lu",
                       (rw == PHYSMEM_READ) ? "read" : "write", buf, copy_uptr,
                       copy_kptr, (unsigned long)copy_len);
                ret = -EFAULT;
            } else {
                offset += copy_len;
            }
        }
    }

    if (!ret) {
        *ppos = offset;
        ret = offset - offset_start;
    }

    return (ret);
}

/** @brief Copy either direction between user and kernel memory */
static int
physmem_copy(enum physmem_read_write rw, void __user *user, void *kernel,
             size_t len) {
    unsigned long ret;

    switch (rw) {
    case PHYSMEM_READ:
        ret = copy_to_user(user, kernel, len);
        break;
    case PHYSMEM_WRITE:
        ret = copy_from_user(kernel, user, len);
        break;
    default:
        ret = 1; /* placate GCC */
    }

    return (!ret ? 0 : -EFAULT);
}

#ifdef PHYSMEM_DEBUG_FILL
/** @brief Fill memory with an incrementing integers */
static void
physmem_debug_fill(void *dest, int *val, size_t len) {
    int *start, *end, *ptr;

    start = (int *)dest;
    end = start + len / 4;
    for (ptr = start; ptr < end; ++ptr, ++*val) {
        *ptr = *val;
    }
}
#endif

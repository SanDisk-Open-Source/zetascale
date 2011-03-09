/*
 * File:   $HeadURL: svn://svn.schoonerinfotech.net/schooner-trunk/trunk/sdf/platform/mem_debug.cc $
 * Author: drew
 *
 * Created on March 12, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: mem_debug.cc 2967 2008-08-21 00:26:33Z drew $
 */

#include <execinfo.h>

#include <cstring>
#include <map>
#include <memory>
#include <sstream>

#include "platform/assert.h"
#include "platform/defs.h"
#include "platform/errno.h"
#include "platform/logging.h"
#include "platform/mem_debug.h"
#include "platform/mman.h"
#include "platform/mutex.h"

enum {
    DEFAULT_BACKTRACE_DEPTH = 10
};

class plat_mem_debug_ref;
class plat_mem_debug_region;

enum plat_mem_debug_ref_type {
    PLAT_MEM_DEBUG_READ_WRITE,
    PLAT_MEM_DEBUG_READ_ONLY
};


/**
 * Memory protection class.  Unused memory from managed regions is debug
 * to disallow access.
 */
class plat_mem_debug {
public:
    plat_mem_debug(const plat_mem_debug_config &config);
    ~plat_mem_debug();

    /** @Brief add unused memory to the pool */
    int add_unused(void *start, size_t len);

    /** Reference memory */
    int reference(void **ref, void *start, size_t len,
                  enum plat_mem_debug_ref_type ref_type, int skip_frames = 1);

    /** Release memory */
    int release(void **ref, void *start, size_t len,
                enum plat_mem_debug_ref_type ref_type);

    /** Return true when the range is entirely free */
    bool check_free(void *start, size_t len);

    /** Return true when the range is entirely referenced */
    bool check_referenced(void *start, size_t len);

    /** Get statistics */
    void get_stats(struct plat_mem_debug_stats *stats);

    /** Log all refererneces at given categorym level and backtrace limit */
    void log_references(int category, enum plat_log_level level,
                        int backtrace_limit);

private:
    typedef std::map<void *, plat_mem_debug_region *> map_t;

    // Declare but don't define
    plat_mem_debug (const plat_mem_debug &);
    plat_mem_debug &operator= (const plat_mem_debug &);

    /** @brief Make a new debug free region */
    void make_free_region(void *ptr, size_t len);

    /** @brief Remove a free region, making it accessable */
    int remove_free_region(void *ptr, size_t len);

    /** @brief Return bounding regions as plat_mem_debug_regions */
    void bounds(plat_mem_debug_region **prev_ptr,
                plat_mem_debug_region **equals_ptr,
                plat_mem_debug_region  **next_ptr, map_t &map, void *ptr);

    /** @brief Return bounding regions as iterators */
    void bounds(map_t::iterator *prev_ptr, map_t::iterator *equals_ptr,
                map_t::iterator *next_ptr, map_t &map, void *ptr);

    plat_mem_debug_config m_config;

    plat_mem_debug_stats m_stats;

    /** @brief pagesize  */
    int m_page_size;

    /** Protect maps */
    plat_mutex_t m_lock;

    /** Map start pointer to free regions. */
    map_t m_unused;

    /** Map start pointer to used regions */
    map_t m_used;
};

/**
 * @brief Describes memory region
 *
 * Protected memory regions encompas the half-open interval from start
 * inclusive to start + end exclusive.  They have an associated reference
 * count and list of references, where removing a reference at a different
 * address is suspicious but not a problem unless the reference count fails
 * to hit zero and the aliasing makes it harder to find the original problem.
 */
class plat_mem_debug_region {
public:
    plat_mem_debug_region(void *start, size_t len);
    ~plat_mem_debug_region();   /* modified by zhuang Apr21 */

    /** @brief Add reference */
    void add_reference(void **ref,
                       enum plat_mem_debug_ref_type ref_type,
                       int backtrace_depth, int skip_frames);

    /** @brief Remove reference.  Returns -errno */
    int remove_reference(void **ref,
                          enum plat_mem_debug_ref_type ref_type,
                          int log_cat);

    /** @brief Log references */
    void log_references(int log_cat, enum plat_log_level level,
                        int backtrace_limit);

    /** @brief Return inclusive start of region  */
    void *get_start() const { return (m_ptr); }

    /** @brief Return length of region */
    size_t get_len() const { return (m_len); }

    /** @brief Return exclusive end of region */
    void *get_end() const { return (reinterpret_cast<char *>(m_ptr) + m_len); }

    int get_write_count() const { return (m_write_count); }
    int get_read_count() const { return (m_read_count); }

private:
    // Declare but don't define
    plat_mem_debug_region (const plat_mem_debug_region &);
    plat_mem_debug_region &operator= (const plat_mem_debug_region &);

    /** @brief Pointer to memory region */
    void *m_ptr;
    /** @briefTotal length */
    size_t m_len;
    /**
     * @brief Reference count to region.
     *
     * 1.  We do not handle sub pointers.
     * 2.  Because of aliasing m_references.size may not be the same
     * as refcount; this can only include the potentially free ponters.
     */
    int m_read_count;
    int m_write_count;

    // XXX: Use boost::shared_pointer
    typedef std::multimap<void **, plat_mem_debug_ref *> map_t;
    map_t m_references;
};

class plat_mem_debug_ref {
public:
    plat_mem_debug_ref(void **reference, enum plat_mem_debug_ref_type ref_type,
                       int backtrace_frames, int skip_frames);
    ~plat_mem_debug_ref();

    /** @brief return reference */
    void **get_reference() const { return (m_reference); }

    /** @brief return reference type */
    enum plat_mem_debug_ref_type get_ref_type() const { return (m_ref_type); }

    /** @brief return pointer to backtrace */
    void **get_backtrace() const { return (m_backtrace); }

    /** @brief return backtrace depth */
    int get_backtrace_depth() const { return (m_backtrace_depth); }

private:
    // Declare but don't define
    plat_mem_debug_ref (const plat_mem_debug_ref &);
    plat_mem_debug_ref &operator= (const plat_mem_debug_ref &);

    void **m_reference;
    enum plat_mem_debug_ref_type m_ref_type;
    void **m_backtrace;
    int m_backtrace_depth;
};

static void *round_up(void *ptr, int page_size);
static void *round_down(void *ptr, int page_size);

struct plat_mem_debug *
plat_mem_debug_alloc(const struct plat_mem_debug_config *config) {
    try {
        return (new plat_mem_debug(*config));
    } catch (std::bad_alloc) {
        plat_errno = ENOMEM;
        return (NULL);
    } catch (...) {
        plat_assert_always(0);
    }
};

void
plat_mem_debug_free(struct plat_mem_debug *debug) {
    try {
        delete debug;
    } catch (...) {
        plat_assert_always(0);
    }
}

int
plat_mem_debug_add_unused(struct plat_mem_debug *debug,
                          void *start, size_t len) {
    PLAT_RETURN_NO_EXCEPTIONS(debug->add_unused(start, len), -ENOMEM);
}

int
plat_mem_debug_reference(struct plat_mem_debug *debug, void **reference,
                         void *start, size_t len, int writeable,
                         int skip_frames) {
    PLAT_RETURN_NO_EXCEPTIONS(debug->reference(reference, start, len,
                                               writeable ? PLAT_MEM_DEBUG_READ_WRITE :
                                               PLAT_MEM_DEBUG_READ_ONLY,
                                               skip_frames + 1),
                              -ENOMEM);
}

int
plat_mem_debug_release(struct plat_mem_debug *debug,
                       void **reference, void *start, size_t len,
                       int writeable) {
    PLAT_RETURN_NO_EXCEPTIONS(debug->release(reference, start, len,
                                             writeable ? PLAT_MEM_DEBUG_READ_WRITE :
                                             PLAT_MEM_DEBUG_READ_ONLY),
                              -ENOMEM);
}

void
plat_mem_debug_log_references(struct plat_mem_debug *debug,
                              int category, enum plat_log_level level,
                              int backtrace_limit) {
    PLAT_RETURN_NO_EXCEPTIONS(debug->log_references(category, level,
                                                    backtrace_limit),
                              /* void  */);
}

plat_mem_debug::plat_mem_debug(const struct plat_mem_debug_config &config) :
    m_config(config),
    m_stats(),
    m_page_size(getpagesize()) {
    plat_mutex_init(&m_lock);
}

plat_mem_debug::~plat_mem_debug() {
    plat_mem_debug_region *region;
    int status;
    size_t len;
    void *end;
    void *start;
    // Make all used regions unused, which has the side effect of
    // unprotecting them
    while (!m_used.empty()) {
        region = m_used.begin()->second;
        m_used.erase(m_used.begin());
        make_free_region(region->get_start(), region->get_len());
        delete region;
    }

    while (!m_unused.empty()) {
        region = m_unused.begin()->second;

        /**
         * Make the protected region(unaccess) be read and writed. And
         * make the block of memory into the original state. Then
         * delete the memory successfully. Sometime we malloc a block
         * memory which is even page alignment. The pointer of this
         * memory is the same as the pointer aligned by "round_up"
         * function. So if we don't make the inaccessible memory into
         * accessible state, we don't free the memory block because we
         * will access the header the memory block with a segment fault.
         * So this must have been done before free the memory pool
         * block.
         */
        start = round_down(region->get_start(), m_page_size);
        end = round_up(region->get_end(), m_page_size);
        len = reinterpret_cast<char *>(end) -
                  reinterpret_cast<char *>(start);
        status = plat_mprotect(start, len, PROT_READ|PROT_WRITE);
        // This should never fail
        plat_assert(!status);

        m_unused.erase(m_unused.begin());
        delete region;
    }
}

int
plat_mem_debug::add_unused(void *start, size_t len) {
    plat_mem_debug_region *prev;
    plat_mem_debug_region *equals;
    plat_mem_debug_region *next;

    int ret;

    plat_mutex_lock(&m_lock);
    try {
        bounds(&prev, &equals, &next, m_used, start);
        /**
         * modified by zhuang Apr 24
         * When equals is not NULL, the start of equals must greater
         * than the end of new unused memory block to add.
         */
        if ((equals && equals->get_start() < reinterpret_cast<char *>(start) + len)
            || (prev && prev->get_end() > start) ||
            (next && next->get_start() <
             reinterpret_cast<char *>(start) + len)) {
            ret = -EEXIST;
        } else {
            make_free_region(start, len);
            ret = 0;
        }
    } catch (...) {
        plat_mutex_unlock(&m_lock);
        throw;
    }
    plat_mutex_unlock(&m_lock);

    return (ret);
}

int
plat_mem_debug::reference(void **reference, void *start, size_t len,
                          enum plat_mem_debug_ref_type ref_type,
                          int skip_frames) {
    /* First case: same start address as existing map */
    int ret = 0;
    plat_mem_debug_region *region = NULL;
    plat_mem_debug_region *prev;
    plat_mem_debug_region *next;

    plat_mutex_lock(&m_lock);
    try {
        map_t::iterator i(m_used.find(start));
        if (i != m_used.end()) {
            region = i->second;
            if (region->get_len() != len) {
                plat_log_msg(PLAT_LOG_ID_INITIAL, m_config.log_category,
                             PLAT_LOG_LEVEL_ERROR,
                             "Reference at 0x%p length %lu not existing %lu",
                             start, (unsigned long)len,
                             (unsigned long)i->second->get_len());
                ret = -EINVAL;
            } else {
                // XXX Need to account for previous and next when increasing
                // memory protection from read-only to read-write
                region->add_reference(reference, ref_type,
                                      m_config.backtrace_depth,
                                      skip_frames + 1);
                m_stats.reference_count++;  /* modified by zhuang Apr 18 */
            }
        } else {
            bounds(&prev, NULL /* no equals */,  &next, m_used, start);
            if (prev != NULL && prev->get_end() > start) {
                plat_log_msg(PLAT_LOG_ID_INITIAL, m_config.log_category,
                             PLAT_LOG_LEVEL_ERROR,
                             "Reference attempt at 0x%p length %lu overlaps"
                             " with  prev at 0x%p length %lu", start,
                             (unsigned long)len,
                             prev->get_start(), (unsigned long)prev->get_len());
                ret = EINVAL;
            }
            if (next != NULL && reinterpret_cast<char *>(start) + len >
                next->get_start()) {
                plat_log_msg(PLAT_LOG_ID_INITIAL, m_config.log_category,
                             PLAT_LOG_LEVEL_ERROR,
                             "Reference attempt at 0x%p length %lu overlaps"
                             " with next at 0x%p length %lu", start,
                             (unsigned long)len, next->get_start(),
                             (unsigned long)next->get_len());
                ret = EINVAL;
            }
            // Remove free
            if (!ret) {
                ret = remove_free_region(start, len);
                if (!ret) {
                    try {
                        region = new plat_mem_debug_region(start, len);
                        try {
                            region->add_reference(reference, ref_type,
                                                  m_config.backtrace_depth,
                                                  skip_frames + 1);
                            std::pair<map_t::iterator,bool> result =
                            m_used.insert(map_t::value_type(start, region));
                            m_stats.reference_count++;  /* modified by zhuang Apr 18 */
                            m_stats.object_count++; /* add by zhuang Apr 22 */
                            plat_assert(result.second);
                        } catch (...) {
                            delete region;
                            throw;
                        }
                    } catch (...) {
                        make_free_region(start, len);
                        throw;
                    }
                }
            }
        }
    } catch (...) {
        plat_mutex_unlock(&m_lock);
        throw;
    }
    plat_mutex_unlock(&m_lock);

    return (ret);
}

int
plat_mem_debug::release(void **reference, void *start, size_t len,
                        enum plat_mem_debug_ref_type ref_type) {
    int ret;
    plat_mem_debug_region *region;

    plat_mutex_lock(&m_lock);
    try {
        map_t::iterator i(m_used.find(start));
        if (i == m_used.end()) {
            plat_log_msg(PLAT_LOG_ID_INITIAL, m_config.log_category,
                         PLAT_LOG_LEVEL_ERROR,
                         "Reference to 0x%p length %lu does not exist", start,
                         (unsigned long)len);
            ret = -EINVAL;
        } else if (i->second->get_len() != len) {
            plat_log_msg(PLAT_LOG_ID_INITIAL, m_config.log_category,
                         PLAT_LOG_LEVEL_ERROR,
                         "Reference to 0x%p length %lu not existing %lu",
                         start, (unsigned long)len,
                         (unsigned long)i->second->get_len());
            ret = -EINVAL;
        } else {
            region = i->second;
            ret = region->remove_reference(reference, ref_type,
                                           m_config.log_category);
            if (!ret) {
                m_stats.reference_count--;  /* add by zhuang Apr18 */
                if (!region->get_write_count() && !region->get_read_count()) {
                    try {
                        make_free_region(start, len);
                        m_used.erase(i);    /* modified by zhuang Apr 21 */
                        delete region;      /* modified by zhuang Apr 21 */
                        m_stats.object_count--; /* add by zhuang Apr 22 */
                    } catch (...) {
                        delete region;
                        throw;
                    }
                }
            }
        }
    } catch (...) {
        plat_mutex_unlock(&m_lock);
        throw;
    }
    plat_mutex_unlock(&m_lock);
    return (ret);
}

bool
plat_mem_debug::check_free(void *start, size_t len) {
    bool ret;
    plat_mem_debug_region *prev;
    plat_mem_debug_region *equals;
    plat_mem_debug_region *next;

    void *end = reinterpret_cast<char *>(start) + len;

    plat_mutex_lock(&m_lock);
    try {
        bounds(&prev, &equals, &next, m_unused, start);
        ret = (equals && equals->get_start() == start && end <= equals->get_end()) ||
            (prev && prev->get_start() <= start && end <= prev->get_end());
    } catch (...) {
        plat_mutex_unlock(&m_lock);
        throw;
    }
    plat_mutex_unlock(&m_lock);

    return (ret);
}

void
plat_mem_debug::get_stats(struct plat_mem_debug_stats *stats) {
    *stats = m_stats;
}

void
plat_mem_debug::log_references(int category, enum plat_log_level level,
                               int backtrace_limit) {

    map_t::iterator i;

    plat_mutex_lock(&m_lock);
    try {
        for (i = m_used.begin(); i != m_used.end(); ++i) {
            i->second->log_references(category, level, backtrace_limit);
        }
    } catch (...) {
        plat_mutex_unlock(&m_lock);
        throw;
    }
    plat_mutex_unlock(&m_lock);
}

void
plat_mem_debug::make_free_region(void *start, size_t len) {
    map_t::iterator prev;
    map_t::iterator equal;
    plat_mem_debug_region *region;
    int status;
    size_t new_len;
    void *new_start = reinterpret_cast<char *>(start);
    void *new_end = reinterpret_cast<char *>(start) + len;

    /**
     * lower_bound() returns an iterator pointing to the first
     * element in the map whose key does not compare less than
     * x.(i.e.it is either equal or greater). When the count of
     * region in the map is not 0, prev and equal are not NULL at
     * the same time.
     */
    prev = equal = m_unused.lower_bound(start);
    if (prev == m_unused.begin()) {
        prev = m_unused.end();
    } else if (!m_unused.empty()) {
        --prev;
    }
    plat_assert(equal == m_unused.end() || equal->second->get_start() >= new_end);

    /*merge the the region if necessary*/
    if (prev != m_unused.end() && prev->second->get_end() >= start) {
        new_start = prev->second->get_start();
        delete prev->second;
        m_unused.erase(prev);
    }

    /*merge the the region if necessary*/
    if (equal != m_unused.end() && new_end >= equal->second->get_start()) {
        new_end = equal->second->get_end();
        delete equal->second;
        m_unused.erase(equal);
    }

    new_len = reinterpret_cast<char *>(new_end) -
        reinterpret_cast<char *>(new_start);
    region = new plat_mem_debug_region(new_start, new_len);
    try {
        m_unused.insert(map_t::value_type(new_start, region));
    } catch (...) {
        delete region;
        throw;
    }

    new_start = round_up(new_start, m_page_size);
    new_end = round_down(new_end, m_page_size);
    new_len = reinterpret_cast<char *>(new_end) -
        reinterpret_cast<char *>(new_start);

    if (new_len > 0) {
        status = plat_mprotect(new_start, new_len, PROT_NONE);  /* modified by zhuang Apr 21 */
        // This should never fail
        plat_assert(!status);
    }
}

int
plat_mem_debug::remove_free_region(void *start, size_t len) {
    plat_mem_debug_region *region;
    map_t::iterator prev;
    map_t::iterator equal;
    int status;
    size_t new_len;
    void *end = reinterpret_cast<char *>(start) + len;
    void *new_start = start;
    void *new_end = end;
    int ret = 0;

    /**
     * lower_bound() returns an iterator pointing to the first
     * element in the map whose key does not compare less than
     * x.(i.e.it is either equal or greater). When the count of
     * region in the map is not 0, prev and equal are not NULL at
     * the same time.
     */
    prev = equal = m_unused.lower_bound(start);
    if (prev == m_unused.begin()) {
        prev = m_unused.end();
    } else if (!m_unused.empty()) {
        --prev;
    }

    /**
     * modified by zhuang Apr 24
     * We found a usefull rule:A memory block to remove must be in
     * only one unused region. It doesn't happen that part of the
     * memory block is in one unused region and another part is in
     * another unused region. So the size of the unused region which
     * includes the memory block must be greater or equal to the
     * memory block size. When the memory block remove form a
     * region, the original region will be in 3 cases:
     * 1, become 2 new regions (region size > len)
     * 2, become 1 new region. (region start == start || region end
     * == memory block end)
     * 3, the region disappear (region size == memory block size)
     * Only remove the region from the prev and equal.
     */

    /**
     *  the region to remove is in the "prev" part
     */
    if (prev != m_unused.end() && prev->second->get_start() < start &&
        prev->second->get_end() >= end) {
        /**
         *  insert the remaining tail part of the original region if
         *  existing
         */
        if (prev->second->get_end() > end) {
            region = new plat_mem_debug_region
                (end, reinterpret_cast<char *>(prev->second->get_end()) -
                    reinterpret_cast<char *>(end));
            try {
                std::pair<map_t::iterator,bool> result =
                    m_unused.insert(map_t::value_type(region->get_start(),
                                                region));
                plat_assert(result.second);
            } catch (...) {
                delete region;
                throw;
            }
        }
        /**
         *  modify the orginal region size if the remaining region is
         *  the head part of the original region
         */
        if (prev->second->get_start() < start) {
            region = new plat_mem_debug_region
                     (prev->second->get_start(), reinterpret_cast<char *>(start) -
                      reinterpret_cast<char *>(prev->second->get_start()));
            delete prev->second;
            prev->second = region;
        }
    } else if (equal != m_unused.end()
        && equal->second->get_start() <= start
        && equal->second->get_end() >= end) {
        plat_assert(new_start == equal->second->get_start());
        /** remain the tail part of the original region */
        if (equal->second->get_end() > end) {
            region = new plat_mem_debug_region
                     (end, reinterpret_cast<char *>(equal->second->get_end()) -
                      reinterpret_cast<char *>(end));
            /** delete the original region */
            delete equal->second;
            m_unused.erase(equal);
            /** insert new region */
            try {
                std::pair<map_t::iterator,bool> result =
                    m_unused.insert(map_t::value_type(end, region));
                plat_assert(result.second);
            } catch (...) {
                delete region;
                throw;
            }
        } else if (equal->second->get_end() == end){
            /**
             * the region to remove is the same as the original region. the
             * original region disapper.
             */
            delete equal->second;
            m_unused.erase(equal);
        }
    } else {
        plat_log_msg(PLAT_LOG_ID_INITIAL, m_config.log_category,
                     PLAT_LOG_LEVEL_ERROR,
                     "Reference attempt at 0x%p length %lu isn't in"
                     " unused region", start, (unsigned long)len);
        ret = -EINVAL;
    }

    if (!ret) {
        new_start = round_down(new_start, m_page_size);
        new_end = round_up(new_end, m_page_size);
        new_len = reinterpret_cast<char *>(new_end) -
                  reinterpret_cast<char *>(new_start);

        /*
        * XXX This is the simplest implementation I could think of at the
        * time.  It would be better to keep track of what's writeable and
        * readable separately, but I wanted something quick instead.
        *
        * Maybe the code looks through adjacent regions in the reference map.
        */
        if (new_len > 0) {
            status = plat_mprotect(new_start, new_len, PROT_READ|PROT_WRITE);   /* modified by zhuang Apr 22 */
            // This should never fail
            plat_assert(!status);
        }
    }

    return (ret);
}

void
plat_mem_debug::bounds(plat_mem_debug_region **prev_ptr,
                       plat_mem_debug_region **equals_ptr,
                       plat_mem_debug_region **next_ptr,
                       plat_mem_debug::map_t &map, void *ptr) {
    map_t::iterator prev;
    map_t::iterator next;
    map_t::iterator equals;

    bounds(prev_ptr ? &prev : NULL,
           equals_ptr ? &equals : NULL,
           next_ptr ? &next : NULL,
           map, ptr);

    if (prev_ptr) {
        *prev_ptr = (prev == map.end()) ? NULL : prev->second; /* modified by zhuang Apr 21 */
    }
    if (equals_ptr) {
        *equals_ptr = (equals == map.end()) ? NULL : equals->second; /* modified by zhuang Apr 21 */
    }
    if (next_ptr) {
        *next_ptr = (next == map.end()) ? NULL : next->second; /* modified by zhuang Apr 21 */
    }
}

void
plat_mem_debug::bounds(plat_mem_debug::map_t::iterator *prev_ptr,
                       plat_mem_debug::map_t::iterator *equals_ptr,
                       plat_mem_debug::map_t::iterator *next_ptr,
                       plat_mem_debug::map_t &map, void *ptr) {
    map_t::iterator next;
    map_t::iterator equals;
    map_t::iterator prev;


    next = map.upper_bound(ptr);
    equals = map.lower_bound(ptr);
    prev = (equals != map.end() ? equals : next);
    if (prev == map.begin()) {
        prev = map.end();
    } else if (!map.empty()) {
        --prev;
    }

    if (prev_ptr) {
        *prev_ptr = prev;
    }
    if (equals_ptr) {
        *equals_ptr = equals;
    }
    if (next_ptr) {
        *next_ptr = next;
    }
}

plat_mem_debug_region::plat_mem_debug_region(void *start, size_t len) :
    m_ptr(start),
    m_len(len),
    m_read_count(0),
    m_write_count(0)
{
}

/* add by zhuang Apr 21 */
plat_mem_debug_region::~plat_mem_debug_region()
{
    plat_mem_debug_ref *ref;
    while (!m_references.empty()) {
        ref = m_references.begin()->second;
        m_references.erase(m_references.begin());
        delete ref;
    }
}

void
plat_mem_debug_region::add_reference(void **ref,
                                     enum plat_mem_debug_ref_type ref_type,
                                     int backtrace_frames,
                                     int skip_frames) {
    plat_mem_debug_ref *reference =
        new plat_mem_debug_ref(ref, ref_type, backtrace_frames, skip_frames);

    try {
        m_references.insert(map_t::value_type(ref, reference));
        switch (ref_type) {
        case PLAT_MEM_DEBUG_READ_WRITE:
            ++m_write_count;
            break;
        case PLAT_MEM_DEBUG_READ_ONLY:
            ++m_read_count;
            break;
        }
    } catch (...) {
        delete reference;
        throw;
    }
}

int
plat_mem_debug_region::remove_reference(void **ref,
                                        enum plat_mem_debug_ref_type ref_type,
                                        int log_cat) {
    int ret = 0;

    map_t::iterator i;
    map_t::iterator last;

    last = m_references.upper_bound(ref);
    for (i = m_references.lower_bound(ref);
         i != last && i->second->get_ref_type() != ref_type; ++i) {
    }
    if (i != last) {
        delete i->second;
        m_references.erase(i);
    } else {
        plat_log_msg(PLAT_LOG_ID_INITIAL, log_cat, PLAT_LOG_LEVEL_WARN,
                     "Remove reference of 0x%p length %lu pointer at %p"
                     " does not exist", m_ptr, (unsigned long)m_len, ref);
    }


    switch (ref_type) {
    case PLAT_MEM_DEBUG_READ_WRITE:
        if (m_write_count > 0) {
            --m_write_count;
        } else {
            plat_log_msg(PLAT_LOG_ID_INITIAL, log_cat, PLAT_LOG_LEVEL_WARN,
                         "Remove reference of 0x%p length %lu pointer at %p no"
                         " read write references", m_ptr,
                         (unsigned long)m_len, ref);
            ret = -EINVAL;
        }
        break;
    case PLAT_MEM_DEBUG_READ_ONLY:
        if (m_read_count > 0) {
            --m_read_count;
        } else {
            plat_log_msg(PLAT_LOG_ID_INITIAL, log_cat, PLAT_LOG_LEVEL_WARN,
                         "Reference of 0x%p length %lu pointer at %p no"
                         " read only references", m_ptr,
                         (unsigned long)m_len, ref);
            ret = -EINVAL;
        }
        break;
    }

    return (ret);
}

void
plat_mem_debug_region::log_references(int log_cat, enum plat_log_level level,
                                      int backtrace_limit) {
    map_t::iterator i;
    int j;
    int depth;
    void **backtrace;

    for (i = m_references.begin(); i != m_references.end(); ++i) {
        plat_mem_debug_ref *reference = i->second;
        std::string backtrace_string;
        std::ostringstream backtrace_stream(backtrace_string);

        backtrace_stream << std::hex;

        depth = PLAT_MIN(reference->get_backtrace_depth(), backtrace_limit);
        backtrace = reference->get_backtrace();
        for (j = 0; j < depth; ++j) {
            if (j) {
                backtrace_stream << ' ';
            }
            backtrace_stream << (long)backtrace[j];
        }
        backtrace_stream << std::ends;
        plat_log_msg(PLAT_LOG_ID_INITIAL, log_cat, level,
                     "Reference of 0x%p length %lu by %p backtrace %s",
                     m_ptr, (unsigned long)m_len, reference->get_reference(),
                     backtrace_string.c_str());
    }
}


/*
 * XXX: Since we don't have dynamically generated code and the backtraces
 * have a finite depth, there should be a small (finite) number of them.  The
 * code should change so that it gets a boost::shared_ptr<backtrace> from
 * a factory.
 */
plat_mem_debug_ref::plat_mem_debug_ref(void **reference,
                                       enum plat_mem_debug_ref_type ref_type,
                                       int backtrace_frames, int skip_frames) :
    m_reference(reference),
    m_ref_type(ref_type)
{
    void *stack_backtrace[backtrace_frames];
    int depth = backtrace(stack_backtrace, backtrace_frames);

    try {
        if (depth > skip_frames + 1) {
            m_backtrace = new void *[depth - skip_frames - 1];
            m_backtrace_depth = depth - skip_frames - 1;
            memcpy(m_backtrace, stack_backtrace + skip_frames + 1,
                   sizeof(m_backtrace[0]) * m_backtrace_depth);
        } else {
            m_backtrace_depth = 0;
            m_backtrace = NULL;
        }
    } catch (...) {
        throw;
    }
}

plat_mem_debug_ref::~plat_mem_debug_ref() {
    delete[] m_backtrace;
}

static void *
round_up(void *ptr, int page_size) {
    return ((void *)(((long)ptr + page_size - 1) & ~(page_size - 1)));
}

static void *
round_down(void *ptr, int page_size) {
    return ((void *)(((long)ptr & ~(page_size - 1))));
}

/*add by zhuang Apr 22*/
void
plat_mem_debug_get_stats(struct plat_mem_debug *debug,
                         struct plat_mem_debug_stats *stats)
{
    try {
        debug->get_stats(stats);
    } catch (...) {
    }
}

/*add by zhuang Apr 22*/
bool
plat_mem_debug::check_referenced(void *start, size_t len)
{
    bool ret;
    plat_mem_debug_region *prev;
    plat_mem_debug_region *equals;
    plat_mem_debug_region *next;

    void *end = reinterpret_cast<char *>(start) + len;

    plat_mutex_lock(&m_lock);
    try {
        bounds(&prev, &equals, &next, m_used, start);
        ret = (equals && end == equals->get_end() && start == equals->get_start());
    } catch (...) {
        plat_mutex_unlock(&m_lock);
        throw;
    }
    plat_mutex_unlock(&m_lock);

    return (ret);
}

/*add by zhuang Apr 22*/
int
plat_mem_debug_check_referenced(struct plat_mem_debug *debug,
                                     void *start, size_t len)
{
    return (debug->check_referenced(start,len));
}

/*add by zhuang Apr 22*/
int
plat_mem_debug_check_free(struct plat_mem_debug *debug,
                               void *start, size_t len)
{
    return (debug->check_free(start,len));
}

void plat_mem_debug_config_init(struct plat_mem_debug_config *config)
{
    config->backtrace_depth = DEFAULT_BACKTRACE_DEPTH;
    config->subobject = PLAT_MEM_SUBOBJECT_DENY;
    config->log_category = PLAT_LOG_CAT_PLATFORM_MEM_DEBUG;
}

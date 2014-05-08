/*
 * File:   sdf/platform/alloc_stack.h
 * Author: drew
 *
 * Created on March 27, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: alloc_stack.c 10527 2009-12-12 01:55:08Z drew $
 */
#ifdef VALGRIND
#include "valgrind/valgrind.h"
#endif

#include "platform/alloc_stack.h"
#include "platform/errno.h"
#include "platform/logging.h"
#include "platform/mman.h"
#include "platform/once.h"
#include "platform/stdlib.h"
#include "platform/unistd.h"

PLAT_LOG_SUBCAT_LOCAL(LOG_CAT, PLAT_LOG_CAT_PLATFORM_ALLOC, "stack");

static int pagesize = 0;

PLAT_ONCE(static, init_pagesize);
PLAT_ONCE_IMPL(static, init_pagesize, pagesize = getpagesize());

enum {
    STACK_MAGIC = 0x73746163
};

struct stack_control {
    /** @brief STACK_MAGIC */
    plat_magic_t magic;

    /** @brief Pointer to pass to malloc for free */
    void *malloc_ptr;

#ifdef VALGRIND
    /** @brief Id for valgrind (default when run normally) */
    unsigned valgrind_id;
#endif

    /** @brief Is this stack protected?  Huge page stacks are not */
    int is_protected;
};

void *
plat_alloc_stack(int len) {
    void *malloc_ptr;
    void *low_page;
    void *ret;
    void *end = NULL;
    struct stack_control *stack_control;
    int status;

    init_pagesize_once();
    /*
     * XXX: The valgrind docs indicate that it does not
     * like overlapping blocks where a user provided memory
     * allocator is using a pool provided by malloc.
     *
     * If this is an issue with stacks, the plat_malloc call should
     * become a private mmap of /dev/zero of the appropriate page count
     * with the corresponding plat_free an munmap.
     */

    /*
     * Since malloc does not guarantee page alignment, we need to
     * allocate a byte less than page size to have enough extra
     * to align the first page.  Just punt on that and pull two
     * pages.
     *
     * XXX: Do we want to map /dev/zero instead? That would work even
     * where plat_malloc or malloc is returning huge pages.
     */
    malloc_ptr = plat_malloc(2 * pagesize + len);
    if (malloc_ptr) {
        low_page = (void *)(((long)malloc_ptr + pagesize - 1) &
                            ~((long)pagesize - 1));
        ret = (char *)low_page + pagesize;
        stack_control = ((struct stack_control *)ret) - 1;

        stack_control->magic.integer = STACK_MAGIC;
        stack_control->malloc_ptr = malloc_ptr;
        end = (char *)ret + len;
#ifdef VALGRIND
        stack_control->valgrind_id = VALGRIND_STACK_REGISTER(ret, end);
#endif
        stack_control->is_protected = 1;

        /* Protection doesn't work for stacks in huge pages */
        status = plat_mprotect(low_page, (size_t)pagesize, PROT_READ);
        if (status) {
            plat_log_msg(20902, LOG_CAT, 
                         PLAT_LOG_LEVEL_DIAGNOSTIC,
                         "can't mprotect %p len %lu: %d", low_page,
                         (unsigned long) pagesize, plat_errno);
#if 1
            stack_control->is_protected = 0;
#else
            plat_free(malloc_ptr);
            ret = NULL;
#endif
        }
    } else {
        ret = NULL;
    }

    if (ret) {
        plat_log_msg(20903, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                     "alloc stack red %p data %p to %p",
                     low_page, ret, end);
    }

    return (ret);
}

void
plat_free_stack(void *stack) {
    void *low_page;
    struct stack_control *stack_control;
    int status;

    stack_control = ((struct stack_control *)stack) - 1;
    plat_assert(stack_control->magic.integer ==  STACK_MAGIC);
    low_page = (char *)stack - pagesize;

    plat_log_msg(20904, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "free stack red %p data %p",
                 low_page, stack);

#ifdef VALGRIND
    VALGRIND_STACK_DEREGISTER(stack_control->valgrind_id);
#endif

    if (stack_control->is_protected) {
        status = plat_mprotect(low_page, (size_t)pagesize,
                               PROT_READ|PROT_WRITE);
        if (status) {
            plat_log_msg(20902, LOG_CAT, PLAT_LOG_LEVEL_ERROR,
                         "can't mprotect %p len %lu: %d", low_page,
                         (unsigned long) pagesize, plat_errno);
            plat_assert_always(0);
        }
    }
    stack_control->magic.integer = 0;
    plat_free(stack_control->malloc_ptr);
}

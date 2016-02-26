/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

#ifndef PLATFORM_ONCE_H
#define PLATFORM_ONCE_H 1

/*
 * File:   sdf/platform/once.h
 * Author: drew
 *
 * Created on March 17, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: once.h 641 2008-03-18 17:58:58Z drew $
 */

#include "platform/defs.h"
#include "platform/mutex.h"

#define PLAT_ONCE_INITIALIZER { 0, PLAT_MUTEX_INITIALIZER }

/**
 * @brief Do something exactly once
 *
 * @code
 * #define FOO(name) \
 *     extern int foo_ ## name ## _log_cat_val;                                \
 *                                                                             \
 *     PLAT_ONCE(, foo_ ## name);                                              \
 *                                                                             \
 *     __inline__ foo_ ## name ## _log_cat(void) {                             \
 *         foo ## _once();                                                     \
 *         return (foo_ ## name ## _log_cat_val;                               \
 *     }                                                                       \
 *                                                                             \
 *     foo_ ## name ## _magic(int arg1) {                                      \
 *         plat_log_msg(__PLAT_LOG_ID_INITIAL, foo_ ## name ## _log_cat(),     \
 *                      "magic %d", arg1);                                     \
 *         foo_magic_helper(#name, arg1);                                      \
 *     }
 *
 * #define FOO_IMPL(name)                                                      \
 *     int foo_ ## name ## _log_cat_val;                                       \
 *                                                                             \
 *     PLAT_ONCE_IMPL(, foo_ ## name,  foo ## name ##_log_cat =                \
 *         plat_log_add_subcategory(FOO, #name));
 *
 * @endcode
 *
 * @param scope <IN> scope string, may be empty
 * @param name <IN> name within scope that's unique
 * @param init <IN> lambda function executed once
 */
#define PLAT_ONCE(scope, name)                                                 \
    __BEGIN_DECLS                                                              \
                                                                               \
    typedef struct {                                                           \
        int initialized;                                                       \
        plat_mutex_t initialized_lock;                                         \
    } name ## _once_t;                                                         \
                                                                               \
    scope name ## _once_t name ## _once_state;                                 \
    scope void name ## _once_init();                                           \
                                                                               \
    static __inline__ void                                                     \
    name ## _once() {                                                          \
        if (!name ## _once_state.initialized) {                                \
            name ## _once_init();                                              \
        }                                                                      \
    }                                                                          \
                                                                               \
    __END_DECLS


#define PLAT_ONCE_IMPL(scope, name, init)                                      \
    scope name ## _once_t name ## _once_state = PLAT_ONCE_INITIALIZER;         \
                                                                               \
    scope void                                                                 \
    name ## _once_init() {                                                     \
        plat_mutex_lock(&name ## _once_state.initialized_lock);                \
        if (!name ## _once_state.initialized) {                                \
            init;                                                              \
            name ## _once_state.initialized = 1;                               \
        }                                                                      \
        plat_mutex_unlock(&name ## _once_state.initialized_lock);              \
    }                                                                          \

#endif /* ndef PLATFORM_ONCE_H */

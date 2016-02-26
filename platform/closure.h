/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

#ifndef PLATFORM_CLOSURE_H
#define PLATFORM_CLOSURE_H 1
#ifdef __cplusplus
extern "C" {
#endif


/*
 * File:   sdf/platform/closure.h
 * Author: drew
 *
 * Created on January 24, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: closure.h 13382 2010-05-03 06:47:44Z drew $
 */

/**
 * Closure abstractions for event driven programming and other
 * callbacks, notably cross-thread asynchronous calls with user defined
 * sets that are mutually non-reentrant.  This can provide implicit locking
 * behavior, inter-thread communication that looks like function calls,
 * and facilitates subssystem processor affinity.
 *
 * Closures are created with an execution context, a function pointer,
 * and its environment.  A closure bound to a one-at-a-time execution context
 * will never be prempted by others in the same context, with the switch
 * occurring only on calls into other functions with a closure interface.
 *
 * For high IO workloads this can be superior to cooperatively scheduled
 * threads because the "stack" is dynamically allocated in discontiguous
 * pieces and state is limited so there's less cache polution.
 *
 * This is less an issue now that we've moved beyond 32 bits, where a thread
 * per connection (potentially 10K or 100K) makes running out of addresses
 * space possible.
 *
 * Closures make call graphs that aren't linear (issue three meta-data write
 * requests in parallel, start three data writes when two of the three
 * complete, respond to the user when two of those complete, and cleanup the
 * whole mess when the reference count hits zero) much simpler to implement.
 *
 * Some problems just smell more like events.
 *
 * Functions API contracts shall be that when called with a closure not bound
 * to the synchronous context #PLAT_CLOSURE_SCHEDULER_SYNCHRONOUS the closure
 * will never be applied until after the function has returned and call stack
 * unwound.
 *
 * Where a function would be useful in an asynchronous context,
 * a #plat_scheduler * argument may be provided which is the #context
 * parameter passed to the previous closure's function and NULL in the initial
 * case.
 */

#include <stddef.h>

#include "platform/assert.h"
#include "platform/attr.h"
#include "platform/logging.h"
#include "platform/defs.h"
#include "platform/types.h"

/** @brief Initialize any closure to null.  */
#define PLAT_CLOSURE_INITIALIZER { { NULL, NULL }, NULL }

enum closure_scheduler_val {
    PLAT_CLOSURE_SYNCHRONOUS_VAL = 1,
    PLAT_CLOSURE_AT_CREATE_VAL,
    PLAT_CLOSURE_ANY_VAL,
    PLAT_CLOSURE_ANY_OR_SYNCHRONOUS_VAL
};

/**
 * Caller's address for diagnostic logging. We probably want to build a deeper
 * call graph but that requires trapping signals.  This will be wrong for
 * out-of-line calls.
 */
#define PLAT_CLOSURE_RETADDR() __builtin_return_address(0)

/** @brief Closure being used as an abstraction with synchronous caller. */
#define PLAT_CLOSURE_SCHEDULER_SYNCHRONOUS ((plat_closure_scheduler_t *)       \
                                            PLAT_CLOSURE_SYNCHRONOUS_VAL)

/** @brief Use scheduler specified when the closure was created */
#define PLAT_CLOSURE_SCHEDULER_AT_CREATE ((plat_closure_scheduler_t *)         \
                                          PLAT_CLOSURE_AT_CREATE_VAL)

/** @brief Use any convienient scheduler */
#define PLAT_CLOSURE_SCHEDULER_ANY ((plat_closure_scheduler_t *)               \
                                    PLAT_CLOSURE_ANY_VAL)

/** @brief No restriction on synchronous/asynchronous application */
#define PLAT_CLOSURE_SCHEDULER_ANY_OR_SYNCHRONOUS ((plat_closure_scheduler_t *)\
                                                   PLAT_CLOSURE_ANY_VAL)

/*
 * Initial _apply implementation just does an immediate call; once we
 * have the execution context goo nailed down we'll do something
 * different
 */

/**
 * Define name_t as a closure which takes no elements.
 *
 * Functions defined:
 *
 * name_t name_create(plat_closure_scheduler_t *context, void(*fn)(), void *env)
 * int name_is_null(name_t *closure)
 * void name_apply(name_t *closure);
 *
 * @param <IN> name name
 */

/*
 * apply takes a pointer to the closure so that it can update statistics
 * on usage.
 */
#define PLAT_CLOSURE(name)                                                     \
    typedef void (*name ## _fn_t)                                              \
        (plat_closure_scheduler_t *context, void *env);                        \
                                                                               \
    typedef struct name ## _args  {                                            \
    } name ## _args_t;                                                         \
                                                                               \
    __BEGIN_DECLS                                                              \
    static __inline__ void                                                     \
    name ## _init_args(name ## _args_t *args) {                                \
    }                                                                          \
                                                                               \
    PLAT_CLOSURE_COMMON(name);                                                 \
                                                                               \
    static __inline__ void                                                     \
    name ## _activation_do(plat_closure_scheduler_t *context,                  \
                           plat_closure_activation_base_t *base) {             \
        name ## _activation_t *activation = (name ## _activation_t *)base;     \
                                                                               \
        plat_log_msg(20906,                                                    \
                     PLAT_LOG_CAT_PLATFORM_CLOSURE_ ## name,                   \
                     PLAT_LOG_LEVEL_TRACE_LOW,                                 \
                     "do closure type %s bound to fn %p env %p",               \
                     #name, (activation)->fn, base->env);                      \
                                                                               \
        (*activation->fn)(context, base->env);                                 \
    }                                                                          \
                                                                               \
    static __inline__ void                                                     \
    name ## _apply(name ## _t *closure) {                                      \
        plat_closure_apply(name, closure);                                     \
    }                                                                          \
    __END_DECLS

/**
 * Define name_ts as a closure which which takes one arg
 *
 * name_t name_create(plat_closure_scheduler_t *context,
 *                    void(*fn)(arg1_t), void *env)
 * int name_is_null(name_t *closure)
 * void name_apply(name_t *closure, arg1_t);
 *
 * @param <IN> name name
 * @param <IN> arg1_t first arg type
 * @param <IN> arg1_name first arg name in related definitions
 */
#define PLAT_CLOSURE1(name, arg1_t, arg1_name)                                 \
    typedef void (*name ## _fn_t)                                              \
        (plat_closure_scheduler_t *context, void *env, arg1_t arg1);           \
                                                                               \
    typedef struct name ## _args  {                                            \
        arg1_t arg1_name;                                                      \
    } name ## _args_t;                                                         \
                                                                               \
    __BEGIN_DECLS                                                              \
    static __inline__ void                                                     \
    name ## _init_args(name ## _args_t *args, arg1_t arg1_name) {              \
        args->arg1_name  = arg1_name;                                          \
    }                                                                          \
                                                                               \
    PLAT_CLOSURE_COMMON(name);                                                 \
                                                                               \
    static __inline__ void                                                     \
    name ## _activation_do(plat_closure_scheduler_t *context,                  \
                           plat_closure_activation_base_t *base) {             \
        name ## _activation_t *activation = (name ## _activation_t *)base;     \
                                                                               \
        plat_log_msg(20906,                                                    \
                     PLAT_LOG_CAT_PLATFORM_CLOSURE_ ## name,                   \
                     PLAT_LOG_LEVEL_TRACE_LOW,                                 \
                     "do closure type %s bound to fn %p env %p",               \
                     #name, (activation)->fn, base->env);                      \
                                                                               \
        (*activation->fn)(context, base->env, activation->args.arg1_name);     \
    }                                                                          \
                                                                               \
    static __inline__ void                                                     \
    name ## _apply(name ## _t *closure, arg1_t arg1_name) {                    \
        plat_closure_apply(name, closure, arg1_name);                          \
    }                                                                          \
    __END_DECLS


/**
 * Define name_t as a closure which takes two args
 *
 * name_t name_create(plat_closure_scheduler_t *context,
 *                    void(*fn)(arg1_t, arg2_t), void *env)
 * int name_is_null(name_t *closure)
 * void name_apply(name_t *closure, arg1_t, arg2_t);
 *
 * @param <IN> name name
 * @param <IN> arg1_t first arg type
 * @param <IN> arg1_name first arg name in related definitions
 * @param <IN> arg2_t second arg type
 * @param <IN> arg2_name second arg name in related definitions
 */
#define PLAT_CLOSURE2(name, arg1_t, arg1_name, arg2_t, arg2_name)              \
    typedef void (*name ## _fn_t)                                              \
        (plat_closure_scheduler_t *context, void *env,                         \
         arg1_t arg1_name, arg2_t arg2_name);                                  \
                                                                               \
    typedef struct name ## _args  {                                            \
        arg1_t arg1_name;                                                      \
        arg2_t arg2_name;                                                      \
    } name ## _args_t;                                                         \
                                                                               \
    __BEGIN_DECLS                                                              \
    static __inline__ void                                                     \
    name ## _init_args(name ## _args_t *args, arg1_t arg1_name,                \
                       arg2_t arg2_name) {                                     \
        args->arg1_name = arg1_name;                                           \
        args->arg2_name = arg2_name;                                           \
    }                                                                          \
                                                                               \
    PLAT_CLOSURE_COMMON(name);                                                 \
                                                                               \
    static __inline__ void                                                     \
    name ## _activation_do(plat_closure_scheduler_t *context,                  \
                           plat_closure_activation_base_t *base) {             \
        name ## _activation_t *activation = (name ## _activation_t *)base;     \
                                                                               \
        plat_log_msg(20906,                                                    \
                     PLAT_LOG_CAT_PLATFORM_CLOSURE_ ## name,                   \
                     PLAT_LOG_LEVEL_TRACE_LOW,                                 \
                     "do closure type %s bound to fn %p env %p",               \
                     #name, (activation)->fn, base->env);                      \
                                                                               \
        (*activation->fn)(context, base->env, activation->args.arg1_name,      \
                          activation->args.arg2_name);                         \
    }                                                                          \
                                                                               \
    static __inline__ void                                                     \
    name ## _apply(name ## _t *closure, arg1_t arg1_name, arg2_t arg2_name) {  \
        plat_closure_apply(name, closure, arg1_name, arg2_name);               \
    }                                                                          \
    __END_DECLS

/**
 * Define name_t as a closure which takes three args
 *
 * name_t name_create(plat_closure_scheduler_t *context,
 *                    void(*fn)(arg1_t, arg2_t, arg3_t), void *env)
 * int name_is_null(name_t *closure)
 * void name_apply(name_t *closure, arg1_t, arg2_t, arg3_t);
 *
 * @param <IN> name name
 * @param <IN> arg1_t first arg type
 * @param <IN> arg1_name first arg name in related definitions
 * @param <IN> arg2_t second arg type
 * @param <IN> arg2_name second arg name in related definitions
 * @param <IN> arg3_t third arg type
 * @param <IN> arg3_name third arg name in related definitions
 */
#define PLAT_CLOSURE3(name, arg1_t, arg1_name, arg2_t, arg2_name,              \
                      arg3_t, arg3_name)                                       \
    typedef void (*name ## _fn_t)                                              \
        (plat_closure_scheduler_t *context, void *env,                         \
         arg1_t arg1_name, arg2_t arg2_name, arg3_t arg3_name);                \
                                                                               \
    typedef struct name ## _args  {                                            \
        arg1_t arg1_name;                                                      \
        arg2_t arg2_name;                                                      \
        arg3_t arg3_name;                                                      \
    } name ## _args_t;                                                         \
                                                                               \
    __BEGIN_DECLS                                                              \
    static __inline__ void                                                     \
    name ## _init_args(name ## _args_t *args, arg1_t arg1_name,                \
                       arg2_t arg2_name, arg3_t arg3_name) {                   \
        args->arg1_name = arg1_name;                                           \
        args->arg2_name = arg2_name;                                           \
        args->arg3_name = arg3_name;                                           \
    }                                                                          \
                                                                               \
    PLAT_CLOSURE_COMMON(name);                                                 \
                                                                               \
    static __inline__ void                                                     \
    name ## _activation_do(plat_closure_scheduler_t *context,                  \
                           plat_closure_activation_base_t *base) {             \
        name ## _activation_t *activation = (name ## _activation_t *)base;     \
                                                                               \
        plat_log_msg(20906,                                                    \
                     PLAT_LOG_CAT_PLATFORM_CLOSURE_ ## name,                   \
                     PLAT_LOG_LEVEL_TRACE_LOW,                                 \
                     "do closure type %s bound to fn %p env %p",               \
                     #name, (activation)->fn, base->env);                      \
                                                                               \
        (*activation->fn)(context, base->env, activation->args.arg1_name,      \
                          activation->args.arg2_name,                          \
                          activation->args.arg3_name);                         \
    }                                                                          \
                                                                               \
    static __inline__ void                                                     \
    name ## _apply(name ## _t *closure, arg1_t arg1_name, arg2_t arg2_name,    \
                   arg3_t arg3_name) {                                         \
        plat_closure_apply(name, closure, arg1_name, arg2_name, arg3_name);    \
    }                                                                          \
    __END_DECLS

/**
 * Define name_t as a closure which takes four args
 *
 * name_t name_create(plat_closure_scheduler_t *context,
 *                    void(*fn)(arg1_t, arg2_t, arg3_t, arg4_t), void *env)
 * int name_is_null(name_t *closure)
 * void name_apply(name_t *closure, arg1_t, arg2_t, arg3_t, arg4_t);
 *
 * @param <IN> name name
 * @param <IN> arg1_t first arg type
 * @param <IN> arg1_name first arg name in related definitions
 * @param <IN> arg2_t second arg type
 * @param <IN> arg2_name second arg name in related definitions
 * @param <IN> arg3_t third arg type
 * @param <IN> arg3_name third arg name in related definitions
 * @param <IN> arg4_t fourth arg type
 * @param <IN> arg4_name fourth arg name in related definitions
 */
#define PLAT_CLOSURE4(name, arg1_t, arg1_name, arg2_t, arg2_name,              \
                      arg3_t, arg3_name, arg4_t, arg4_name)                    \
    typedef void (*name ## _fn_t)                                              \
        (plat_closure_scheduler_t *context, void *env,                         \
         arg1_t arg1_name, arg2_t arg2_name, arg3_t arg3_name,                 \
         arg4_t arg4_name);                                                    \
                                                                               \
    typedef struct name ## _args  {                                            \
        arg1_t arg1_name;                                                      \
        arg2_t arg2_name;                                                      \
        arg3_t arg3_name;                                                      \
        arg4_t arg4_name;                                                      \
    } name ## _args_t;                                                         \
                                                                               \
    __BEGIN_DECLS                                                              \
    static __inline__ void                                                     \
    name ## _init_args(name ## _args_t *args, arg1_t arg1_name,                \
                       arg2_t arg2_name, arg3_t arg3_name,                     \
                       arg4_t arg4_name) {                                     \
        args->arg1_name = arg1_name;                                           \
        args->arg2_name = arg2_name;                                           \
        args->arg3_name = arg3_name;                                           \
        args->arg4_name = arg4_name;                                           \
    }                                                                          \
                                                                               \
    PLAT_CLOSURE_COMMON(name);                                                 \
                                                                               \
    static __inline__ void                                                     \
    name ## _activation_do(plat_closure_scheduler_t *context,                  \
                           plat_closure_activation_base_t *base) {             \
        name ## _activation_t *activation = (name ## _activation_t *)base;     \
                                                                               \
        plat_log_msg(20906,                                                    \
                     PLAT_LOG_CAT_PLATFORM_CLOSURE_ ## name,                   \
                     PLAT_LOG_LEVEL_TRACE_LOW,                                 \
                     "do closure type %s bound to fn %p env %p",               \
                     #name, (activation)->fn, base->env);                      \
                                                                               \
        (*activation->fn)(context, base->env, activation->args.arg1_name,      \
                          activation->args.arg2_name,                          \
                          activation->args.arg3_name,                          \
                          activation->args.arg4_name);                         \
    }                                                                          \
                                                                               \
    static __inline__ void                                                     \
    name ## _apply(name ## _t *closure, arg1_t arg1_name, arg2_t arg2_name,    \
                   arg3_t arg3_name, arg4_t arg4_name) {                       \
        plat_closure_apply(name, closure, arg1_name, arg2_name, arg3_name,     \
                           arg4_name);                                         \
    }                                                                          \
    __END_DECLS

/**
 * Define name_t as a closure which takes five args
 *
 * name_t name_create(plat_closure_scheduler_t *context,
 *                    void(*fn)(arg1_t, arg2_t, arg3_t, arg4_t, arg5_t),
 *                    void *env)
 * int name_is_null(name_t *closure)
 * void name_apply(name_t *closure, arg1_t, arg2_t, arg3_t, arg4_t, arg5_t)
 *
 * @param <IN> name name
 * @param <IN> arg1_t first arg type
 * @param <IN> arg1_name first arg name in related definitions
 * @param <IN> arg2_t second arg type
 * @param <IN> arg2_name second arg name in related definitions
 * @param <IN> arg3_t third arg type
 * @param <IN> arg3_name third arg name in related definitions
 * @param <IN> arg4_t fourth arg type
 * @param <IN> arg4_name fourth arg name in related definitions
 * @param <IN> arg5_t fifth arg type
 * @param <IN> arg5_name fifth arg name in related definitions
 */
#define PLAT_CLOSURE5(name, arg1_t, arg1_name, arg2_t, arg2_name,              \
                      arg3_t, arg3_name, arg4_t, arg4_name, arg5_t, arg5_name) \
    typedef void (*name ## _fn_t)                                              \
        (plat_closure_scheduler_t *context, void *env,                         \
         arg1_t arg1_name, arg2_t arg2_name, arg3_t arg3_name,                 \
         arg4_t arg4_name, arg5_t arg5_name);                                  \
                                                                               \
    typedef struct name ## _args  {                                            \
        arg1_t arg1_name;                                                      \
        arg2_t arg2_name;                                                      \
        arg3_t arg3_name;                                                      \
        arg4_t arg4_name;                                                      \
        arg5_t arg5_name;                                                      \
    } name ## _args_t;                                                         \
                                                                               \
    __BEGIN_DECLS                                                              \
    static __inline__ void                                                     \
    name ## _init_args(name ## _args_t *args, arg1_t arg1_name,                \
                       arg2_t arg2_name, arg3_t arg3_name,                     \
                       arg4_t arg4_name, arg5_t arg5_name) {                   \
        args->arg1_name = arg1_name;                                           \
        args->arg2_name = arg2_name;                                           \
        args->arg3_name = arg3_name;                                           \
        args->arg4_name = arg4_name;                                           \
        args->arg5_name = arg5_name;                                           \
    }                                                                          \
                                                                               \
    PLAT_CLOSURE_COMMON(name);                                                 \
                                                                               \
    static __inline__ void                                                     \
    name ## _activation_do(plat_closure_scheduler_t *context,                  \
                           plat_closure_activation_base_t *base) {             \
        name ## _activation_t *activation = (name ## _activation_t *)base;     \
                                                                               \
        plat_log_msg(20906,                                                    \
                     PLAT_LOG_CAT_PLATFORM_CLOSURE_ ## name,                   \
                     PLAT_LOG_LEVEL_TRACE_LOW,                                 \
                     "do closure type %s bound to fn %p env %p",               \
                     #name, (activation)->fn, base->env);                      \
                                                                               \
        (*activation->fn)(context, base->env, activation->args.arg1_name,      \
                          activation->args.arg2_name,                          \
                          activation->args.arg3_name,                          \
                          activation->args.arg4_name,                          \
                          activation->args.arg5_name);                         \
    }                                                                          \
                                                                               \
    static __inline__ void                                                     \
    name ## _apply(name ## _t *closure, arg1_t arg1_name, arg2_t arg2_name,    \
                   arg3_t arg3_name, arg4_t arg4_name, arg5_t arg5_name) {     \
        plat_closure_apply(name, closure, arg1_name, arg2_name, arg3_name,     \
                           arg4_name, arg5_name);                              \
    }                                                                          \
    __END_DECLS

/**
 * Define name_t as a closure which takes six args
 *
 * name_t name_create(plat_closure_scheduler_t *context,
 *                    void(*fn)(arg1_t, arg2_t, arg3_t, arg4_t,
 *                    arg5_t, arg6_t)
 * int name_is_null(name_t *closure)
 * void name_apply(name_t *closure, arg1_t, arg2_t, arg3_t, arg4_t,
 *                 arg5_t, arg6_t)
 *
 * @param <IN> name name
 * @param <IN> arg1_t first arg type
 * @param <IN> arg1_name first arg name in related definitions
 * @param <IN> arg2_t second arg type
 * @param <IN> arg2_name second arg name in related definitions
 * @param <IN> arg3_t second arg type
 * @param <IN> arg3_name second arg name in related definitions
 * @param <IN> arg4_t second arg type
 * @param <IN> arg4_name second arg name in related definitions
 * @param <IN> arg5_t second arg type
 * @param <IN> arg5_name second arg name in related definitions
 * @param <IN> arg6_t second arg type
 * @param <IN> arg6_name second arg name in related definitions
 */
#define PLAT_CLOSURE6(name, arg1_t, arg1_name, arg2_t, arg2_name,              \
                      arg3_t, arg3_name, arg4_t, arg4_name,                    \
                      arg5_t, arg5_name, arg6_t, arg6_name)                    \
    typedef void (*name ## _fn_t)                                              \
        (plat_closure_scheduler_t *context, void *env,                         \
         arg1_t arg1_name, arg2_t arg2_name, arg3_t arg3_name,                 \
         arg4_t arg4_name, arg5_t arg5_name, arg6_t arg6_name);                \
                                                                               \
    typedef struct name ## _args  {                                            \
        arg1_t arg1_name;                                                      \
        arg2_t arg2_name;                                                      \
        arg3_t arg3_name;                                                      \
        arg4_t arg4_name;                                                      \
        arg5_t arg5_name;                                                      \
        arg6_t arg6_name;                                                      \
    } name ## _args_t;                                                         \
                                                                               \
    __BEGIN_DECLS                                                              \
    static __inline__ void                                                     \
    name ## _init_args(name ## _args_t *args, arg1_t arg1_name,                \
                       arg2_t arg2_name, arg3_t arg3_name,                     \
                       arg4_t arg4_name, arg5_t arg5_name,                     \
                       arg6_t arg6_name) {                                     \
        args->arg1_name = arg1_name;                                           \
        args->arg2_name = arg2_name;                                           \
        args->arg3_name = arg3_name;                                           \
        args->arg4_name = arg4_name;                                           \
        args->arg5_name = arg5_name;                                           \
        args->arg6_name = arg6_name;                                           \
    }                                                                          \
                                                                               \
    PLAT_CLOSURE_COMMON(name);                                                 \
                                                                               \
    static __inline__ void                                                     \
    name ## _activation_do(plat_closure_scheduler_t *context,                  \
                           plat_closure_activation_base_t *base) {             \
        name ## _activation_t *activation = (name ## _activation_t *)base;     \
                                                                               \
        plat_log_msg(20906,                                                    \
                     PLAT_LOG_CAT_PLATFORM_CLOSURE_ ## name,                   \
                     PLAT_LOG_LEVEL_TRACE_LOW,                                 \
                     "do closure type %s bound to fn %p env %p",               \
                     #name, (activation)->fn, base->env);                      \
                                                                               \
        (*activation->fn)(context, base->env, activation->args.arg1_name,      \
                          activation->args.arg2_name,                          \
                          activation->args.arg3_name,                          \
                          activation->args.arg4_name,                          \
                          activation->args.arg5_name,                          \
                          activation->args.arg6_name);                         \
    }                                                                          \
                                                                               \
    static __inline__ void                                                     \
    name ## _apply(name ## _t *closure, arg1_t arg1_name, arg2_t arg2_name,    \
                   arg3_t arg3_name, arg4_t arg4_name,                         \
                   arg5_t arg5_name, arg6_t arg6_name) {                       \
        plat_closure_apply(name, closure, arg1_name, arg2_name, arg3_name,     \
                           arg4_name, arg5_name, arg6_name);                   \
    }                                                                          \
    __END_DECLS


/**
 * Define name_t as a closure which takes nine args
 *
 * name_t name_create(plat_closure_scheduler_t *context,
 *                    void(*fn)(arg1_t, arg2_t, arg3_t, arg4_t,
 *                    arg5_t, arg6_t, arg7_t, arg8_t, arg9_t), void *env)
 * int name_is_null(name_t *closure)
 * void name_apply(name_t *closure, arg1_t, arg2_t, arg3_t, arg4_t,
 *                 arg5_t, arg6_t, arg7_t, arg8_t, arg9_t);
 *
 * @param <IN> name name
 * @param <IN> arg1_t first arg type
 * @param <IN> arg1_name first arg name in related definitions
 * @param <IN> arg2_t second arg type
 * @param <IN> arg2_name second arg name in related definitions
 * @param <IN> arg3_t second arg type
 * @param <IN> arg3_name second arg name in related definitions
 * @param <IN> arg4_t second arg type
 * @param <IN> arg4_name second arg name in related definitions
 * @param <IN> arg5_t second arg type
 * @param <IN> arg5_name second arg name in related definitions
 * @param <IN> arg6_t second arg type
 * @param <IN> arg6_name second arg name in related definitions
 * @param <IN> arg7_t second arg type
 * @param <IN> arg7_name second arg name in related definitions
 * @param <IN> arg8_t second arg type
 * @param <IN> arg8_name second arg name in related definitions
 * @param <IN> arg9_t second arg type
 * @param <IN> arg9_name second arg name in related definitions
 */
#define PLAT_CLOSURE9(name, arg1_t, arg1_name, arg2_t, arg2_name,              \
                      arg3_t, arg3_name, arg4_t, arg4_name,                    \
                      arg5_t, arg5_name, arg6_t, arg6_name,                    \
                      arg7_t, arg7_name, arg8_t, arg8_name,                    \
                      arg9_t, arg9_name)                                       \
    typedef void (*name ## _fn_t)                                              \
        (plat_closure_scheduler_t *context, void *env,                         \
         arg1_t arg1_name, arg2_t arg2_name, arg3_t arg3_name,                 \
         arg4_t arg4_name, arg5_t arg5_name, arg6_t arg6_name,                 \
         arg7_t arg7_name, arg8_t arg8_name, arg9_t arg9_name);                \
                                                                               \
    typedef struct name ## _args  {                                            \
        arg1_t arg1_name;                                                      \
        arg2_t arg2_name;                                                      \
        arg3_t arg3_name;                                                      \
        arg4_t arg4_name;                                                      \
        arg5_t arg5_name;                                                      \
        arg6_t arg6_name;                                                      \
        arg7_t arg7_name;                                                      \
        arg8_t arg8_name;                                                      \
        arg9_t arg9_name;                                                      \
    } name ## _args_t;                                                         \
                                                                               \
    __BEGIN_DECLS                                                              \
    static __inline__ void                                                     \
    name ## _init_args(name ## _args_t *args, arg1_t arg1_name,                \
                       arg2_t arg2_name, arg3_t arg3_name,                     \
                       arg4_t arg4_name, arg5_t arg5_name,                     \
                       arg6_t arg6_name, arg7_t arg7_name,                     \
                       arg8_t arg8_name, arg9_t arg9_name) {                   \
        args->arg1_name = arg1_name;                                           \
        args->arg2_name = arg2_name;                                           \
        args->arg3_name = arg3_name;                                           \
        args->arg4_name = arg4_name;                                           \
        args->arg5_name = arg5_name;                                           \
        args->arg6_name = arg6_name;                                           \
        args->arg7_name = arg7_name;                                           \
        args->arg8_name = arg8_name;                                           \
        args->arg9_name = arg9_name;                                           \
    }                                                                          \
                                                                               \
    PLAT_CLOSURE_COMMON(name);                                                 \
                                                                               \
    static __inline__ void                                                     \
    name ## _activation_do(plat_closure_scheduler_t *context,                  \
                           plat_closure_activation_base_t *base) {             \
        name ## _activation_t *activation = (name ## _activation_t *)base;     \
                                                                               \
        plat_log_msg(20906,                                                    \
                     PLAT_LOG_CAT_PLATFORM_CLOSURE_ ## name,                   \
                     PLAT_LOG_LEVEL_TRACE_LOW,                                 \
                     "do closure type %s bound to fn %p env %p",               \
                     #name, (activation)->fn, base->env);                      \
                                                                               \
        (*activation->fn)(context, base->env, activation->args.arg1_name,      \
                          activation->args.arg2_name,                          \
                          activation->args.arg3_name,                          \
                          activation->args.arg4_name,                          \
                          activation->args.arg5_name,                          \
                          activation->args.arg6_name,                          \
                          activation->args.arg7_name,                          \
                          activation->args.arg8_name,                          \
                          activation->args.arg9_name);                         \
    }                                                                          \
                                                                               \
    static __inline__ void                                                     \
    name ## _apply(name ## _t *closure, arg1_t arg1_name, arg2_t arg2_name,    \
                   arg3_t arg3_name, arg4_t arg4_name,                         \
                   arg5_t arg5_name, arg6_t arg6_name, arg7_t arg7_name,       \
                   arg8_t arg8_name, arg9_t arg9_name) {                       \
        plat_closure_apply(name, closure, arg1_name, arg2_name, arg3_name,     \
                           arg4_name, arg5_name, arg6_name, arg7_name,         \
                           arg8_name, arg9_name);                              \
    }                                                                          \
    __END_DECLS

/*
 * Functions  are static not extern __inline__ since we don't care about
 * setting a break point which fires on all _apply() calls and don't want to
 * provide expanded implementations.
 */
#define PLAT_CLOSURE_COMMON(name)                                              \
    static int PLAT_LOG_CAT_PLATFORM_CLOSURE_ ## name;                         \
                                                                               \
    static void plat_closure_init ## name() __attribute__((constructor));      \
    static void plat_closure_init ## name() {                                  \
        PLAT_LOG_CAT_PLATFORM_CLOSURE_ ##name =                                \
            plat_log_add_subcategory(PLAT_LOG_CAT_PLATFORM_CLOSURE, #name);    \
        plat_assert(PLAT_LOG_CAT_PLATFORM_CLOSURE_ ##name > 0);                \
    }                                                                          \
                                                                               \
    typedef struct {                                                           \
        plat_closure_base_t base;                                              \
        name ## _fn_t fn;                                                      \
    } name ## _t;                                                              \
                                                                               \
    typedef struct name ## _activation {                                       \
        plat_closure_activation_base_t base;                                   \
        name ## _fn_t fn;                                                      \
        name ## _args_t args;                                                  \
    } name ## _activation_t;                                                   \
                                                                               \
    static const name ## _t name ## _null = PLAT_CLOSURE_INITIALIZER;          \
                                                                               \
    __BEGIN_DECLS                                                              \
    static __inline__ name ## _t                                               \
    name ## _create(plat_closure_scheduler_t *context,                         \
                    name ## _fn_t fn, void *env) {                             \
        name ## _t ret = { { context == PLAT_CLOSURE_SCHEDULER_AT_CREATE ?     \
            plat_closure_get_scheduler() : context, env }, fn };               \
        plat_assert(ret.base.context);                                         \
                                                                               \
        plat_log_msg(20907,                                                    \
                     PLAT_LOG_CAT_PLATFORM_CLOSURE_ ## name,                   \
                     PLAT_LOG_LEVEL_TRACE_LOW,                                 \
                     "create closure type %s bound to fn %p env %p from %p",   \
                     #name, fn, env, PLAT_CLOSURE_RETADDR());                  \
                                                                               \
        return (ret);                                                          \
    }                                                                          \
                                                                               \
    static __inline__ name ## _t                                               \
    name ## _create_sync(name ## _fn_t fn, void *env) {                        \
        name ##_t ret = { { PLAT_CLOSURE_SCHEDULER_SYNCHRONOUS, env }, fn };   \
        return (ret);                                                          \
    }                                                                          \
                                                                               \
    static __inline__ int                                                      \
    name ## _is_null(name ## _t *closure) {                                    \
        return (!closure->fn);                                                 \
    }                                                                          \
                                                                               \
    static __inline__ int                                                      \
    name ## _is_sync(name ## _t *closure) {                                    \
        return (closure->base.context == PLAT_CLOSURE_SCHEDULER_SYNCHRONOUS);  \
    }                                                                          \
                                                                               \
    __END_DECLS


typedef struct plat_closure_scheduler plat_closure_scheduler_t;
typedef struct plat_closure_base plat_closure_base_t;
typedef struct plat_closure_activation_base plat_closure_activation_base_t;

typedef void (*plat_activation_do_fn_t)
    (plat_closure_scheduler_t *context, plat_closure_activation_base_t *base);

/**
 * Closure base. Separate so that additional information (statistics, etc.)
 * can be added.  Including void (*fn)(void) might be OK (we can dump the
 * structure without a cast to derived) for debugging and would not hurt
 * type safety.
 */
struct plat_closure_base {
    plat_closure_scheduler_t *context;
    void *env;
};

struct plat_closure_activation_base {
    void *env;
    plat_activation_do_fn_t do_fn;

    /* For debugging */

    /** @brief File in which this was applied */
    const char *apply_file;

    /** @brief Line in which this was applied */
    int apply_line;
};

__BEGIN_DECLS

/* Forward declare inlines for plat_closure_scheduler_shutdown */
static __inline__ plat_closure_activation_base_t *
plat_closure_scheduler_alloc_activation(plat_closure_scheduler_t *context,
                                        size_t size);
static __inline__ void
plat_closure_scheduler_add_activation_helper(plat_closure_scheduler_t *context,
                                             plat_closure_activation_base_t
                                             *base);

static __inline__ plat_closure_scheduler_t *plat_closure_get_scheduler(void);

/**
 * @brief Apply closure
 *
 * @param type <IN> closure type without trailing _t
 * @param closure <IN> closure to apply
 * @param ... <IN> 0 or more additional arguments as appropriate for the
 * given macro
 */
#define plat_closure_apply(type, closure, ...)                                 \
    do {                                                                       \
        /* Name prefix to avoid coverity collision */                          \
        plat_closure_scheduler_t *pca_context;                                 \
                                                                               \
        plat_assert((closure)->fn);                                            \
                                                                               \
        plat_log_msg(20908,                                                    \
                     PLAT_LOG_CAT_PLATFORM_CLOSURE_ ## type,                   \
                     PLAT_LOG_LEVEL_TRACE_LOW,                                 \
                     "apply closure type %s %s bound to fn %p env %p from %p", \
                     #type, #closure, (closure)->fn, (closure)->base.env,      \
                     PLAT_CLOSURE_RETADDR());                                  \
                                                                               \
        if ((closure)->base.context ==                                         \
                   PLAT_CLOSURE_SCHEDULER_SYNCHRONOUS ||                       \
                   (closure)->base.context ==                                  \
                   PLAT_CLOSURE_SCHEDULER_ANY_OR_SYNCHRONOUS) {                \
            (*(closure)->fn)(NULL, (closure)->base.env, ##__VA_ARGS__);        \
        } else if ((closure)->base.context == PLAT_CLOSURE_SCHEDULER_ANY) {    \
            pca_context = plat_closure_get_scheduler();                        \
            plat_assert(pca_context);                                          \
            plat_closure_scheduler_add_activation(pca_context, type,           \
                                                  (closure), ##__VA_ARGS__);   \
        } else {                                                               \
            plat_assert((closure)->base.context);                              \
            plat_closure_scheduler_add_activation((closure)->base.context,     \
                                                  type,                        \
                                                  closure, ##__VA_ARGS__);     \
        }                                                                      \
    } while (0)

/**
 * @brief Chain closure activations together
 *
 * A common usage model is for a user to pass a closure into into an
 * asynchronous interface which in turn gets applied when the last
 * of the lower level asynchronous APIs complete.
 *
 * Run the chained closure immediately in the current context if that's
 * allowed.
 *
 * Another usage model is chain of closures which may be scheduled from
 * a function (like an asynchronous free call to an object with a zero
 * reference count by other operations) or executed from within a running
 * chain of closures (the same asynchronous free logic when the reference
 * count decrements to zero in the handler for a normal IO op).
 *
 * A NULL #context parameter is used for this use case.
 *
 * @param type <IN> closure type passed to PLAT_CLOSURE macro (without _t)
 *
 * @param context <IN> execution context passed in to previous closure
 * function, NULL when #plat_closure_chain is being called directly.
 *
 * @param closure <IN> closure to apply.  It is an error to chain a null closure
 */
#define plat_closure_chain(type, context, closure, ...)                        \
    do {                                                                       \
        plat_assert((closure)->fn);                                            \
        if ((closure)->base.context ==                                         \
                   PLAT_CLOSURE_SCHEDULER_ANY ||                               \
                   (closure)->base.context ==                                  \
                   PLAT_CLOSURE_SCHEDULER_ANY_OR_SYNCHRONOUS ||                \
                   (closure)->base.context == context) {                       \
            (*(closure)->fn)(NULL, (closure)->base.env, ##__VA_ARGS__);        \
        } else {                                                               \
            plat_assert((closure)->base.context !=                             \
                        PLAT_CLOSURE_SCHEDULER_SYNCHRONOUS);                   \
            plat_closure_apply(type, closure, ##__VA_ARGS__);                  \
        }                                                                      \
    } while (0)

/*
 * @brief Internal function to create activation
 *
 * @param type <IN> closure type without trailing _t
 * @param closure <IN> closure to apply
 */
#define plat_closure_scheduler_add_activation(context, type, closure, ...)     \
    do {                                                                       \
        type ## _activation_t *activation = (type ## _activation_t *)          \
            plat_closure_scheduler_alloc_activation((context),                 \
                                                    sizeof (*activation));     \
        plat_assert_always(activation);                                        \
        activation->base.env = (closure)->base.env;                            \
        activation->base.do_fn = &type ## _activation_do;                      \
        activation->base.apply_file = __FILE__;                                \
        activation->base.apply_line = __LINE__;                                \
        activation->fn = (closure)->fn;                                        \
        type ## _init_args(&activation->args, ##__VA_ARGS__);                  \
        plat_closure_scheduler_add_activation_helper((context),                \
                                                     &activation->base);       \
    } while (0)


PLAT_CLOSURE(plat_closure_scheduler_shutdown);

struct plat_closure_scheduler {
    /** @brief Allocate (but do not yet schedule) activation */
    plat_closure_activation_base_t *(*alloc_activation_fn)
        (plat_closure_scheduler_t *self, size_t size);

    /** @brief Schedule allocation for execution in FIFO order */
    void (*add_activation_fn) (plat_closure_scheduler_t *self,
                               plat_closure_activation_base_t *act);

    /** @brief Shutdown asynchronously after all pending closures have fired */
    void (*shutdown_fn)(plat_closure_scheduler_t *self,
                        plat_closure_scheduler_shutdown_t closure);
};

/**
 * @brief Shutdown and free the scheduler
 *
 * @param context <IN> scheduler to shutdown
 * @param closure <IN> invoked when there are no closures remaining to
 * execute, follows normal scheduling conventions.
 */
static __inline__ void
plat_closure_scheduler_shutdown(plat_closure_scheduler_t *context,
                                plat_closure_scheduler_shutdown_t
                                shutdown) {
    (*context->shutdown_fn)(context, shutdown);
}

/* @brief Allocate activation record (for internal use) */
static __inline__ plat_closure_activation_base_t *
plat_closure_scheduler_alloc_activation(plat_closure_scheduler_t *context,
                                        size_t size) {
    return ((*context->alloc_activation_fn)(context, size));
}

/* @brief Allocate activation record (for internal use) */
static __inline__ void
plat_closure_scheduler_add_activation_helper(plat_closure_scheduler_t *context,
                                             plat_closure_activation_base_t
                                             *base) {
    (*context->add_activation_fn)(context, base);
}

/**
 * @brief Get current scheduler
 *
 * Table driven
 */
static __inline__ plat_closure_scheduler_t *
plat_closure_get_scheduler(void) {
    return (plat_attr_closure_scheduler_get());
}

static __inline__ void
plat_closure_scheduler_set(plat_closure_scheduler_t *scheduler) {
    plat_attr_closure_scheduler_set(scheduler);
}

__END_DECLS

#ifdef __cplusplus
}
#endif
#endif /* ndef PLATFORM_CLOSURE_H */

/*
 * File:   utils/sdfkey.h
 * Author: Darryl Ouye
 *
 * Created on August 2, 2008, 1:08 PM
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: sdfkey.h $
 */

#ifndef _SDFKEY_H
#define _SDFKEY_H

#ifdef __cplusplus
extern "C" {
#endif

#include <inttypes.h>
#include "platform/shmem.h"
#include <stdio.h>
  
#if defined(PLAT_NEED_OUT_OF_LINE)
#define PLAT_SDFKEY_C_INLINE
#else
#define PLAT_SDFKEY_C_INLINE PLAT_INLINE
#endif


typedef uint64_t SDF_blocknum_t;
struct _SDF_object_key;
struct _SDF_key;
typedef struct _SDF_key * SDF_key_t;

PLAT_SP_VAR_OPAQUE(char_sp, char);

#ifndef SDFCC_SHMEM_FAKE
// PLAT_SP(SDF_object_key_sp, struct _SDF_object_key);
PLAT_SP(SDF_key_sp, struct _SDF_key);
#else
typedef struct _SDF_key SDF_key_sp;
typedef struct _SDF_key * SDF_key_sp_t;
#define SDF_key_sp_eq(a,b) (a==b)
#define SDF_key_sp_null NULL
#define SDF_key_sp_is_null(a) ((a)==NULL)
#define SDF_key_sp_rref(a, b) (*(a)=b)
#define SDF_key_sp_rwref(a, b) (*(a)=b)
#define SDF_key_sp_rrelease(a)
#define SDF_key_sp_rwrelease(a)

typedef struct _SDF_object_key SDF_object_key_sp;
typedef struct _SDF_object_key * SDF_object_key_sp_t;
#define SDF_object_key_sp_eq(a,b) (a==b)
#define SDF_object_key_sp_null NULL
#define SDF_object_key_sp_is_null(a) ((a)==NULL)
#define SDF_object_key_sp_rref(a, b) (*(a)=b)
#define SDF_object_key_sp_rwref(a, b) (*(a)=b)
#define SDF_object_key_sp_rrelease(a)
#define SDF_object_key_sp_rwrelease(a)

typedef void char_sp;
typedef void * char_sp_t;
#define char_sp_null NULL
#define char_sp_is_null(a) ((a)==NULL)
#define char_sp_rref(a, b) (*a=b)
#define char_sp_rwref(a, b) (*a=b)
#define char_sp_rrelease(a)
#define char_sp_rwrelease(a)
#define char_sp_var_rref(a,b, size) (*a=b)
#define char_sp_var_rwref(a,b, size) (*a=b)
#define char_sp_var_rwrelease(a, size)
#define char_sp_var_rrelease(a, size)
#endif
/*
struct _SDF_object_key {
    uint32_t len;
    char_sp_t name;
};

struct _SDF_key {
    union {
        SDF_blocknum_t block_id;    // For block containers
        SDF_object_key_sp_t object_id; // For object containers
    };
};
*/

#define _SDF_OBJECT_KEY_MAX_LEN 256

struct _SDF_key {
    uint16_t key_len; // not including null terminator
    char key[_SDF_OBJECT_KEY_MAX_LEN];
};

PLAT_SDFKEY_C_INLINE SDF_key_t
Key_getLocalPtr(SDF_key_t *localKey, SDF_key_sp_t key)
{
    return (SDF_key_sp_rwref(localKey, key));
}

PLAT_SDFKEY_C_INLINE void
Key_releaseLocalPtr(SDF_key_t *localKey)
{
    SDF_key_sp_rwrelease(localKey);
}

PLAT_SDFKEY_C_INLINE void
Key_setNull(SDF_key_sp_t key)
{
    key = SDF_key_sp_null;
}

PLAT_SDFKEY_C_INLINE SDF_key_sp_t
Key_createBlockKey(uint64_t block_id)
{
    SDF_key_sp_t key = SDF_key_sp_null;
    struct _SDF_key *localKey = NULL;

#ifndef SDFCC_SHMEM_FAKE
    key = plat_shmem_alloc(SDF_key_sp);
#else
    key = plat_alloc(sizeof(struct _SDF_key));
#endif
    if (SDF_key_sp_is_null(key)) {
        plat_log_msg(20866, PLAT_LOG_CAT_SDF_SHARED, PLAT_LOG_LEVEL_FATAL, "Not enough shared memory, plat_shmem_alloc() failed.");
        plat_assert_always(0);
    }

    SDF_key_sp_rwref(&localKey, key);
    // localKey->block_id = block_id;
    snprintf(localKey->key, _SDF_OBJECT_KEY_MAX_LEN, "%"PRIu64, block_id);
    localKey->key_len = strlen(localKey->key);
    SDF_key_sp_rwrelease(&localKey);
    
    return (key);
}

PLAT_SDFKEY_C_INLINE void
Key_freeBlockKey(SDF_key_sp_t key)
{
#ifndef SDFCC_SHMEM_FAKE
    plat_shmem_free(SDF_key_sp, key);
#else
    plat_free(key);
#endif
}

PLAT_SDFKEY_C_INLINE uint64_t
Key_getBlockId(SDF_key_sp_t key)
{
    struct _SDF_key *localKey = NULL;
    uint64_t ret = 0;

    SDF_key_sp_rwref(&localKey, key);
    // ret = localKey->block_id;
    ret = strtoull(localKey->key, NULL, 10);
    
    SDF_key_sp_rwrelease(&localKey);

    return (ret);
}

#define LocalKey_getBlockIdPtr(localKey) strtoull(localKey->key, NULL, 10)
#define LocalKey_getBlockId(localKey) strtoull(localKey.key, NULL, 10)

PLAT_SDFKEY_C_INLINE void
Key_setBlockId(SDF_key_sp_t key, uint64_t block_id)
{
#ifndef SDFCC_SHMEM_FAKE
    struct _SDF_key *localKey = NULL;

    SDF_key_sp_rwref(&localKey, key);
    snprintf(localKey->key, _SDF_OBJECT_KEY_MAX_LEN, "%"PRIu64, block_id);
    localKey->key_len = strlen(localKey->key);
    SDF_key_sp_rwrelease(&localKey);
#else
    snprintf(key->key, _SDF_OBJECT_KEY_MAX_LEN, "%"PRIu64, block_id);
    key->key_len = strlen(key->key);
#endif
}

PLAT_SDFKEY_C_INLINE void
LocalKey_setBlockId(SDF_key_t localKey, uint64_t block_id)
{
    snprintf(localKey->key, _SDF_OBJECT_KEY_MAX_LEN, "%"PRIu64, block_id);
    localKey->key_len = strlen(localKey->key);
}



PLAT_SDFKEY_C_INLINE SDF_key_sp_t
Key_createObjectKey(const char *src, uint16_t len)
{
    if (len > _SDF_OBJECT_KEY_MAX_LEN) {
        plat_assert_always(0);
    }

    SDF_key_sp_t key = SDF_key_sp_null;
    struct _SDF_key *localKey = NULL;

#ifndef SDFCC_SHMEM_FAKE
    key = plat_shmem_alloc(SDF_key_sp);
#else
    key = plat_alloc(sizeof(struct _SDF_key));
#endif
    if (SDF_key_sp_is_null(key)) {
        plat_log_msg(20866, PLAT_LOG_CAT_SDF_SHARED, PLAT_LOG_LEVEL_FATAL, "Not enough shared memory, plat_shmem_alloc() failed.");
        plat_assert_always(0);
    }

    SDF_key_sp_rwref(&localKey, key);
    strncpy(localKey->key, src, len);
    localKey->key_len = len;
    SDF_key_sp_rwrelease(&localKey);
    
    return (key);
    /*
    SDF_key_sp_t key = SDF_key_sp_null;
    struct _SDF_key *localKey = NULL;
    struct _SDF_object_key *lok = NULL;
    char *str = NULL;

#ifndef SDFCC_SHMEM_FAKE
    key = plat_shmem_alloc(SDF_key_sp);
#else
    key = plat_alloc(sizeof(SDF_key_sp));
#endif
    if (SDF_key_sp_is_null(key)) {
        plat_log_msg(20866, PLAT_LOG_CAT_SDF_SHARED, PLAT_LOG_LEVEL_FATAL, "Not enough shared memory, plat_shmem_alloc() failed.");
        plat_assert_always(0);
    }
    SDF_key_sp_rwref(&localKey, key);
#ifndef SDFCC_SHMEM_FAKE
    localKey->object_id = plat_shmem_alloc(SDF_object_key_sp);
#else
    localKey->object_id = plat_alloc(sizeof(SDF_object_key_sp));
#endif
    if (SDF_object_key_sp_is_null(localKey->object_id)) {
        plat_log_msg(20866, PLAT_LOG_CAT_SDF_SHARED, PLAT_LOG_LEVEL_FATAL, "Not enough shared memory, plat_shmem_alloc() failed.");
        plat_assert_always(0);
    }
    SDF_object_key_sp_rwref(&lok, localKey->object_id);

    lok->len = len;
#ifndef SDFCC_SHMEM_FAKE
    lok->name = plat_shmem_var_alloc(char_sp, sizeof (char) * len);
#else
    lok->name = plat_alloc(len * sizeof(char));
#endif
    char_sp_var_rwref(&str, lok->name, len);
    memcpy(str, src, len);
    char_sp_var_rwrelease(&str, len);

    SDF_object_key_sp_rwrelease(&lok);
    SDF_key_sp_rwrelease(&localKey);

    return (key);
     */
}

PLAT_SDFKEY_C_INLINE void
Key_freeObjectKey(SDF_key_sp_t key)
{ 
    /*
// freaking painful
    struct _SDF_key *localKey = NULL;
    struct _SDF_object_key *lok = NULL;
    
    SDF_key_sp_rwref(&localKey, key);
    SDF_object_key_sp_rwref(&lok, localKey->object_id);
#ifndef SDFCC_SHMEM_FAKE
    plat_shmem_var_free(char_sp, lok->name, lok->len);
    SDF_object_key_sp_rwrelease(&lok);
    
    plat_shmem_free(SDF_object_key_sp, localKey->object_id);
    SDF_key_sp_rwrelease(&localKey);

    plat_shmem_free(SDF_key_sp, key);
#else
    plat_free(lok->name);
    plat_free(localKey->object_id);
    plat_free(key);
#endif
     */
#ifndef SDFCC_SHMEM_FAKE
    plat_shmem_free(SDF_key_sp, key);
#else
    plat_free(key);
#endif
}

/*
PLAT_SDFKEY_C_INLINE const char *
ObjectKey_getName(SDF_object_key_sp_t object_id)
{
    const struct _SDF_object_key *lok = NULL;
    const char *str = NULL;

    SDF_object_key_sp_rref(&lok, object_id);
    char_sp_var_rref(&str, lok->name, lok->len);
    //    char_sp_var_rrelease(&str, lok->len);
    SDF_object_key_sp_rrelease(&lok);

    return (str);
}

PLAT_SDFKEY_C_INLINE uint32_t
ObjectKey_getLen(SDF_object_key_sp_t object_id)
{
    const struct _SDF_object_key *lok = NULL;
    uint32_t ret = 0;

    SDF_object_key_sp_rref(&lok, object_id);
    ret = lok->len;
    SDF_object_key_sp_rrelease(&lok);

    return (ret);
}
*/
#if 0
/** Deprecated. Set length and name together to reduce errors */
PLAT_SDFKEY_C_INLINE void
ObjectKey_setLen(SDF_object_key_sp_t object_id, uint32_t val) {
    struct _SDF_object_key *lok = NULL;

    SDF_object_key_sp_rwref(&lok, object_id);
    lok->len = val;
    SDF_object_key_sp_rwrelease(&lok);
}
#endif

PLAT_SDFKEY_C_INLINE const char *
Key_getObjectName(SDF_key_sp_t key)
{
#ifndef SDFCC_SHMEM_FAKE
    const char *ret = NULL;
    const struct _SDF_key *localKey = NULL;

    SDF_key_sp_rref(&localKey, key);
    ret = localKey->key;
    SDF_key_sp_rrelease(&localKey);

    return (ret);
#else
    return key->key;
#endif
}

PLAT_SDFKEY_C_INLINE void
Key_setObjectName(SDF_key_sp_t key, const char *src, uint16_t len)
{
#ifndef SDFCC_SHMEM_FAKE
    struct _SDF_key *localKey = NULL;

    SDF_key_sp_rwref(&localKey, key);

    plat_assert(_SDF_OBJECT_KEY_MAX_LEN <= len);
    snprintf(localKey->key, _SDF_OBJECT_KEY_MAX_LEN, "%s", src);
    localKey->key_len = strlen(localKey->key);

    SDF_key_sp_rwrelease(&localKey);
#else
    snprintf(key->key, _SDF_OBJECT_KEY_MAX_LEN, "%s", src);
    key->key_len = strlen(key->key);
#endif
}

PLAT_SDFKEY_C_INLINE uint32_t
Key_getObjectLen(SDF_key_sp_t key)
{
#ifndef SDFCC_SHMEM_FAKE
    const struct _SDF_key *localKey = NULL;
    uint32_t ret = 0;

    SDF_key_sp_rref(&localKey, key);
    ret = localKey->key_len;
    SDF_key_sp_rrelease(&localKey);

    return (ret);
#else
    return key->key_len;
#endif
}

#ifdef __cplusplus
}
#endif

#endif /* _SDFKEY_H */

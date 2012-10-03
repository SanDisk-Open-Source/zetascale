//  sdf_internal.h

#ifndef __SDF_INTERNAL_H
#define __SDF_INTERNAL_H

#include <stdint.h>
#include <inttypes.h>
//#include "hash.h"
//#include "tlmap.h"

#define MAX_CONTAINERS  1000

typedef struct ctnr_map {
    char           *cname;
    SDF_cguid_t     cguid;
    SDF_CONTAINER   sdf_container;
} ctnr_map_t;

typedef struct SDF_state {
#if 0
    struct SDFTLMap   *ctnr_map_by_name;
    struct SDFTLMap   *ctnr_map_by_cguid[MAX_CONTAINERS];
#endif
    uint64_t           cguid_cntr;

} SDF_state_t;

typedef struct SDF_iterator {
    uint64_t          addr;
    uint64_t          prev_seq;
    uint64_t          curr_seq;
    struct shard     *shard;
    SDF_cguid_t       cguid;
} SDF_iterator_t;

#if 0
typedef struct SDF_thread_state {
    SDF_state_t           *sdf_state;
    struct SDFTLIterator  *iterator;
} SDF_thread_state_t;
#endif

extern int get_ctnr_from_cguid(SDF_cguid_t cguid);
extern int get_ctnr_from_cname(char *cname);
#endif // __SDF_INTERNAL_H

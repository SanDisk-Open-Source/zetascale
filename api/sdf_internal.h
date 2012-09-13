//  sdf_internal.h

#ifndef __SDF_INTERNAL_H
#define __SDF_INTERNAL_H

#include <stdint.h>
#include <inttypes.h>
//#include "hash.h"
//#include "tlmap.h"

#define MAX_CONTAINERS  1000

typedef struct SDF_state {
#if 0
    struct SDFTLMap   *ctnr_map_by_name;
    struct SDFTLMap   *ctnr_map_by_cguid[MAX_CONTAINERS];
#endif
    uint64_t           cguid_cntr;

} SDF_state_t;

#if 0
typedef struct SDF_thread_state {
    SDF_state_t           *sdf_state;
    struct SDFTLIterator  *iterator;
} SDF_thread_state_t;
#endif

#endif // __SDF_INTERNAL_H

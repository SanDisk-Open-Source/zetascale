#ifndef _SDF_APPLIB_SIMPLEWAIT_H
#define _SDF_APPLIB_SIMPLEWAIT_H

#include <semaphore.h>
#include "platform/assert.h"

#ifdef __cplusplus
"C" {
#endif

typedef struct ftopSimpleSignal {
    sem_t  sem;
} ftopSimpleSignal_t;

void ftopSimpleWait(ftopSimpleSignal_t * sig);
void ftopSimplePost(ftopSimpleSignal_t * sig);
void ftopSimpleSignalInit(ftopSimpleSignal_t * sig);
void ftopSimpleSignalDestroy(ftopSimpleSignal_t * sig);

#ifdef __cplusplus
}
#endif

#endif /* _SDF_APPLIB_SIMPLEWAIT_H */

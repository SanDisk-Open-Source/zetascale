/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

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

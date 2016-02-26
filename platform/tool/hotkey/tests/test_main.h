/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

#ifndef TEST_MAIN_H
#define TEST_MAIN_H 1
/*
 * File: $URL:$
 *
 * Created on March 1, 2010
 *
 * $Id:$
 * 
 * Author:  drew
 */

/*
 * XXX: drew 2010-03-01 Test cases should share the same startup code 
 * where practical, so it's not a cut-and-paste mess to change things.
 *
 * Originally I was going to move all of the cases into one binary; 
 * although some had different structure definitions with the same name
 * and identically named functions with different type signatuers so that
 * didn't work.
 *
 * So this just assume
 */

#include <stdint.h>

#include "platform/defs.h"

/*
 * XXX: drew 2010-03-01 This is currently meaningless, because all the tests 
 * only start one fthread and they don't float between schedulers.
 *
 * Values larger than 1 just cause additional pthreads to spin and eat CPU.
 */
#define NUM_PTHREADS 1

extern int nfailed;

__BEGIN_DECLS

void threadTest(uint64_t);

__END_DECLS

#endif /* ndef TEST_MAIN_H */

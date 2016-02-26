/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */


#ifndef SDF_ERR_H
#define SDF_ERR_H



/* Define the error base at some convenient number outside
   of unix errors range. Currently arbitrarily setup at 1<<16.
   Salt to taste later.
*/
#define SDF_ERR_BASE 1<<16
#define ERR_MAX      (SDF_ERR_BASE + 1000)
/* This can be made more useful as need arises  */
#define ERR_DEF(err_name, err_num)    \
    int err_name = (err_num);

ERR_DEF(SDF_ERR_FAILED_AGENT_RENDEZVOUS, (SDF_ERR_BASE)+2)

#endif  /* SDF_ERR_H */

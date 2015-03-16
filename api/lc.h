/*
 * ZS Logging Container
 *
 * Copyright 2015 SanDisk Corporation.  All rights reserved.
 */
#include "common/zstypes.h"

void	lc_init( );
ZS_status_t LC_init(void **lc, ZS_cguid_t cguid);
ZS_status_t NVR_write(void *lc, char *, int, char *, int);

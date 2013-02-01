/*
 * An easier interface to FDF.
 * Author: Johann George
 *
 * Copyright (c) 2012-2013, Sandisk Corporation.  All rights reserved.
 */
#include <dlfcn.h>
#include <stdio.h>
#include <stdlib.h>


static void
dlo(const char *lib)
{
    void *dl = dlopen(lib, RTLD_NOW|RTLD_GLOBAL);
    if (!dl) {
        char *err = dlerror();
        fprintf(stderr, "%s\n", err);
        exit(1);
    }
}


void
fdf_link_(void)
{
    dlo("librt.so");
    dlo("libaio.so");
}

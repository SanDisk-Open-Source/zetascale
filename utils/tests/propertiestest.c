//----------------------------------------------------------------------------
// ZetaScale
// Copyright (c) 2016, SanDisk Corp. and/or all its affiliates.
//
// This program is free software; you can redistribute it and/or modify it under
// the terms of the GNU Lesser General Public License version 2.1 as published by the Free
// Software Foundation;
//
// This program is distributed in the hope that it will be useful, but WITHOUT
// ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
// FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License v2.1 for more details.
//
// A copy of the GNU Lesser General Public License v2.1 is provided with this package and
// can also be found at: http://opensource.org/licenses/LGPL-2.1
// You should have received a copy of the GNU Lesser General Public License along with
// this program; if not, write to the Free Software Foundation, Inc., 59 Temple
// Place, Suite 330, Boston, MA 02111-1307 USA.
//----------------------------------------------------------------------------

/* 
 * File:   propertiestest.c
 * Author: Darpan Dinker
 *
 * Created on July 2, 2008, 6:24 PM
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id$
 */

#include <stdio.h>

#include "platform/stdlib.h"
#include "utils/properties.h"

int test_properties()
{
    printf("test_properties()\n");
    printf("getProperty_String('%s') returned '%s'\n", "1SDF_CC_MAXSIZE", getProperty_String("1SDF_CC_MAXSIZE", NULL));
    printf("getProperty_Int('%s') returned '%d'\n", "1SDF_CC_MAXSIZE", getProperty_Int("1SDF_CC_MAXSIZE", -1));
    printf("getProperty_LongInt('%s') returned '%ld'\n", "1SDF_CC_MAXSIZE", getProperty_LongInt("1SDF_CC_MAXSIZE", -1L));
    printf("getProperty_LongLongInt('%s') returned '%lld'\n", "1SDF_CC_MAXSIZE", getProperty_LongLongInt("1SDF_CC_MAXSIZE", -1L));
    printf("getProperty_Float('%s') returned '%f'\n", "1SDF_CC_MAXSIZE", getProperty_Float("1SDF_CC_MAXSIZE", -1.0F));
    printf("getProperty_Double('%s') returned '%lf'\n", "1SDF_CC_MAXSIZE", getProperty_Double("1SDF_CC_MAXSIZE", -1.0F));
    printf("getProperty_LongDouble('%s') returned '%llf'\n", "1SDF_CC_MAXSIZE", getProperty_LongDouble("1SDF_CC_MAXSIZE", -1.0F));
    printf("getProperty_uLongInt('%s') returned '%ld'\n", "1SDF_CC_MAXSIZE", getProperty_uLongInt("1SDF_CC_MAXSIZE", 0));
    printf("getProperty_uLongLong('%s') returned '%lld'\n", "1SDF_CC_MAXSIZE", getProperty_uLongLong("1SDF_CC_MAXSIZE", 0));
    printf("setProperty(1SDF_CC_MAXSIZE) returned %d\n", setProperty("1SDF_CC_MAXSIZE", "00000000000"));
    printf("getProperty_String('%s') returned '%s'\n", "1SDF_CC_MAXSIZE", getProperty_String("1SDF_CC_MAXSIZE", NULL));
    printf("setProperty(blah) returned %d\n", setProperty("blah", "11111111111"));
    printf("getProperty_String('%s') returned '%s'\n", "blah", getProperty_String("blah", NULL));

    return 0;
}

/*
 * 
 */
int main(int argc, char** argv) {
    int ret = 0;

    ret = loadProperties(argv[1]);
    if (0 == ret) {
        ret = test_properties();
        if (0 != ret) {
            printf("test_properties() failed, returned %d\n", ret);
        }
    } else {
        printf("loadProperties() failed, returned %d\n", ret);
    }
    
    return (ret);
}

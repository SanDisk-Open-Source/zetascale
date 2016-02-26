/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

/* 
 * File:   properties.h
 * Author: Darpan Dinker
 *
 * Created on July 2, 2008, 6:25 PM
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id$
 */

#ifndef _PROPERTIES_H
#define	_PROPERTIES_H

#ifdef	__cplusplus
extern "C" {
#endif

int ZS_log_level;

void initializeProperties();

/**
* Precedence: path > env > default
*/
int loadProperties(const char *path);

int getPropertyFromFile(const char *prop_file, char *inKey, char *outVal);

/**
* @return 0 on success, 1 if duplicate and replacement not done
*/
int insertProperty(const char *key, void* value);

/** 
* @return 0 on success, 1 if a bizarre condition occurred
*/
int setProperty(const char *key, void* value);

const char *
getProperty_String(const char *key, const char *defaultVal);

int
getProperty_Int(const char *key, const int defaultVal);

long int
getProperty_LongInt(const char *key, const long int defaultVal);

long long int
getProperty_LongLongInt(const char *key, const long long int defaultVal);

float
getProperty_Float(const char *key, const float defaultVal);

double
getProperty_Double(const char *key, const double defaultVal);

long double
getProperty_LongDouble(const char *key, const long double defaultVal);

unsigned long int
getProperty_uLongInt(const char *key, const unsigned long int defaultVal);

unsigned long long
getProperty_uLongLong(const char *key, const unsigned long long defaultVal);


#ifdef	__cplusplus
}
#endif

#endif	/* _PROPERTIES_H */


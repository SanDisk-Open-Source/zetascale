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

void initializeProperties();

/**
* Precedence: path > env > default
*/
int loadProperties(const char *path);

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


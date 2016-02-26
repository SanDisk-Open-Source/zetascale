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


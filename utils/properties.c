/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

/* 
 * File:   properties.c
 * Author: Darpan Dinker
 *
 * Created on July 2, 2008, 2:39 PM
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id$
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "platform/stdlib.h"
#include "platform/logging.h"

#include "hashmap.h"
#include "properties.h"
#include "sdftcp/tools.h"

int ZS_log_level;

static const char propertiesDefaultFile[] = 
    "/opt/schooner/config/schooner-med.properties";

HashMap _sdf_globalPropertiesMap = NULL;

void initializeProperties()
{
    _sdf_globalPropertiesMap = HashMap_create(23, 0);
}

int unloadProperties()
{
    int ret = -1;

    if (_sdf_globalPropertiesMap) {
        HashMap_destroy(_sdf_globalPropertiesMap);
        _sdf_globalPropertiesMap = NULL;
        plat_log_msg(21754, PLAT_LOG_CAT_PRINT_ARGS, PLAT_LOG_LEVEL_DEBUG, 
                     "unloaded properties");
        ret = 0;
    }

    return (ret);
}

int getPropertyFromFile(const char *prop_file, char *inKey, char *outVal) {
    int ret = 0;
    if (!prop_file || !prop_file[0])
        return 0;
    FILE *fp = fopen(prop_file, "r");

    if (!fp) {
        plat_log_msg(21756, PLAT_LOG_CAT_PRINT_ARGS,
                     PLAT_LOG_LEVEL_ERROR,
                     "Reading properties file '%s' has an error!\n", propertiesDefaultFile);
        return -1;
    }

    char *line = (char *) plat_alloc(2048), *beg, *str, *key, *val;
    while(fgets(line, 2048, fp)) {
        beg = line;
        while(' ' == *beg) { // trim beginning
            beg++;
        }
        
        if('#' == *beg || '\0' == *beg || '\n' == *beg) { // search for comment
            continue;
        }
        
        str = beg;
        while('=' != *str && '\0' != *str && ' ' != *str && '\n' != *str) { // get key
            str++;
        }
	if (str-beg) {
		key = strndup(beg, str-beg);
	} else {
		continue;
	}
        
        beg = str++;
        while(' ' == *beg || '=' == *beg) { // trim beginning
            beg++;
        }
        str = beg;
        while('=' != *str && '\0' != *str && ' ' != *str && '\n' != *str) { // get value
            str++;
        }
	if (str - beg) {
		val = strndup(beg, str-beg);
	} else {
		free(key);
		continue;
	}

        if ( strcmp(inKey,key) == 0 ) {
            strcpy(outVal,val);
	    free(key);
	    free(val);
            break;
        } else {
	    free(key);
	    free(val);
	}
    }
    fclose(fp);
    plat_free(line);
    return (ret);
}


/*
 * Set the log level.
 */
static void
set_log_level(char *val)
{
    if (streq(val, "devel"))
        ZS_log_level = PLAT_LOG_LEVEL_DEVEL;
    else if (streq(val, "trace_low"))
        ZS_log_level = PLAT_LOG_LEVEL_TRACE_LOW;
    else if (streq(val, "trace"))
        ZS_log_level = PLAT_LOG_LEVEL_TRACE;
    else if (streq(val, "debug"))
        ZS_log_level = PLAT_LOG_LEVEL_DEBUG;
    else if (streq(val, "diagnostic"))
        ZS_log_level = PLAT_LOG_LEVEL_DIAGNOSTIC;
    else if (streq(val, "info"))
        ZS_log_level = PLAT_LOG_LEVEL_INFO;
    else if (streq(val, "warning"))
        ZS_log_level = PLAT_LOG_LEVEL_WARN;
    else if (streq(val, "error"))
        ZS_log_level = PLAT_LOG_LEVEL_ERROR;
    else if (streq(val, "fatal"))
        ZS_log_level = PLAT_LOG_LEVEL_FATAL;
    else if (streq(val, "none"))
        ZS_log_level = PLAT_LOG_LEVEL_FATAL;
}


int loadProperties(const char *path_arg)
{
#ifndef SDFAPIONLY
    /* In SDF library the hash can be initialized already by SDFSetPropery API*/
    if (NULL != _sdf_globalPropertiesMap) {
        return 0;
    }
#endif

    int ret = 0;
    const char *path = NULL;

    path = path_arg;
    if (!path)
        return 0;

    FILE *fp = fopen(path, "r");

    if (!fp) {
        plat_log_msg(21756, PLAT_LOG_CAT_PRINT_ARGS,
                     PLAT_LOG_LEVEL_ERROR,
                     "Reading properties file '%s' has an error!\n", path);
        return -1;
    }
    if (!_sdf_globalPropertiesMap) {
        initializeProperties();
    }
    
    char *line = (char *) plat_alloc(2048), *beg, *str, *key, *val;
    while(fgets(line, 2048, fp)) {
        
        // aah... really needed Boost here
        beg = line;
        while(' ' == *beg) { // trim beginning
            beg++;
        }
        
        if('#' == *beg || '\0' == *beg || '\n' == *beg) { // search for comment
            continue;
        }
        
        str = beg;
        while('=' != *str && '\0' != *str && ' ' != *str && '\n' != *str) { // get key
            str++;
        }
	if (str-beg) {
		key = strndup(beg, str-beg);
	} else {
		continue;
	}
        
        beg = str++;
        while(' ' == *beg || '=' == *beg) { // trim beginning
            beg++;
        }
        str = beg;
        while('=' != *str && '\0' != *str && ' ' != *str && '\n' != *str) { // get value
            str++;
        }
	if (str-beg) {
		val = strndup(beg, str-beg);
	} else {
		free(key);
		continue;
	}
       
#ifdef SDFAPIONLY 
		/* in SDF library properties from file override properties 
           set in runtime using SDFSetProperty */
        setProperty(key, val);
#else
        if (0 != insertProperty(key, val)) {
            ret--;
            plat_log_msg(21757, PLAT_LOG_CAT_PRINT_ARGS,
                     PLAT_LOG_LEVEL_ERROR,
                     "Parsed property error (ret:%d)('%s', '%s')", ret, key, val);
        }
#endif

        /**
         * XXX: drew 2008-12-17 It would be better to log at the point of use
         * so we can output whether the default value was being used; but 
         * this will get us the current settings in Patrick's memcached
         * runs.
         */
        if (ZS_log_level <= PLAT_LOG_LEVEL_TRACE_LOW) {
            plat_log_msg(21758, PLAT_LOG_CAT_PRINT_ARGS,
                         PLAT_LOG_LEVEL_TRACE_LOW,
                         "Parsed property ('%s', '%s')", key, val);
        }
    }
    
    if (ZS_log_level <= PLAT_LOG_LEVEL_TRACE_LOW) {
        plat_log_msg(70124, PLAT_LOG_CAT_PRINT_ARGS,
                     PLAT_LOG_LEVEL_TRACE_LOW,
                     "Read from properties file '%s'", path);
    }
    fclose(fp);
    plat_free(line);
    return (ret);
}

int insertProperty(const char *key, void* value)
{
#ifdef SDFAPIONLY
    /* SDFSetPropery may be called before loadProperties, initialize the hash here in this case */
    if (!_sdf_globalPropertiesMap) {
        initializeProperties();
    }
#endif
    if (strcmp(key, "ZS_LOG_LEVEL") == 0)
        set_log_level(value);
    return ((SDF_TRUE == HashMap_put(_sdf_globalPropertiesMap, key, value)) ? 0 : 1);
}

int setProperty(const char *key, void* value)
{
#ifdef SDFAPIONLY
    /* SDFSetPropery may be called before loadProperties, initialize the hash here in this case */
    if (!_sdf_globalPropertiesMap) {
        initializeProperties();
    }
#endif
    if (strcmp(key, "ZS_LOG_LEVEL") == 0)
        set_log_level(value);
    if (SDF_TRUE != HashMap_put(_sdf_globalPropertiesMap, key, value)) {
        void *p = HashMap_replace(_sdf_globalPropertiesMap, key, value);
        if (p) {
            plat_free(p);
            return 0;
        } else {
            return 1;
        }
    }
    return 0;
}

const char *
getProperty_String(const char *key, const char *defaultVal)
{
    char *val = (char *) HashMap_get(_sdf_globalPropertiesMap, key);
    return ((val) ? val : defaultVal);
}

int
getProperty_Int(const char *key, const int defaultVal)
{
    char *val = (char *) HashMap_get(_sdf_globalPropertiesMap, key);
    return ((val) ? atoi(val) : defaultVal);
}

long int
getProperty_LongInt(const char *key, const long int defaultVal)
{
    char *val = (char *) HashMap_get(_sdf_globalPropertiesMap, key);
    return ((val) ? strtol(val, NULL, 0) : defaultVal);
}

long long int
getProperty_LongLongInt(const char *key, const long long int defaultVal)
{
    char *val = (char *) HashMap_get(_sdf_globalPropertiesMap, key);
    return ((val) ? strtoll(val, NULL, 0) : defaultVal);
}

float
getProperty_Float(const char *key, const float defaultVal)
{
    char *val = (char *) HashMap_get(_sdf_globalPropertiesMap, key);
    return ((val) ? strtof(val, NULL) : defaultVal);
}

double
getProperty_Double(const char *key, const double defaultVal)
{
    char *val = (char *) HashMap_get(_sdf_globalPropertiesMap, key);
    return ((val) ? strtod(val, NULL) : defaultVal);
}

long double
getProperty_LongDouble(const char *key, const long double defaultVal)
{
    char *val = (char *) HashMap_get(_sdf_globalPropertiesMap, key);
    return ((val) ? strtold(val, NULL) : defaultVal);
}

unsigned long int
getProperty_uLongInt(const char *key, const unsigned long int defaultVal)
{
    char *val = (char *) HashMap_get(_sdf_globalPropertiesMap, key);
    return ((val) ? strtoul(val, NULL, 0) : defaultVal);
}

unsigned long long
getProperty_uLongLong(const char *key, const unsigned long long defaultVal)
{
    char *val = (char *) HashMap_get(_sdf_globalPropertiesMap, key);
    return ((val) ? strtoull(val, NULL, 0) : defaultVal);
}

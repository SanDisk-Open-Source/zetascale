/* 
 * File:   platform_check.h
 * Author: darryl
 *
 * Created on  February 17, 2010, 10:35 AM
 *
 * Copyright (c) 2010 Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 */


/* <PREFACE>
 * Check for a valid hardware platform.
 * Intended for use by applications that must only run on
 * authorized hardware.
 * 
 * Returns 1 if ok to run, 0 if not ok.
 */
#include <stdio.h>

int is_valid_schooner_platform();
int test_schooner_platform_check();

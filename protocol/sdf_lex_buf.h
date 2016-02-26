/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

/*
 * File:   sdf_lex_buf.h
 * Author: Brian O'Krafka
 *
 * Created on April 2, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: sdf_lex_buf.h 802 2008-03-29 00:44:48Z darpan $
 */

#ifndef _SDF_LEX_BUF_H
#define _SDF_LEX_BUF_H

#ifdef __cplusplus
extern "C" {
#endif

#ifndef YY_TYPEDEF_YY_SCANNER_T
#define YY_TYPEDEF_YY_SCANNER_T
typedef void* yyscan_t;
#endif

typedef struct {
    char *myinput;    /* character buffer */
    char *myinputptr; /* current position in myinput */
    char *myinputlim; /* end of data */
} sdf_lex_buf;

#ifdef	__cplusplus
}
#endif

#endif /* _SDF_LEX_BUF_H */

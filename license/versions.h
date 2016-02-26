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
 * Copyright (c) 2013 SanDisk Corporation.  All rights reserved.
 *
 * File:   versions.h
 * Author: Niranjan Neelakanta
 *
 * Created on March 25, 2013.
 */

#ifndef __ZSLICVERSIONS_H
#define __ZSLICVERSIONS_H
#include <stdio.h>
#include "interface.h"

/*
 * This has details of various versions of file format supported.
 * flf_major: This is the major number of file format version.
 * flf_minor: This is the minor number of file format version.
 * flf_gen_lic_file: This interface is used to generate blank license file.
 * flf_gen_lic: This interface is used to generate license key.
 * flf_val_lic_file: This interface is used to validate the license file.
 * flf_check_lic_comp: This interface check license compatibility for this mac.
 * flf_get_license_details: This interface returns details stored in license.
 */
typedef struct license_format {
	int             flf_major;
	int             flf_minor;
	int		(*flf_gen_lic_file)(FILE *);
	enum lic_state	(*flf_gen_lic)(char *, char **);
	enum lic_state	(*flf_val_lic_file)(char *);
	int		(*flf_check_lic_comp)(char *, char *, char *, int);
	void		(*flf_get_license_details)(char *, lic_data_t *);
} lic_fmt_t;

extern lic_fmt_t lic_list[];
void    get_latest_file_format(int *, int *, int *);
void    get_fileops_indx(int, int, int *);



#endif

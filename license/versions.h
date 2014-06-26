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

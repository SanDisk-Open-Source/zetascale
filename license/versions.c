/*
 * Copyright (c) 2013 SanDisk Corporation.  All rights reserved.
 *
 * File:   versions.c
 * Author: Niranjan Neelakanta
 *
 * Created on March 25, 2013.
 */

#include "interface.h"
#include "versions.h"
#include "version_1_0/interface.h"
#include "version_2_0/interface.h"

/*
 * This array has the details of each format. For introducing new file 
 * format version, add major, minor and interfaces to this array.
 * 
 * IMPORTANT: Keep this array sorted w.r.t major and minor number.
 */
lic_fmt_t lic_list[] =
{
	{ 1, 0,
	  flf_gen_blank_file_v1_0,
	  flf_gen_lic_file_v1_0,
	  flf_val_lic_file_v1_0,
	  flf_check_lic_comp_v1_0,
	  flf_get_license_details_v1_0,
	},
	{ 2, 0,
	  flf_gen_blank_file_v2_0,
	  flf_gen_lic_file_v2_0,
	  flf_val_lic_file_v2_0,
	  flf_check_lic_comp_v2_0,
	  flf_get_license_details_v2_0,
	},
};

/*
 * This returns the latest file format.
 */
void
get_latest_file_format(int *maj, int *min, int *indx)
{
	int	nelem;

	nelem = sizeof(lic_list)/sizeof(lic_list[0]);
	*maj = lic_list[nelem - 1].flf_major;
	*min = lic_list[nelem - 1].flf_minor;
	*indx = nelem - 1;
}

/*
 * This returns the index of <maj, min> in lic_list.
 * Returns -1 if the pair is not found.
 */
void
get_fileops_indx(int maj, int min, int *indx)
{

	int	i, nelem;
	*indx = -1;

	nelem = sizeof(lic_list)/sizeof(lic_list[0]);
	for (i = 0; i < nelem;) {
		if (lic_list[i].flf_major < maj) {
			i++;
		} else if (lic_list[i].flf_major == maj) {
			for (;i < nelem;) {
				if (lic_list[i].flf_minor < min) {
					i++;
				} else if (lic_list[i].flf_minor == min) {
					*indx = i;
					return;
				} else {
					return;
				}
			}
		} else {
			return;
		}
	}
}


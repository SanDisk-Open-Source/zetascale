/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

/*
 * Copyright (c) 2013 SanDisk Corporation.  All rights reserved.
 *
 * File:   interface.c
 * Author: Niranjan Neelakanta
 *
 * Created on March 25, 2013.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include "interface.h"
#include "license.h"
#include "license/interface.h"

#define NO_OF_CONTACT_FIELDS	3
#define NO_OF_PART_FIELDS	9


const char contact_header_v2_0[]= "@@CONTACT@@\n";
const char *contact_section_v2_0[]= {"Contact:", "Company:", "Email:"};
const char particulars_header_v2_0[]= "@@PARTICULARS@@\n";
/*
 * IMPORTANT:
 * On adding new element to particulars_section_v2_0, update corresponding index
 * in comments_section_v2_0 and mandatory_field.
 * In addition, also update index enum, to reflect the location of fields.
 */
const char *particulars_section_v2_0[]= {"Product:", "Version:", "Scope:", "Type:",
       				"MAC Address:", "License ID:",
				"Date valid from (GMT):", "Date valid to (GMT):", "Grace period (in days):"};

const char *comments_section_v2_0[]= {"ZetaScale", "<major>.<minor>", " Single/Multiple (Valid on only one machine or multiple)", " Permanent/Periodic", 
			 " 00:00:00:00:00:00 (Use 'ifconfig or ip addr' to get HWaddr of eth0)",
			 " Optional identifier",
			 " mm/dd/yyyy HH:MM:SS",
			 " mm/dd/yyyy HH:MM:SS",
			 " -1 for Forever or specify number of days"};

const int mandatory_field_v2_0[]={1, 1, 1, 1, 0, 0, 0, 0, 0};

enum index {PRD_INDX, VER_INDX, INST_INDX, TYPE_INDX, MAC_INDX, ID_INDX, FROM_INDX, TO_INDX, GRACE_INDX};
static const char key_header_v2_0[]= "@@CIPHERTEXT@@\n";

static int getstring(char *in, char *field, char **value);
static lic_type get_license_type(char *);
static lic_type get_installation_type(char *);

/*
 * Generate blank license file. Write contents to file pointed by fd.
 */
int
flf_gen_blank_file_v2_0(FILE *fd)
{
	int	i;

	if (fwrite(contact_header_v2_0, strlen(contact_header_v2_0), 1, fd) < 1) {
		return -1;
	}
	for (i = 0; i < NO_OF_CONTACT_FIELDS; i++) {
		if (fwrite(contact_section_v2_0[i], strlen(contact_section_v2_0[i]), 1, fd) < 1) {
			return -1;
		}
		if (fwrite("\n", sizeof(char), 1, fd) < 1) {
			return -1;
		}
	}
	if (fwrite(particulars_header_v2_0, strlen(particulars_header_v2_0), 1, fd) < 1) {
		return -1;
	}
	for (i = 0; i < NO_OF_PART_FIELDS; i++) {
		if (fwrite(particulars_section_v2_0[i], 
					strlen(particulars_section_v2_0[i]), 1, fd) < 1) {
			return -1;
		}
		if (comments_section_v2_0[i] != NULL) {
			if (fwrite(comments_section_v2_0[i], 
					strlen(comments_section_v2_0[i]), 1, fd) < 1) {
				return -1;
			}
		}
		if (fwrite("\n", sizeof(char), 1, fd) < 1) {
			return -1;
		}
	}

	return 0;
}	

/*
 * Generate key for the details sent in parameter, in.
 * The input sent must be in the format as expected. This shouldn't include any
 * general license information.
 * The output contents with key will be added to out.
 */

enum lic_state
flf_gen_lic_file_v2_0(char *in, char **out)
{
	int		i, j, outlen = 0;
	char		*cnt_hdr, *prt_hdr;
	char		*cnt_ent[NO_OF_CONTACT_FIELDS] = {0};
	char		*prt_ent[NO_OF_PART_FIELDS] = {0};
	char		encr_lic[4096] = {0};
	char		msg[4096]={0};
	char		*tmp = NULL;
	int		mac_license[6] = {0};
	enum lic_state	ret = LS_VALID;
	double		from, to;
	lic_type	type;
	lic_type inst_type;
	int			grace_days;

	cnt_hdr = strstr(in, contact_header_v2_0);
	if (NULL == cnt_hdr || in != cnt_hdr) {
		printf("License file is not in valid format\n");
		return LS_FORMAT_INVALID;
	}
	i = strlen(contact_header_v2_0);
	cnt_hdr += i;
	outlen = i;
	if (NULL == (prt_hdr = strstr(in, particulars_header_v2_0))) {
		printf("License file is not in valid format\n");
		return LS_FORMAT_INVALID;
	}
	i = strlen(particulars_header_v2_0);
	outlen += i;
	for ( i = 0; i < NO_OF_CONTACT_FIELDS; i++) {
		if ((ret = getstring(in, (char *)contact_section_v2_0[i], &cnt_ent[i])) != LS_VALID) {
			printf("Value for field %s, not spcified\n", contact_section_v2_0[i]);
			goto out;
		}
		outlen += strlen(contact_section_v2_0[i]);
		outlen += strlen(cnt_ent[i]) + 1;
	}
	j = 0;	
	for ( i = 0; i < NO_OF_PART_FIELDS; i++) {
		ret = getstring(in, (char *)particulars_section_v2_0[i], &prt_ent[i]);
		if (ret == LS_VALID) {
			outlen += strlen(particulars_section_v2_0[i]);
			outlen += strlen(prt_ent[i]) + 1;
			j += strlen(particulars_section_v2_0[i]);
			j += strlen(prt_ent[i]) + 1;
		} else if (ret == LS_DATA_MISSING) {
			if (mandatory_field_v2_0[i]) {
				printf("Value for field %s, not spcified\n", particulars_section_v2_0[i]);
				goto out;
			}
		} else {
			goto out;
		}
	}
	j += 8;
	tmp =  (char *)malloc(sizeof(char) * j);
	if (!tmp) {
		ret = LS_INTERNAL_ERR;
		goto out;
	}
	bzero(tmp, sizeof(char) * j);

	type = get_license_type(prt_ent[TYPE_INDX]);
	if (type == LPT_INVAL) {
		printf("Value for license type must be Permanent or Periodic");
		ret = LS_FORMAT_INVALID;
		goto out;
	}
	if (type != LPT_PERPETUAL) {
		if (!prt_ent[FROM_INDX] || !prt_ent[TO_INDX]) {
			printf("Validity period not entered\n");
			ret = LS_DATA_MISSING;
			goto out;
		}
		if (getTimeDiff(prt_ent[FROM_INDX], prt_ent[TO_INDX], &from,
							&to) == -1) {
			printf("Validitiy period is not entered in valid format\n");
			ret = LS_FORMAT_INVALID;
			goto out;
		}
	}

	inst_type = get_installation_type(prt_ent[INST_INDX]);
	if (inst_type == LIT_INVAL) {
		printf("Value for license scope must be Single or Multiple");
		ret = LS_FORMAT_INVALID;
		goto out;
	}
	if (inst_type == LIT_STAND_ALONE) {
		if (sscanf(prt_ent[MAC_INDX], "%x:%x:%x:%x:%x:%x",
				&mac_license[0], &mac_license[1], &mac_license[2],
				&mac_license[3], &mac_license[4], &mac_license[5]) != 6) {
			printf("MAC address must be specified in xx:xx:xx:xx:xx:xx format\n");
			ret = LS_FORMAT_INVALID;
			goto out;
		}
	}

	if (type != LPT_PERPETUAL) {
		if (prt_ent[GRACE_INDX] == NULL) {
			printf("Grace period not entered\n");
			ret = LS_DATA_MISSING;
			goto out;
		}
		if (sscanf(prt_ent[GRACE_INDX], "%d", &grace_days) != 1) {
			printf("Value for grace period must be -1 or positive integer\n");
			ret = LS_FORMAT_INVALID;
			goto out;
		}
	}


	j = 0;
	for ( i = 0; i < NO_OF_PART_FIELDS; i++) {
		if (prt_ent[i] == NULL) {
			continue;
		}
		strcpy(tmp + j, particulars_section_v2_0[i]);
		j += strlen(particulars_section_v2_0[i]);
		strcpy(tmp + j, prt_ent[i]);
		j += strlen(prt_ent[i]);
		strcpy(tmp + j, "\n");
		j += 1;
	}
	strcpy(tmp + j, "\0");
	if ((ret = GenerateLicense_v2_0(tmp, j, 4096, encr_lic, msg)) != LS_VALID) {
		goto out;
	}
	outlen += (int)strlen(key_header_v2_0) + (int)strlen(encr_lic) + 8;
	*out = (char *)malloc(outlen);
	if (*out == NULL) {
		ret = LS_INTERNAL_ERR;
		goto out;
	}
	bzero(*out, outlen);

	j = 0;

#define COPY_AT(out, i, in) do {		\
	strcpy((out) + (i), (in));		\
	(i) += strlen((in));			\
} while (0)

	COPY_AT(*out, j, contact_header_v2_0);
	for ( i = 0; i < NO_OF_CONTACT_FIELDS; i++) {
		COPY_AT(*out, j, contact_section_v2_0[i]);
		COPY_AT(*out, j, cnt_ent[i]);
		COPY_AT(*out, j, "\n");
	}
	COPY_AT(*out, j, particulars_header_v2_0); 
	COPY_AT(*out, j, tmp); 
	COPY_AT(*out, j, key_header_v2_0); 
	COPY_AT(*out, j, encr_lic); 
	COPY_AT(*out, j, "\n"); 
	strcpy(*out + j, "\0");
out:
	if (tmp) {
		free(tmp);
	}
	for ( i = 0; i < NO_OF_CONTACT_FIELDS; i++) {
		if (cnt_ent[i]) free(cnt_ent[i]);
	}
	for ( i = 0; i < NO_OF_PART_FIELDS; i++) {
		if (prt_ent[i]) free(prt_ent[i]);
	}
	return ret;
}	

/*
 * Validate the license file. This will not compare MAC address.
 */
enum lic_state
flf_val_lic_file_v2_0(char *in)
{
	char	*prtstart, *keystart, *end;
	char 	*decr, *key;
	char	encr_lic[4096] = {0};
	char	msg[4096]={0};
	int	ret;
	int	len;

	if ((prtstart = strstr(in, particulars_header_v2_0)) == NULL) {
		return LS_FORMAT_INVALID;
	}
	prtstart += strlen(particulars_header_v2_0);
	if ((keystart = strstr(in, key_header_v2_0)) == NULL) {
		return LS_FORMAT_INVALID;
	}

	len = (int)(keystart - prtstart) + 8;
	if ((decr = (char *)malloc(sizeof(char) * len)) 
			== NULL) {
		return LS_INTERNAL_ERR;
	}
	bzero(decr, len);
	strncpy(decr, prtstart, (int)(keystart - prtstart));

	keystart += strlen(key_header_v2_0);
	end = strstr(keystart, "\n");
	end--;
	len = (int)(end - keystart) + 8;
	if ((key = (char *)malloc(sizeof(char) * len)) == NULL) {
		free(decr);
		return LS_INTERNAL_ERR;
	}
	bzero(key, len);
	strncpy(key, keystart, (int)(end - keystart) + 1);

	if ((ret = GenerateLicense_v2_0(decr, strlen(decr), 4096, encr_lic, msg)) != LS_VALID) {
		goto out;
	}
	if (strcmp(key, encr_lic) == 0) {
		ret = LS_VALID;
	} else {
		ret = LS_KEY_MISMATCH;
	}
out:
	free(decr);
	free(key);
	return ret;
}	
int
flf_check_lic_comp_v2_0(char *in, char *prod, char *version, int check_mac_addr)
{
	enum lic_state	ret = LS_VALID;
#if 0
	char	*prtstart, *keystart, *end;
	char 	*decr, *key;
	int	len;
	char 	*msg;

	prtstart = strstr(in, particulars_header_v2_0) + strlen(particulars_header_v2_0);
	keystart = strstr(in, key_header_v2_0);

	len = (int)(keystart - prtstart) + 8;
	if ((decr = (char *)malloc(sizeof(char) * len))  ///1
			== NULL) {
		return -1;
	}
	bzero(decr, len);
	strncpy(decr, prtstart, (int)(keystart - prtstart));

	keystart += strlen(key_header_v2_0);
	end = strstr(keystart, "\n");
	end--;
	len = (int)(end - keystart) + 8;
	if ((key = (char *)malloc(sizeof(char) * len)) == NULL) {
		free(decr);
		return -1;
	}
	bzero(key, len);
	strncpy(key, keystart, (int)(end - keystart) + 1);

	if ((msg = (char *)malloc(sizeof(char) * 4096)) == NULL) {
		free(key);
		free(decr);
		return -1;
	}
	ret = isLicenseValid2(decr, key, prod, version, check_mac_addr, msg);
	if (ret != LS_VALID) {
		fprintf(stderr, "Warning: %s\n", msg);
	}
	free(key);
	free(decr);
	free(msg);
#endif
	return ret;
}	

/*
 * Return details of license only if it is valid.
 */
void
flf_get_license_details_v2_0(char *in, lic_data_t *data)
{
	int				i;
	char			*cnt_hdr;
	char			*cnt_ent[NO_OF_CONTACT_FIELDS] = {0};
	char			*prt_ent[NO_OF_PART_FIELDS] = {0};
	int				len;
	char			*prtstart, *keystart, *end;
	char 			*decr = NULL, *key = NULL, *msg = NULL;
	char			maj[32]={0}, min[32] = {0};
	int				check_mac_addr = 0;
	enum lic_state	ret = LS_VALID;
	double			from, to;
	lic_type		type;
	lic_inst_type	inst_type;
	int				grace_prd;


	cnt_hdr = strstr(in, contact_header_v2_0);
	if (NULL == cnt_hdr || in != cnt_hdr) {
		data->fld_state = LS_FORMAT_INVALID;
		goto out;
	}

	if ((prtstart = strstr(in, particulars_header_v2_0)) == NULL) {
		data->fld_state = LS_FORMAT_INVALID;
		goto out;
	}
	prtstart += strlen(particulars_header_v2_0);
	if ((keystart = strstr(in, key_header_v2_0)) == NULL) {
		data->fld_state = LS_FORMAT_INVALID;
		goto out;
	}

	for ( i = 0; i < NO_OF_CONTACT_FIELDS; i++) {
		if ((ret = getstring(in, (char *)contact_section_v2_0[i], &cnt_ent[i])) != LS_VALID) {
			goto out;
		}
	}
	for ( i = 0; i < NO_OF_PART_FIELDS; i++) {
		ret = getstring(in, (char *)particulars_section_v2_0[i], 
				&prt_ent[i]);
		if (ret == LS_DATA_MISSING) {
			if (mandatory_field_v2_0[i]) {
				goto out;
			}
		} else if (ret != LS_VALID) {
			goto out;
		}
	}

	len = (int)(keystart - prtstart) + 8;
	if ((decr = (char *)malloc(sizeof(char) * len)) == NULL) {
		data->fld_state = LS_INTERNAL_ERR;
		goto out;
	}
	bzero(decr, len);
	strncpy(decr, prtstart, (int)(keystart - prtstart));

	keystart += strlen(key_header_v2_0);
	if ((end = strstr(keystart, "\n")) == NULL) {
		data->fld_state = LS_FORMAT_INVALID;
		goto out;
	}

	end--;
	len = (int)(end - keystart) + 8;
	if ((key = (char *)malloc(sizeof(char) * len)) == NULL) {
		data->fld_state = LS_INTERNAL_ERR;
		goto out;
	}
	bzero(key, len);
	strncpy(key, keystart, (int)(end - keystart) + 1);
	strcpy(key + (int)(end - keystart) + 1, "\0");

	if ((msg = (char *)malloc(sizeof(char) * 4096)) == NULL) {
		data->fld_state = LS_INTERNAL_ERR;
		goto out;
	}
	data->fld_state = ret = (int)isLicenseValid2_v2_0(
				decr, key, NULL, NULL, check_mac_addr, msg);


	cnt_hdr = strstr(prt_ent[1], "."); 	
	if (cnt_hdr) {
		len = (int)(cnt_hdr - prt_ent[1]);
		if (len > 0) {
			strncpy(maj, prt_ent[1], len);
			data->fld_data[LDI_PROD_MAJ] = (void *)strdup(maj);
		} 
		if ((strlen(prt_ent[1]) - len - 1) > 0) {
			strncpy(min, cnt_hdr + 1, strlen(prt_ent[1]) - len - 1);
			data->fld_data[LDI_PROD_MIN] = (void *)strdup(min);
		}
	} else {
		strcpy(maj, prt_ent[1]);
		data->fld_data[LDI_PROD_MAJ] = (void *)strdup(maj);
	}

	type = get_license_type(prt_ent[TYPE_INDX]);
	inst_type = get_installation_type(prt_ent[INST_INDX]);
	
	if (inst_type == LIT_STAND_ALONE) {
		if ((ret == LS_VALID) || (ret == LS_EXPIRED)) {
			ret = mac_addr_matches(prt_ent[MAC_INDX]);
			if (ret != LS_VALID) {
				data->fld_state = ret;
			}
		}
	}

	data->fld_data[LDI_PROD_NAME] = (void *)strdup(prt_ent[0]);
#define copyas(indx, type, val) do {				\
	data->fld_data[indx] = malloc(sizeof(type));		\
	if (data->fld_data[indx]) {				\
		*((type *)(data->fld_data[indx])) = val;	\
	} else {						\
		data->fld_state = LS_INTERNAL_ERR;		\
		goto out;					\
	}							\
}while (0)						

	copyas(LDI_LIC_TYPE, lic_type, type);
	copyas(LDI_INST_TYPE, lic_inst_type, inst_type);

	if ((data->fld_state == LS_VALID) && (type != LPT_PERPETUAL)) {
		if (getTimeDiff(prt_ent[FROM_INDX], prt_ent[TO_INDX], &from, &to) != -1) {
			copyas(LDI_DIFF_FROM, double, from);
			copyas(LDI_DIFF_TO, double, to);
			if (from < 0) {
				data->fld_state = LS_NOTBEGUN;
			} else if (to <= 0) {
				data->fld_state = LS_EXPIRED;
			}	
		} else {
			data->fld_state = LS_FORMAT_INVALID;
		}

		if (prt_ent[GRACE_INDX] == NULL) {
			ret = LS_FORMAT_INVALID;
			goto out;
		}

		if (sscanf(prt_ent[GRACE_INDX], "%d", &grace_prd) != 1) {
			ret = LS_FORMAT_INVALID;
			goto out;
		}

		if (grace_prd != -1) {
			grace_prd = grace_prd * 60 * 60 * 24;
		}
		copyas(LDI_GRACE_PRD, double, grace_prd);
	}

	
	data->fld_data[LDI_CUST_NAME] = (void *)strdup(cnt_ent[0]);
	data->fld_data[LDI_CUST_COMPANY] = (void *)strdup(cnt_ent[1]);
	data->fld_data[LDI_CUST_MAIL] = (void *)strdup(cnt_ent[2]);

out:
	if (decr) free(decr);
	if (key) free(key);
	if (msg) free(msg);
	for ( i = 0; i < NO_OF_PART_FIELDS; i++) {
		if (prt_ent[i]) free(prt_ent[i]);
	}
	return;
}	

static lic_type
get_license_type(char *s)
{
	lic_type type;

	if (strncmp(s, "Permanent", strlen("Permanent")) == 0) {
		type = LPT_PERPETUAL;
	} else if (strncmp(s, "Periodic", strlen("Periodic")) == 0) {
		type = LPT_PERIODIC;
	} else {
		type = LPT_INVAL;
	}
	return (type);
}		

static lic_type
get_installation_type(char *s)
{
	lic_inst_type type;

	if (strncmp(s, "Multiple", strlen("Multiple")) == 0) {
		type = LIT_MULTI_INST;
	} else if (strncmp(s, "Single", strlen("Single")) == 0) {
		type = LIT_STAND_ALONE;
	} else {
		type = LIT_INVAL;
	}
	return (type);
}		

static int 
getstring(char *in, char *field, char **value)
{
	char	*start, *end;
	char	*p, *q;

	if ((start = strstr(in, field)) == NULL) {
		return LS_DATA_MISSING;
	}
	p = start + strlen(field);
	while (*p == ' ') p++;		/* remove begining spaces */
	if ((end = strstr(start, "\n")) == NULL) {
		return LS_FORMAT_INVALID;
	}
	q = end - 1;
	while (*q == ' ') q--;		/* remove trailing spaces */

	if ((p > q) ||
			((p == q) && (*p == ' ' || *p == '\n'))) {
		return LS_DATA_MISSING;	/* It has been left blank */
	}


	if ((*value = (char *)malloc(sizeof(char) * (int)(q - p + 2))) == NULL) {
		return LS_INTERNAL_ERR;
	}
	strncpy(*value, p, (int)(q - p + 1));
	strcpy(*value + (q - p + 1), "\0");
	return LS_VALID;
}


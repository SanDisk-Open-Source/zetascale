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
#define NO_OF_PART_FIELDS	6


const char contact_header[]= "@@CONTACT@@\n";
const char *contact_section[]= {"Contact:", "Company:", "Email:"};
const char particulars_header[]= "@@PARTICULARS@@\n";
const char *particulars_section[]= {"Product:", "Version:", "Type:", "MAC Address:", "Date valid from (GMT):", "Date valid to (GMT):"};
const char *comments_section[]= {NULL, NULL, " Permanent/Periodic", " Use 'ifconfig or ip addr' to get HWaddr of eth0", " mm/dd/yyyy HH:MM:SS", " mm/dd/yyyy HH:MM:SS"};
const int mandatory_field[]={1, 1, 1, 1, 0, 0};
const char key_header[]= "@@CIPHERTEXT@@\n";

#define TYPE_INDX	2
#define MAC_INDX	3
#define FROM_INDX	4
#define	TO_INDX		5

static int getstring(char *in, char *field, char **value);
lic_type get_license_type(char *);

/*
 * Generate blank license file. Write contents to file pointed by fd.
 */
int
flf_gen_blank_file_v1_0(FILE *fd)
{
	int	i;

	if (fwrite(contact_header, strlen(contact_header), 1, fd) < 1) {
		return -1;
	}
	for (i = 0; i < NO_OF_CONTACT_FIELDS; i++) {
		if (fwrite(contact_section[i], strlen(contact_section[i]), 1, fd) < 1) {
			return -1;
		}
		if (fwrite("\n", sizeof(char), 1, fd) < 1) {
			return -1;
		}
	}
	if (fwrite(particulars_header, strlen(particulars_header), 1, fd) < 1) {
		return -1;
	}
	for (i = 0; i < NO_OF_PART_FIELDS; i++) {
		if (fwrite(particulars_section[i], 
					strlen(particulars_section[i]), 1, fd) < 1) {
			return -1;
		}
		if (comments_section[i] != NULL) {
			if (fwrite(comments_section[i], 
					strlen(comments_section[i]), 1, fd) < 1) {
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
flf_gen_lic_file_v1_0(char *in, char **out)
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

	cnt_hdr = strstr(in, contact_header);
	if (NULL == cnt_hdr || in != cnt_hdr) {
		return LS_FORMAT_INVALID;
	}
	i = strlen(contact_header);
	cnt_hdr += i;
	outlen = i;
	if (NULL == (prt_hdr = strstr(in, particulars_header))) {
		return LS_FORMAT_INVALID;
	}
	i = strlen(particulars_header);
	outlen += i;
	for ( i = 0; i < NO_OF_CONTACT_FIELDS; i++) {
		if ((ret = getstring(in, (char *)contact_section[i], &cnt_ent[i])) != LS_VALID) {
			goto out;
		}
		outlen += strlen(contact_section[i]);
		outlen += strlen(cnt_ent[i]) + 1;
	}
	j = 0;	
	for ( i = 0; i < NO_OF_PART_FIELDS; i++) {
		ret = getstring(in, (char *)particulars_section[i], &prt_ent[i]);
		if (ret == LS_VALID) {
			outlen += strlen(particulars_section[i]);
			outlen += strlen(prt_ent[i]) + 1;
			j += strlen(particulars_section[i]);
			j += strlen(prt_ent[i]) + 1;
		} else if (ret == LS_DATA_MISSING) {
			if (mandatory_field[i]) {
				goto out;
			}
		} else {
			goto out;
		}
	}
	j += 8;

	type = get_license_type(prt_ent[TYPE_INDX]);
	if (type == LPT_INVAL) {
		ret = LS_FORMAT_INVALID;
		goto out;
	}
	if (GET_PER_TYPE(type) != LPT_PERPETUAL) {
		if (!prt_ent[FROM_INDX] || !prt_ent[TO_INDX]) {
			ret = LS_DATA_MISSING;
			goto out;
		}
		if (getTimeDiff(prt_ent[FROM_INDX], prt_ent[TO_INDX], &from,
							&to) == -1) {
			ret = LS_FORMAT_INVALID;
			goto out;
		}
	}
	if (sscanf(prt_ent[MAC_INDX], "%x:%x:%x:%x:%x:%x",
			&mac_license[0], &mac_license[1], &mac_license[2],
			&mac_license[3], &mac_license[4], &mac_license[5]) != 6) {
		ret = LS_FORMAT_INVALID;
		goto out;
	}
	tmp =  (char *)malloc(sizeof(char) * j);
	if (!tmp) {
		ret = LS_INTERNAL_ERR;
		goto out;
	}
	bzero(tmp, sizeof(char) * j);

	j = 0;
	for ( i = 0; i < NO_OF_PART_FIELDS; i++) {
		if (prt_ent[i] == NULL) {
			continue;
		}
		strcpy(tmp + j, particulars_section[i]);
		j += strlen(particulars_section[i]);
		strcpy(tmp + j, prt_ent[i]);
		j += strlen(prt_ent[i]);
		strcpy(tmp + j, "\n");
		j += 1;
	}
	strcpy(tmp + j, "\0");
	if ((ret = GenerateLicense(tmp, j, 4096, encr_lic, msg)) != LS_VALID) {
		goto out;
	}
	outlen += (int)strlen(key_header) + (int)strlen(encr_lic) + 8;
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

	COPY_AT(*out, j, contact_header);
	for ( i = 0; i < NO_OF_CONTACT_FIELDS; i++) {
		COPY_AT(*out, j, contact_section[i]);
		COPY_AT(*out, j, cnt_ent[i]);
		COPY_AT(*out, j, "\n");
	}
	COPY_AT(*out, j, particulars_header); 
	COPY_AT(*out, j, tmp); 
	COPY_AT(*out, j, key_header); 
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
flf_val_lic_file_v1_0(char *in)
{
	char	*prtstart, *keystart, *end;
	char 	*decr, *key;
	char	encr_lic[4096] = {0};
	char	msg[4096]={0};
	int	ret;
	int	len;

	if ((prtstart = strstr(in, particulars_header)) == NULL) {
		return LS_FORMAT_INVALID;
	}
	prtstart += strlen(particulars_header);
	if ((keystart = strstr(in, key_header)) == NULL) {
		return LS_FORMAT_INVALID;
	}

	len = (int)(keystart - prtstart) + 8;
	if ((decr = (char *)malloc(sizeof(char) * len)) 
			== NULL) {
		return LS_INTERNAL_ERR;
	}
	bzero(decr, len);
	strncpy(decr, prtstart, (int)(keystart - prtstart));

	keystart += strlen(key_header);
	end = strstr(keystart, "\n");
	end--;
	len = (int)(end - keystart) + 8;
	if ((key = (char *)malloc(sizeof(char) * len)) == NULL) {
		free(decr);
		return LS_INTERNAL_ERR;
	}
	bzero(key, len);
	strncpy(key, keystart, (int)(end - keystart) + 1);

	if ((ret = GenerateLicense(decr, strlen(decr), 4096, encr_lic, msg)) != LS_VALID) {
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
flf_check_lic_comp_v1_0(char *in, char *prod, char *version, int check_mac_addr)
{
	enum lic_state	ret = LS_VALID;
#if 0
	char	*prtstart, *keystart, *end;
	char 	*decr, *key;
	int	len;
	char 	*msg;

	prtstart = strstr(in, particulars_header) + strlen(particulars_header);
	keystart = strstr(in, key_header);

	len = (int)(keystart - prtstart) + 8;
	if ((decr = (char *)malloc(sizeof(char) * len))  ///1
			== NULL) {
		return -1;
	}
	bzero(decr, len);
	strncpy(decr, prtstart, (int)(keystart - prtstart));

	keystart += strlen(key_header);
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
flf_get_license_details_v1_0(char *in, lic_data_t *data)
{
	int		i;
	char		*cnt_hdr;
	char		*prt_ent[NO_OF_PART_FIELDS];
	int		len;
	char		*prtstart, *keystart, *end;
	char 		*decr = NULL, *key = NULL, *msg = NULL;
	char		maj[32]={0}, min[32] = {0};
	int		check_mac_addr = 0;
	enum lic_state	ret = LS_VALID;
	double		from, to;
	lic_type	type;


	cnt_hdr = strstr(in, contact_header);
	if (NULL == cnt_hdr || in != cnt_hdr) {
		data->fld_state = LS_FORMAT_INVALID;
		goto out;
	}

	if ((prtstart = strstr(in, particulars_header)) == NULL) {
		data->fld_state = LS_FORMAT_INVALID;
		goto out;
	}
	prtstart += strlen(particulars_header);
	if ((keystart = strstr(in, key_header)) == NULL) {
		data->fld_state = LS_FORMAT_INVALID;
		goto out;
	}

	for ( i = 0; i < NO_OF_PART_FIELDS; i++) {
		ret = getstring(in, (char *)particulars_section[i], 
				&prt_ent[i]);
		if (ret == LS_DATA_MISSING) {
			if (mandatory_field[i]) {
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

	keystart += strlen(key_header);
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
	data->fld_state = ret = (int)isLicenseValid2(
				decr, key, NULL, NULL, check_mac_addr, msg);
	if ((ret == LS_VALID) || (ret == LS_EXPIRED)) {
		ret = mac_addr_matches(prt_ent[MAC_INDX]);
		if (ret != LS_VALID) {
			data->fld_state = ret;
		}
	}


	cnt_hdr = strstr(prt_ent[1], "."); 	
	if (cnt_hdr) {
		len = (int)(cnt_hdr - prt_ent[1]);
		strncpy(maj, prt_ent[1], len);
		data->fld_data[LDI_PROD_MAJ] = (void *)strdup(maj);
		strncpy(min, cnt_hdr + 1, strlen(prt_ent[1]) - len - 1);
		data->fld_data[LDI_PROD_MIN] = (void *)strdup(min);

	} else {
		strcpy(maj, prt_ent[1]);
		data->fld_data[LDI_PROD_MAJ] = (void *)strdup(maj);
	}



	type = get_license_type(prt_ent[TYPE_INDX]);
	
	data->fld_data[LDI_PROD_NAME] = (void *)strdup(prt_ent[0]);

#define copyas(indx, type, val) do {				\
	data->fld_data[indx] = malloc(sizeof(type));		\
	if (data->fld_data[indx]) {				\
		*((type *)(data->fld_data[indx])) = val;	\
	} else {						\
		data->fld_state = LS_FORMAT_INVALID;		\
		goto out;					\
	}							\
}while (0)						

	copyas(LDI_LIC_TYPE, lic_type, type);

	if ((data->fld_state == LS_VALID) && (GET_PER_TYPE(type) != LPT_PERPETUAL)) {
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
	}

out:
	if (decr) free(decr);
	if (key) free(key);
	if (msg) free(msg);
	return;
}	

lic_type
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
	return ((type << 4) | LIT_STAND_ALONE);
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

	if (p >= q) return LS_DATA_MISSING;	/* It has been left blank */

	if ((*value = (char *)malloc(sizeof(char) * (int)(q - p + 2))) == NULL) {
		return LS_INTERNAL_ERR;
	}
	strncpy(*value, p, (int)(q - p + 1));
	strcpy(*value + (q - p + 1), "\0");
	return LS_VALID;
}


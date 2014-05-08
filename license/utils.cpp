/*
 * Copyright (c) 2013 SanDisk Corporation.  All rights reserved.
 *
 * File:   license.cpp
 * Author: Niranjan Neelakanta
 *
 * Created on March 25, 2013.
 */

#include <cstdlib>
#include <unistd.h>
#include <iostream>
#include <time.h>
#include <math.h>
#include <string.h>
#include <stdio.h>
#include <unistd.h>
#include <errno.h>

#include <rpc/des_crypt.h>
#include "utils.h"
#include "interface.h"
#include "license/interface.h"

#define DEBUG 0
#define LICENSE_LEN 0
#define ENCR_TO_CLEAR_RATIO 4

const char text_valid_from[] = "Date valid from:";
const char text_valid_to[] = "Date valid to:";

const char STR_PARSE_DATE_FAILURE[] = "Could not parse dates. Invalid format.";
const char STR_DECRYPT_NO_MATCH[] = "License does not match. Corrupted or Invalid.";
const char STR_DECRYPT_FAILED[] = "Corrupted or invalid Ciphertext.";
const char STR_ENCRYPT_FAILED[] = "Encryption failed.";
const char STR_CIPHER_LEN_LOW[] = "Ciphertext buffer length is low compared to cleartext to encrypt.";
const char STR_GENLIC_NULL_PARAMS[] = "GenerateLicense() passed null parameters. Exiting.";
const int N_STR_PARSE_DATE_FAILURE = sizeof(STR_PARSE_DATE_FAILURE);
const int N_STR_DECRYPT_NO_MATCH = sizeof(STR_DECRYPT_NO_MATCH);
const int N_STR_DECRYPT_FAILED = sizeof(STR_DECRYPT_FAILED);
const int N_STR_ENCRYPT_FAILED = sizeof(STR_ENCRYPT_FAILED);
const int N_STR_CIPHER_LEN_LOW = sizeof(STR_CIPHER_LEN_LOW);
const int N_STR_GENLIC_NULL_PARAMS = sizeof(STR_GENLIC_NULL_PARAMS);

#define KEY_LEN 8
#define CRYPT_ITERATIONS 4 // 4DES

/**
 * Minimum length is 32 chars.
 */
#if 0
//char passPhrase[] = "3076020100300D06092A864886F70D010101050004623060020100021100A7851438A4F0E4E0849D0980EE8295BB020111021004ED536B13E8F7AC33DF2A8DAA2BB375020900E89B76ADD4CB0CC7020900B85DEC027FE9B36D0208290C603CCB32E42302090097D4E07A87752A59020873F15179EE27ED05";
static unsigned char passPhrase[KEY_LEN * CRYPT_ITERATIONS + 1] =
          {0x60, 0x02, 0x01, 0xFE, 0x02, 0x11, 0xA7, 0x85,
           0x14, 0x38, 0xA4, 0xF0, 0xE4, 0xE0, 0x84, 0x9D,
           0x09, 0x80, 0xEE, 0x82, 0x95, 0xBB, 0x02, 0x01,
           0x03, 0xCC, 0xB3, 0x2E, 0x42, 0x30, 0x20, 0x90, 0x00};

static unsigned char cbc_ivec[] =
    {0x85, 0xDE, 0xC0, 0x27, 0xFE, 0x9B, 0x36, 0xD0};

static char hex_tbl[] = "0123456789ABCDEF";
#define TO_HEX(a) hex_tbl[a]
#define FROM_HEX(a) (((a) >= '0' && (a) <= '9') ? (a) - '0' : 10 + (a) - 'A')
#define IS_HEX(a) ((a) >= '0' && (a) <= 'F')
#endif

/**
 *
 * @param instr
 * @param textpassPhrase
 * @return
 */
#if 0
std::string EncryptString(const char *in_data, const char *textpassPhrase)
{
	std::string outstr;
	char ivec[KEY_LEN];

	int data_len = strlen(in_data) + 1;
	int data_len_pad = ((data_len + KEY_LEN - 1) / KEY_LEN) * KEY_LEN;

	unsigned char* out_data = (unsigned char*)malloc(data_len_pad);
	if(!out_data)
		return outstr;

	memcpy(out_data, in_data, data_len);
	if(data_len_pad - data_len)
		memset(out_data + data_len, 0, data_len_pad - data_len);

	for(int i = 0; i < CRYPT_ITERATIONS; i++)
	{
		memcpy(ivec, cbc_ivec, KEY_LEN);

		char *key = (char*)textpassPhrase + i * KEY_LEN;

		des_setparity(key);

		int status = cbc_crypt(key, (char*)out_data, data_len_pad, DES_ENCRYPT, ivec);

		if(DES_FAILED(status))
			goto error_exit;
	}

	for(int i = 0; i < data_len_pad; i++)
	{
		outstr.push_back(TO_HEX(out_data[i] >> 4));
		outstr.push_back(TO_HEX(out_data[i] & 0xF));
	}

error_exit:
	free(out_data);

  return outstr;
}

/**
 *
 * @param instr
 * @param textpassPhrase
 * @return
 */
extern
std::string DecryptString(const char *in_data, const char *textpassPhrase)
{
	int i;
	std::string outstr;
	char ivec[KEY_LEN];

	int data_len = strlen(in_data);

	if(data_len % (KEY_LEN * 2))
		return outstr;

	data_len /= 2;

	char* out_data = (char*)malloc(data_len);
	if(!out_data)
		return outstr;

	int j = 0;
	for(i = 0; i < data_len; i++)
	{
		if(!IS_HEX(in_data[j]) || !IS_HEX(in_data[j + 1]))
			goto error_exit;

		out_data[i] = (FROM_HEX((unsigned char)in_data[j]) << 4) |
		  FROM_HEX((unsigned char)in_data[j + 1]);

		j += 2;
	}

	i = CRYPT_ITERATIONS;
	while(i--)
	{
		memcpy(ivec, cbc_ivec, KEY_LEN);

		char *key = (char*)textpassPhrase + i * KEY_LEN;

		des_setparity(key);

		int status = cbc_crypt(key, out_data, data_len, DES_DECRYPT, ivec);

		if(DES_FAILED(status))
			goto error_exit;
	}

	outstr.assign(out_data);

error_exit:
	free(out_data);

  return outstr;
}
////////////////////////////////////////////////////////////////////////////////
#endif

/**
 *
 * @param buff
 * @return
 */
time_t convertDateTime(const char *buff) {
    int yy, mm, dd, hour, min, sec;
    struct tm when;
    time_t tme;

    if (sscanf(buff, "%d/%d/%d %d:%d:%d", &mm, &dd, &yy, &hour, &min, &sec) != 6) {
        return(0);
    }

    if (yy > 1900) {
	    int CC = (yy / 100) * 100;
	    if ((yy - CC) < 70) {
		    yy = yy - (CC - 100);
	    } else {
		    yy = yy - CC;
	    }
    } else if (yy > 100) {
	return 0;
    } else {
	    if (yy < 70) {
		    yy += 100;
	    }
    }

    time(&tme);
    when = *gmtime(&tme);
    when.tm_year = yy;
    when.tm_mon = mm - 1;
    when.tm_mday = dd;
    when.tm_hour = hour;
    when.tm_min = min;
    when.tm_sec = sec;
    return ( mktime(&when));
}

/**
 *
 * @param validfrom_gmt
 * @param validto_gmt
 * @param message_out
 * @return
 */
int
getTimeDiff(char * validfrom_gmt, char * validto_gmt, double *from, double *to)
{
    time_t from_gmt = convertDateTime(validfrom_gmt);
    time_t to_gmt = convertDateTime(validto_gmt);


    // get current time in GMT
    time_t now = time(NULL);
    tm *n = gmtime(&now);
    time_t nowgmt = mktime(n);

    if (from_gmt == 0) {
        return -1;
    } else {
	    // do the diff and verify if now is before validfrom
	    *from = difftime(nowgmt, from_gmt);
    }
    if (to_gmt == 0) {
        return -1;
    } else {
	    // do the diff and verify if now is after validto
	    *to = difftime(to_gmt, nowgmt);
    }

	return 0;
}

lic_state
verifyDates(char * validfrom_gmt, char * validto_gmt, char *message_out) {
    int nmsg = 0; // count number of chars written to message_out
    time_t from_gmt = convertDateTime(validfrom_gmt);
    time_t to_gmt = convertDateTime(validto_gmt);

    if (from_gmt == 0) {
        nmsg = sprintf(message_out + nmsg, "Invalid 'from' time.");
        return LS_FORMAT_INVALID;
    }
    if (to_gmt == 0) {
        nmsg = sprintf(message_out + nmsg, "Invalid 'to' time.");
        return LS_FORMAT_INVALID;
    }

    // get current time in GMT
    time_t now = time(NULL);
    tm *n = gmtime(&now);
    time_t nowgmt = mktime(n);

    // do the diff and verify if now is after validto
    double diff = difftime(to_gmt, nowgmt);
    if (diff < 0) {
        nmsg = sprintf(message_out + nmsg, "License has expired.");
        return LS_EXPIRED;
    } else {
        float hours = diff / 3600;
        nmsg = sprintf(message_out + nmsg, "Time left: %.0lf hours", floor(hours));
        float mins = 60 * (hours - floor(hours));
        nmsg += sprintf(message_out + nmsg, " %.0lf mins", floor(mins));
        float sec = 60 * (mins - floor(mins));
        nmsg += sprintf(message_out + nmsg, " %.0lf secs. ", floor(sec));
    }

    // do the diff and verify if now is before validfrom
    diff = difftime(nowgmt, from_gmt);
    if (diff < 0) { // allow start dates in the future
        nmsg = sprintf(message_out + nmsg, "License period has not begun.");
        return LS_NOTBEGUN;
    } else {
        float hours = diff / 3600;
        nmsg += sprintf(message_out + nmsg, "Time passed: %.0lf hours", floor(hours));
        float mins = 60 * (hours - floor(hours));
        nmsg += sprintf(message_out + nmsg, " %.0lf mins", floor(mins));
        float sec = 60 * (mins - floor(mins));
        nmsg += sprintf(message_out + nmsg, " %.0lf secs\n", floor(sec));
    }
    return LS_VALID;
}

/*
 * @param clear_license IN
 * @param validfrom_gmt OUT
 * @param validto_gmt OUT
 * @return
 */
#if 0
lic_state
parseDates(char *clear_license, char *validfrom_gmt, char *validto_gmt) {
    char *p = NULL, *q = NULL;

    ////////
    if (!(p = strstr(clear_license, particulars_section[3]))) {
        //fprintf(stderr, "License format incorrect, missing tag: %s\n", text_valid_from);
        return LS_FORMAT_INVALID;
    }

    if (!(q = strstr(p, "\n"))) {
        //fprintf(stderr, "License format incorrect, new line not found after tag: %s\n", text_valid_from);
        return LS_FORMAT_INVALID;
    }
    p += strlen(particulars_section[3]);
    while (*p == ' ') p++; // remove beginning whitespaces
    strncpy(validfrom_gmt, p, q - p);
    strcpy(validfrom_gmt + (q - p), "\0");
    q = validfrom_gmt + strlen(validfrom_gmt) - 1;
    for (; *q == ' '; q--) *q = '\0'; // remove ending whitespaces
    ////////
    p = q = NULL;
    if (!(p = strstr(clear_license, particulars_section[4]))) {
        //fprintf(stderr, "License format incorrect, missing tag: %s\n", text_valid_to);
        return LS_FORMAT_INVALID;
    }
    if (!(q = strstr(p, "\n"))) {
        //fprintf(stderr, "License format incorrect, new line not found after tag: %s\n", text_valid_to);
        return LS_FORMAT_INVALID;
    }
    p += strlen(particulars_section[4]);
    while (*p == ' ') p++; // remove beginning whitespaces
    strncpy(validto_gmt, p, q - p);
    strcpy(validto_gmt + (q - p), "\0");
    q = validto_gmt + strlen(validto_gmt) - 1;
    for (; *q == ' '; q--) *q = '\0'; // remove ending whitespaces
    ////////
    if (DEBUG > 0) {
        //fprintf(stderr, "In method int parseDates(char *clear_license, char *validfrom_gmt, char *validto_gmt)\n");
        //fprintf(stderr, "FROM: [%s] TO: [%s]\n", validfrom_gmt, validto_gmt);
    }

    return LS_VALID;
}
////////////////////////////PUBLIC METHODS//////////////////////////////////////

/**
 * Can generate licenses for any date range - in the past or the future, for any number of days.
 * No error checking is done and is assumed to be done in the caller.
 *
 * @param clear_license IN
 * @param clear_len IN
 * @param encr_len IN
 * @param encr_license OUT
 * @param message_out OUT
 * @return
 */
extern "C" int GenerateLicense(char *clear_license, unsigned clear_len, unsigned encr_len, char *encr_license, char *message_out) {

    if (!clear_license || !encr_license || !message_out) {
        strncpy(message_out, STR_GENLIC_NULL_PARAMS, N_STR_GENLIC_NULL_PARAMS);
        return LS_INTERNAL_ERR;
    }

    if (31 > strlen(((char *) passPhrase))) {
        //fprintf(stderr, "GenerateLicense() not set with min 32 char pass phrase. Exiting.\n");
        return LS_INTERNAL_ERR;
    }

    if (encr_len < clear_len * ENCR_TO_CLEAR_RATIO) {
        strncpy(message_out, STR_CIPHER_LEN_LOW, N_STR_CIPHER_LEN_LOW);
        return LS_INTERNAL_ERR;
    }

    if (DEBUG > 0) {
        //fprintf(stderr, "method called: int GenerateLicense(const char *clear_license IN, char *encr_license OUT, char *message_out OUT)\n");
    }

    message_out[0] = '\0'; // reset outgoing message, just in case
    std::string ciphertext = EncryptString(clear_license, (char *) passPhrase);
    if(!ciphertext.length())
    {
        strncpy(message_out, STR_ENCRYPT_FAILED, N_STR_ENCRYPT_FAILED);
        return LS_INTERNAL_ERR;
    }

    if (DEBUG > 0) {
        //fprintf(stderr, "LICENSE FOLLOWS:\n %s \nCiphertext: %s\n", clear_license, ciphertext.c_str());
    }
    strncpy(encr_license, ciphertext.c_str(), ciphertext.length());

    return LS_VALID;
}

extern "C" lic_state isLicenseValid(char *clear_license, char *encr_license, char *message_out) {
 //   char validfrom_gmt[32], validto_gmt[32];

    if (!clear_license || !encr_license || !message_out) {
        //fprintf(stderr, "isLicenseValid() passed null parameters. Exiting.\n");
        return LS_FORMAT_INVALID;
    }

    if (31 > strlen((char *) passPhrase)) {
        //fprintf(stderr, "GenerateLicense() not set with min 32 char pass phrase. Exiting.\n");
        return LS_FORMAT_INVALID;
    }

    if (DEBUG > 0) {
        //fprintf(stderr, "method called: int isLicenseValid(char *clear_license IN, char *encr_license IN, char *message_out OUT)\n");
    }

    message_out[0] = '\0'; // reset outgoing message, just in case

    std::string decrypted = DecryptString(encr_license, (char *) passPhrase);
    if(!decrypted.length()) {
        strncpy(message_out, STR_DECRYPT_FAILED, N_STR_DECRYPT_FAILED);
        return LS_INTERNAL_ERR;
    }

    const char *strDecrypted = decrypted.c_str();

    if (0 != strncmp(clear_license, strDecrypted, strlen(clear_license))) {
        strncpy(message_out, STR_DECRYPT_NO_MATCH, N_STR_DECRYPT_NO_MATCH);
        //fprintf(stderr, "\nDecrypted: [%s]\n", strDecrypted);
        //fprintf(stderr, "Orig: [%s]\n", clear_license);
        return LS_KEY_MISMATCH;
    }
    /*
    enum lic_state ret;
    if (LS_VALID != 
	(ret = parseDates(clear_license, validfrom_gmt, validto_gmt))) {
        strncpy(message_out, STR_PARSE_DATE_FAILURE, N_STR_PARSE_DATE_FAILURE);
        return ret;
    }

    return verifyDates(validfrom_gmt, validto_gmt, message_out);
    */
    return LS_VALID;
}
#endif

 
enum lic_state mac_addr_matches(char *clear_license)
{
    int    i;
    int    itmp;
    enum lic_state    ret = LS_MAC_MISMATCH;
    int    sysret;
    int    match;
    char   fname[128];
    char   syscmd[256];
    char   line[1024];
    int    linelen = 1024;
    int    mac[6];
    int    mac_license[6];
    FILE  *f;
    char  *s;

    /* get the MAC address from the license */
#if 0
    s = strstr(clear_license, particulars_section[2]);
    if (s == NULL) {
	(void) fprintf(stderr, "License does not include a MAC address:\n%s\n", clear_license);
	return LS_FORMAT_INVALID;
    } else {
	    char *t = strstr(s, "\n");
	            s += strlen(particulars_section[2]);
		            strncpy(line, s, (int)(t-s));
			    	
	if (sscanf(line, "%x:%x:%x:%x:%x:%x",
	    &mac_license[0], &mac_license[1], &mac_license[2], 
	    &mac_license[3], &mac_license[4], &mac_license[5]) != 6)
	{
	    (void) fprintf(stderr, "License has an improperly formatted MAC address:\n%s\n", clear_license);
	    return LS_FORMAT_INVALID;
	}
    }
#endif
	if (sscanf(clear_license, "%x:%x:%x:%x:%x:%x",
	    &mac_license[0], &mac_license[1], &mac_license[2], 
	    &mac_license[3], &mac_license[4], &mac_license[5]) != 6)
	{
	    //(void) fprintf(stderr, "License has an improperly formatted MAC address:%s\n", clear_license);
	    return LS_FORMAT_INVALID;
	}

    /* check it against all MAC addresses in this system */

    sprintf(fname, "/tmp/mbmacXXXXXX");

    itmp = mkstemp(fname);
    if (itmp != -1) {
        sprintf(syscmd, "ip addr > %s", fname);
	sysret = system(syscmd);
	if (sysret == -1) {
	    //(void) fprintf(stderr, "'system' command failed to execute 'ip addr' to check MAC address\n");
	} else {
	    /*   The system command succeeded, so
	     *   open the file and compare MAC addresses against
	     *   the one provided in the license.
	     */
	    f = fdopen(itmp, "r");

	    while (fgets(line, linelen, f) != NULL) {
	        s = strstr(line, "link/ether");
		if (s != NULL) {
		    if (sscanf(s, "link/ether %x:%x:%x:%x:%x:%x",
		        &mac[0], &mac[1], &mac[2], 
			&mac[3], &mac[4], &mac[5]) == 6)
		    {
		        match = 1;
		        for (i=0; i<6; i++) {
			    if (mac[i] != mac_license[i]) {
			        match = 0;
				break;
			    }
			}
			if (match) {
			    ret = LS_VALID;
			    break;
			} else {
				ret = LS_MAC_MISMATCH;
			}
		    }
		}
	    }

	    if (unlink(fname) != 0) {
	        //(void) fprintf(stderr, "Could not unlink mac address check file '%s' (errno=%d, '%s')\n", fname, errno, strerror(errno));
	    }

	    if (fclose(f) != 0) {
	        //(void) fprintf(stderr, "Could not close mac address check file '%s' (errno=%d, '%s')\n", fname, errno, strerror(errno));
	    }
	}
    }
    return(ret);
}

#if 0
extern "C" lic_state  isLicenseValid2(char *clear_license, char *encr_license,
			char *product, char *version, int check_mac_addr,
			char *message_out) 
{
    lic_state ret = LS_VALID;


    ret = isLicenseValid(clear_license, encr_license, message_out);
    if (ret == LS_VALID) {
        /* check the product string if required */
	if (product != NULL) {
	    if (strstr(clear_license, product) == NULL) {
		/* product doesn't match */
		(void) sprintf(message_out, "License is not for this product.");
		ret = LS_PROD_MISMATCH;
	    }
	}
	/* product does match, so check the MAC address if required */
	if (check_mac_addr) {
	    if ((ret = mac_addr_matches(clear_license)) != LS_VALID) {
	        /* MAC address does not match! */
		(void) sprintf(message_out, "License is not for this machine.");
	    }
	}
    }

    return ret;
}
#endif

/* 
 * File:   zslicense.c
 * Author: Niranjan Neelakanta
 * Description: Command to generate license files.
 *
 * SanDisk Proprietary Material, Copyright 2013 SanDisk, all rights reserved.
 * http://www.sandisk.com
 */
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <getopt.h>
#include <fcntl.h>
#include <errno.h>
#include <limits.h>
#include "license/interface.h"

#define ZS_LICENSE_OUTFILE	"license"	/* Default license file */

static int Gflag;
static int Cflag;

static struct option options[] = {
	{"generate", no_argument, 0, 'g'},	/* Generate license file */
	{"check", no_argument, 0, 'c'},		/* Check license file */
	{"version", required_argument, 0, 'v'},	/* Version of format */
	{"output-file", required_argument, 0, 'o'},/* Output file */
	{"input-file", required_argument, 0, 'i'},/* Input file for license */
	{0, 0, 0, 0}
};
void usage();

int
main(int argc, char **argv)
{
	int		ch, option_index;
	int		major, minor;
	char		*version = NULL;
	char		*output_file = NULL;
	char		*input_file = NULL;

	if (argc <= 1) {
		usage();
		return 1;
	}

	while ((ch = getopt_long(argc, argv, "gcv:o:i:", options, &option_index)) 
									!= -1) {
		switch (ch)
		{
			case 0	:
				if (options[option_index].flag != 0)
					break;
				break;

			case 'g': Gflag = 1;
				  break;

			case 'c': Cflag = 1;
				  break;

			case 'v': 
				version = strdup(optarg);
				if (!version) {
					printf("Insufficient memory available\n");
					return 1;
				}
				break;

			case 'i':
				input_file = strdup(optarg);
				if (!input_file) {
					printf("Insufficient memory available\n");
					return 1;
				}
				break;

			case 'o':
				output_file = strdup(optarg);
				if (!output_file) {
					printf("Insufficient memory available\n");
					return 1;
				}
				break;

			default: printf("error\n");
				 break;
		}
	}
	/*
	 * "-c" and "-g" must not be given together. Either generate license
	 * file or check the existing file.
	 */
	if (Gflag == Cflag) {
		usage();
		return 1;
	} else {
		if (version) {
			if (sscanf(version, "%d.%d", &major, &minor) < 2) {
				fprintf(stderr, "Version parameter is not \
								in valid (<major>.<minor>) format\n");
				return -1;
			}
		}

		if (Gflag) {
			/*
			 * If input file specified, read it and generate cypher 
			 * text for it.
			 * If not, then generate blank file for inputs based on
			 * the version specified.
			 */
			if (input_file) {
				printf("Generating license for input file %s\n",
						input_file);
				return generate_license_for_file(input_file, 
						  output_file ? output_file :
						  	ZS_LICENSE_OUTFILE);
			} else if (output_file) {
				printf("Generating blank license file\n");
				return generate_license_file(version, output_file);
			} else {
				usage();
				return -1;
			}

		} else if (Cflag) {
			if (input_file) {
				printf("Validating the file %s\n", input_file);
				if ( check_license_file(input_file) == -1) {
					printf("License is invalid\n");
				} else {
					printf("License is valid\n");
				}
			} else {
				usage();
				return -1;
			}
		}
	}
	return 0;
}

void
usage()
{
	fprintf(stderr, 
	"Usage: zslicense --generate [-v format-version] -o output-file\n");
	fprintf(stderr,
	"                  --generate -i input-file [-o output-file]\n");
	fprintf(stderr,
	"                  --check -i input-file\n");
	return;
}

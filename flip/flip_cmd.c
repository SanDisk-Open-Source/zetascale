/* 

Copyright (c) 2008, 2010 QUE Hongyu
All rights reserved.
 
Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions
are met:
1. Redistributions of source code must retain the above copyright
   notice, this list of conditions and the following disclaimer.
2. Redistributions in binary form must reproduce the above copyright
   notice, this list of conditions and the following disclaimer in the
   documentation and/or other materials provided with the distribution.
 
THIS SOFTWARE IS PROVIDED BY AUTHOR AND CONTRIBUTORS ``AS IS'' AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
ARE DISCLAIMED.  IN NO EVENT SHALL AUTHOR OR CONTRIBUTORS BE LIABLE
FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
SUCH DAMAGE.
*/

#ifdef FLIP_ENABLED
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <assert.h>
#include <pthread.h>
#include "flip.h"

extern pthread_rwlock_t flip_lock;

static void parse_token_option_value(char *str, char delim, char **popt, char **pvalue)
{
	*popt = str;

	while (*str != '\0') {
		if (*str == delim) {
			*str = '\0';
			*pvalue = str + 1;
		}
		str++;
	}
}

static void flip_cmd_set(FILE *fp, cmd_token_t *tokens, size_t ntokens, bool is_modify)
{
	flip_info_t *f;
	flip_cond_t *fc;
	size_t t = 0;
	int flip_cnt = 1; /* Default one flip */
	char *option;
	char *value = NULL;
	uint32_t i;
	uint32_t pr;
	flip_param_t *p;

	if(is_modify) {
		fprintf(fp, "Flip modify is not supported yet\n");
		return;
	}

	pthread_rwlock_wrlock(&flip_lock);

	f = lookup_flip_instance(tokens[t++].value);
	if (f == NULL) {
		fprintf(fp, "Error: No such flip '%s' defined\n", tokens[0].value);
		pthread_rwlock_unlock(&flip_lock);
		return;
	}

	for (i = 0; i < f->cond_cnt; i++) {
		fc = &f->conditions[i];
		if (!fc->is_set) {
			break;
		}
	}

	if (i == f->cond_cnt) {
		assert(i < MAX_COND_PER_FLIP);

		fc = &f->conditions[i];
		f->cond_cnt++; // TODO: Sync fetch and add
	}

	/* Only for first condition add would have initialized.
	 * Anything else have to be initialized again */
	if (i != 0) {
		flip_cond_t *fc0 = &f->conditions[0];

		/* Set the default return parameter */
		fc->return_param.data_type = fc0->return_param.data_type;
		fc->return_param.name =  "return";
		fc->return_param.data = (void *)(int64_t)true;
		fc->return_param.any_data = 0;

		/* Set all the params */
		for (pr = 0; pr < f->num_params; pr++) {
			p = &fc->param_list[pr];

			p->data_type = fc0->param_list[pr].data_type;
			p->name = fc0->param_list[pr].name;
			p->any_data = true;
		}
	}
#if 0
	if (!is_modify && f->is_set) {
		fprintf(fp, "Warning: Flip '%s' already set for a condition. "
		            "Multiple conditions for same flip is not supported yet\n"
		            "Hence trying to overriding the condition\n", f->name);
	}
#endif

	while (t < ntokens) {
		parse_token_option_value(tokens[t].value, '=', &option, &value);
		if (strcmp(option, "--count") == 0) {
			flip_cnt = atoi(value);
		} else if (strcmp(option, "--set") == 0) {
			continue;
		} else {
			flip_set_param(f, fc, option, value);
		}
		t++;
	}
	fc->count = flip_cnt;
	fc->is_set = true;

	fprintf(fp, "Flip '%s' is %s\n", tokens[0].value, 
	           is_modify ? "modified" : "set");

	pthread_rwlock_unlock(&flip_lock);

	/* TODO: Modularize the flip file variable */
//	flip_dump_file(FLIP_FILE);
}

static void flip_cmd_add(FILE *fp, cmd_token_t *tokens, size_t ntokens)
{
	flip_info_t *f;
	flip_cond_t *fc;
	size_t t = 0;
	int flip_cnt = 1; /* Default one flip */
	char *option;
	char *value = NULL;
	uint32_t valid_params = 0;
	flip_type_t type;
	flip_param_t *p;

	pthread_rwlock_wrlock(&flip_lock);
	f = lookup_flip_instance(tokens[t].value);
	if (f != NULL) {
		fprintf(fp, "Warning: flip '%s' already defined. Setting value\n", tokens[0].value);
		pthread_rwlock_unlock(&flip_lock);
		flip_cmd_set(fp, tokens, ntokens, true);
		return;
	} else {
		f = get_new_flip_instance();
		f->cond_cnt = 1;
		fc = &f->conditions[0];
	}

	strcpy(f->name, tokens[t++].value);
	f->num_params = 0;

	fc->is_set = false;

	/* Set the default return parameter */
	fc->return_param.data_type = FLIP_BOOL;
	fc->return_param.name = "return";
	fc->return_param.data = (void *)(int64_t)true;
	fc->return_param.any_data = 0;

	while (t < ntokens) {
		/* Get the data type if present */
		type = flip_str_to_type(tokens[t].value);
		if (type == 0) {
			type = FLIP_BOOL;
		} else {
			t++;
		}

		parse_token_option_value(tokens[t].value, '=', &option, &value);
		if (strcmp(option, "--count") == 0) {
			flip_cnt = atoi(value);
		} else if (strcmp(option, "--set") == 0) {
			if (strcmp(value, "true") == 0) {
				fc->is_set = true;
			}
		} else {
			/* Set the new parameter */
			if (strcmp(option, "return") == 0) {
				p = &fc->return_param;
			} else {
				p = &fc->param_list[f->num_params++];
			}
			p->data_type = type;
			p->name = malloc(strlen(option)+1);
			assert(p->name);
			strcpy(p->name, option);

			if (value && strlen(value) != 0) {
				valid_params++;
				flip_set_param_ptr(p, value);
			} else {
				p->any_data = true;
			}
		}
		t++;
	}
	fc->count = flip_cnt;
	pthread_rwlock_unlock(&flip_lock);

	fprintf(fp, "Flip '%s' is added\n", f->name);
}

static void flip_cmd_list(FILE *fp, cmd_token_t *tokens, size_t ntokens)
{
	flip_info_t *f = NULL;
	flip_cond_t *fc = NULL;
	int i = 0;
	uint32_t c;
	int j;
	bool list_set_only = false;
	flip_param_t *p = NULL;
	size_t t = 0;

	if ((ntokens != 0) && (strcmp(tokens[t].value, "--set-only") == 0))
		list_set_only = true;

	fprintf(fp, "# Flip List\n");
	fprintf(fp, "#--------------\n");

	while ((f = get_flip_instance(i++)) != NULL) {

		for (c = 0; c < f->cond_cnt; c++) {
			fc = &f->conditions[c];

			if (list_set_only && !fc->is_set) {
				continue;
			}

			fprintf(fp, "%s ", f->name);

			for (j = 0; j < f->num_params; j++) {
				p = &fc->param_list[j];
				flip_print_param(fp, f, fc, p->name);
			}
			flip_print_param(fp, f, fc, "return");

			if (fc->is_set) {
				fprintf(fp, "--count=%d --set=true\n", fc->count);
			} else {
				fprintf(fp, "--set=false\n");
			}
		}
	}
}

static void flip_cmd_reset(FILE *fp, cmd_token_t *tokens, size_t ntokens)
{
	flip_info_t *f = NULL;
	flip_cond_t *fc = NULL;
	int i = 0;
	uint32_t c;
	int j;
	bool list_set_only = false;
	flip_param_t *p = NULL;
	size_t t = 0;

	while ((f = get_flip_instance(i++)) != NULL) {
		for (c = 0; c < f->cond_cnt; c++) {
			fc = &f->conditions[c];

			if (!fc->is_set) {
				continue;
			}

			fc->is_set = false;
			fprintf(fp, "%s condition %u is reset", f->name, c);
		}
	}
}


static size_t tokenize_flip_cmd(char *command, cmd_token_t *tokens, 
                                const size_t max_tokens) 
{
    char *s, *e;
    size_t ntokens = 0;

    for (s = e = command; ntokens < max_tokens - 1; ++e) {
        if ((*e == ' ')  || (*e == '\n')) {
            if (s != e) {
                tokens[ntokens].value = s;
                tokens[ntokens].length = e - s;
                ntokens++;
                *e = '\0';
            }
            s = e + 1;
        }
        else if (*e == '\0') {
            if (s != e) {
                tokens[ntokens].value = s;
                tokens[ntokens].length = e - s;
                ntokens++;
            }
            
            break; /* string end */
        }
    }
    
#if 0
    /*
     * If we scanned the whole string, the terminal value pointer is null,
     * otherwise it is the first unprocessed character.
     */
    tokens[ntokens].value =  *e == '\0' ? NULL : e;
    tokens[ntokens].length = 0;
    ntokens++;
#endif

    return ntokens;
}

/* Handle all the flip commands */
void process_flip_cmd(FILE *fp, cmd_token_t *tokens, size_t ntokens)
{
	assert(strcmp(tokens[0].value, "flip") == 0);
	if (ntokens < 2) {
		fprintf(fp, "flip set <flip_cmd> [--count=<count>]\n");
		fprintf(fp, "flip list [--set-only]\n");
		fprintf(fp, "flip modify <flip_cmd> [--count=<count>]\n");
		return;
	}

	if (strcmp(tokens[1].value, "set") == 0) {
		flip_cmd_set(fp, &tokens[2], ntokens - 2, false /* is_modify */);
	} else if (strcmp(tokens[1].value, "list") == 0) {
		flip_cmd_list(fp, &tokens[2], ntokens - 2);
	} else if (strcmp(tokens[1].value, "reset") == 0) {
		flip_cmd_reset(fp, &tokens[2], ntokens - 2);
	} else if (strcmp(tokens[1].value, "modify") == 0) {
		flip_cmd_set(fp, &tokens[2], ntokens - 2, true /* is_modify */);
	} else {
		fprintf(fp, "Invalid command '%s'\n", tokens[1].value);
	}
}

void process_flip_cmd_str(FILE *fp, char *cmd)
{
	cmd_token_t tokens[MAX_FLIP_CMD_WORDS];
	size_t ntokens;

	ntokens = tokenize_flip_cmd(cmd, tokens, MAX_FLIP_CMD_WORDS);
	process_flip_cmd(fp, tokens, ntokens);
}

void flip_parse_file(char *filename)
{
	FILE *fp;
	char line[MAX_FLIP_LINE_LEN];
	cmd_token_t tokens[MAX_FLIP_CMD_WORDS];
	size_t ntokens;

	fp = fopen(filename, "r+");
	if (fp == NULL) {
		fprintf(stderr, "Error: Unable to open "
		        "flip file %s\n", filename);
		return;
	}

	while (fgets(line, MAX_FLIP_LINE_LEN, fp)) {
		if (line[0] == '#') {
			continue;
		}

		/* Add the flip into the database */
		ntokens = tokenize_flip_cmd(line, tokens, MAX_FLIP_CMD_WORDS);
		if (ntokens == 0) {
			continue;
		}
//		printf("ntokens = %u\n", (uint32_t)ntokens);
		flip_cmd_add(stderr, tokens, ntokens);
	}
	fclose(fp);
}

void flip_dump_file(char *filename)
{
	FILE *fp;

	fp = fopen(filename, "w+");
	if (fp == NULL) {
		fprintf(stderr, "Error: Unable to open "
		        "flip file %s\n", filename);
		return;
	}

	/* Dump the flips into the file */
	flip_cmd_list(fp, NULL, 0);
	fclose(fp);

}

#endif

#ifdef FLIP_ENABLED
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <assert.h>
#include "flip.h"

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
	size_t t = 0;
	int flip_cnt = 1; /* Default one flip */
	char *option;
	char *value = NULL;

	f = lookup_flip_instance(tokens[t++].value);
	if (f == NULL) {
		fprintf(fp, "Error: No such flip '%s' defined\n", tokens[0].value);
		return;
	}

	if (!is_modify && f->is_set) {
		fprintf(fp, "Warning: Flip '%s' already set for a condition. "
		            "Multiple conditions for same flip is not supported yet\n"
		            "Hence trying to overriding the condition\n", f->name);
	}

	while (t < ntokens) {
		parse_token_option_value(tokens[t].value, '=', &option, &value);
		if (strcmp(option, "--count") == 0) {
			flip_cnt = atoi(value);
		} else if (strcmp(option, "--set") == 0) {
			continue;
		} else {
			flip_set_param(f, option, value);
		}
		t++;
	}
	f->count = flip_cnt;
	f->is_set = true;

	fprintf(fp, "Flip '%s' is %s\n", tokens[0].value, 
	           is_modify ? "modified" : "set");

	/* TODO: Modularize the flip file variable */
	flip_dump_file(FLIP_FILE);
}

static void flip_cmd_add(FILE *fp, cmd_token_t *tokens, size_t ntokens)
{
	flip_info_t *f;
	size_t t = 0;
	int flip_cnt = 1; /* Default one flip */
	char *option;
	char *value = NULL;
	uint32_t valid_params = 0;
	flip_type_t type;
	flip_param_t *p;

	f = lookup_flip_instance(tokens[t].value);
	if (f != NULL) {
		fprintf(fp, "Warning: flip '%s' already defined. Setting value\n", tokens[0].value);
		flip_cmd_set(fp, tokens, ntokens, true);
		return;
	} else {
		f = get_new_flip_instance();
	}

	strcpy(f->name, tokens[t++].value);
	f->num_params = 0;
	f->is_set = false;

	/* Set the default return parameter */
	f->return_param.data_type = FLIP_BOOL;
	f->return_param.name = "return";
	f->return_param.data = (void *)(int64_t)true;
	f->return_param.any_data = 0;

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
				f->is_set = true;
			}
		} else {
			/* Set the new parameter */
			if (strcmp(option, "return") == 0) {
				p = &f->return_param;
			} else {
				p = &f->param_list[f->num_params++];
			}
			p->data_type = type;
			p->name = malloc(strlen(option)+1);
			assert(p->name);
			strcpy(p->name, option);

			if (value && strlen(value) != 0) {
				valid_params++;
				flip_set_param_ptr(p, value);
			}
		}
		t++;
	}
	f->count = flip_cnt;

	fprintf(fp, "Flip '%s' is added\n", f->name);
}

static void flip_cmd_list(FILE *fp, cmd_token_t *tokens, size_t ntokens)
{
	flip_info_t *f = NULL;
	int i = 0;
	int j;
	bool list_set_only = false;
	flip_param_t *p = NULL;
	size_t t = 0;

	if ((ntokens != 0) && (strcmp(tokens[t].value, "--set-only") == 0))
		list_set_only = true;

	fprintf(fp, "# Flip List\n");
	fprintf(fp, "#--------------\n");

	while ((f = get_flip_instance(i++)) != NULL) {

		if (list_set_only && !f->is_set) {
			continue;
		}

		fprintf(fp, "%s ", f->name);

		for (j = 0; j < f->num_params; j++) {
			p = &f->param_list[j];
			flip_print_param(fp, f, p->name);
		}
		flip_print_param(fp, f, "return");

		if (f->is_set) {
			fprintf(fp, "--count=%d --set=true\n", f->count);
		} else {
			fprintf(fp, "--set=false\n");
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
	} else if (strcmp(tokens[1].value, "modify") == 0) {
		flip_cmd_set(fp, &tokens[2], ntokens - 2, true /* is_modify */);
	} else {
		fprintf(fp, "Invalid command '%s'\n", tokens[1].value);
	}
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
		if (line[0] == '#') 
			continue;

		printf("Line read: %s\n", line);

		/* Add the flip into the database */
		ntokens = tokenize_flip_cmd(line, tokens, MAX_FLIP_CMD_WORDS);
		printf("ntokens = %u\n", (uint32_t)ntokens);
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

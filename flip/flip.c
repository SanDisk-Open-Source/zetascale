#ifdef FLIP_ENABLED
#include <stdio.h>
#include <stdbool.h>
#include <stdlib.h>
#include <inttypes.h>
#include <stdarg.h>
#include <assert.h>
#include <string.h>
#include "flip.h"
//#include "flip_defs.h"

#if 0
static flip_info_t defined_flips[] = 
{
	{
		.name = "sw_crash_on_mput", 
		.return_param = 
		{ 
			.name = "return",
			.data_type = FLIP_BOOL
		},
		.num_params  = 1, 
		.is_set     = false,
		.param_list = 
		{
			{ 
				.data_type = FLIP_UINT32,
				.name = "descend",
			},
		}
	},

	{
		.name = "hw_crash_on_mput", 
		.return_param =
		{ 
			.name = "return",
			.data_type = FLIP_BOOL
		},
		.num_params  = 1, 
		.is_set     = false,
		.param_list = 
		{
			{ 
				.data_type = FLIP_UINT32,
				.name = "descend",
			},
		}
	}
};
#endif

flip_info_t defined_flips[MAX_FLIP_POINTS];
uint32_t cur_flip_cnt;

static bool flip_check_param_va_list(flip_param_t *p, va_list arg_list)
{
	int ival;
	uint32_t u32_val;
	uint64_t u64_val;
	char *str_val;
	bool bval;

	if (p->data_type == FLIP_INT) {
		ival = va_arg(arg_list, int);
		return (p->data == (void *)(int64_t)ival);
	} else if (p->data_type == FLIP_UINT32) {
		u32_val = va_arg(arg_list, uint32_t);
		return (p->data == (void *)(uint64_t)u32_val);
	} else if (p->data_type == FLIP_UINT64) {
		u64_val = va_arg(arg_list, uint64_t);
		return (p->data == (void *)(uint64_t)u64_val);
	} else if (p->data_type == FLIP_BOOL) {
		bval = va_arg(arg_list, int);
		return (p->data == (void *)(int64_t)bval);
	} else if (p->data_type == FLIP_STR) {
		str_val = va_arg(arg_list, char *);
		return (strcmp((char *)p->data, str_val) != 0);
	}

	return (false);
}

#if 0
static void flip_set_param_va_list(flip_param_t *p, va_list arg_list)
{
	int ival;
	uint32_t u32_val;
	uint64_t u64_val;
	char *str_val;
	bool bval;

	if (p->data_type == FLIP_INT) {
		ival = va_arg(arg_list, int);
		p->data = (void *)(int64_t)ival;
	} else if (p->data_type == FLIP_UINT32) {
		u32_val = va_arg(arg_list, uint32_t);
		p->data = (void *)(uint64_t)u32_val;
	} else if (p->data_type == FLIP_UINT64) {
		u64_val = va_arg(arg_list, uint64_t);
		p->data = (void *)u64_val;
	} else if (p->data_type == FLIP_BOOL) {
		bval = va_arg(arg_list, int);
		p->data = (void *)(int64_t)bval;
	} else if (p->data_type == FLIP_STR) {
		str_val = va_arg(arg_list, char *);
		p->data = (void *)str_val;
	} else {
		assert(0);
	}
}
#endif

bool flip_set_param_ptr(flip_param_t *p, char *val_str)
{
	int ival;
	uint32_t u32_val;
	uint64_t u64_val;
	int bval;

	p->any_data = 0;
	if (val_str[0] == '*') {
		p->any_data = 1;
	} else if (p->data_type == FLIP_INT) {
		sscanf(val_str, "%d", &ival);
		p->data = (void *)(int64_t)ival;
	} else if (p->data_type == FLIP_UINT32) {
		sscanf(val_str, "%u", &u32_val);
		p->data = (void *)(uint64_t)u32_val;
	} else if (p->data_type == FLIP_UINT64) {
		sscanf(val_str, "%"PRIu64, &u64_val);
		p->data = (void *)u64_val;
	} else if (p->data_type == FLIP_BOOL) {
		sscanf(val_str, "%d", &bval);
		p->data = (void *)(int64_t)bval;
	} else if (p->data_type == FLIP_STR) {
		if (p->data != NULL) {
			free(p->data);
		}
		p->data = malloc(strlen(val_str) + 1);
		assert(p->data);
		strcpy(p->data, val_str);
	} else {
		assert(0);
	}

	return true;
}

static void flip_get_param_va_list(flip_param_t *p, va_list arg_list)
{
	void *data = va_arg(arg_list, void *);

	if (p->data_type == FLIP_INT) {
		memcpy(data, &p->data, sizeof(int));
	} else if (p->data_type == FLIP_UINT32) {
		memcpy(data, &p->data, sizeof(uint32_t));
	} else if (p->data_type == FLIP_UINT64) {
		memcpy(data, &p->data, sizeof(uint64_t));
	} else if (p->data_type == FLIP_BOOL) {
		memcpy(data, &p->data, sizeof(int));
	} else if (p->data_type == FLIP_STR) {
		char *d = va_arg(arg_list, char *);
		strcpy(d, (char *)p->data);
	} else {
		assert(0);
	}
}

/* Exported functions, but used only within the internals of flip */
flip_param_t *lookup_flip_param(flip_info_t *f, char *param_str)
{
	flip_param_t *p;
	uint32_t i;

	if (strcmp(param_str, "return") == 0) {
		return (&f->return_param);
	}

	for (i = 0; i < f->num_params; i++) {
		p = &f->param_list[i];
		if (strcmp(p->name, param_str) == 0) {
			return p;
		}
	}

	return NULL;
}

flip_info_t *get_new_flip_instance(void)
{
	return (&defined_flips[cur_flip_cnt++]);
}

flip_info_t *get_flip_instance(int index)
{
	flip_ioctl_data_t d;

	d.flip_ioctl_type = FLIP_IOCTL_GET_INSTANCE;
	d.index = index;

	(void) FDFIoctl(NULL, 0, FDF_IOCTL_FLIP, &d);
	return (d.flip_instance);
}

flip_info_t *lookup_flip_instance(char *name)
{
	int i = 0;
	flip_info_t *f;

	while ((f = get_flip_instance(i++)) != NULL) {
		if (strcmp(name, f->name) == 0) {
			return (f);
		}
	}
		
	return (NULL);
}

static void flip_handle_ioctl_get_instance(flip_ioctl_data_t *d)
{
	if (d->index >= cur_flip_cnt) {
		d->flip_instance = NULL;
		return;
	}

	d->flip_instance = &defined_flips[d->index];
}

void flip_handle_ioctl(void *opaque)
{
	flip_ioctl_data_t *d = (flip_ioctl_data_t *)opaque;

	if (d->flip_ioctl_type == FLIP_IOCTL_GET_INSTANCE) {
		flip_handle_ioctl_get_instance(d);
	}
}

char *flip_type_to_str(flip_param_t *p)
{
	if (p->data_type == FLIP_BOOL) {
		return "bool";
	} else if (p->data_type == FLIP_INT) {
		return "int";
	} else if (p->data_type == FLIP_UINT32) {
		return "uint32";
	} else if (p->data_type == FLIP_UINT64) {
		return "uint64";
	} else if (p->data_type == FLIP_STR) {
		return "char *";
	} else {
		return "";
	}
}

flip_type_t flip_str_to_type(char *str_type)
{
	if (strcmp(str_type, "bool") == 0) {
		return FLIP_BOOL;
	} else if (strcmp(str_type, "int") == 0) {
		return FLIP_INT;
	} else if (strcmp(str_type, "uint32") == 0) {
		return FLIP_UINT32;
	} else if (strcmp(str_type, "uint64") == 0) {
		return FLIP_UINT64;
	} else if (strcmp(str_type, "char *") == 0) {
		return FLIP_STR;
	} else {
		return 0;
	}
}

bool flip_is_valid_param(flip_info_t *f, char *param_str)
{
	return (lookup_flip_param(f, param_str) != NULL);
}

bool flip_set_param(flip_info_t *f, char *param_str, char *val_str)
{
	/* Search for the param name in the list */
	flip_param_t *p = lookup_flip_param(f, param_str);
	if (p == NULL) {
		return false;
	}

	return (flip_set_param_ptr(p, val_str));
}

void flip_print_param(FILE *fp, flip_info_t *f, char *param_str)
{
	char *type;

	flip_param_t *p = lookup_flip_param(f, param_str);
	if ((p == NULL) || (p->data_type == 0)) {
		return;
	}

	/* No need to print boolean return */
/*	if ((p->data_type == FLIP_BOOL) && (strcmp(p->name, "return") == 0)) {
		return;
	} */

	type = flip_type_to_str(p);

	if (!f->is_set) {
		fprintf(fp, "%s %s ", type, param_str);
		return;
	}

	fprintf(fp, "%s %s=", type, param_str);
	if (p->any_data) {
		fprintf(fp, "%s ", "*");
	} else if (p->data_type == FLIP_INT) {
		fprintf(fp, "%d ", (int)(int64_t)p->data);
	} else if (p->data_type == FLIP_UINT32) {
		fprintf(fp, "%u ", (uint32_t)(uint64_t)p->data);
	} else if (p->data_type == FLIP_UINT64) {
		fprintf(fp, "%"PRIu64" ", (uint64_t)p->data);
	} else if (p->data_type == FLIP_BOOL) {
		fprintf(fp, "%d ", (int)(int64_t)p->data);
	} else if (p->data_type == FLIP_STR) {
		fprintf(fp, "%s ", (char *)p->data);
	} else {
		assert(0);
	}
}

#if 0
bool flip_get_param(flip_info_t *f, char *param_str, char **ptype, char **pdata)
{
	char *data;

	flip_param_t *p = lookup_flip_param(f, param_str);
	if (p == NULL) {
		return false;
	}

	*ptype = flip_type_to_str(p);
	if (!f->is_set) {
		return true;
	}

	if (p->any_data) {
		sprintf(*data_str, "%s", "*");
		return true;
	}

	if (p->data_type == FLIP_STR) {
		*data_str = p->data;
		return true;
	}

	*data_str = malloc(sizeof(uint64_t));
	if (p->data_type == FLIP_INT) {
		sprintf(*data_str, "%d", (int)(int64_t)p->data);
	} else if (p->data_type == FLIP_UINT32) {
		sprintf(*data_str, "%u", (uint32_t)(uint64_t)p->data);
	} else if (p->data_type == FLIP_UINT64) {
		sprintf(*data_str, "%"PRIu64, (uint64_t)p->data);
	} else if (p->data_type == FLIP_BOOL) {
		sprintf(*data_str, "%d", (int)(int64_t)p->data);
	} else {
		assert(0);
	}
}
#endif

/* Exported functions used by outside modules */
void flip_init()
{
	flip_parse_file(FLIP_FILE);
}

bool flip_get(char *fname, ...)
{
	uint32_t j;
	va_list arg_list;
	flip_info_t *f = NULL;
	flip_param_t *p = NULL;
	bool is_on = true;

	f = lookup_flip_instance(fname);
	if (f == NULL) {
		return 0;
	}

	if (!f->is_set) 
		return 0;

	/* Get the parameters based on its type. */
	va_start(arg_list, fname);

	for (j = 0; j < f->num_params; j++) {
		p = &f->param_list[j];

		/* Check each parameter values defined earlier 
		 * matches input in this flip */
		if (!flip_check_param_va_list(p, arg_list)) {
			is_on = false;
			break;
		}
	}

	if (is_on) {
		if (f->return_param.data_type != FLIP_BOOL) {
			flip_get_param_va_list(&f->return_param, arg_list);
		}
		f->count--;
		if (f->count == 0) {
			f->is_set = false;
			flip_dump_file(FLIP_FILE);
		}

		printf("flip '%s' is hit\n", fname);
	}

	va_end(arg_list);
	return (is_on);
}

#if 0
int flip_set(char *name, ...)
{
	int i;
	uint32_t j;
	va_list arg_list;
	flip_info_t *f = NULL;

	f = flip_str_to_info(name);
	if (f == NULL) {
		fprintf(stderr, "flip %s not defined\n", name);
		return -1;
	}

	/* Get the parameters based on its type. */
	va_start(arg_list, name);

	for (j = 0; j < f->num_params; j++) {
		set_param(&f->param_list[j], arg_list);
	}

	if (f->return_param.data_type != FLIP_BOOL) {
		set_param(&f->return_param, arg_list);
	}

	f->is_set = true;
	va_end(arg_list);

	return (0);
}
#endif
#endif

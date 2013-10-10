/************************************************************************
 * 
 *  btest_range.c  Jan. 21, 2013   Harihara Kadayam
 * 
 *  Built-in self-Test program for btree range query.
 * 
 * NOTES: xxxzzz
 * 
 ************************************************************************/

#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <string.h>
#include "btest_common.h"
#include "../btree_range.h"
#include "../btree_raw.h"
#include "../btree_internal.h"

enum {
	NONE = 0,
	LESS_THAN = 1,
	LESS_THAN_EQUALS,
	GREATER_THAN,
	GREATER_THAN_EQUALS,
};

uint32_t flags = 0;
uint64_t begin_value = 0;
uint64_t end_value = 0;
int      chunks  = 1;
uint32_t start_seq = 0;
uint32_t end_seq = 0;

int btest_range_parse(btest_cfg_t *cfg, int argc, char **argv);

void btest_range_test(btest_cfg_t *cfg)
{
	btree_status_t status;
	btree_range_cursor_t *cursor;
	btree_range_meta_t rmeta;
	btree_range_data_t *values;
	int i;
	int n_in;
	int n_out;
	int n_in_iter = 0;
	int n_in_chunk;

	/* Initialize rmeta */
	rmeta.flags = flags;
	rmeta.key_start = NULL;
	rmeta.key_end = NULL;

	if (begin_value != 0) {
		rmeta.key_start = (char *)malloc(cfg->max_key_size);
		assert(rmeta.key_start);
		sprintf(rmeta.key_start, "%08"PRIu64"", (uint64_t)begin_value);
		rmeta.keylen_start = strlen(rmeta.key_start) + 1;
	}

	if (end_value != 0) {
		rmeta.key_end = (char *)malloc(cfg->max_key_size);
		assert(rmeta.key_end);
		sprintf(rmeta.key_end, "%08"PRIu64"", (uint64_t)end_value);
		rmeta.keylen_end = strlen(rmeta.key_end) + 1;
	}

	if (end_seq != 0) {
		rmeta.end_seq = end_seq;
		if (start_seq == 0) {
			rmeta.flags |= RANGE_SEQNO_LE;
		} else {
			rmeta.flags |= RANGE_SEQNO_GT_LE;
			rmeta.start_seq = start_seq;
		}
	}

	status = btree_range_query_start(cfg->bt, BTREE_RANGE_PRIMARY_INDEX, &cursor, &rmeta);
	if (status != BTREE_SUCCESS) {
		fprintf(stderr, "start range query failed with status = %d\n", status);
		exit(1);
	}

	/* Allocate for sufficient values */
	if ((begin_value == 0) && (end_value == 0)) {
		n_in = cfg->n_test_keys;
	} else if (end_value == 0) {
		n_in = cfg->n_test_keys - begin_value + 2;
	} else {
		n_in = end_value - begin_value;
		if (n_in < 0) {
			n_in = n_in * (-1);
		}
		n_in += 2; /* count for <= and >= cases */
	}

	n_in_chunk = n_in / chunks + n_in % chunks;
	printf("Each chunk of n_in %d\n", n_in_chunk);
	while (n_in_iter < n_in) {
		values = (btree_range_data_t *)malloc(sizeof(btree_range_data_t) * n_in_chunk);
		assert(values);

		/* Do the query now */
		status = btree_range_get_next(cursor, n_in_chunk, &n_out, values);
		printf("Status of range query: %d\n", status);
		printf("n_out = %d\n", n_out);

		for (i = 0; i < n_out; i++) {
			printf("%-4d: status=%d ", i, values[i].status);
			if (values[i].status == BTREE_RANGE_SUCCESS) {
				printf("Key=%s Keylen=%d Seqno=%"PRIu64"\n", values[i].key, values[i].keylen, values[i].seqno);
			} else {
				printf("\n");
			}
		}
		n_in_iter += n_in_chunk;
	}

	status = btree_range_query_end(cursor);
}

static void btest_range_usage(void)
{
	fprintf(stderr, "Additional options for btest_ranges\n");
	fprintf(stderr, "\t{-blt | -ble | -bgt | -bge } <begin key> { -elt | -ele | -egt | -ege } <end key> [-c <chunks>]\n");
	fprintf(stderr, "\t   these options stands for \n");
	fprintf(stderr, "\t   -b: begin, -e: end\n");
	fprintf(stderr, "\t   -lt: less than\n");
	fprintf(stderr, "\t   -le: less than equals\n");
	fprintf(stderr, "\t   -gt: greater than\n");
	fprintf(stderr, "\t   -gt: greater than equals\n");
	fprintf(stderr, "\t   -gt: greater than equals\n");
	fprintf(stderr, "\t   -c: how many chunks query range should be given\n");
	fprintf(stderr, "\n=============================================================================\n");
}

int btest_range_parse(btest_cfg_t *cfg, int argc, char **argv)
{
	int error;
	int i;
	char str[200];

	error = btest_basic_parse(cfg, argc, argv);
	if (error) {
		btest_range_usage();
		return error;
	}

	strcpy(str, "Range query with paramters:\n");
	strcat(str, "----------------------------\n");
	for (i=1; i<argc; i++) {
		if (argv[i][0] != '-') {
			continue;
		}

		switch (argv[i][1]) {
		case 'b': 
			if (strcmp(argv[i], "-blt") == 0) {
				strcat(str, "start < ");
				flags |= RANGE_START_LT;
			} else if (strcmp(argv[i], "-ble") == 0) {
				strcat(str, "start <= ");
				flags |= RANGE_START_LE;
			} else if (strcmp(argv[i], "-bgt") == 0) {
				strcat(str, "start > ");
				flags |= RANGE_START_GT;
			} else if (strcmp(argv[i], "-bge") == 0) {
				strcat(str, "start >= ");
				flags |= RANGE_START_GE;
			} else {
				btest_common_usage("btree_range_test");
				btest_range_usage();
				return (1);
			}
			sscanf(argv[i+1], "%"PRIu64"", &begin_value);
			strcat(str, argv[i+1]);
			strcat(str, "\n");
			break;
		case 'e':
			if (strcmp(argv[i], "-elt") == 0) {
				strcat(str, "end < ");
				flags |= RANGE_END_LT;
			} else if (strcmp(argv[i], "-ele") == 0) {
				strcat(str, "end <= ");
				flags |= RANGE_END_LE;
			} else if (strcmp(argv[i], "-egt") == 0) {
				strcat(str, "end > ");
				flags |= RANGE_END_GT;
			} else if (strcmp(argv[i], "-ege") == 0) {
				strcat(str, "end >= ");
				flags |= RANGE_END_GE;
			} else {
				btest_common_usage("btree_range_test");
				btest_range_usage();
				return (1);
			}
			sscanf(argv[i+1], "%"PRIu64"", &end_value);
			strcat(str, argv[i+1]);
			strcat(str, "\n");
			break;
		case 'c':
			chunks = atoi(argv[i+1]);
			if (chunks == 0) {
				chunks = 1;
			}
			break;
		case 'q':
			sscanf(argv[i+1], "%u,%u", &start_seq, &end_seq);
			if (end_seq == 0) {
				btest_common_usage("btree_range_test");
				btest_range_usage();
				return (1);
			}
			break;
		}
		i++;
	}
	
	printf("%s", str);
	printf("----------------------------\n");
	return (0);
}

int main(int argc, char *argv[])
{
	btest_cfg_t *cfg;

	cfg = btest_init(argc, argv, "btree_range_test", btest_range_parse);
	if (cfg == NULL) {
		exit(1);
	}
	
	btest_serial_data_gen(cfg);
	btest_range_test(cfg);

	return 0;
}

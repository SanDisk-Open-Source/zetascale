//----------------------------------------------------------------------------
// ZetaScale
// Copyright (c) 2016, SanDisk Corp. and/or all its affiliates.
//
// This program is free software; you can redistribute it and/or modify it under
// the terms of the GNU Lesser General Public License version 2.1 as published by the Free
// Software Foundation;
//
// This program is distributed in the hope that it will be useful, but WITHOUT
// ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
// FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License v2.1 for more details.
//
// A copy of the GNU Lesser General Public License v2.1 is provided with this package and
// can also be found at: http://opensource.org/licenses/LGPL-2.1
// You should have received a copy of the GNU Lesser General Public License along with
// this program; if not, write to the Free Software Foundation, Inc., 59 Temple
// Place, Suite 330, Boston, MA 02111-1307 USA.
//----------------------------------------------------------------------------

#include "common/sdftypes.h"
#include "platform/prng.h"
#include "platform/time.h"
#include "platform/assert.h"
#include "platform/stdlib.h"
#include "platform/stdio.h"
#include "platform/string.h"
#include "fth/fth.h"
#include "platform/fth_scheduler.h"
#include "platform/mbox_scheduler.h"
#include "../../../shared/shard_meta.h"

#include "../test_model.h"
#include "../test_config.h"
#include "../test_common.h"

typedef struct replication_test_model rtm_t;

#define LOG_ID      PLAT_LOG_ID_INITIAL
#define LOG_CAT     PLAT_LOG_CAT_SDF_PROT
#define LOG_DBG     PLAT_LOG_LEVEL_DEBUG
#define LOG_INFO    PLAT_LOG_LEVEL_INFO
#define LOG_ERR     PLAT_LOG_LEVEL_ERROR
#define LOG_WARN    PLAT_LOG_LEVEL_WARN
#define LOG_FATAL   PLAT_LOG_LEVEL_FATAL

#define MAX_WORD_LEN 200
#define MAX_DATA_NUM 10
#define MAX_OP_NUM 1000
#define MAX_SHARD_NUM 100

struct test_state {
    char *file_name;
    int   status;
};

typedef enum {
    EXP_XINT,
    EXP_CSTR,
    EXP_XSTR,
    EXP_NXSTR,
    EXP_EMPTY,
} expect_type_t;

typedef enum {
    EO_NC, // not complete
    EO_INVALID,
    EO_EMPTY,
    EO_CREATE_SHARD_S,
    EO_CREATE_SHARD_C,
    EO_DEL_SHARD_S,
    EO_DEL_SHARD_C,
    EO_WRITE_S,
    EO_WRITE_C,
    EO_READ_S,
    EO_READ_C,
    EO_DEL_S,
    EO_DEL_C,
    EO_CHECK_SUCCEED,
    EO_CHECK_FAILURE,
} entry_op_t;

struct expect_data_t {
    expect_type_t type;
    char *data;
    entry_op_t op;
    const struct expect_data_t *format;
};

typedef struct expect_data_t expect_data_t;

typedef struct {
    expect_type_t type;
    char *string;
    int id; // used for integer
} ret_data_t;

typedef ret_data_t data_group[MAX_DATA_NUM];

#define BEGIN(type, exp)      { EXP_ ## type, exp,
#define CONTINUE              EO_NC, (expect_data_t []) {
#define END_POINT(eo)         eo, NULL },
#define CONTINUE_OR_END(eo)   eo, (expect_data_t []) {
#define EMPTY_ENTRY           { EXP_EMPTY, },
#define END                   EMPTY_ENTRY }, },

const expect_data_t format[] = {
    BEGIN(CSTR, "check") CONTINUE
        BEGIN(CSTR, "succeed") END_POINT(EO_CHECK_SUCCEED) // check succeed
        BEGIN(CSTR, "failed") END_POINT(EO_CHECK_FAILURE)  // check failed
    END
    BEGIN(XINT, "ltime") CONTINUE
        BEGIN(XINT, "op_id") CONTINUE
            BEGIN(CSTR, "shard") CONTINUE
                BEGIN(CSTR, "create") CONTINUE
                    BEGIN(CSTR, "start") CONTINUE
                        BEGIN(XINT, "shard_id") CONTINUE_OR_END(EO_CREATE_SHARD_S) // shard create start
                            BEGIN(XINT, "vnode") END_POINT(EO_CREATE_SHARD_S) // shard create start
                        END
                    END
                    BEGIN(CSTR, "complete") END_POINT(EO_CREATE_SHARD_C) // shard create complete
                END
                BEGIN(CSTR, "delete") CONTINUE
                    BEGIN(CSTR, "start") CONTINUE
                        BEGIN(XINT, "shard_id") CONTINUE_OR_END(EO_DEL_SHARD_S)
                            BEGIN(XINT, "vnode") END_POINT(EO_DEL_SHARD_S) // shard delete start
                        END
                    END
                    BEGIN(CSTR, "complete") END_POINT(EO_DEL_SHARD_C) // shard delete complete
                END
            END
            BEGIN(CSTR, "read") CONTINUE
                BEGIN(CSTR, "start") CONTINUE
                    BEGIN(XINT, "shard_id") CONTINUE
                        BEGIN(XINT, "vnode") CONTINUE
                            BEGIN(XSTR, "key") END_POINT(EO_READ_S) // read start
                        END
                    END
                END
                BEGIN(CSTR, "complete") CONTINUE
                    BEGIN(CSTR, "NOT_FOUND") END_POINT(EO_READ_C) // read complete not found
                    BEGIN(XSTR, "data") END_POINT(EO_READ_C) // read complete
                END
            END
            BEGIN(CSTR, "write") CONTINUE
                BEGIN(CSTR, "start") CONTINUE
                    BEGIN(XINT, "shard_id") CONTINUE
                        BEGIN(XINT, "vnode") CONTINUE
                            BEGIN(XSTR, "key") CONTINUE
                                BEGIN(XSTR, "data") END_POINT(EO_WRITE_S) // write start
                            END
                        END
                    END
                END
                BEGIN(CSTR, "complete") END_POINT(EO_WRITE_C) // write complete
            END
            BEGIN(CSTR, "delete") CONTINUE
                BEGIN(CSTR, "start") CONTINUE
                    BEGIN(XINT, "shard_id") CONTINUE
                        BEGIN(XINT, "vnode") CONTINUE
                            BEGIN(XSTR, "key") END_POINT(EO_DEL_S) // delete start
                        END
                    END
                END
                BEGIN(CSTR, "complete") END_POINT(EO_DEL_C) // delete complete
            END
        END
    END
};

static int
is_a_integer(char *str)
{
    int i;
    int len = strlen(str);
    if (len < 1) {
        return (0);
    }
    if (str[0] == '0' && len > 1) {
        return (0);
    }
    for (i = 0; i < len; i ++) {
        if (str[i] < '0' || str[i] > '9') {
            return (0);
        }
    }
    return (1);
}

entry_op_t
parse(char *line, const expect_data_t fmt[], ret_data_t *data)
{
    int ret_index = 0;
    expect_data_t tmp = {EXP_EMPTY, NULL, EO_EMPTY, NULL};
    char string[MAX_WORD_LEN];
    int i = 0;

    while (sscanf(line, "%s", string) != EOF) {
        if (string[0] == '#') {
            break;
        }
        int match_flag = 0;
        int jump_flag = 0;
        if (fmt == NULL) {
            plat_log_msg(LOG_ID, LOG_CAT, LOG_ERR,
                         "redundant input: %s", line);
            return (EO_INVALID);
        }
        for (i = 0; jump_flag == 0 && match_flag == 0; i ++) {
            tmp = fmt[i];
            switch (tmp.type) {
            case EXP_XSTR:
                match_flag = 1;
                data[ret_index].type = EXP_XSTR;
                data[ret_index].string = (char *)plat_alloc((sizeof(char)+1) * strlen(string));
                strcpy(data[ret_index].string, string);
                data[ret_index].id = strlen(string);
                ret_index ++;
                break;
            case EXP_CSTR:
                plat_assert(string != NULL);
                plat_assert(tmp.data != NULL);
                if (strcmp(tmp.data, string) == 0) {
                    match_flag = 1;
                }
                break;
            case EXP_XINT:
                if (is_a_integer(string)) {
                    match_flag = 1;
                    data[ret_index].type = EXP_XINT;
                    data[ret_index].id = atoi(string);
                    ret_index ++;
                }
                break;
            case EXP_EMPTY:
                jump_flag = 1;
                break;
            default: break;
            }
        }
        if (match_flag == 0) {
            plat_log_msg(LOG_ID, LOG_CAT, LOG_ERR,
                         "invalid input: %s", string);
            return (EO_INVALID);
        }
        fmt = tmp.format;
        do {
            line ++;
        } while (*line != '\0' && *line != ' ');
    }

    return (tmp.op);
}

static void
test_main(uint64_t args)
{
    struct test_state *state = (struct test_state *)args;
    char *file_name = (char *)(state->file_name);
    FILE *fp = fopen(file_name, "r");
    if (!fp) {
        plat_log_msg(LOG_ID, LOG_CAT, LOG_ERR,
                     "can not open file %s.",file_name);
        return;
    }

    plat_log_msg(LOG_ID, LOG_CAT, LOG_INFO,
                 "reading %s", file_name);

    plat_log_parse_arg("sdf/prot=info");
    struct replication_test_config config;

    rtm_t *rtm = replication_test_model_alloc(&config);
    rtm_general_op_t *ops[MAX_OP_NUM];
    memset(ops, 0, sizeof(struct rtm_op_entry_t *) * MAX_OP_NUM);
    int ops_status[MAX_OP_NUM];
    memset(ops_status, 0, sizeof(int) * MAX_OP_NUM);
    data_group ret_data[MAX_OP_NUM];
    memset(ret_data, 0, sizeof(data_group) * MAX_OP_NUM);
    struct SDF_shard_meta shard_meta[MAX_SHARD_NUM];
    memset(shard_meta, 0, sizeof(struct SDF_shard_meta) * MAX_SHARD_NUM);
    int ret_index = 0;
    struct timeval now;
    plat_gettimeofday(&now, NULL);
    size_t size;
    char *line;
    int line_num = 0;
    while (getline(&line, &size, fp) != EOF) {
        line_num ++;
        int shard_id;
        ret_data_t *data = ret_data[ret_index++];
        entry_op_t ret_op = parse(line, format, data);
        switch (ret_op) {
        case EO_CREATE_SHARD_S:
        case EO_DEL_SHARD_S:
        case EO_WRITE_S:
        case EO_READ_S:
        case EO_DEL_S:
            if (ops[data[1].id] != NULL) {
                plat_log_msg(LOG_ID, LOG_CAT, LOG_ERR,
                             "duplicated op id %d found in %s:%d",
                             data[1].id, file_name, line_num);
                goto aborted;
            }
            break;
        case EO_CREATE_SHARD_C:
        case EO_DEL_SHARD_C:
        case EO_WRITE_C:
        case EO_READ_C:
        case EO_DEL_C:
            if (ops[data[1].id] == NULL) {
                plat_log_msg(LOG_ID, LOG_CAT, LOG_ERR,
                             "a complete %d without start found in %s:%d",
                             data[1].id, file_name, line_num);
                goto aborted;
            } else if (ops_status[data[1].id] == 1) {
                plat_log_msg(LOG_ID, LOG_CAT, LOG_ERR,
                             "unexpected complete %d in %s:%d",
                             data[1].id, file_name, line_num);
                goto aborted;
            }
            ops_status[data[1].id] = 1;
            break;
        default: break;
        }

        switch (ret_op) {
        case EO_CREATE_SHARD_S:
            shard_id = data[2].id;
            shard_meta[shard_id].sguid = shard_id;
            ops[data[1].id] = rtm_start_create_shard(rtm, now, data[3].id, &(shard_meta[shard_id]));
            break;
        case EO_CREATE_SHARD_C:
            rtm_create_shard_complete(ops[data[1].id], now, SDF_SUCCESS);
            break;
        case EO_DEL_SHARD_S:
            ops[data[1].id] = rtm_start_delete_shard(rtm, data[0].id, now, data[3].id, data[2].id);
            break;
        case EO_DEL_SHARD_C:
            rtm_delete_shard_complete(ops[data[1].id], now, SDF_SUCCESS);
            break;
        case EO_WRITE_S:
            ops[data[1].id] = rtm_start_write(rtm, data[2].id,
                                              data[0].id, now, data[3].id, NULL,
                                              data[4].string, data[4].id, data[5].string, data[5].id,
                                              NULL);
            break;
        case EO_WRITE_C:
            rtm_write_complete(ops[data[1].id], now, SDF_SUCCESS);
            break;
        case EO_READ_S:
            ops[data[1].id] = rtm_start_read(rtm, data[2].id,
                                             data[0].id, now, data[3].id,
                                             data[4].string, data[4].id);
            break;
        case EO_READ_C:
            rtm_read_complete(ops[data[1].id], now, SDF_SUCCESS, data[2].string, data[2].id);
            break;
        case EO_DEL_S:
            ops[data[1].id] = rtm_start_delete(rtm, data[2].id, data[0].id, now, data[3].id,
                                               data[4].string, data[4].id);
            break;
        case EO_DEL_C:
            rtm_delete_complete(ops[data[1].id], now, SDF_SUCCESS);
            break;
        case EO_CHECK_SUCCEED:
            if (rtm_failed(rtm) == 0) {
                plat_log_msg(LOG_ID, LOG_CAT, LOG_INFO,
                             "[PASS]");
                state->status = 0;
            } else {
                plat_log_msg(LOG_ID, LOG_CAT, LOG_INFO,
                             "[FAILED]");
                state->status = 1;
            }
            goto finished;
        case EO_CHECK_FAILURE:
            if (rtm_failed(rtm) == 1) {
                plat_log_msg(LOG_ID, LOG_CAT, LOG_INFO,
                             "[PASS]");
                state->status = 0;
            } else {
                plat_log_msg(LOG_ID, LOG_CAT, LOG_INFO,
                             "[FAILED]");
                state->status = 1;
            }
            goto finished;
        case EO_NC:
        case EO_INVALID:
            plat_log_msg(LOG_ID, LOG_CAT, LOG_ERR,
                         "invalid line %d: %s", line_num, line);
            goto aborted;
        default: break;
        }
    }

aborted:
    plat_log_msg(LOG_ID, LOG_CAT, LOG_ERR,
                 "[ABORTED]");
    state->status = 2;

finished:
    fclose(fp);
    rtm_free(rtm);

    /* Terminate scheduler */
    fthKill(1);
}

int
main(int argc, char *argv[]) {
    if (argc < 2) {
        plat_log_msg(LOG_ID, LOG_CAT, LOG_ERR, "[ABORTED]");
        return (2);
    }

    int status;
    struct test_state state;
    
    state.file_name = argv[1];
    state.status = 0;
   
    struct plat_opts_config_replication_test_framework_sm *sm_config;
    plat_calloc_struct(&sm_config);
    plat_assert(sm_config);
    /* start shared memory */
    status = framework_sm_init(0, NULL, sm_config);

    fthInit();

    XResume(fthSpawn(&test_main, 409600), (uint64_t)&state);

    fthSchedulerPthread(0);
    framework_sm_destroy(sm_config);

    return state.status;
}



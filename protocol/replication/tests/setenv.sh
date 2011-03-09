#!/bin/bash

#. setenv.sh
export REP_NODE_NUM="12"
export REP_REPLICA_NUM="4"
export REP_SHARD_NUM="2"

export REP_RANDOM_SEED="10"

export MAX_OBJECTS_NUM="200"
export REP_STRESS_WORKERS="1"

export IS_DIFF_KEYS="1"
export RANDOM_OBJ_SIZE="1"
export RANDOM_KEY_LEN="1"

export MIN_OBJECT_SIZE="1024"
export MAX_OBJECT_SIZE="4096"
export MIN_KEY_LEN="10"
export MAX_KEY_LEN="20"

export MISS_RATE="100"

export FIRST_RUN="1"
export FIRST_HOME_NODE="1"
export BT_EVENT_FREQ="10"

# put:0 set:1 delete:2 get:3 random:4 none:5(just do crash/restart node)
export OP_TYPE="0"
export DATA_TABLE_FILE_NAME="df_name"
export KEY_SET_FILE_NAME="kf_name"


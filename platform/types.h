#ifndef PLATFORM_TYPES_H
#define PLATFORM_TYPES_H  1

/*
 * File:   types.h
 * Author: drew
 *
 * Created on January 24, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: types.h 676 2008-03-20 05:00:53Z drew $
 */

#include <sys/types.h>
/* For uint<size>_t which are our standard integral types */
#include <stdint.h>

/*
 * Operation label.  To facilitate debugging this must be unique within a
 * cluster across a reasonable failure + recovery time frame.
 *
 * FIXME: Should have helper functions to create.
 */
typedef struct {
    int64_t node_id;
    int64_t op_id;
} plat_op_label_t;

typedef union {
    uint32_t integer;
    char text[4];
} plat_magic_t;

#endif /* ndef PLATFORM_TYPES_H */

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

#include "zs.h"
#include "btree_sync_mbox.h"
#include "btree_raw_internal.h"

struct Scavenge_Arg {
	scs_t				type;
	ZS_cguid_t			cguid;
	uint64_t			snap_seq;
	uint64_t			active_seqno;
	int					btree_index;
	int					throttle_value;
	struct btree_raw	*btree;
	struct btree		*bt;
	struct ZS_state		*zs_state;
};
typedef struct Scavenge_Arg Scavenge_Arg_t;

void scavenger_init(int nthreads);
void scavenger_stop();
void astats_init(int nthreads);
void astats_stop();

ZS_status_t btree_scavenge(struct ZS_state  *zs_state, Scavenge_Arg_t S);

typedef struct astats_arg_ {
	ZS_cguid_t			cguid;
	int					btree_index;
	struct btree_raw	*btree;
	struct btree		*bt;
	struct ZS_state		*zs_state;
	int					suspend_duration;
	int					suspend_after_node_count;
} astats_arg_t;

ZS_status_t btree_start_astats(struct ZS_state  *zs_state, astats_arg_t S);

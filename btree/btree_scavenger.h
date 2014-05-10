#include "fdf.h"
#include "btree_sync_mbox.h"

btSyncMbox_t mbox_scavenger;
btSyncMbox_t mbox_astats;

struct Scavenge_Arg {
        int             type;
        FDF_cguid_t     cguid;
        uint64_t        snap_seq;
        uint64_t        active_seqno;
        int             btree_index;
	int		throttle_value;
        struct btree_raw        *btree;
        struct btree *bt;
        struct FDF_state        *fdf_state;
};
typedef struct Scavenge_Arg Scavenge_Arg_t;

void scavenger_worker(uint64_t arg);
FDF_status_t btree_scavenge(struct FDF_state  *fdf_state, Scavenge_Arg_t S);

typedef struct astats_arg_ {
    FDF_cguid_t     cguid;
    int             btree_index;
    struct btree_raw        *btree;
    struct btree *bt;
    struct FDF_state        *fdf_state;
    int suspend_duration;
    int suspend_after_node_count;
} astats_arg_t;

void astats_worker(uint64_t arg);
FDF_status_t btree_start_astats(struct FDF_state  *fdf_state, astats_arg_t S);

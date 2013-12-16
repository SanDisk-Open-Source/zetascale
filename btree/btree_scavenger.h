#include "fdf.h"
#include "btree_sync_mbox.h"

btSyncMbox_t mbox_scavenger;

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

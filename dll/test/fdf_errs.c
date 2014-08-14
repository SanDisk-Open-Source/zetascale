/*
 * ZS messages.
 * Author: Johann George
 *
 * Copyright (c) 2012-2013, Sandisk Corporation.  All rights reserved.
 */
#include <string.h>
#include <stdlib.h>
#include "zs.h"


/*
 * Strings corresponding to ZS messages.
 */
static char *msgs[] ={
    "ZS: zero status",
    "ZS: success",
    "ZS: failure",
    "ZS: generic failure",
    "ZS: generic container failure",
    "ZS: container not open",
    "ZS: invalid container type",
    "ZS: invalid parameter",
    "ZS: unknown container",
    "ZS: unpreload container failed",
    "ZS: container exists",
    "ZS: shard not found",
    "ZS: object unknown",
    "ZS: object exists",
    "ZS: object too big",
    "ZS: storage read failure",
    "ZS: storage write failure",
    "ZS: memory alloc failure",
    "ZS: lock invalid op",
    "ZS: already unlocked",
    "ZS: already read locked",
    "ZS: already write locked",
    "ZS: object not cached",
    "ZS: sm waiting",
    "ZS: too many opids",
    "ZS: trans conflict",
    "ZS: pin conflict",
    "ZS: object deleted",
    "ZS: trans nontrans conflict",
    "ZS: already read pinned",
    "ZS: already write pinned",
    "ZS: trans pin conflict",
    "ZS: pin nonpinned conflict",
    "ZS: trans flush",
    "ZS: trans lock",
    "ZS: trans unlock",
    "ZS: unsupported request",
    "ZS: unknown request",
    "ZS: bad pbuf pointer",
    "ZS: bad pdata pointer",
    "ZS: bad success pointer",
    "ZS: not pinned",
    "ZS: not read locked",
    "ZS: not write locked",
    "ZS: pin flush",
    "ZS: bad context",
    "ZS: in trans",
    "ZS: noncacheable container",
    "ZS: out of contexts",
    "ZS: invalid range",
    "ZS: out of mem",
    "ZS: not in trans",
    "ZS: trans aborted",
    "ZS: mbox failure",
    "ZS: msg alloc failure",
    "ZS: msg send failure",
    "ZS: msg receive failure",
    "ZS: enumeration end",
    "ZS: bad key",
    "ZS: container open failure",
    "ZS: bad pexptime pointer",
    "ZS: bad pinvtime pointer",
    "ZS: bad pstat pointer",
    "ZS: bad ppcbuf pointer",
    "ZS: bad size pointer",
    "ZS: expired",
    "ZS: expired fail",
    "ZS: protocol error",
    "ZS: too many containers",
    "ZS: stopped container",
    "ZS: get metadata failed",
    "ZS: put metadata failed",
    "ZS: get direntry failed",
    "ZS: expiry get failed",
    "ZS: expiry delete failed",
    "ZS: exist failed",
    "ZS: no pshard",
    "ZS: shard delete service failed",
    "ZS: start shard map entry failed",
    "ZS: stop shard map entry failed",
    "ZS: delete shard map entry failed",
    "ZS: create shard map entry failed",
    "ZS: flash delete failed",
    "ZS: flash eperm",
    "ZS: flash enoent",
    "ZS: flash eagain",
    "ZS: flash enomem",
    "ZS: flash edatasize",
    "ZS: flash ebusy",
    "ZS: flash eexist",
    "ZS: flash eacces",
    "ZS: flash einval",
    "ZS: flash emfile",
    "ZS: flash enospc",
    "ZS: flash enobufs",
    "ZS: flash edquot",
    "ZS: flash stale cursor",
    "ZS: flash edelfail",
    "ZS: flash eincons",
    "ZS: stale ltime",
    "ZS: wrong node",
    "ZS: unavailable",
    "ZS: test fail",
    "ZS: test crash",
    "ZS: version check no peer",
    "ZS: version check bad version",
    "ZS: version check failed",
    "ZS: meta data version too new",
    "ZS: meta data invalid",
    "ZS: bad meta seqno",
    "ZS: bad ltime",
    "ZS: lease exists",
    "ZS: busy",
    "ZS: shutdown",
    "ZS: timeout",
    "ZS: node dead",
    "ZS: shard does not exist",
    "ZS: state changed",
    "ZS: no meta",
    "ZS: test model violation",
    "ZS: replication not ready",
    "ZS: replication bad type",
    "ZS: replication bad state",
    "ZS: node invalid",
    "ZS: corrupt msg",
    "ZS: queue full",
    "ZS: rmt container unknown",
    "ZS: flash rmt edelfail",
    "ZS: lock reserved",
    "ZS: key too long",
    "ZS: no writeback in store mode",
    "ZS: writeback caching disabled",
    "ZS: update duplicate",
    "ZS: container too small",
    "ZS: container full",
    "ZS: cannot reduce container size",
    "ZS: cannot change container size",
    "ZS: out of storage space",
    "ZS: already in active transaction",
    "ZS: no active transaction",
};


/*
 * Attempt to ensure that the error messages have not been re-numbered.
 */
static __attribute__((constructor)) void
init(void)
{
    if (ZS_FAILURE_NO_TRANS == 139)
        return;

    fprintf(stderr, "ZS errors have been re-numbered; fix zs_errs.c\n");
    exit(1);
}


/*
 * Return a string corresponding to a particular ZS message.
 */
char *
zs_errmsg_(ZS_status_t ss)
{
    if (ss < 0 || ss > sizeof(msgs)/sizeof(*msgs))
        return NULL;
    return msgs[ss];
}

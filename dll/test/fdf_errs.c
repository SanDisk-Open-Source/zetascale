/*
 * FDF messages.
 * Author: Johann George
 *
 * Copyright (c) 2012,  Sandisk Corporation.  All rights reserved.
 */
#include <string.h>
#include <stdlib.h>
#include "fdf.h"


/*
 * Strings corresponding to FDF messages.
 */
static char *msgs[] ={
    "FDF: zero status",
    "FDF: success",
    "FDF: failure",
    "FDF: generic failure",
    "FDF: generic container failure",
    "FDF: container not open",
    "FDF: invalid container type",
    "FDF: invalid parameter",
    "FDF: unknown container",
    "FDF: unpreload container failed",
    "FDF: container exists",
    "FDF: shard not found",
    "FDF: object unknown",
    "FDF: object exists",
    "FDF: object too big",
    "FDF: storage read failure",
    "FDF: storage write failure",
    "FDF: memory alloc failure",
    "FDF: lock invalid op",
    "FDF: already unlocked",
    "FDF: already read locked",
    "FDF: already write locked",
    "FDF: object not cached",
    "FDF: sm waiting",
    "FDF: too many opids",
    "FDF: trans conflict",
    "FDF: pin conflict",
    "FDF: object deleted",
    "FDF: trans nontrans conflict",
    "FDF: already read pinned",
    "FDF: already write pinned",
    "FDF: trans pin conflict",
    "FDF: pin nonpinned conflict",
    "FDF: trans flush",
    "FDF: trans lock",
    "FDF: trans unlock",
    "FDF: unsupported request",
    "FDF: unknown request",
    "FDF: bad pbuf pointer",
    "FDF: bad pdata pointer",
    "FDF: bad success pointer",
    "FDF: not pinned",
    "FDF: not read locked",
    "FDF: not write locked",
    "FDF: pin flush",
    "FDF: bad context",
    "FDF: in trans",
    "FDF: noncacheable container",
    "FDF: out of contexts",
    "FDF: invalid range",
    "FDF: out of mem",
    "FDF: not in trans",
    "FDF: trans aborted",
    "FDF: mbox failure",
    "FDF: msg alloc failure",
    "FDF: msg send failure",
    "FDF: msg receive failure",
    "FDF: enumeration end",
    "FDF: bad key",
    "FDF: container open failure",
    "FDF: bad pexptime pointer",
    "FDF: bad pinvtime pointer",
    "FDF: bad pstat pointer",
    "FDF: bad ppcbuf pointer",
    "FDF: bad size pointer",
    "FDF: expired",
    "FDF: expired fail",
    "FDF: protocol error",
    "FDF: too many containers",
    "FDF: stopped container",
    "FDF: get metadata failed",
    "FDF: put metadata failed",
    "FDF: get direntry failed",
    "FDF: expiry get failed",
    "FDF: expiry delete failed",
    "FDF: exist failed",
    "FDF: no pshard",
    "FDF: shard delete service failed",
    "FDF: start shard map entry failed",
    "FDF: stop shard map entry failed",
    "FDF: delete shard map entry failed",
    "FDF: create shard map entry failed",
    "FDF: flash delete failed",
    "FDF: flash eperm",
    "FDF: flash enoent",
    "FDF: flash eagain",
    "FDF: flash enomem",
    "FDF: flash edatasize",
    "FDF: flash ebusy",
    "FDF: flash eexist",
    "FDF: flash eacces",
    "FDF: flash einval",
    "FDF: flash emfile",
    "FDF: flash enospc",
    "FDF: flash enobufs",
    "FDF: flash edquot",
    "FDF: flash stale cursor",
    "FDF: flash edelfail",
    "FDF: flash eincons",
    "FDF: stale ltime",
    "FDF: wrong node",
    "FDF: unavailable",
    "FDF: test fail",
    "FDF: test crash",
    "FDF: version check no peer",
    "FDF: version check bad version",
    "FDF: version check failed",
    "FDF: meta data version too new",
    "FDF: meta data invalid",
    "FDF: bad meta seqno",
    "FDF: bad ltime",
    "FDF: lease exists",
    "FDF: busy",
    "FDF: shutdown",
    "FDF: timeout",
    "FDF: node dead",
    "FDF: shard does not exist",
    "FDF: state changed",
    "FDF: no meta",
    "FDF: test model violation",
    "FDF: replication not ready",
    "FDF: replication bad type",
    "FDF: replication bad state",
    "FDF: node invalid",
    "FDF: corrupt msg",
    "FDF: queue full",
    "FDF: rmt container unknown",
    "FDF: flash rmt edelfail",
    "FDF: lock reserved",
    "FDF: key too long",
    "FDF: no writeback in store mode",
    "FDF: writeback caching disabled",
    "FDF: update duplicate",
};


/*
 * Attempt to ensure that the error messages have not been re-numbered.
 */
static __attribute__((constructor)) void
init(void)
{
    if (FDF_UPDATE_DUPLICATE == 132)
        return;

    fprintf(stderr, "FDF errors have been re-numbered; fix fdf_errs.c\n");
    exit(1);
}


/*
 * Return a string corresponding to a particular FDF message.
 */
char *
fdf_errmsg(FDF_status_t ss)
{
    if (ss < 0 || ss > sizeof(msgs)/sizeof(*msgs))
        return NULL;
    return msgs[ss];
}

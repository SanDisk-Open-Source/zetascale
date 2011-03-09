/*
 * File:   sdf/platform/mprobe.c
 * Author: drew
 *
 * Created on September 3, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: mprobe.c 10527 2009-12-12 01:55:08Z drew $
 */

#include <setjmp.h>

#include "platform/defs.h"
#include "platform/errno.h"
#include "platform/logging.h"
#include "platform/mman.h"
#include "platform/platform.h"
#include "platform/signal.h"
#include "platform/string.h"
#include "platform/unistd.h"

PLAT_LOG_SUBCAT_LOCAL(LOG_CAT, PLAT_LOG_CAT_PLATFORM_ALLOC, "mprobe");

static sig_atomic_t mprobe_signal_received;

static jmp_buf mprobe_jmpbuf;

static void
sighandler(int signo) {
    mprobe_signal_received = signo;
    longjmp(mprobe_jmpbuf, 1);
}

int
plat_mprobe(void *addr, size_t len, int prot, int not_prot) {
    struct sigaction old_sigbus;
    struct sigaction old_sigsegv;
    struct sigaction new_action;
    int pagesize;
    int status;
    int ret = 0;

    volatile char *ptr;
    volatile char *end;
    char tmp;

    memset(&new_action, 0, sizeof (new_action));
    new_action.sa_handler = &sighandler;

    status = plat_sigaction(SIGBUS, &new_action, &old_sigbus);
    plat_assert_always(!status);
    status = plat_sigaction(SIGSEGV, &new_action, &old_sigsegv);
    plat_assert_always(!status);

    for (ptr = (volatile char *)addr,
         end = ((volatile char *)addr) + len,
         pagesize = getpagesize(); !ret && ptr < end; ptr += pagesize) {
        if ((prot & PROT_READ) || (not_prot && PROT_READ)) {
            mprobe_signal_received = 0;
            if (!sigsetjmp(mprobe_jmpbuf, 1)) {
                tmp = *ptr;
            }
            if ((prot & PROT_READ) && mprobe_signal_received) {
                plat_log_msg(20954, LOG_CAT, PLAT_LOG_LEVEL_DEBUG,
                             "buffer %p page %lx not readable",
                             addr, ((long)ptr)&~pagesize);
                ret = -EPERM;
            } else if ((not_prot & PROT_READ) && !mprobe_signal_received) {
                plat_log_msg(20955, LOG_CAT, PLAT_LOG_LEVEL_DEBUG,
                             "buffer %p page %lx readable",
                             addr, ((long)ptr)&~pagesize);
                ret = -EPERM;
            }
        }
        if (!ret && ((prot & PROT_WRITE) || (not_prot && PROT_WRITE))) {
            mprobe_signal_received = 0;
            if (!sigsetjmp(mprobe_jmpbuf, 1)) {
                tmp = (not_prot & PROT_READ) ? 0 : *ptr;
                *ptr = tmp;
            }
            if ((prot & PROT_WRITE) && mprobe_signal_received) {
                plat_log_msg(20956, LOG_CAT, PLAT_LOG_LEVEL_DEBUG,
                             "buffer %p page %lx not writable",
                             addr, ((long)ptr)&~pagesize);
                ret = -EPERM;
            } else if ((not_prot & PROT_READ) && !mprobe_signal_received) {
                plat_log_msg(20957, LOG_CAT, PLAT_LOG_LEVEL_DEBUG,
                             "buffer %p page %lx writable",
                             addr, ((long)ptr)&~pagesize);
                ret = -EPERM;
            }
        }
    }

    status = plat_sigaction(SIGBUS, &old_sigbus, NULL);
    plat_assert_always(!status);
    status = plat_sigaction(SIGSEGV, &old_sigsegv, NULL);
    plat_assert_always(!status);

    return (ret);
}

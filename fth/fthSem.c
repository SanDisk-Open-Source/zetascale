/*
 * File:   fthSem.c
 * Author: drew
 *
 * Created on April 22, 2009
 *
 * (c) Copyright 2009, Schooner Information Technology, Inc.
 * http:                                     //www.schoonerinfotech.com/
 *
 * $Id: fthSem.c 6914 2009-04-23 06:05:29Z drew $
 */

/*
 * XXX: drew 2009-04-22 Not optimal, but quite simple and probably light
 * weight compared to the things that are using the semphore.  Switch
 * to a count field with an fthThreadQ for multiple waiters if it becomes
 * a problem.
 */

#include "fthSem.h"

void
fthSemInit(struct fthSem *sem, int count) {
    fthMboxInit(&sem->mbox);
    fthSemUp(sem, count);
}

void
fthSemUp(struct fthSem *sem, int count) {
    int i;
    for (i = 0; i < count; ++i) {
        fthMboxPost(&sem->mbox, 0);
    }
}

void
fthSemDown(struct fthSem *sem, int count) {
    int i;
    for (i = 0; i < count; ++i) {
        fthMboxWait(&sem->mbox);
    }
}

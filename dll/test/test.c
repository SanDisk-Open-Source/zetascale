/*
 * Test some FDF functions.
 *
 * Author: Johann George
 * Copyright (c) 2012-2013, Sandisk Corporation.  All rights reserved.
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "tlib.h"


/*
 * Types.
 */
typedef void (tfunc_t)();


/*
 * Tests information structure.
 */
typedef struct tinfo {
    int           nlen;
    struct tinfo *next;
    char         *name;
    char         *desc;
    char         *help;
    tfunc_t      *func;
} tinfo_t;


/*
 * Static variables.
 */
tinfo_t *Tinfo;


/*
 * Add a test.
 */
void
test_info(char *name, char *desc, char *help, tfunc_t *func)
{
    tinfo_t *tinfo = alloc(sizeof(tinfo_t));
    tinfo->nlen = strlen(name);
    tinfo->name = name;
    tinfo->desc = desc;
    tinfo->help = help;
    tinfo->func = func;
    tinfo->next = Tinfo;
    Tinfo = tinfo;
}


/*
 * Find a test.
 */
static tinfo_t *
tfind(char *name)
{
    tinfo_t *tinfo;

    for (tinfo = Tinfo; tinfo; tinfo = tinfo->next)
        if (streq(name, tinfo->name))
            return tinfo;
    return NULL;
}


/*
 * Compare function for qsorting tinfo entries.
 */
static int
compare(const void *a1, const void *a2)
{
    const tinfo_t *t1 = *((tinfo_t **) a1);
    const tinfo_t *t2 = *((tinfo_t **) a2);

    if (t1->nlen < t2->nlen)
        return -1;
    if (t1->nlen > t2->nlen)
        return 1;
    return strcmp(t1->name, t2->name);
}


/*
 * Print out a usage message and exit.
 */
static void
usage(void)
{
    int i;
    tinfo_t *tinfo;
    const char *str[] = {
        "Usage:",
        "    test Name",
        "Name:",
    };

    for (i = 0; i < sizeof(str)/sizeof(*str); i++)
        printf("%s\n", str[i]);

    int num = 0;
    int max = 0;
    for (tinfo = Tinfo; tinfo; tinfo = tinfo->next) {
        num++;
        if (tinfo->nlen > max)
            max = tinfo->nlen;
    }

    tinfo_t **sorted = alloc(num * sizeof(tinfo_t *));
    for (i = 0, tinfo = Tinfo; tinfo; tinfo = tinfo->next)
        sorted[i++] = tinfo;

    qsort(sorted, num, sizeof(tinfo_t *), compare);
    for (i = 0; i < num; i++) {
        tinfo = sorted[i];
        printf("    %-*s - %s\n", max, tinfo->name, tinfo->desc);
    }
    exit(0);
}


int
main(int argc, char *argv[])
{
    if (argc < 2)
        usage();

    int i;
    for (i = 1; i < argc; i++) {
        char *name = argv[i];
        tinfo_t *tinfo = tfind(name);
        if (tinfo)
            tinfo->func();
        else
            die("unknown test: %s", name);
    }
    return 0;
}

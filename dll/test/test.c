/*
 * Test some FDF functions.
 *
 * Author: Johann George
 * Copyright (c) 2012-2013, Sandisk Corporation.  All rights reserved.
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include "tlib.h"


/*
 * Configurable parameters.
 */
#define BUF_SIZE            256
#define FDF_LIB             "FDF_LIB"
#define FDF_PROPERTY_FILE   "FDF_PROPERTY_FILE"


/*
 * Macro functions.
 */
#define nel(a) (sizeof(a)/sizeof(*(a)))


/*
 * Types.
 */
typedef void (tfunc_t)(fdf_t *);


/*
 * The various options for persistent storage.
 *  NONE - Don't set any options
 *  AUTO - Decide if we use SLOW or FAST based on memory availability
 *  SLOW - Use /tmp as our perisistent store
 *  FAST - Use memory as our persistent store
 */
typedef enum {
    NONE,
    AUTO,
    SLOW,
    FAST
} speed_t;


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
static tinfo_t *Tinfo;
static speed_t  Speed = AUTO;


/*
 * Determine whether we have enough memory for it to substitute as flash.
 */
static speed_t
set_speed(fdf_t *fdf)
{
    unsigned long flash;
    if (!fdf_utoi(fdf_get_prop(fdf, "FDF_FLASH_SIZE", "0"), &flash))
        return SLOW;

    FILE *fp = fopen("/proc/meminfo", "r");
    if (!fp)
        return SLOW;

    char buf[64];
    char *line = fgets(buf, sizeof(buf), fp);
    fclose(fp);

    if (!line)
        return SLOW;
    if (strncmp(line, "MemTotal:", 9) != 0)
        return SLOW;
    unsigned long mem = atol(&line[9]) * 1024;

    if (mem - 2L*1024*1024*1024 < flash)
        return SLOW;
    return FAST;
}


/*
 * Set the properties relating to the speed.
 */
static void
set_speed_props(fdf_t *fdf)
{
    if (Speed == AUTO)
        Speed = set_speed(fdf);

    if (Speed == SLOW)
        fdf_set_prop(fdf, "FDF_FLASH_FILENAME", "/tmp/fdf_disk%d");
    else if (Speed == FAST) {
        fdf_set_prop(fdf, "FDF_O_DIRECT",       "0");
        fdf_set_prop(fdf, "FDF_TEST_MODE",      "1");
        fdf_set_prop(fdf, "FDF_LOG_FLUSH_DIR",  "/dev/shm");
        fdf_set_prop(fdf, "FDF_FLASH_FILENAME", "/dev/shm/fdf_disk%d");
    }
}


/*
 * Initialize the property file.
 */
static void
init_prop_file(fdf_t *fdf, char *name)
{
    char *err;
    char buf[BUF_SIZE];
    struct stat sbuf;
    char *prop = getenv(FDF_PROPERTY_FILE);

    if (prop)
        unsetenv(FDF_PROPERTY_FILE);
    else {
        snprintf(buf, sizeof(buf), "./%s.prop", name);
        if (stat(buf, &sbuf) < 0) {
            snprintf(buf, sizeof(buf), "./test.prop");
            if (stat(buf, &sbuf) < 0)
                die("cannot determine property file; set FDF_PROPERTY_FILE");
        }
        prop = buf;
    }

    if (!fdf_load_prop_file(fdf, prop, &err))
        die_err(err, "fdf_load_prop_file failed");
}


/*
 * Initialize the test framework.  We are passed the test name.
 */
void
test_init(fdf_t *fdf, char *name)
{
    char *err;

    init_prop_file(fdf, name);
    set_speed_props(fdf);

    if (!fdf_start(fdf, &err))
        die_err(err, "fdf_start failed");
}


/*
 * Initialize properties.
 */
static void
set_def_props(fdf_t *fdf)
{
    fdf_set_prop(fdf, "FDF_LOG_LEVEL",  "warning");
    fdf_set_prop(fdf, "HUSH_FASTCC",    "1");
    fdf_set_prop(fdf, "FDF_REFORMAT",   "1");
    fdf_set_prop(fdf, "FDF_FLASH_SIZE", "4G");
}


/*
 * Initialize the FDF library.
 */
static void
init_fdf_lib()
{
    int n;
    char **l;
    struct stat sbuf;
    char *lib = getenv(FDF_LIB);
    char *libs[] ={
        "../../output/lib/libfdf.so",
        "/tmp/libfdf.so"
    };

    if (lib)
        return;
    for (n = nel(libs), l = libs; n--; l++) {
        if (stat(*l, &sbuf) < 0)
            continue;
        setenv(FDF_LIB, *l, 0);
        return;
    }
    die("cannot determine FDF library; set FDF_LIB");
}


/*
 * Run a particular test.
 */
static void
run_test(tinfo_t *tinfo)
{
    char *err;

    fdf_t *fdf = fdf_init(0, &err);
    if (!fdf)
        die_err(err, "fdf_init failed");

    init_fdf_lib();
    set_def_props(fdf);
    tinfo->func(fdf);
    fdf_done(fdf);
}


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
    int i;
    char *test = NULL;

    if (argc < 2)
        usage();

    for (i = 1; i < argc; i++) {
        char *arg = argv[i];
        if (streq(arg, "-sn") || streq(arg, "--none"))
            Speed = NONE;
        else if (streq(arg, "-sa") || streq(arg, "--auto"))
            Speed = AUTO;
        else if (streq(arg, "-ss") || streq(arg, "--slow"))
            Speed = SLOW;
        else if (streq(arg, "-sf") || streq(arg, "--fast"))
            Speed = FAST;
        else if (arg[0] == '-')
            die("bad option: %s", arg);
        else {
            test = arg;
            break;
        }
    }

    if (!test)
        die("must specify test");

    tinfo_t *tinfo = tfind(test);
    if (!tinfo)
        die("unknown test: %s", test);
    run_test(tinfo);
    return 0;
}

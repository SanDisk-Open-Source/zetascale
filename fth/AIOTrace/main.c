/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

/******************************************************************
 *
 * ata -- A program to to analyze AIO trace files.
 *
 * Brian O'Krafka   9/16/09
 *
 *  NOTES:
 *
 ******************************************************************/

#include <stdio.h>
#include <stdarg.h>
#include <stdint.h>
#include <string.h>
#include <stdlib.h>
#include <dirent.h>
#include <inttypes.h>
#include <math.h>
#include <assert.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include "tlmap.h"
#include "xtlmap.h"
#include "aiotrace.h"

// #define DEBUG

#define LINE_LENGTH  1024

static int  LineLength = LINE_LENGTH;
static char *Line;
static int  LineNum;
static int  Debug = 0;

static char *Program = NULL;

static void usage()
{
    fprintf(stderr, "usage: %s <trace_file_directory> [-s <n_skip>] [-d <n_dump_recs>] [-m <merge_file>] [-p <plot_file_name>] [-x <n_skip_post_merge>] > <output_file>\n", Program);
    fprintf(stderr, "<trace_file_directory>: directory containing AIO trace files\n");
    fprintf(stderr, "-s <n_skip>: skip the first n_skip trace records (pre-merge)\n");
    fprintf(stderr, "-d <n_dump_recs>: number of trace records to dump [optional]\n");
    fprintf(stderr, "-m <merge_file>: skip the merge step and use this merge file\n");
    fprintf(stderr, "-p <plot_file_name>: generate plots of aio response time vs. time for each device\n");
    fprintf(stderr, "-x <n_skip_post_merge>: skip this many records when processing the merged file\n");
    exit(1);
}

    /* predeclarations */

static void InitGetLine();
static void GetLineString(char *s, FILE *f);
static char *GetLine(FILE *f);
static void LineWarning(char *fmt, ...);
static void LineError(char *fmt, ...);
static void DebugMsg(char *fmt, ...);
static void Message(char *fmt, ...);
static void Warning(char *fmt, ...);
static void Error(char *fmt, ...);

static int get_trace_file_names(char *trace_dir, struct dirent ***peps);
static BOOLEAN get_record(FILE *f, RECORD *pr);
static BOOLEAN put_record(FILE *f, RECORD *pr);
static void init_stats(STATS *ps);
static int get_fth_map(SDFxTLMap_t *pfthmap, uint64_t fth);
static void get_profile(AIO_ANALYZER *paa, FILE *fout);
static int file_count_sortfn(const void *e1_in, const void *e2_in);
static FILE *merge_trace_files(AIO_ANALYZER *paa);
static void load_fth_table(AIO_ANALYZER *paa);
static void dump_stats(AIO_ANALYZER *paa, FILE *fout);
static void check_trace(AIO_ANALYZER *paa, int n_dump_recs);
static void dump_trace_rec(AIO_ANALYZER *paa, FILE *f, RECORD *pr);
static void get_base_time(AIO_ANALYZER *paa);
static void dump_plot_files(AIO_ANALYZER *paa);
static void dump_histo_files(AIO_ANALYZER *paa);
static void plot_bad_intervals(AIO_ANALYZER *paa);
static void dump_interval_histo_files(AIO_ANALYZER *paa);

AIO_ANALYZER  AIOAnalyzer;

main(int argc, char **argv)
{
    FILE          *fout;
    char          *trace_dir;
    char          *plot_fname;
    AIO_ANALYZER  *paa;
    int            i;
    int            n_dump_recs;
    int            already_merged = 0;

    InitGetLine();
    paa = &AIOAnalyzer;
    // normalization should have been done by tracer
    paa->f_time_normalize = 1.0; 

    Program = argv[0];

    paa->n_dump_recs           = 100;
    paa->nrecs_skip            = 0;
    paa->nrecs_skip_post_merge = 0;
    paa->trace_dir             = NULL;
    paa->plot_fname            = NULL;

    for (i=1; i<argc; i++) {
        if (argv[i][0] == '-') {

	    /* switches without second arguments */

	    switch (argv[i][1]) {
	        case 'h': // help
		    usage();
		    break;
	        default:
		    /* purposefully empty */
		    break;
	    }

	    /* switches with second arguments */

	    if (i == (argc - 1)) {
	        // second argument is missing!
	        usage();
	    }

	    switch (argv[i][1]) {
	        case 'm': // use an existing merge file
		    already_merged = 1;
		    paa->mergefile = argv[i+1];
		    break;
	        case 'p': // plot device access delay versus time
		    paa->plot_fname = argv[i+1];
		    break;
	        case 's': // number of records to skip, pre-merge
		    paa->nrecs_skip = atoi(argv[i+1]);
		    break;
	        case 'x': // number of records to skip, post-merge
		    paa->nrecs_skip_post_merge = atoi(argv[i+1]);
		    break;
	        case 'd': // number of records to dump
		    paa->n_dump_recs = atoi(argv[i+1]);
		    break;
		default:
		    usage();
		    break;
	    }
	    i++; // skip the second argument
	} else {
	    if (paa->trace_dir == NULL) {
	        paa->trace_dir = argv[i];
	    } else {
	        usage();
	    }
	}
    }
    if (paa->trace_dir == NULL) {
        usage();
    }

    /* usage: %s <trace_file_directory> [-s <n_skip>] [-d <n_dump_recs>] */


    paa->n_neg_time     = 0;


    Message("trace directory: %s", paa->trace_dir);
    Message("number of trace records to skip (pre-merge): %"PRIu64, paa->nrecs_skip);
    Message("number of trace records to skip (post-merge): %"PRIu64, paa->nrecs_skip_post_merge);
    Message("number of trace records to dump: %d", paa->n_dump_recs);

    fout = stdout;

    /* initialization */

    // xxxzzz

    if (!already_merged) {
	/* load trace file names */

	paa->nfiles = get_trace_file_names(paa->trace_dir, &paa->eps);

	/* merge trace files into one, sorted by time */

	paa->mergefile = "AT_merge";
	paa->fmerge = merge_trace_files(paa);
    } else {
	paa->fmerge = fopen(paa->mergefile, "r");
	if (paa->fmerge == NULL) {
	    Error("Could not open merge file '%s'", paa->mergefile);
	}
    }

    /* check trace */

    check_trace(paa, paa->n_dump_recs);

    /* load fth map */

    load_fth_table(paa);

    /* generate report */

    init_stats(&(paa->stats));
    get_profile(paa, fout);
    dump_stats(paa, fout);

    /* generate plot files */

    if (paa->plot_fname != NULL) {
        // dump_plot_files(paa);
	plot_bad_intervals(paa);
        dump_histo_files(paa);
	dump_interval_histo_files(paa);
    }

    if (paa->n_neg_time > 0) {
        Warning("%lld instances of negative AIO times!", paa->n_neg_time);
    }
    Message("%lld total trace records (%"PRIu64" skipped)", paa->nrecs, paa->nrecs_skip);

    return(0);
}

static void dump_trace_rec(AIO_ANALYZER *paa, FILE *f, RECORD *pr)
{
    char      sflags[10];
    int64_t  t_delta;

    sflags[0] = '\0';
    if (pr->flags & AIO_TRACE_WRITE_FLAG) {
        strcat(sflags, "W");
    }
    if (pr->flags & AIO_TRACE_SCHED_MISMATCH_FLAG) {
        strcat(sflags, "M");
    }
    if (pr->flags & AIO_TRACE_ERR_FLAG) {
        strcat(sflags, "E");
    }

    t_delta = pr->t_end - pr->t_start;

    // (void) fprintf(f, "ts=%16"PRIu64", te=%16"PRIu64", td=%8"PRIu64", fd=%d, size=%7d, sub=%2d, flags=%s\n", pr->t_start, pr->t_end, t_delta, pr->fd, pr->size, pr->submitted, sflags);
    (void) fprintf(f, "td=%10"PRIu64", fd=%3d, size=%7d, sub=%2d, flags=%s\n", t_delta, pr->fd, pr->size, pr->submitted, sflags);
}

static void get_base_time(AIO_ANALYZER *paa)
{
    long long  nrec;
    uint64_t   tbase;
    RECORD     rec;
    BOOLEAN    found_first;

    tbase = 0;
    found_first = FALSE;
    rewind(paa->fmerge);
    while (get_record(paa->fmerge, &rec)) {
        if (!found_first) {
	    tbase       = rec.t_start;
	    paa->t_base = tbase;
	    Message("*****  Base Init Time: %"PRIu64"  *****", paa->t_base);
	}
	if (rec.t_start < tbase) {
	    Warning("rec %d: t_s (%"PRIu64") < tbase (%"PRIu64")!", rec.t_start, tbase);
	}
    }
}

static void check_trace(AIO_ANALYZER *paa, int n_dump_recs)
{
    int        rectype, nsched_max;
    long long  nmismatch, nrec, n_rd, n_wr, n_err;
    long long  dt, n_neg, n_neg_max;
    RECORD     rec;

    nmismatch = 0;
    nrec      = 0;
    n_rd      = 0;
    n_wr      = 0;
    n_err     = 0;

    n_neg_max    = 100;
    n_neg        = 0;
    nsched_max   = 0;

    rewind(paa->fmerge);
    while (get_record(paa->fmerge, &rec)) {
        nrec++;

	if (rec.nsched > nsched_max) {
	    nsched_max = rec.nsched;
	}

	if (rec.flags & AIO_TRACE_WRITE_FLAG) {
	    n_wr++;
	} else {
	    n_rd++;
	}
	if (rec.flags & AIO_TRACE_SCHED_MISMATCH_FLAG) {
	    nmismatch++;
	}
	if (rec.flags & AIO_TRACE_SCHED_MISMATCH_FLAG) {
	    n_err++;
	}

        if (nrec < n_dump_recs) {
            dump_trace_rec(paa, stdout, &rec);
	}
    }
    paa->nsched = nsched_max + 1;

    Message(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
    Message(">>>>>>>>>>>>>>   Total:    %10lld           <<<<<<<<<<<<<<<<<<<<<<<", nrec);
    Message(">>>>>>>>>>>>>>   Read:     %10lld (%5.3g%%)  <<<<<<<<<<<<<<<<<<<<<<<", n_rd, (((double) n_rd)/nrec)*100.0);
    Message(">>>>>>>>>>>>>>   Write:    %10lld (%5.3g%%)  <<<<<<<<<<<<<<<<<<<<<<<", n_wr, (((double) n_wr)/nrec)*100.0);
    Message(">>>>>>>>>>>>>>   Mismatch: %10lld (%5.3g%%)  <<<<<<<<<<<<<<<<<<<<<<<", nmismatch, (((double) nmismatch)/nrec)*100.0);
    Message(">>>>>>>>>>>>>>   Error:    %10lld (%5.3g%%)  <<<<<<<<<<<<<<<<<<<<<<<", n_err, (((double) n_err)/nrec)*100.0);
    Message(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
}

static void init_stats(STATS *ps)
{
    int  i, j, k;

    for (i=0; i<MAX_SCHED; i++) {
	(ps->sched_counts[i]) = 0;
	for (j=0; j<MAX_FTH; j++) {
	    for (k=0; k<N_RECTYPES; k++) {
		(ps->counts[i][j][k]) = 0;
	    }
	}
    }

    for (i=0; i<MAX_SCHED; i++) {
	for (k=0; k<N_RECTYPES; k++) {
	    (ps->schedrec_counts[i][k]) = 0;
	}
    }

    for (j=0; j<MAX_FTH; j++) {
	(ps->fth_counts[j]) = 0;
	for (k=0; k<N_RECTYPES; k++) {
	    (ps->fthrec_counts[j][k]) = 0;
	}
    }
}

static void do_separator_line(FILE *fout)
{
    (void) fprintf(fout, "-------------------------------------------------------------------------------\n");
}

static void dump_stats(AIO_ANALYZER *paa, FILE *fout)
{
    int                i, j, k, i_fth;
    int                n_lock_dump;
    char              *s;
    uint64_t           fth;
    double             tot_read[MAX_FTH], tot_write[MAX_FTH];
    SDFxTLMapEntry_t  *pxme;
    FTHDATA           *pfd;
    STATS             *ps;
    int                n;
    double             av_rd, av2_rd, sd_rd;
    double             av_wr, av2_wr, sd_wr;
    double             f;
    char               stmp[10000];

    f = paa->f_time_normalize;
    n_lock_dump = 50;

    ps = &(paa->stats);

    /************************************************************************
     *   Counts
     ************************************************************************/

    do_separator_line(fout);

    (void) fprintf(fout, "Total Requests Per Scheduler\n");
    for (i=0; i<paa->nsched; i++) {
	(void) fprintf(fout, "     %2d    ", i);
    }
    (void) fprintf(fout, "\n");
    for (i=0; i<paa->nsched; i++) {
	(void) fprintf(fout, " %10g", ps->sched_counts[i]);
    }
    (void) fprintf(fout, "\n");

    do_separator_line(fout);
    (void) fprintf(fout, "Total Requests Per fth Thread\n");
    for (i=0; i<paa->nfth; i++) {
	(void) fprintf(fout, "     %2d    ", i);
    }
    (void) fprintf(fout, "\n");
    for (i=0; i<paa->nfth; i++) {
	(void) fprintf(fout, " %10g", ps->fth_counts[i]);
    }
    (void) fprintf(fout, "\n");

    do_separator_line(fout);
    (void) fprintf(fout, "Requests Per Scheduler\n");
    (void) fprintf(fout, "%10s", "Scheduler");
    for (k=0; k<N_RECTYPES; k++) {
	(void) fprintf(fout, " %10s", RecType[k].name);
    }
    (void) fprintf(fout, "\n");
    for (i=0; i<paa->nsched; i++) {
	(void) fprintf(fout, "%5d        ", i);
	for (k=0; k<N_RECTYPES; k++) {
	    (void) fprintf(fout, " %10g", ps->schedrec_counts[i][k]);
	}
	(void) fprintf(fout, "\n");
    }

    do_separator_line(fout);
    (void) fprintf(fout, "Requests Per fth Thread\n");
    for (k=0; k<N_RECTYPES; k++) {
	(void) fprintf(fout, " %10s", RecType[k].name);
    }
    (void) fprintf(fout, "      %10s", "fthread");
    (void) fprintf(fout, "\n");
    for (j=0; j<paa->nfth; j++) {
	(void) fprintf(fout, "    ");
	for (k=0; k<N_RECTYPES; k++) {
	    (void) fprintf(fout, " %10g", ps->fthrec_counts[j][k]);
	}
	(void) fprintf(fout, "%10d", j);
	fth = paa->inv_fthmap[j];
	(void) fprintf(fout, "\n");
    }

    do_separator_line(fout);
}

static void dump_fth_info(FTHDATA *pfd, FILE *fout)
{
    int                rectype, nsched;

    rectype = (pfd->rec.flags & AIO_TRACE_WRITE_FLAG) ? WRITE_REC : READ_REC;
    nsched  = pfd->rec.nsched;

    (void) fprintf(fout, "fthread (0x%"PRIx64") on sched %d\n", pfd->fth, nsched);
}

static int get_fth_map(SDFxTLMap_t *pfthmap, uint64_t fth)
{
    int                i;
    SDFxTLMapEntry_t  *pxme;
    FTHDATA           *pfd;

    pxme = SDFxTLMapGet(pfthmap, fth);
    if (pxme == NULL) {
	Warning("Missing fth mapping for fth=%"PRIu64, fth);
	return(-1);
    } else {
        pfd = (FTHDATA *) pxme->contents;
	assert(pfd != NULL);
	i = pfd->i;
        return(i);
    }
}

static void get_profile(AIO_ANALYZER *paa, FILE *fout)
{
    RECORD             cur_rec[MAX_SCHED];
    int                rectype, nsched;
    int                i_rectype, i_fth, i_sched;
    long long          nrec;
    SDFTLMapEntry_t   *pxme;
    long long          dt;
    STATS             *ps;
    RECORD             rec;
    char               skey[100];

    ps = &(paa->stats);

    rewind(paa->fmerge);
    nrec = 0;
    while (get_record(paa->fmerge, &rec)) {
        nrec++;

        rectype = (rec.flags & AIO_TRACE_WRITE_FLAG) ? WRITE_REC : READ_REC;
        nsched  = rec.nsched;

        i_rectype = RecType[rectype].i;
	i_fth     = get_fth_map(&(paa->fthmap), rec.fth);
	i_sched   = nsched;

	(ps->counts[i_sched][i_fth][i_rectype])++;
	(ps->schedrec_counts[i_sched][i_rectype])++;
	(ps->fthrec_counts[i_fth][i_rectype])++;
	(ps->sched_counts[i_sched])++;
	(ps->fth_counts[i_fth])++;
    }
}

static int file_count_sortfn(const void *e1_in, const void *e2_in)
{
    long long t1, t2;

    t1 = *((long long *) e1_in);
    t2 = *((long long *) e2_in);

    if (t1 < t2) {
        return(-1);
    } else if (t1 > t2) {
        return(1);
    } else {
        return(0);
    }
}

static FILE *merge_trace_files(AIO_ANALYZER *paa)
{
    /* merge into a single trace file */

    char         scanstring[1024];
    int          i, j, nsched, nrecs;
    int64_t      i_pick;
    uint64_t     t, nrec;
    uint64_t     t_min;
    int          curfile[MAX_SCHED];
    RECORD       cur_rec[MAX_SCHED];
    FILE        *fmerge;
    STRING       tfilename[MAX_SCHED];
    FILE        *tfiles[MAX_SCHED*MAX_FILES_PER_SCHED];
    char        *mergefile;
    FILEDATA    *pfd;

    mergefile = paa->mergefile;
    pfd       = &(paa->filedata);

    /* initialize file data */

    pfd->nsched = 0;
    for (i=0; i<MAX_SCHED; i++) {
	pfd->files_per_sched[i] = 0;
	pfd->schedmap[i]        = 0;
	for (j=0; j<MAX_FILES_PER_SCHED; j++) {
	    pfd->file_counts[i][j] = 0;
	}
    }

    /* find the number of schedulers */

    for (i=0; i<paa->nfiles; i++) {
        if (sscanf(paa->eps[i]->d_name, "at_%d", &nsched) != 1) {
	    continue;
	}
	if (pfd->nsched <= nsched) {
	    pfd->nsched = nsched + 1;
	}
    }

    for (i=0; i<paa->nfiles; i++) {
        if (sscanf(paa->eps[i]->d_name, "at_%d", &nsched) != 1) {
	    continue;
	}

	pfd->file_counts[nsched][pfd->files_per_sched[nsched]] = nrecs;
	pfd->files_per_sched[nsched]++;
	if (pfd->files_per_sched[nsched] >= MAX_FILES_PER_SCHED) {
	    Error("Too many files per scheduler (%d max)", MAX_FILES_PER_SCHED);
	}
    }

    /* sort file_counts array */

    for (i=0; i<pfd->nsched; i++) {
	qsort((void *) &(pfd->file_counts[i]), (size_t) pfd->files_per_sched[i], 
	      (size_t) sizeof(long long), file_count_sortfn);
    }

    /* open the merge file */

    fmerge = fopen(mergefile, "w+");
    if (fmerge == NULL) {
        Error("Could not open merge file '%s'", mergefile);
    }

    /* load the first record for each file */

    for (i=0; i<pfd->nsched; i++) {
        curfile[i] = 0;
	cur_rec[i].nsched = END_REC;
	pfd->schedmap[i] = i;
    }

    for (i=0; i<pfd->nsched; i++) {
        (void) sprintf(tfilename[i], "%s/at_%d", paa->trace_dir, pfd->schedmap[i]);
        tfiles[i] = fopen(tfilename[i], "r");
	if (tfiles[i] == NULL) {
	    cur_rec[i].nsched = END_REC;
	    Error("Could not open trace file '%s'", tfilename[i]);
	} else {
	    if (!get_record(tfiles[i], &(cur_rec[i]))) {
		Warning("No trace records found in trace file '%s'", tfilename[i]);
	    }
	}
    }

    /* do the merge */

    nrec = 0;
    while (TRUE) {
        /* find the smallest timestamp among the different trace files */

        i_pick = -1;
	for (i=0; i<pfd->nsched; i++) {
	    if (cur_rec[i].nsched != END_REC) {
	        if (i_pick == -1) {
		    t_min = cur_rec[i].t_end;
		    i_pick = i;
		} else {
		    if (cur_rec[i].t_end < t_min) {
			t_min = cur_rec[i].t_end;
			i_pick = i;
		    }
		}
	    }
	}
	if (i_pick == -1) {
	    break;
	}

	/* dump it to the common trace file */

        nrec++;
	if (nrec > paa->nrecs_skip) {
	    if (!put_record(fmerge, &(cur_rec[i_pick]))) {
		Error("Write to merged trace file '%s' failed", mergefile);
	    }
	}

	/* get the next record for this scheduler */
	{
	    if ((!get_record(tfiles[i_pick], &(cur_rec[i_pick]))) ||
	        (cur_rec[i_pick].nsched == END_REC))
	    {
	        /* go to the next file for this scheduler */
		if (fclose(tfiles[i_pick]) != 0) {
		    Warning("Could not close trace file '%s'", tfilename[i_pick]);
		}
		curfile[i_pick]++;
		if (curfile[i_pick] >= pfd->files_per_sched[i_pick]) {
		    /* I am out of files for this scheduler */
		    cur_rec[i_pick].nsched = END_REC;
		} else {
		    (void) sprintf(tfilename[i_pick], "%s/at_%d", paa->trace_dir, pfd->schedmap[i_pick]);
		    tfiles[i_pick] = fopen(tfilename[i_pick], "r");
		    if (tfiles[i_pick] == NULL) {
			Error("Could not open trace file '%s'", tfilename[i_pick]);
		    }
		    if (!get_record(tfiles[i_pick], &(cur_rec[i_pick]))) {
			Error("No trace records found in trace file '%s'", tfilename[i_pick]);
			cur_rec[i_pick].nsched = END_REC;
		    }
		}
	        
	    }
	}
    }
    paa->nsched = pfd->nsched;
    return(fmerge);
}

static void load_fth_table(AIO_ANALYZER *paa)
{
    int               i;
    long long         nrec;
    RECORD            rec;
    SDFxTLMapEntry_t  *pxme;
    FTHDATA          *pfd;
    int               rectype, nsched, fthcnt;
    int               nlocks;
    FILE             *fmerge;
    SDFxTLMap_t      *pfthmap;

    fmerge   = paa->fmerge;
    pfthmap  = &(paa->fthmap);

    /* load the lock table */

    rewind(fmerge);

    SDFxTLMapInit(pfthmap,  FTH_MAP_SIZE,    NULL);

    fthcnt = 0;
    nrec   = 0;
    while (get_record(fmerge, &rec)) {
        nrec++;

        rectype = (rec.flags & AIO_TRACE_WRITE_FLAG) ? WRITE_REC : READ_REC;
        nsched  = rec.nsched;

	pxme = SDFxTLMapGetCreate(pfthmap, rec.fth);
	pfd = (FTHDATA *) pxme->contents;
	if (pfd == NULL) {
	    pfd = (FTHDATA *) malloc(sizeof(FTHDATA));
	    pfd->i = fthcnt;
	    pfd->rec = rec;
	    pfd->fth = rec.fth;
	    pxme->contents = (void *) pfd;
	    paa->inv_fthmap[fthcnt] = rec.fth;
	    fthcnt++;
	    if (fthcnt >= MAX_FTH) {
	        Error("Too many fth threads (%d max)", MAX_FTH);
	    }
	}
    }
    paa->nfth   = fthcnt;
    paa->nrecs  = nrec;
}

static int one (const struct dirent *unused)
{
  return 1;
}

static int get_trace_file_names(char *trace_dir, struct dirent ***peps)
{
    struct dirent **eps;
    int n;

    n = scandir(trace_dir, peps, one, alphasort);
    eps = *peps;

    if (n >= 0) {
	if (Debug) {
	    int cnt;
	    for (cnt = 0; cnt < n; ++cnt) {
		puts(eps[cnt]->d_name);
	    }
	}
    } else {
        perror ("Couldn't open the directory");
    }

    return(n);
}

static void dump_plot_files(AIO_ANALYZER *paa)
{
    int                rectype, nsched;
    int                i_rectype;
    long long          nrec;
    RECORD             rec;
    int                i;
    char               fname[MAX_SCHED][1024];
    FILE              *fout[MAX_SCHED];
    uint64_t           t_base[MAX_SCHED];
    int                first_rec[MAX_SCHED];
    int                fd_map[MAX_FD];
    int64_t            t_delta;
    int                i_fd, fd, nfd;

    Message("Dumping plots of AIO delay versus time");

    nfd = 0;
    for (i=0; i<MAX_FD; i++) {
        fd_map[i] = -1;
    }

    for (i=0; i<MAX_SCHED; i++) {
        t_base[i]    = 0;
        first_rec[i] = 1;
    }

    rewind(paa->fmerge);
    nrec = 0;
    while (get_record(paa->fmerge, &rec)) {
        nrec++;
	if (nrec < paa->nrecs_skip_post_merge) {
	    continue;
	}
	if (rec.flags & AIO_TRACE_SCHED_MISMATCH_FLAG) {
	    continue;
	}

        rectype   = (rec.flags & AIO_TRACE_WRITE_FLAG) ? WRITE_REC : READ_REC;
        nsched    = rec.nsched;
	fd        = rec.fd;
        i_rectype = RecType[rectype].i;

	if (first_rec[nsched]) {
	    first_rec[nsched] = 0;
	    t_base[nsched]    = rec.t_start;
	}

	if (fd >= MAX_FD) {
	    Error("fd=%d is too large (%d is max)", fd, MAX_FD);
	}
	if (fd_map[fd] == -1) {
	    (void) sprintf(fname[nfd], "%s_%d.dat", paa->plot_fname, nfd);
	    fout[nfd] = fopen(fname[nfd], "w");
	    if (fout[nfd] == NULL) {
		Error("Could not open plot data file '%s'", fname[nfd]);
	    }
	    fd_map[fd] = nfd;
	    nfd++;
	}
	i_fd = fd_map[fd];

	t_delta = rec.t_end - rec.t_start;

	(void) fprintf(fout[i_fd], "%"PRIu64" %"PRIu64"\n", rec.t_end - t_base[nsched], t_delta);
    }

    paa->nfd = nfd;
    Message("%d AIO devices found.", nfd);

    for (i=0; i<nfd; i++) {
	if (fclose(fout[i]) != 0) {
	    Error("Could not close plot data file '%s'", fname[i]);
	}
    }
}

static void plot_bad_intervals(AIO_ANALYZER *paa)
{
    int                rectype, nsched;
    int                i_rectype;
    long long          nrec;
    RECORD             rec;
    int                i;
    char               fname[MAX_SCHED][1024];
    FILE              *fout[MAX_SCHED];
    uint64_t           t_base[MAX_SCHED];
    int                first_rec[MAX_SCHED];
    int                fd_map[MAX_FD];
    int64_t            t_delta, t_len;
    int                i_fd, fd, nfd;
    int                in_interval[MAX_FD];
    uint64_t           t_interval[MAX_FD];
    uint64_t           old_t_delta[MAX_FD];
    uint64_t           threshold;

    Message("Finding intervals with high AIO delay");

    nfd = 0;
    for (i=0; i<MAX_FD; i++) {
        fd_map[i]      = -1;
	in_interval[i] = 0;
	t_interval[i]  = 0;
	old_t_delta[i] = 0;
    }

    for (i=0; i<MAX_SCHED; i++) {
        t_base[i]    = 0;
        first_rec[i] = 1;
    }

    nrec = 0;
    threshold = 1000;  // 1 msec

    rewind(paa->fmerge);
    while (get_record(paa->fmerge, &rec)) {
        nrec++;
	if (nrec < paa->nrecs_skip_post_merge) {
	    continue;
	}
	if (rec.flags & AIO_TRACE_SCHED_MISMATCH_FLAG) {
	    continue;
	}

        rectype   = (rec.flags & AIO_TRACE_WRITE_FLAG) ? WRITE_REC : READ_REC;
        nsched    = rec.nsched;
	fd        = rec.fd;
        i_rectype = RecType[rectype].i;

	if (first_rec[nsched]) {
	    first_rec[nsched] = 0;
	    t_base[nsched]    = rec.t_start;
	}

	if (fd >= MAX_FD) {
	    Error("fd=%d is too large (%d is max)", fd, MAX_FD);
	}
	if (fd_map[fd] == -1) {
	    (void) sprintf(fname[nfd], "%s_%d_slow.dat", paa->plot_fname, nfd);
	    fout[nfd] = fopen(fname[nfd], "w");
	    if (fout[nfd] == NULL) {
		Error("Could not open plot data file '%s'", fname[nfd]);
	    }
	    fd_map[fd] = nfd;
	    nfd++;
	}
	i_fd = fd_map[fd];

	t_delta = rec.t_end - rec.t_start;

	if (in_interval[i_fd]) {
	    if (t_delta < threshold) {
	        in_interval[i_fd] = 0;
		(void) fprintf(fout[i_fd], "# end %"PRIu64"\n", rec.t_start - t_interval[i_fd]);
	        t_interval[i_fd]  = rec.t_start;
	    }
	} else {
	    if (t_delta >= threshold) {
	        in_interval[i_fd] = 1;
		if (t_interval[i_fd] == 0) {
		    t_interval[i_fd] = t_base[nsched];
		}
		if (rec.t_start > (t_interval[i_fd] + old_t_delta[i_fd])) {
		    t_len = rec.t_start - t_interval[i_fd];
		} else {
		    t_len = old_t_delta[i_fd];
		}
		(void) fprintf(fout[i_fd], "# start %"PRIu64"\n", t_len);
	        t_interval[i_fd]  = rec.t_start;
		old_t_delta[i_fd] = t_delta;
	    }
	}

	if (1+in_interval[i_fd]) {
	    (void) fprintf(fout[i_fd], "%"PRIu64" %"PRIu64"\n", rec.t_end - t_base[nsched], t_delta);
	}
    }

    paa->nfd = nfd;
    Message("%d AIO devices found.", nfd);

    for (i=0; i<nfd; i++) {
	if (fclose(fout[i]) != 0) {
	    Error("Could not close plot data file '%s'", fname[i]);
	}
    }
}

static int log_nbits(uint64_t x)
{
    int        i;
    uint64_t   z;

    for (i=0; i<64; i++) {
	z = 1<<i;
	if (x < z) {
	    break;
	}
    }

    return(i);
}

static void dump_histo_files(AIO_ANALYZER *paa)
{
    int                rectype, nsched;
    int                i_rectype;
    long long          nrec;
    RECORD             rec;
    int                i, j;
    char               fname[MAX_SCHED][1024];
    char               ofname[1024];
    FILE	      *fo;
    FILE              *fout[MAX_SCHED];
    int                fd_map[MAX_FD];
    int                inv_fd_map[MAX_FD];
    int64_t            t_delta;
    int                i_fd, fd, nfd, nbits;
    double             d_n[MAX_FD];
    double             d_mean[MAX_FD];
    double             d_sd[MAX_FD];
    double             d_min[MAX_FD];
    double             d_max[MAX_FD];
    double             s_n, s_mean, s_sd, s_min, s_max;
    double             sum, tsum;

    uint64_t           histo[MAX_FD][64];
    uint64_t           ohisto[64];
    uint64_t           x;

    Message("Dumping histogram of AIO delays");

    for (j=0; j<64; j++) {
	ohisto[j] = 0;
    }

    nfd = 0;
    for (i=0; i<MAX_FD; i++) {
        fd_map[i]     = -1;
        inv_fd_map[i] = -1;
	for (j=0; j<64; j++) {
	    histo[i][j] = 0;
	}
	d_n[i]    = 0;
	d_mean[i] = 0;
	d_sd[i]   = 0;
	d_min[i]  = 1.0e99;
	d_max[i]  = 0;
    }

    rewind(paa->fmerge);
    nrec = 0;
    while (get_record(paa->fmerge, &rec)) {
        nrec++;
	if (nrec < paa->nrecs_skip_post_merge) {
	    continue;
	}
	if (rec.flags & AIO_TRACE_SCHED_MISMATCH_FLAG) {
	    continue;
	}

        rectype   = (rec.flags & AIO_TRACE_WRITE_FLAG) ? WRITE_REC : READ_REC;
        nsched    = rec.nsched;
	fd        = rec.fd;
        i_rectype = RecType[rectype].i;

	if (fd >= MAX_FD) {
	    Error("fd=%d is too large (%d is max)", fd, MAX_FD);
	}
	if (fd_map[fd] == -1) {
	    (void) sprintf(fname[nfd], "%s_%d.histo", paa->plot_fname, nfd);
	    fout[nfd] = fopen(fname[nfd], "w");
	    if (fout[nfd] == NULL) {
		Error("Could not open histo data file '%s'", fname[nfd]);
	    }
	    fd_map[fd] = nfd;
	    nfd++;
	}
	i_fd = fd_map[fd];
	inv_fd_map[i_fd] = fd;

	t_delta = rec.t_end - rec.t_start;

        d_n[i_fd]++;
	d_mean[i_fd] += t_delta;
	d_sd[i_fd]   += (((double) t_delta)*t_delta);
	if (t_delta < d_min[i_fd]) {
	    d_min[i_fd] = t_delta;
	}
	if (t_delta > d_max[i_fd]) {
	    d_max[i_fd] = t_delta;
	}

	nbits = log_nbits(t_delta);
	histo[i_fd][nbits]++;
	ohisto[nbits]++;
    }

    s_n    = 0;
    s_mean = 0;
    s_sd   = 0;
    s_min  = 1.0e99;
    s_max  = 0;

    for (i=0; i<nfd; i++) {
        s_n    += d_n[i];
        s_mean += d_mean[i];
	s_sd   += d_sd[i];
	if (d_min[i] < s_min) {
	    s_min = d_min[i];
	}
	if (d_max[i] > s_max) {
	    s_max = d_max[i];
	}

        if (d_n[i] > 0) {
	    d_mean[i] /= d_n[i];
	    d_sd[i]   /= d_n[i];
	    d_sd[i]    = sqrt(d_sd[i] - d_mean[i]*d_mean[i]);
	}

	(void) fprintf(fout[i], "# AIO access time histogram for fd %d\n", inv_fd_map[i]);
	(void) fprintf(fout[i], "# n:    %g\n", d_n[i]);
	(void) fprintf(fout[i], "# mean: %g\n", d_mean[i]);
	(void) fprintf(fout[i], "# sd:   %g\n", d_sd[i]);
	(void) fprintf(fout[i], "# min:  %g\n", d_min[i]);
	(void) fprintf(fout[i], "# max:  %g\n", d_max[i]);
	x = 1;
	tsum = 0;
	for (j=0; j<64; j++) {
	    if (histo[i][j] > 0) {
		(void) fprintf(fout[i], "%10"PRIu64" %"PRIu64"\n", x, histo[i][j]);
	    }
	    tsum += histo[i][j];
	    x <<= 1;
	}

	(void) fprintf(fout[i], "\n# Cummulative response time distribution for fd %d\n\n", inv_fd_map[i]);
	sum = 0;
	x = 1;
	for (j=0; j<64; j++) {
	    if (histo[i][j] > 0) {
		sum += histo[i][j];
		(void) fprintf(fout[i], "%10"PRIu64" %g%%\n", x, 100.0*sum/tsum);
	    }
	    x <<= 1;
	}
    }
    if (s_n > 0) {
	s_mean /= s_n;
	s_sd   /= s_n;
	s_sd    = sqrt(s_sd - s_mean*s_mean);
    }
        
    (void) sprintf(ofname, "%s.histo", paa->plot_fname);
    fo = fopen(ofname, "w");
    if (fo == NULL) {
	Error("Could not open histo data file '%s'", ofname);
    }


    (void) fprintf(fo, "# AIO access time histogram over all devices\n");
    (void) fprintf(fo, "# n:    %g\n", s_n);
    (void) fprintf(fo, "# mean: %g\n", s_mean);
    (void) fprintf(fo, "# sd:   %g\n", s_sd);
    (void) fprintf(fo, "# min:  %g\n", s_min);
    (void) fprintf(fo, "# max:  %g\n", s_max);

    x = 1;
    tsum = 0;
    for (j=0; j<64; j++) {
	if (ohisto[j] > 0) {
	    (void) fprintf(fo, "%10"PRIu64" %"PRIu64"\n", x, ohisto[j]);
	}
	tsum += ohisto[j];
	x <<= 1;
    }

    (void) fprintf(fo, "\n# Cummulative response time distribution for all devices\n\n");

    x = 1;
    sum = 0;
    for (j=0; j<64; j++) {
	if (ohisto[j] > 0) {
	    (void) fprintf(fo, "%10"PRIu64" %g%%\n", x, 100.0*sum/tsum);
	}
	sum += ohisto[j];
	x <<= 1;
    }

    if (fclose(fo) != 0) {
	Error("Could not close histo data file '%s'", ofname);
    }

    for (i=0; i<nfd; i++) {
	if (fclose(fout[i]) != 0) {
	    Error("Could not close histo data file '%s'", fname[i]);
	}
    }

    paa->nfd = nfd;
    Message("%d AIO devices found.", nfd);
}

static void dump_interval_histo_files(AIO_ANALYZER *paa)
{
    int                rectype, nsched;
    int                i_rectype;
    long long          nrec;
    RECORD             rec;
    int                i, j;
    char               fname[MAX_SCHED][1024];
    char               ofname[1024];
    FILE              *fout[MAX_SCHED];
    FILE              *fo;
    int                fd_map[MAX_FD];
    int                inv_fd_map[MAX_FD];
    int64_t            t_delta;
    int                i_fd, fd, nfd, nbits;
    double             d_n[MAX_FD];
    double             d_mean[MAX_FD];
    double             d_sd[MAX_FD];
    double             d_min[MAX_FD];
    double             d_max[MAX_FD];
    double             s_n, s_mean, s_sd, s_min, s_max;
    uint64_t           histo[MAX_FD][64];
    uint64_t           ohisto[64];
    uint64_t           x;
    int                in_interval[MAX_FD];
    uint64_t           t_interval[MAX_FD];
    uint64_t           old_t_delta[MAX_FD];
    uint64_t           threshold;
    uint64_t           d_interval;
    double             sum, tsum;

    Message("Dumping histogram of AIO slow intervals");

    for (j=0; j<64; j++) {
	ohisto[j] = 0;
    }

    nfd = 0;
    for (i=0; i<MAX_FD; i++) {
        fd_map[i]     = -1;
        inv_fd_map[i] = -1;
	for (j=0; j<64; j++) {
	    histo[i][j] = 0;
	}
	d_n[i]    = 0;
	d_mean[i] = 0;
	d_sd[i]   = 0;
	d_min[i]  = 1.0e99;
	d_max[i]  = 0;

	in_interval[i] = 0;
	t_interval[i]  = 0;
	old_t_delta[i] = 0;
    }

    nrec        = 0;
    threshold   = 1000;  // 1 msec

    rewind(paa->fmerge);
    while (get_record(paa->fmerge, &rec)) {
        nrec++;
	if (nrec < paa->nrecs_skip_post_merge) {
	    continue;
	}
	if (rec.flags & AIO_TRACE_SCHED_MISMATCH_FLAG) {
	    continue;
	}

        rectype   = (rec.flags & AIO_TRACE_WRITE_FLAG) ? WRITE_REC : READ_REC;
        nsched    = rec.nsched;
	fd        = rec.fd;
        i_rectype = RecType[rectype].i;

	if (fd >= MAX_FD) {
	    Error("fd=%d is too large (%d is max)", fd, MAX_FD);
	}
	if (fd_map[fd] == -1) {
	    (void) sprintf(fname[nfd], "%s_%d_slow.histo", paa->plot_fname, nfd);
	    fout[nfd] = fopen(fname[nfd], "w");
	    if (fout[nfd] == NULL) {
		Error("Could not open histo data file '%s'", fname[nfd]);
	    }
	    fd_map[fd] = nfd;
	    nfd++;
	}
	i_fd = fd_map[fd];
	inv_fd_map[i_fd] = fd;

	t_delta = rec.t_end - rec.t_start;

	if (in_interval[i_fd]) {
	    if (t_delta < threshold) {
	        in_interval[i_fd] = 0;

		if (rec.t_start > (t_interval[i_fd] + old_t_delta[i_fd])) {
		    d_interval = rec.t_start - t_interval[i_fd];
		} else {
		    d_interval = old_t_delta[i_fd];
		}

		d_n[i_fd]++;
		d_mean[i_fd] += d_interval;
		d_sd[i_fd]   += (((double) d_interval)*d_interval);
		if (d_interval < d_min[i_fd]) {
		    d_min[i_fd] = d_interval;
		}
		if (d_interval > d_max[i_fd]) {
		    d_max[i_fd] = d_interval;
		}

		nbits = log_nbits(d_interval);
		histo[i_fd][nbits]++;
		ohisto[nbits]++;
	    }
	} else {
	    if (t_delta >= threshold) {
	        in_interval[i_fd] = 1;
		t_interval[i_fd]  = rec.t_start;
		old_t_delta[i_fd] = t_delta;
	    }
	}

    }

    s_n    = 0;
    s_mean = 0;
    s_sd   = 0;
    s_min  = 1.0e99;
    s_max  = 0;

    for (i=0; i<nfd; i++) {
        s_n    += d_n[i];
        s_mean += d_mean[i];
	s_sd   += d_sd[i];
	if (d_min[i] < s_min) {
	    s_min = d_min[i];
	}
	if (d_max[i] > s_max) {
	    s_max = d_max[i];
	}

        if (d_n[i] > 0) {
	    d_mean[i] /= d_n[i];
	    d_sd[i]   /= d_n[i];
	    d_sd[i]    = sqrt(d_sd[i] - d_mean[i]*d_mean[i]);
	}

	(void) fprintf(fout[i], "# AIO slow interval histogram for fd %d\n", inv_fd_map[i]);
	(void) fprintf(fout[i], "# n:    %g\n", d_n[i]);
	(void) fprintf(fout[i], "# mean: %g\n", d_mean[i]);
	(void) fprintf(fout[i], "# sd:   %g\n", d_sd[i]);
	(void) fprintf(fout[i], "# min:  %g\n", d_min[i]);
	(void) fprintf(fout[i], "# max:  %g\n", d_max[i]);
	x = 1;
	tsum = 0;
	for (j=0; j<64; j++) {
	    if (histo[i][j] > 0) {
		(void) fprintf(fout[i], "%10"PRIu64" %"PRIu64"\n", x, histo[i][j]);
	    }
	    tsum += histo[i][j];
	    x <<= 1;
	}

	(void) fprintf(fout[i], "\n# Cummulative slow interval distribution for fd %d\n\n", inv_fd_map[i]);
	sum = 0;
	x = 1;
	for (j=0; j<64; j++) {
	    if (histo[i][j] > 0) {
		sum += histo[i][j];
		(void) fprintf(fout[i], "%10"PRIu64" %g%%\n", x, 100.0*sum/tsum);
	    }
	    x <<= 1;
	}
    }
    if (s_n > 0) {
	s_mean /= s_n;
	s_sd   /= s_n;
	s_sd    = sqrt(s_sd - s_mean*s_mean);
    }
        
    (void) sprintf(ofname, "%s_slow.histo", paa->plot_fname);
    fo = fopen(ofname, "w");
    if (fo == NULL) {
	Error("Could not open histo data file '%s'", ofname);
    }

    (void) fprintf(fo, "# AIO slow interval histogram over all devices\n");
    (void) fprintf(fo, "# n:    %g\n", s_n);
    (void) fprintf(fo, "# mean: %g\n", s_mean);
    (void) fprintf(fo, "# sd:   %g\n", s_sd);
    (void) fprintf(fo, "# min:  %g\n", s_min);
    (void) fprintf(fo, "# max:  %g\n", s_max);

    x = 1;
    tsum = 0;
    for (j=0; j<64; j++) {
	if (ohisto[j] > 0) {
	    (void) fprintf(fo, "%10"PRIu64" %"PRIu64"\n", x, ohisto[j]);
	}
	tsum += ohisto[j];
	x <<= 1;
    }

    (void) fprintf(fo, "\n# Cummulative slow interval distribution for all devices\n\n");

    x = 1;
    sum = 0;
    for (j=0; j<64; j++) {
	if (ohisto[j] > 0) {
	    (void) fprintf(fo, "%10"PRIu64" %g%%\n", x, 100.0*sum/tsum);
	}
	sum += ohisto[j];
	x <<= 1;
    }

    if (fclose(fo) != 0) {
	Error("Could not close histo data file '%s'", ofname);
    }

    paa->nfd = nfd;
    Message("%d AIO devices found.", nfd);

    for (i=0; i<nfd; i++) {
	if (fclose(fout[i]) != 0) {
	    Error("Could not close histo data file '%s'", fname[i]);
	}
    }
}


/************************************************************************/

static BOOLEAN get_record(FILE *f, RECORD *pr)
{
    BOOLEAN  ret;
    int      rectype;

    ret = TRUE;

    if (fread((void *) pr, sizeof(RECORD), 1, f) != 1) {
	return(FALSE);
    }

    return(ret);
}

static BOOLEAN put_record(FILE *f, RECORD *pr)
{
    BOOLEAN  ret;
    int      rectype;

    ret = TRUE;

    if (fwrite((void *) pr, sizeof(RECORD), 1, f) != 1) {
	ret = FALSE;
    }

    return(ret);
}

/************************************************************************/

static char *skip_white(char *sin)
{
    char *s;

    for (s=sin; (*s == ' ') || (*s == '\t'); s++);

    return(s);
}

static void Error(char *fmt, ...)
{
   char     stmp[1024];
   va_list  args;

   va_start(args, fmt);

   (void) fprintf(stderr, "Error: ");
   vfprintf(stderr, fmt, args);
   (void) fprintf(stderr, "\n");

   va_end(args);

   exit(1);
}

static void Warning(char *fmt, ...)
{
   char     stmp[1024];
   va_list  args;

   va_start(args, fmt);

   (void) fprintf(stderr, "Warning: ");
   vfprintf(stderr, fmt, args);
   (void) fprintf(stderr, "\n");

   va_end(args);
}

static void Message(char *fmt, ...)
{
   char     stmp[1024];
   va_list  args;

   va_start(args, fmt);

   // (void) fprintf(stderr, "Message: ");
   vfprintf(stderr, fmt, args);
   (void) fprintf(stderr, "\n");

   va_end(args);
}

static void DebugMsg(char *fmt, ...)
{
#ifdef DEBUG
   char     stmp[1024];
   va_list  args;

   va_start(args, fmt);

   (void) fprintf(stderr, "Debug: ");
   vfprintf(stderr, fmt, args);
   (void) fprintf(stderr, "\n");

   va_end(args);
#endif
}

static void LineError(char *fmt, ...)
{
   char     stmp[1024];
   va_list  args;

   va_start(args, fmt);

   (void) fprintf(stderr, "Error at line %d: ", LineNum);
   vfprintf(stderr, fmt, args);
   (void) fprintf(stderr, "\n");

   va_end(args);

   exit(1);
}

static void LineWarning(char *fmt, ...)
{
   char     stmp[1024];
   va_list  args;

   va_start(args, fmt);

   (void) fprintf(stderr, "Warning at line %d: ", LineNum);
   vfprintf(stderr, fmt, args);
   (void) fprintf(stderr, "\n");

   va_end(args);
}

static void InitGetLine()
{
    Line = malloc(LineLength);
}

static char *GetLine(FILE *f)
{
    size_t    n;
    STRING    stmp;

    while (TRUE) {
        if (getline(&Line, &LineLength, f) == -1) {
	    return(NULL);
	}
        LineNum++;
	if (sscanf(Line, "%s", stmp) == 1) {
	    /* this line is NOT blank */
	    return(Line);
	}
    }
    return(Line);
}

static void GetLineString(char *s, FILE *f)
{
    size_t    n;

    while (TRUE) {
        if (getline(&Line, &LineLength, f) != -1) {
	    break;
	}
        LineNum++;
	if (strstr(Line, s)) {
	    return;
	}
    }
    LineWarning("Reached end of file!");
}


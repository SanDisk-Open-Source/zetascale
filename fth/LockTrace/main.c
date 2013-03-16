/******************************************************************
 *
 * locktite -- A program to to analyze fth lock trace files.
 *
 * Brian O'Krafka   10/15/08
 *
 *  NOTES:
 *     - adjust all times so they start at zero?
 *     - put common defs into a single file that is shared by fthLock.h
 *     - add option to skip merging files if this has already been done
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
#include "locktite.h"

// #define DEBUG

#define LINE_LENGTH  1024

static int  LineLength = LINE_LENGTH;
static char *Line;
static int  LineNum;
static int  Debug = 0;

#define MAKE_LOCKMAP_KEY(skey, rec) {(void) sprintf(skey, "%p,%p", rec.plock, rec.bt[1]);}

static void usage(char *sprog)
{
    fprintf(stderr, "usage: %s <trace_file_directory> <executable> <n_skip> [<n_dump_recs>] > <output_file>\n", sprog);
    fprintf(stderr, "<trace_file_directory>: directory containing lock trace files\n");
    fprintf(stderr, "<executable>: executable that generated the lock trace files\n");
    fprintf(stderr, "<n_skip>: skip the first n_skip trace records\n");
    fprintf(stderr, "<n_dump_recs>: number of trace records to dump [optional]\n");
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

static void check_rectype_info();
static int get_trace_file_names(char *trace_dir, struct dirent ***peps);
static char *get_srcline(char *fname, int line);
static BOOLEAN get_record(FILE *f, RECORD *pr);
static BOOLEAN put_record(FILE *f, RECORD *pr);
static void init_stats(STATS *ps);
static char *find_srcline(SRCDATA *psd, int line, char *basedir, char *linefile);
static int get_fth_map(SDFxTLMap_t *pfthmap, uint64_t fth);
static void get_profile(LOCKANALYZER *pla, FILE *fout);
static int file_count_sortfn(const void *e1_in, const void *e2_in);
static FILE *merge_trace_files(LOCKANALYZER *pla);
static void process_backtrace_info(LOCKANALYZER *pla);
static void load_line_map(FILE *fgdbout, LOCKANALYZER *pla);
static void load_fth_table(LOCKANALYZER *pla);
static LOCKDATA *load_lock_table(LOCKANALYZER *pla, RECORD *pr, long long nrec);
static void init_srcdata(SRCDATA *psd, char *srcdirfile);
static void dump_lock_info(SDFxTLMap_t *psrcmap, LOCKDATA *pld, FILE *fout, uint64_t t_base);
static void dump_stats(LOCKANALYZER *pla, FILE *fout);
static int lock_time_sortfn(const void *e1_in, const void *e2_in);
static void get_file_line(char *sin, char *linefile, int *pline);
static void check_trace(LOCKANALYZER *pla, int n_dump_recs, int n_dump_lock);
static void dump_trace_rec(LOCKANALYZER *pla, FILE *f, RECORD *pr);
static void get_base_time(LOCKANALYZER *pla);

LOCKANALYZER  LockAnalyzer;

main(int argc, char **argv)
{
    FILE          *fout;
    FILE          *fmerge;
    char          *trace_dir, *exec_name, *src_dir;
    struct dirent **eps;
    LOCKANALYZER  *pla;
    int            n_dump_recs, n_dump_lock;

    InitGetLine();
    pla = &LockAnalyzer;
    pla->f_time_normalize = 1.0/(3000.0); /* tics per usec */

    if ((argc != 4) && (argc != 5)) {
	usage(argv[0]);
    }

    /* usage: %s <trace_file_directory> <executable> <n_skip> [<n_dump_recs>] */

    n_dump_recs = 0;
    n_dump_lock = 0;

    pla->n_neg_time     = 0;

    pla->trace_dir      = argv[1];
    pla->executable     = argv[2];
    pla->nrecs_skip     = atoi(argv[3]);
    if (argc == 5) {
	n_dump_recs = atoi(argv[4]);
    }
    pla->srcdir         = NULL;
    pla->srcpaths_file  = NULL;

    Message("trace directory: %s", pla->trace_dir);
    Message("executable: %s", pla->executable);
    Message("number of trace records to skip: %"PRIu64, pla->nrecs_skip);
    Message("number of trace records to dump: %d", n_dump_recs);

    fout = stdout;

    /* initialization */

    check_rectype_info();

    /* load trace file names */

    pla->nfiles = get_trace_file_names(pla->trace_dir, &pla->eps);

    /* merge trace files into one, sorted by time */

    pla->mergefile = "LT_merge";
    pla->fmerge = merge_trace_files(pla);

    /* check trace */

    check_trace(pla, n_dump_recs, n_dump_lock);

    /* get backtrace data */

    process_backtrace_info(pla);

    /* load table of lock info and fth map */

    load_fth_table(pla);

    /* generate report */

    init_stats(&(pla->stats));
    get_profile(pla, fout);
    dump_stats(pla, fout);

    if (pla->n_neg_time > 0) {
        Warning("%lld instances of negative lock times!", pla->n_neg_time);
    }
    Message("%lld total trace records (%"PRIu64" skipped)", pla->nrecs, pla->nrecs_skip);

    return(0);
}

static void dump_trace_rec(LOCKANALYZER *pla, FILE *f, RECORD *pr)
{
    int       i, rectype, nsched;
    char     *stype;

    rectype = GET_RECTYPE(*pr);
    nsched  = GET_NSCHED(*pr);

    if (rectype >= N_RECTYPES) {
        stype = "--BAD--";
    } else {
        stype = RecType[rectype].name;
    }

    (void) fprintf(f, "%s(%"PRIu64") 0x%"PRIx64", 0x%"PRIx64, stype, pr->t, pr->plock, pr->fth);

    switch (rectype) {
	case LOCK_ADHOC_INIT_REC:
	case LOCK_INIT_REC:
	    for (i=0; i<BT_SIZE; i++) {
	        if (i > 0) {
		    (void) fprintf(f, ",");
		}
		(void) fprintf(f, " %"PRIx64, pr->bt[i]);
	    }
	    break;
	default:
	    /* purposefully empty */
	    break;
    }
    (void) fprintf(f, "\n");
}

static void get_base_time(LOCKANALYZER *pla)
{
    int        rectype, nsched;
    long long  nrec;
    uint64_t   tbase;
    int        n_found_lock;
    RECORD     rec;
    BOOLEAN    found_first;

    tbase = 0;
    found_first = FALSE;
    rewind(pla->fmerge);
    while (get_record(pla->fmerge, &rec)) {
        nrec++;

	rectype = GET_RECTYPE(rec);
	nsched  = GET_NSCHED(rec);

	switch (rectype) {
	    case LOCK_ADHOC_INIT_REC:
	    case LOCK_INIT_REC:
	        if (!found_first) {
		    found_first = TRUE;
		    tbase       = rec.t;
		    pla->t_base = tbase;
		    Message("*****  Base Init Time: %"PRIu64"  *****", pla->t_base);
		}
		if (rec.t < tbase) {
		    Warning("rec %d: t (%"PRIu64") < tbase (%"PRIu64")!", rec.t, tbase);
		}
		break;
	    default:
	        /* purposefully empty */
		break;
	}
    }
}

static void check_trace(LOCKANALYZER *pla, int n_dump_recs, int n_dump_lock)
{
    int        rectype, nsched;
    long long  nbad, nrec, n_adhoc, n_init, n_rdlk, n_wrlk;
    int        n_found_lock;
    long long  dt, n_neg, n_neg_max;
    RECORD     rec;
    BOOLEAN    found_lock;

    nbad    = 0;
    nrec    = 0;
    n_adhoc = 0;
    n_init  = 0;
    n_rdlk  = 0;
    n_wrlk  = 0;

    found_lock   = FALSE;
    n_found_lock = 0;
    n_neg_max    = 100;
    n_neg        = 0;

    rewind(pla->fmerge);
    while (get_record(pla->fmerge, &rec)) {
        nrec++;

        if (nrec < n_dump_recs) {
            dump_trace_rec(pla, stdout, &rec);
	}

	rectype = GET_RECTYPE(rec);
	nsched  = GET_NSCHED(rec);

	if (found_lock && 
	    (n_found_lock < n_dump_lock) &&
	    ((rectype == LOCK_RD_REC) || (rectype == LOCK_WR_REC))) 
	{
            dump_trace_rec(pla, stdout, &rec);
	}

	switch (rectype) {
	    case LOCK_RD_REC:
	        found_lock = TRUE;
		n_found_lock++;
	        n_rdlk++;
		dt = rec.t;
		if (dt < 0) {
		    if (n_neg < n_neg_max) {
			Warning("Record %lld for lock %p has negative time: %lld", nrec, rec.plock, dt);
		    }
		    n_neg++;
		}
		break;
	    case LOCK_WR_REC:
	        found_lock = TRUE;
		n_found_lock++;
	        n_wrlk++;
		dt = rec.t;
		if (dt < 0) {
		    if (n_neg < n_neg_max) {
			Warning("Record %lld for lock %p has negative time: %lld", nrec, rec.plock, dt);
		    }
		    n_neg++;
		}
		break;
	    case LOCK_ADHOC_INIT_REC:
	    case LOCK_INIT_REC:
	    default:
		nbad++;
		if (nbad < 50) {
		    Message("*****  Bad trace rectype: %d  *****", rectype);
		}
		break;
	}
    }
    Message(">>>>>>>>>>>>>>   Total bad rectype's: %d out of %lld (%lld adhoc, %lld init, %lld rdlk, %lld wrlk   <<<<<<<<<<<<<<<<<<<<<<<", nbad, nrec, n_adhoc, n_init, n_rdlk, n_wrlk);
}

static void init_lock_stats(PER_LOCK_STATS *pls)
{
    int  i;

    for (i=0; i<N_RECTYPES; i++) {
	pls->counts[i] = 0;
    }
    pls->n_rd          = 0;
    pls->sum_time_rd   = 0;
    pls->sum2_time_rd  = 0;

    pls->n_wr          = 0;
    pls->sum_time_wr   = 0;
    pls->sum2_time_wr  = 0;
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
    (void) fprintf(fout, "-------------------------------------------------------------------------------------------\n");
}

static void dump_stats(LOCKANALYZER *pla, FILE *fout)
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
    LOCKDATA          *pld;
    double             f;
    SRCENTRY          *pse;
    SDFxTLMap_t       *psrcmap;
    char               stmp[10000];

    psrcmap  = &(pla->srcmap);

    f = pla->f_time_normalize;
    n_lock_dump = 50;

    ps = &(pla->stats);

    #ifdef notdef
    /************************************************************************
     *   Counts
     ************************************************************************/

    do_separator_line(fout);

    (void) fprintf(fout, "Total Lock Requests Per Scheduler\n");
    for (i=0; i<=pla->nsched; i++) {
	(void) fprintf(fout, "     %2d    ", i);
    }
    (void) fprintf(fout, "\n");
    for (i=0; i<=pla->nsched; i++) {
	(void) fprintf(fout, " %10g", ps->sched_counts[i]);
    }
    (void) fprintf(fout, "\n");

    do_separator_line(fout);
    (void) fprintf(fout, "Total Lock Requests Per fth Thread\n");
    for (i=0; i<pla->nfth; i++) {
	(void) fprintf(fout, "     %2d    ", i);
    }
    (void) fprintf(fout, "\n");
    for (i=0; i<pla->nfth; i++) {
	(void) fprintf(fout, " %10g", ps->fth_counts[i]);
    }
    (void) fprintf(fout, "\n");
    #endif

    do_separator_line(fout);
    (void) fprintf(fout, "Lock Requests Per Scheduler\n");
    (void) fprintf(fout, "%10s", "Scheduler");
    for (k=1; k<N_RECTYPES; k++) {
	(void) fprintf(fout, " %10s", RecType[k].name);
    }
    (void) fprintf(fout, "\n");
    for (i=0; i<=pla->nsched; i++) {
	(void) fprintf(fout, "%10d", i);
	for (k=1; k<N_RECTYPES; k++) {
	    (void) fprintf(fout, " %10g", ps->schedrec_counts[i][k]);
	}
	(void) fprintf(fout, "\n");
    }

    do_separator_line(fout);
    (void) fprintf(fout, "Lock Requests Per fth Thread\n");
    for (k=1; k<N_RECTYPES; k++) {
	(void) fprintf(fout, " %10s", RecType[k].name);
    }
    (void) fprintf(fout, "%30s", "fthread");
    (void) fprintf(fout, "\n");
    for (j=0; j<pla->nfth; j++) {
	for (k=1; k<N_RECTYPES; k++) {
	    (void) fprintf(fout, " %10g", ps->fthrec_counts[j][k]);
	}
	(void) fprintf(fout, "%10d", j);
	fth = pla->inv_fthmap[j];
	
	pxme = SDFxTLMapGet(&(pla->fthmap), fth);
	if (pxme != NULL) {
	    pfd = (FTHDATA *) pxme->contents;
	    assert(pfd != NULL);
	    s = pfd->func_name;
	} else {
	    s = NULL;
	}
	if (s == NULL) {
	    s = " ";
	}
	(void) fprintf(fout, " %s", s);
	(void) fprintf(fout, "\n");
    }

    /************************************************************************
     *   Locks sorted by wait times
     ************************************************************************/

    qsort((void *) &(pla->sorted_locks), (size_t) pla->nlocks, 
	  (size_t) sizeof(LOCKDATA *), lock_time_sortfn);

    do_separator_line(fout);
    (void) fprintf(fout, "Lock Wait Times for %d Hottest Locks and %d Coolest Locks (Times in usec) (%d locks total)\n", n_lock_dump, n_lock_dump, pla->nlocks);
    (void) fprintf(fout, "       (Assuming %g rdtsc ticks/usec)\n\n", 1.0/f);

    (void) fprintf(fout, "%15s %10s %10s %10s %10s %10s %10s %15s\n\n",
        "Lock", "Read Tot", "(Avg)", "[N]", "Write Tot", "(Avg)", "[N]", "Where");
    n = 0;
    for (i=0; i<pla->nfth; i++) {
	tot_read[i]  = 0;
	tot_write[i] = 0;
    }
    for (i=0; i<pla->nlocks; i++) {
        n++;
	pld = pla->sorted_locks[i];
	i_fth = get_fth_map(&(pla->fthmap), pld->rec.fth);
	tot_read[i_fth]  += pld->stats.sum_time_rd;
	tot_write[i_fth] += pld->stats.sum_time_wr;

	if ((n > n_lock_dump) && (n < (pla->nlocks - n_lock_dump))) {
	    continue;
	}

	if (pld->stats.n_rd > 0) {
	    av_rd  = pld->stats.sum_time_rd/pld->stats.n_rd;
	    av2_rd = pld->stats.sum2_time_rd/pld->stats.n_rd;
	    sd_rd  = sqrt(av2_rd - av_rd*av_rd);
	} else {
	    av_rd = 0;
	    sd_rd = 0;
	}
	if (pld->stats.n_wr > 0) {
	    av_wr  = pld->stats.sum_time_wr/pld->stats.n_wr;
	    av2_wr = pld->stats.sum2_time_wr/pld->stats.n_wr;
	    sd_wr  = sqrt(av2_wr - av_wr*av_wr);
	} else {
	    av_wr = 0;
	    sd_wr = 0;
	}

        if ((pld->stats.n_rd + pld->stats.n_wr) > 0) {
	    pxme = SDFxTLMapGet(psrcmap, pld->rec.bt[0]);
	    if (pxme != NULL) {
		if (pse != NULL) {
		    pse = (SRCENTRY *) pxme->contents;
		    (void) sprintf(stmp, "%s %d: %s", pse->fname, pse->line, pse->sline_single);
		    s = stmp;
		} else {
		    s = " ";
		}
	    } else {
	        s = " ";
	    }
	    (void) fprintf(fout, "%15p %10.3g (%8.3g) [%8.3g] %10.3g (%8.3g) [%8.3g] %s\n",
                pld->rec.plock, pld->stats.sum_time_rd*f, av_rd*f, pld->stats.n_rd, pld->stats.sum_time_wr*f, av_wr*f, pld->stats.n_wr, s);
	}
    }
    do_separator_line(fout);

    /************************************************************************
     *   Total lock wait times per fthread
     ************************************************************************/

    (void) fprintf(fout, "\n   Total Lock Wait Times (sec) Per fThread\n\n");
    (void) fprintf(fout, "%20s %10s %10s\n", "fthread  ", "Read  ", "Write   ");
    for (i=0; i<pla->nfth; i++) {
	(void) fprintf(fout, "%15d      %8.3g %8.3g\n", i, tot_read[i]/1.0e6, tot_write[i]/1.0e6);
    }
    (void) fprintf(fout, "\n");

    do_separator_line(fout);

    /************************************************************************
     *   Lock detail for locks sorted by wait times
     ************************************************************************/
    (void) fprintf(fout, "Detail for Hottest Locks (Times in usec)\n\n");
    (void) fprintf(fout, "    (Assuming %g rdtsc ticks/usec)\n\n", 1.0/f);
    
    n = 0;
    for (i=0; i<pla->nlocks; i++) {
        n++;

	pld = pla->sorted_locks[i];

	if (pld->stats.n_rd > 0) {
	    av_rd  = pld->stats.sum_time_rd/pld->stats.n_rd;
	    av2_rd = pld->stats.sum2_time_rd/pld->stats.n_rd;
	    sd_rd  = sqrt(av2_rd - av_rd*av_rd);
	} else {
	    av_rd = 0;
	    sd_rd = 0;
	}
	if (pld->stats.n_wr > 0) {
	    av_wr  = pld->stats.sum_time_wr/pld->stats.n_wr;
	    av2_wr = pld->stats.sum2_time_wr/pld->stats.n_wr;
	    sd_wr  = sqrt(av2_wr - av_wr*av_wr);
	} else {
	    av_wr = 0;
	    sd_wr = 0;
	}

        if ((av_rd + av_wr) > 0) {
	    (void) fprintf(fout, "Lock(%15p) Rd: %8.3g (%8.3g) [%8.3g], Wr: %8.3g (%8.3g) [%8.3g]\n", pld->rec.plock, pld->stats.sum_time_rd*f, av_rd*f, pld->stats.n_rd, pld->stats.sum_time_wr*f, av_wr*f, pld->stats.n_wr);
	    dump_lock_info(&(pla->srcmap), pld, fout, pla->t_base);
	    (void) fprintf(fout, "\n");
	}

	if (n > n_lock_dump) {
	    break;
	}
    }
    do_separator_line(fout);
}

static int lock_time_sortfn(const void *e1_in, const void *e2_in)
{
    LOCKDATA  *pld1, *pld2;
    double     t1, t2;
    PER_LOCK_STATS  *pls1, *pls2;

    pld1 = *((LOCKDATA **) e1_in);
    pld2 = *((LOCKDATA **) e2_in);
    pls1 = &(pld1->stats);
    pls2 = &(pld2->stats);

    t1 = pls1->sum_time_wr + pls1->sum_time_rd;
    t2 = pls2->sum_time_wr + pls2->sum_time_rd;

    #ifdef notdef
	if (t1 > 0) {
	    fprintf(stderr, "t1=%g, t2=%g\n", t1, t2);
	}
    #endif

    if (t1 < t2) {
        return(1);
    } else if (t1 > t2) {
        return(-1);
    } else {
        return(0);
    }
}

static void dump_fth_info(SDFxTLMap_t *psrcmap, FTHDATA *pfd, FILE *fout)
{
    int                rectype, nsched;

    rectype = GET_RECTYPE(pfd->rec);
    nsched  = GET_NSCHED(pfd->rec);

    (void) fprintf(fout, "fthread (0x%"PRIx64") '%s' on sched %d\n", pfd->fth, pfd->func_name, nsched);
}

static void dump_lock_info(SDFxTLMap_t *psrcmap, LOCKDATA *pld, FILE *fout, uint64_t t_base)
{
    int                i, rectype, nsched;
    SDFxTLMapEntry_t  *pxme;
    SRCENTRY          *pse;
    char              *s;

    rectype = GET_RECTYPE(pld->rec);
    nsched  = GET_NSCHED(pld->rec);

    (void) fprintf(fout, "    Lock(0x%"PRIx64") %s on sched %d\n", pld->rec.plock, RecType[rectype].name, - t_base, nsched);
    for (i=0; i<BT_SIZE; i++) {
        if (pld->rec.bt[i] == 0) {
	    break;
	}
	#ifdef notdef
	if (i == 0) {
	    continue; /* skip call to backtrace */
	}
	#endif

	pxme = SDFxTLMapGet(psrcmap, pld->rec.bt[i]);
	if (pxme == NULL) {
	    // Warning("Missing source line data for address 0x%"PRIx64, pld->rec.bt[i]);
	    (void) fprintf(fout, "       ----  UNKNOWN  ----\n");
	} else {
	    pse = (SRCENTRY *) pxme->contents;
	    if (pse != NULL) {
	        s = pse->sline_single;
	    } else {
	        s = " ";
	    }
	    (void) fprintf(fout, "       %20s %5d: %s\n", pse->fname, pse->line, s);
	}
    }
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

static void get_profile(LOCKANALYZER *pla, FILE *fout)
{
    RECORD             cur_rec[MAX_SCHED];
    int                rectype, nsched;
    int                i_rectype, i_fth, i_sched;
    long long          nrec;
    LOCKDATA          *pld;
    SDFTLMapEntry_t   *pxme;
    long long          dt;
    STATS             *ps;
    RECORD             rec;
    char               skey[100];

    ps = &(pla->stats);

    rewind(pla->fmerge);
    nrec = 0;
    while (get_record(pla->fmerge, &rec)) {
        nrec++;

        rectype = GET_RECTYPE(rec);
        nsched  = GET_NSCHED(rec);

        if (rectype == LOCK_END_REC) {
	    break;
	}

        i_rectype = RecType[rectype].i;
	i_fth     = get_fth_map(&(pla->fthmap), rec.fth);
	i_sched   = nsched;

	(ps->counts[i_sched][i_fth][i_rectype])++;
	(ps->schedrec_counts[i_sched][i_rectype])++;
	(ps->fthrec_counts[i_fth][i_rectype])++;
	(ps->sched_counts[i_sched])++;
	(ps->fth_counts[i_fth])++;

	switch (rectype) {
	    case LOCK_RD_REC:
	    case LOCK_WR_REC:
		MAKE_LOCKMAP_KEY(skey, rec);
		pxme = SDFTLMapGet(&(pla->lockmap), skey);
		if (pxme == NULL) {
		    pld = load_lock_table(pla, &rec, nrec);
		} else {
		    pld = (LOCKDATA *) pxme->contents;
		    assert(pld != NULL);
		}
		break;
	    default:
	        Error("Bad record type in get_profile.");
	        break;
	}

	switch (rectype) {
	    case LOCK_WR_REC:
	        dt = rec.t;
		if (dt < 0) {
		    if (pla->n_neg_time == 0) {
			Warning("negative time difference!");
		    }
		    (pla->n_neg_time)++;
		}
		(pld->stats.n_wr)++;
		pld->stats.sum_time_wr  += dt;
		pld->stats.sum2_time_wr += (dt*dt);
		break;
	    case LOCK_RD_REC:
		dt = rec.t;
		if (dt < 0) {
		    if (pla->n_neg_time == 0) {
			Warning("negative time difference!");
		    }
		    (pla->n_neg_time)++;
		}
		(pld->stats.n_rd)++;
		pld->stats.sum_time_rd  += dt;
		pld->stats.sum2_time_rd += (dt*dt);
		break;
	    default:
	        /* purposefully empty */
		break;
	}
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

static FILE *merge_trace_files(LOCKANALYZER *pla)
{
    /* merge into a single trace file */

    char         scanstring[1024];
    int          i, j, nsched, nrecs;
    int64_t      i_pick;
    uint64_t     t, nrec;
    int          curfile[MAX_SCHED];
    RECORD       cur_rec[MAX_SCHED];
    FILE        *fmerge;
    STRING       tfilename[MAX_SCHED];
    FILE        *tfiles[MAX_SCHED*MAX_FILES_PER_SCHED];
    char        *mergefile;
    FILEDATA    *pfd;
    BOOLEAN      sched99_flag;

    mergefile = pla->mergefile;
    pfd       = &(pla->filedata);

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

    sched99_flag = FALSE;
    for (i=0; i<pla->nfiles; i++) {
        if (sscanf(pla->eps[i]->d_name, "lt_%d_%d", &nsched, &nrecs) != 2) {
	    continue;
	}
	if (nsched != 99) {
	    if (pfd->nsched <= nsched) {
	        pfd->nsched = nsched + 1;
	    }
	} else {
	  sched99_flag = TRUE;
	}
    }

    for (i=0; i<pla->nfiles; i++) {
        if (sscanf(pla->eps[i]->d_name, "lt_%d_%d", &nsched, &nrecs) != 2) {
	    continue;
	}
	if (nsched == 99) {
	    /* store the "init" trace stuff at the end of the array */
	    nsched = pfd->nsched;
	}

	pfd->file_counts[nsched][pfd->files_per_sched[nsched]] = nrecs;
	pfd->files_per_sched[nsched]++;
	if (pfd->files_per_sched[nsched] >= MAX_FILES_PER_SCHED) {
	    Error("Too many files per scheduler (%d max)", MAX_FILES_PER_SCHED);
	}
    }

    /* sort file_counts array */

    if (sched99_flag) {
        (pfd->nsched)++;
    }

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
	cur_rec[i].type_sched= LOCK_END_REC;
	pfd->schedmap[i] = i;
    }
    if (sched99_flag) {
	pfd->schedmap[pfd->nsched-1] = 99;
    }

    for (i=0; i<pfd->nsched; i++) {
        (void) sprintf(tfilename[i], "%s/lt_%d_%d", pla->trace_dir, pfd->schedmap[i], pfd->file_counts[i][0]);
        tfiles[i] = fopen(tfilename[i], "r");
	if (tfiles[i] == NULL) {
	    cur_rec[i].type_sched = LOCK_END_REC;
	    // Error("Could not open trace file '%s'", tfilename[i]);
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
	    if (cur_rec[i].type_sched != LOCK_END_REC) {
		i_pick = i;
		break;
	    }
	}
	if (i_pick == -1) {
	    break;
	}

	/* dump it to the common trace file */

        /*   Insert sched number into type_sched field 
	 *   Note that the original traces do NOT have the
	 *   scheduler number in the type_sched field.
	 */

        SET_NSCHED(cur_rec[i_pick], i_pick);

        nrec++;
	if (nrec > pla->nrecs_skip) {
	    if (!put_record(fmerge, &(cur_rec[i_pick]))) {
		Error("Write to merged trace file '%s' failed", mergefile);
	    }
	}

	/* get the next record for this scheduler */
	{
	    if ((!get_record(tfiles[i_pick], &(cur_rec[i_pick]))) ||
	        (cur_rec[i_pick].type_sched == LOCK_END_REC))
	    {
	        /* go to the next file for this scheduler */
		if (fclose(tfiles[i_pick]) != 0) {
		    Warning("Could not close trace file '%s'", tfilename[i_pick]);
		}
		curfile[i_pick]++;
		if (curfile[i_pick] >= pfd->files_per_sched[i_pick]) {
		    /* I am out of files for this scheduler */
		    cur_rec[i_pick].type_sched = LOCK_END_REC;
		} else {
		    (void) sprintf(tfilename[i_pick], "%s/lt_%d_%d", pla->trace_dir, pfd->schedmap[i_pick], pfd->file_counts[i_pick][curfile[i_pick]]);
		    tfiles[i_pick] = fopen(tfilename[i_pick], "r");
		    if (tfiles[i_pick] == NULL) {
			Error("Could not open trace file '%s'", tfilename[i_pick]);
		    }
		    if (!get_record(tfiles[i_pick], &(cur_rec[i_pick]))) {
			Warning("No trace records found in trace file '%s'", tfilename[i_pick]);
			cur_rec[i_pick].type_sched = LOCK_END_REC;
		    }
		}
	        
	    }
	}
    }
    pla->nsched = pfd->nsched;
    return(fmerge);
}

static void process_backtrace_info(LOCKANALYZER *pla)
{
    /* process backtrace info */

    int          i, rectype, nsched;
    char         gdbfile[1000];
    char         gdbfile2[1000];
    char         gdboutfile[1000];
    FILE        *fgdb, *fgdbout;
    RECORD       rec;
    char         scmd[10000];
    FILE        *fmerge;
    SRCDATA     *psd;
    char        *srcdir;
    SDFxTLMap_t *psrcmap;
    SDFxTLMap_t *pgdbmap;

    fmerge  = pla->fmerge;
    psd     = &pla->srcdata;
    srcdir  = pla->srcdir;
    psrcmap = &pla->srcmap;
    pgdbmap = &pla->gdbmap;

    SDFxTLMapEntry_t  *pxme, *pxgdb;

    init_srcdata(psd, pla->srcpaths_file);

    SDFxTLMapInit(pgdbmap, SOURCE_MAP_SIZE, NULL);

    /* see if an existing gdboutfile already exists */

    (void) sprintf(gdboutfile, "%s/gdb_bt.do.out", pla->trace_dir);
    fgdbout = fopen(gdboutfile, "r");

    if (fgdbout == NULL) {
        /* no gdboutfile exists */
	rewind(fmerge);

	(void) sprintf(gdbfile, "gdb_bt.do");
	(void) sprintf(gdbfile2, "gdb_bt2.do");
	(void) sprintf(gdboutfile, "gdb_bt.do.out");
	fgdb = fopen(gdbfile, "w");
	if (fgdb == NULL) {
	    Error("Could not open gdb script file '%s'", gdbfile);
	}
	(void) fprintf(fgdb, "set height 0\n");

	while (get_record(fmerge, &rec)) {

	    rectype = GET_RECTYPE(rec);
	    nsched  = GET_NSCHED(rec);

	    if (rectype == LOCK_END_REC) {
		break;
	    }
	    if ((rectype != LOCK_RD_REC) &&
		(rectype != LOCK_WR_REC))
	    {
		continue;
	    }
	    for (i=0; i<BT_SIZE; i++) {
		if (rec.bt[i] > 0) {
		    pxgdb = SDFxTLMapGet(pgdbmap, rec.bt[i]);
		    if (pxgdb == NULL) {
			(void) fprintf(fgdb, "list * 0x%"PRIx64"\n", rec.bt[i]);
			pxgdb = SDFxTLMapGetCreate(pgdbmap, rec.bt[i]);
			if (pxgdb->contents != NULL) {
			    Error("Inconsistency in process_backtrace_info.");
			} else {
			    pxgdb->contents = (void *) rec.bt[i];
			}
		    }
		}
	    }
	}

	if (fflush(fgdb) != 0) {
	    Error("Could not flush gdb script file '%s'", gdbfile);
	}
	if (fclose(fgdb) != 0) {
	    Error("Could not close gdb script file '%s'", gdbfile);
	}

	(void) sprintf(scmd, "cat  %s | sort -r | uniq > %s", gdbfile, gdbfile2);
	Message("%s", scmd);
	if (system(scmd) == -1) {
	    Error("Could not execute '%s'", scmd);
	}
	(void) sprintf(scmd, "gdb %s -batch -x %s > %s", pla->executable, gdbfile2, gdboutfile);
	Message("%s", scmd);
	if (system(scmd) == -1) {
	    Error("Could not execute '%s'", scmd);
	}

	fgdbout = fopen(gdboutfile, "r");
	if (fgdbout == NULL) {
	    Error("Could not open gdb result file '%s'", gdboutfile);
	}
    }

    load_line_map(fgdbout, pla);

    if (fclose(fgdbout) != 0) {
        Warning("Could not close gdb result file '%s'", gdboutfile);
    }
}

static void get_file_line(char *sin, char *linefile, int *pline)
{
    char   *s;

    for (s = sin; (*s != '\0') && (*s != ':'); s++);
    if (*s != ':') {
        (void) strcpy(linefile, "--UNKNOWN--");
	*pline = -1;
    } else {
        *s = '\0';
        /* skip leading '(' */
        (void) strcpy(linefile, sin+1);
        *s = ':';
	if (sscanf(s+1, "%d", pline) != 1) {
	    *pline = -1;
	}
    }
}

static void load_line_map(FILE *fgdbout, LOCKANALYZER *pla)
{
    SDFxTLMapEntry_t  *pxme;
    int               line, listline, len;
    char              linefile[LINE_LEN];
    char              funcname[LINE_LEN];
    SRCENTRY         *pse;
    char              sline[10000];
    char              stmp[10000];
    char              sline_single[10000];
    long long         addr, dummy;
    SDFxTLMap_t *psrcmap;
    SRCDATA *psd;

    psrcmap = &pla->srcmap;
    psd     = &pla->srcdata;

    SDFxTLMapInit(psrcmap, SOURCE_MAP_SIZE, NULL);

    LineNum = 0;
    if (GetLine(fgdbout)) {
	while (TRUE) {
	    if (strstr(Line, "0x") != Line) {
		if (!GetLine(fgdbout)) {
		    break;
		}
		continue;
	    } else {

		#ifdef notdef
		/**********************************************************/

		(gdb) info line * 0x4309cc
		Line 2338 of "../mcd_fth.c" starts at address 0x4309cc <mcd_fth_base_loop+668> and ends at 0x4309d8 <mcd_fth_base_loop+680>.

		(gdb) list * 0x4309cc
		0x4309cc is in mcd_fth_base_loop (../mcd_fth.c:2338).
		2333        for ( i = 0; i < num_sched; i++ ) {
		2334            rc = sem_wait( &Mcd_fsched_sem );
		2335            plat_assert( 0 == rc );
		2336        }
		2337
		2338        for ( i = 0; i < num_sched; i++ ) {
		2339            pthread_join( fth_pthreads[i], NULL );
		2340        }
		2341
		2342        mcd_log_msg(20165, PLAT_LOG_LEVEL_DEBUG, "schedulers terminated" );

		/**********************************************************/
		#endif

		if (sscanf(Line, "0x%llx is in %s %s", &addr, funcname, stmp) != 3) {
		    if (!GetLine(fgdbout)) {
		        break;
		    }
		} else {
		    get_file_line(stmp, linefile, &line);

		    /* add an entry to the source code map */

		    pse        = (SRCENTRY *) malloc(sizeof(SRCENTRY));
		    pse->addr  = addr;
		    pse->line  = line;
		    pse->fname = (char *) malloc(strlen(linefile)+1);
		    (void) strcpy(pse->fname, linefile);
		    pse->func  = (char *) malloc(strlen(funcname)+1);
		    (void) strcpy(pse->func, funcname);

		    /* get the lines of code */

		    sline[0] = '\0';
		    sline_single[0] = '\0';
		    while (TRUE) {
			if (!GetLine(fgdbout)) {
			    break;
			}
			if (strstr(Line, "0x") == Line) {
			    break;
			}
			if (sscanf(Line, "%d", &listline) != 1) {
			    LineWarning("Expecting line number followed by line of code");
			}
			if (listline == line) {
			    (void) strcpy(sline_single, Line);
			}
			(void) strcat(sline, Line);
		    }

		    if (sline[0] != '\0') {
			pse->sline = (char *) malloc(strlen(sline)+1);
			(void) strcpy(pse->sline, sline);
		    } else {
			pse->sline = "--UNKNOWN--";
		    }

		    if (sline_single[0] != '\0') {
		        len = strlen(sline_single);
			sline_single[len-1] = '\0'; /* remove '\n' */
			pse->sline_single = (char *) malloc(len);
			(void) strcpy(pse->sline_single, sline_single);
		    } else {
			pse->sline_single = "--UNKNOWN--";
		    }

		    pxme = SDFxTLMapGetCreate(psrcmap, (uint64_t) addr);
		    if (pxme->contents != NULL) {
			LineWarning("Duplicate entry in source map '%s'", addr);
			continue;
		    } else {
			pxme->contents = (void *) pse;
		    }
		}
	    }
	}
    }

}

static void load_fth_table(LOCKANALYZER *pla)
{
    int               i;
    long long         nrec;
    RECORD            rec;
    SDFxTLMapEntry_t  *pxme;
    FTHDATA          *pfd;
    int               rectype, nsched, fthcnt;
    int               nlocks;
    char              func_name[LINE_LEN];
    FILE             *fmerge;
    SDFxTLMap_t      *pfthmap;
    SDFTLMap_t       *plockmap;
    SRCENTRY         *pse;
    SDFxTLMap_t      *psrcmap;

    fmerge   = pla->fmerge;
    pfthmap  = &(pla->fthmap);
    psrcmap  = &(pla->srcmap);
    plockmap = &(pla->lockmap);

    /* load the lock table */

    rewind(fmerge);

    SDFTLMapInit(plockmap, SOURCE_MAP_SIZE, NULL);
    SDFxTLMapInit(pfthmap,  FTH_MAP_SIZE,    NULL);

    fthcnt = 0;
    nrec   = 0;
    nlocks = 0;
    while (get_record(fmerge, &rec)) {
        nrec++;

        rectype = GET_RECTYPE(rec);
        nsched  = GET_NSCHED(rec);

        if (rectype == LOCK_END_REC) {
	    break;
	}

	pxme = SDFxTLMapGetCreate(pfthmap, rec.fth);
	pfd = (FTHDATA *) pxme->contents;
	if (pfd == NULL) {
	    pfd = (FTHDATA *) malloc(sizeof(FTHDATA));
	    pfd->i = fthcnt;
	    pfd->rec = rec;
	    pfd->fth = rec.fth;
	    pfd->func_name = NULL;
	    pxme->contents = (void *) pfd;
	    pla->inv_fthmap[fthcnt] = rec.fth;
	    fthcnt++;
	    if (fthcnt >= MAX_FTH) {
	        Error("Too many fth threads (%d max)", MAX_FTH);
	    }
	}

	if (pfd->func_name == NULL) {
	    /*  Find the topmost function for this fthread 
	     *  (for easy identification!)
	     */
            for (i=0; i<BT_SIZE; i++) {
	        if (rec.bt[i] == 0) {
		    break;
		}
	    }
	    if (i > 0) {
		pxme = SDFxTLMapGet(psrcmap, rec.bt[i-1]);
		if (pxme != NULL) {
		    pse = (SRCENTRY *) pxme->contents;
		    if (pse != NULL) {
			// (void) sprintf(func_name, "%s (%s:%d:'%s')", pse->func, pse->fname, pse->line, pse->sline_single);
			(void) sprintf(func_name, "%s (%s:%d)", pse->func, pse->fname, pse->line);
			pfd->func_name = malloc(strlen(func_name)+1);
			(void) strcpy(pfd->func_name, func_name);
		    } else {
		        pfd->func_name = "--UNKNOWN--";
		    }
		} else {
		    pfd->func_name = "--UNKNOWN--";
		}
	    }
	}
    }
    pla->nfth   = fthcnt;
    pla->nrecs  = nrec;
}

static LOCKDATA *load_lock_table(LOCKANALYZER *pla, RECORD *pr, long long nrec)
{
    SDFTLMapEntry_t   *pxme;
    SDFTLMap_t        *plockmap;
    LOCKDATA          *pld;
    char               skey[100];

    plockmap = &(pla->lockmap);

    pld        = (LOCKDATA *) malloc(sizeof(LOCKDATA));
    pld->nrec  = nrec;
    pld->rec   = *pr;
    pld->next  = NULL;
    init_lock_stats(&(pld->stats));

    pla->sorted_locks[pla->nlocks] = pld;
    pla->nlocks++;
    if (pla->nlocks >= MAX_LOCKS) {
	Error ("Too many locks (%d max)", MAX_LOCKS);
    }

    MAKE_LOCKMAP_KEY(skey, (*pr));
    #ifdef DEBUG
        (void) fprintf(stderr, "New lockmap entry: plock=%p, bt[1]=%p, skey='%s'\n", pr->plock, pr->bt[1], skey);
    #endif
    pxme = SDFTLMapGetCreate(plockmap, skey);
    if (pxme->contents != NULL) {
        Error("Attempt to overwrite entry in lock table");
    }
    pxme->contents = (void *) pld;
    return(pld);
}

static void init_srcdata(SRCDATA *psd, char *srcpathfile)
{
    FILE   *f;
    char    stmp[1024];

    return; // I don't use this stuff anymore

    f = fopen(srcpathfile, "w");
    if (f == NULL) {
        Error("Could not open source directory file '%s'", srcpathfile);
    }

    SDFTLMapInit(&psd->map, MAX_SRC_DIRS, NULL);

    psd->ndirs = 0;
    LineNum = 0;
    while (GetLine(f)) {
        if (sscanf(Line, "%s", stmp) != 1) {
	    continue;
	}
	psd->dirs[psd->ndirs] = (char *) malloc(strlen(stmp)+1);
	(void) strcpy(psd->dirs[psd->ndirs], stmp);
	psd->ndirs++;
	if (psd->ndirs >= MAX_SRC_DIRS) {
	    Error("Maximum number of source directories exceeded (%d)", MAX_SRC_DIRS);
	}
    }

    if (fclose(f) != 0) {
        Warning("Could not close source directory file '%s'", srcpathfile);
    }
}

static char *find_srcline(SRCDATA *psd, int line, char *basedir, char *linefile)
{
    int               i;
    FILE             *f;
    SDFTLMapEntry_t  *pme;
    struct stat       statbuf;
    char             *srcline;
    char              stmp[LINE_LEN];

    /* check hashmap first */

    pme = SDFTLMapGetCreate(&psd->map, linefile);
    if (pme->contents != NULL) {
	srcline = get_srcline((char *) pme->contents, line);
        return(srcline);
    }

    for (i=0; i<psd->ndirs; i++) {
	(void) sprintf(stmp, "%s/%s/%s", basedir, psd->dirs[i], linefile);
	if (stat(stmp, &statbuf) != -1) {
	    srcline = get_srcline(stmp, line);
	    pme->contents = (void *) malloc(strlen(stmp)+1);
	    (void) strcpy((char *) pme->contents, stmp);
	    break;
	}
    }
    return(srcline);
}

static char *get_srcline(char *fname, int line)
{
    int       i, n;
    FILE     *f;
    BOOLEAN   failed;
    static char   srcline[LINE_LEN];

    f = fopen(fname, "r");
    if (f == NULL) {
	Error("Could not open source directory file '%s'", fname);
    }

    n      = LINE_LEN;
    failed = FALSE;
    for (i=1; i<=line; i++) {
	if (getline(&srcline, &n, f) == -1) {
	    failed = TRUE;
	    break;
	}
    }

    if (fclose(f) != 0) {
	Warning("Could not close source directory file '%s'", fname);
    }

    if (failed) {
	return(NULL);
    } else {
	return(srcline);
    }
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



/************************************************************************/

static BOOLEAN get_record(FILE *f, RECORD *pr)
{
    BOOLEAN  ret;
    int      rectype;

    ret = TRUE;

    if (fread((void *) pr, sizeof(uint64_t), LONG_REC_SIZE, f) != LONG_REC_SIZE) {
	return(FALSE);
    }

    rectype = GET_RECTYPE(*pr);

    switch (rectype) {
        case LOCK_END_REC:
	    /* end of this trace file */
	    ret = FALSE;
	    break;
        case LOCK_RD_REC:
        case LOCK_WR_REC:
	    /* purposefully empty */
	    break;
	default:
	    Error("Bad record in get_record! (type %d)", rectype);
	    break;
    }

    return(ret);
}

static BOOLEAN put_record(FILE *f, RECORD *pr)
{
    BOOLEAN  ret;
    int      rectype;

    ret = TRUE;

    rectype = GET_RECTYPE(*pr);
    switch (rectype) {
        case LOCK_END_REC:
        case LOCK_RD_REC:
        case LOCK_WR_REC:
	    if (fwrite((void *) pr, sizeof(uint64_t), LONG_REC_SIZE, f) != LONG_REC_SIZE) {
		ret = FALSE;
	    }
	    break;
	default:
	    Error("Bad record in put_record! (type %d)", rectype);
	    break;
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

   (void) fprintf(stderr, "Message: ");
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

static void check_rectype_info()
{
    int i;

    for (i=0; i<N_RECTYPES; i++) {
        if (RecType[i].i != i) {
	    Error("RecType[%d].i is out of order (it should be %d)!", RecType[i].i, i);
	}
    }
}


/******************************************************************
 *
 * stopleak -- A program to find SDF memory leaks.
 *
 * Brian O'Krafka   10/22/08
 *
 *  NOTES:
 *
 ******************************************************************/

#define _MAIN_C

#include <stdio.h>
#include <stdarg.h>
#include <stdint.h>
#include <string.h>
#include <stdlib.h>
#include <dirent.h>
#include <inttypes.h>
#include <math.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include "tlmap.h"
#include "xtlmap.h"
#include "stopleak.h"

// #define DEBUG

#define DUMP_INTERVAL 10000000

    /* for sorting results */
static NAME_DATA *NameDataSortArray[NAME_MAP_SIZE];

static char *Line;
static int  LineNum;
static int  LineLength = 1024;
static int  Debug = 0;

static void usage(char *sprog)
{
    fprintf(stderr, "usage: %s <malloc_trace_file> > <output_file>\n", sprog);
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

LEAKDATA LeakData;

static void dump_name_data(FILE *fout, SDFTLMap_t *pm);
static void init_leak_data(LEAKDATA *pld);
static void init_attr_arrays();
static char *skip_white(char *sin);
static char *skip_not_white(char *sin);

main(int argc, char **argv)
{
    int                i;
    FILE              *fout;
    FILE              *fin;
    LEAKDATA          *pld;
    SDFxTLMapEntry_t  *pxe;
    SDFTLMapEntry_t   *pe, *pe_alloc;
    char              *line;
    char               saccess[128];
    long long          ptr;
    uint64_t           size, size_alloc;
    PTR_DATA          *ppd, *ppd_alloc;
    NAME_DATA         *pnd, *pnd_alloc, *pnd_debug;
    char              *s, *name;
    ACCESS_TYPE        atype;
    int                dump_interval;
    BOOLEAN            found_it, first_flag;

    first_flag = TRUE;

    pld = &LeakData;

    if (argc != 2) {
	usage(argv[0]);
    }

    /* initialization */

    InitGetLine();
    pld->fin_name    = argv[1];
    pld->fout        = stdout;
    init_leak_data(pld);
    dump_interval    = DUMP_INTERVAL;

    (void) fprintf(pld->fout, "---------------------------------------------------\n");
    LineNum = 0;
    while (line = GetLine(pld->fin)) {
        if ((LineNum % dump_interval) == 0) {
	    dump_name_data(pld->fout, &(pld->name_map));
	}
        if (sscanf(line, "%s %llx %lld", saccess, &ptr, &size) != 3) {
	    continue;
	}
	if (strcmp(saccess, "Lalloc") == 0) {
	    atype = LALLOC;
	} else if (strcmp(saccess, "Salloc") == 0) {
	    atype = SALLOC;
	} else if (strcmp(saccess, "Lfree") == 0) {
	    atype = LFREE;
	} else if (strcmp(saccess, "Sfree") == 0) {
	    atype = SFREE;
	} else {
	    Error("Unknown access type '%s'", saccess);
	}
	s = line;
	s = skip_not_white(s);
	s = skip_white(s);
	s = skip_not_white(s);
	s = skip_white(s);
	s = skip_not_white(s);
	s = skip_white(s);
	name = s;
	name[strlen(name)-1] = '\0';

	#ifdef DEBUG
	    found_it = FALSE;
	    if (strstr(name, "pbuf_in")) {
	        found_it = TRUE;
	        LineWarning("found it!");
	    }
	#endif // DEBUG

        pe = SDFTLMapGetCreate(&(pld->name_map), name);
	pnd = (NAME_DATA *) pe->contents;
	if (pnd == NULL) {
	    pnd = (NAME_DATA *) malloc(sizeof(NAME_DATA));
	    // if (!AllocInc[atype]) {
	        // LineWarning("First reference to '%s' is not an alloc", name);
	    // }
	    pe->contents = (void *) pnd;
	    pnd->used_bytes       = 0;
	    pnd->sum_alloc_bytes  = 0;
	    pnd->n_alloc          = 0;
	    pnd->n_free           = 0;
	    pnd->alloc_flag       = AllocInc[atype];
	    pnd_alloc             = pnd;
	}
	if (FreeInc[atype]) {
	    /* get the size from the matching alloc */
	    pxe = SDFxTLMapGetCreate(&(pld->ptr_map), ptr);
	    ppd_alloc = (PTR_DATA *) pxe->contents;
	    if (ppd_alloc == NULL) {
		LineWarning("No matching alloc for free(%p)", ptr);
	    } else {
		size_alloc = ppd_alloc->size;
	    }
	    if (ppd_alloc->alloc_name == NULL) {
		LineWarning("No alloc name (%p)", ptr);
	    }
	    pe_alloc = SDFTLMapGetCreate(&(pld->name_map), ppd_alloc->alloc_name);
	    pnd_alloc = (NAME_DATA *) pe_alloc->contents;
	    if (pnd_alloc == NULL) {
		LineWarning("No name table entry for alloc name '%s' (%p)", ppd->alloc_name, ptr);
	    }
	} else {
	    pnd_alloc  = pnd;
	    size_alloc = size;
	}
	#ifdef DEBUG
	    if (found_it) {
		(void) fprintf(stderr, "INFO BEFORE: pnd_alloc: %p, pnd_alloc->used_bytes: %lld, size_alloc: %"PRIu64"\n", pnd_alloc, pnd_alloc->used_bytes, size_alloc);
		if (FreeInc[atype] && (pnd_alloc->used_bytes != size_alloc)) {
		    LineWarning("pnd_alloc->used_bytes != size_alloc!");
		}
		if (AllocInc[atype] && (pnd_alloc->used_bytes != 0)) {
		    LineWarning("pnd_alloc->used_bytes != 0!");
		}
	    }
	#endif // DEBUG

	pnd_alloc->used_bytes      += AllocInc[atype]*size_alloc;
	pnd_alloc->used_bytes      -= FreeInc[atype]*size_alloc;
	pnd_alloc->sum_alloc_bytes += AllocInc[atype]*size_alloc;
	pnd_alloc->n_alloc         += AllocInc[atype];
	pnd_alloc->n_free          += FreeInc[atype];

	#ifdef DEBUG
	    if (first_flag) {
	        first_flag = FALSE;
		pnd_debug = pnd_alloc;
	    } else {
	        if ((pnd_alloc == pnd_debug) &&
		    (!strstr(line, "pbuf_in")))
		{
		    LineWarning("Oh-oh!");
		}
	    }

	    if (found_it) {
		(void) fprintf(stderr, "INFO AFTER: pnd_alloc: %p, pnd_alloc->used_bytes: %lld, size_alloc: %d\n", pnd_alloc, pnd_alloc->used_bytes, size_alloc);
		if ((pnd_alloc->used_bytes < 0)) {
		    LineWarning("pnd_alloc->used_bytes < 0!");
		}
	    }
	#endif // DEBUG

        pxe = SDFxTLMapGetCreate(&(pld->ptr_map), ptr);
	ppd = (PTR_DATA *) pxe->contents;
	if (ppd == NULL) {
	    ppd = (PTR_DATA *) malloc(sizeof(PTR_DATA));
	    pxe->contents   = (void *) ppd;
	    ppd->size       = size;
	    ppd->n_alloc    = AllocInc[atype];
	    ppd->n_free     = FreeInc[atype];
	    ppd->in_use     = InUse[atype];
	    ppd->alloc_name = NULL;
	    if (!ppd->in_use) {
	        LineWarning("First ptr reference is not an alloc! (0x%llx)", ptr);
	    }
	} else {
	    if (ppd->in_use == InUse[atype]) {
	        if (AllocInc[atype]) {
		    LineWarning("alloc is not preceeded by a free! (0x%llx)", ptr);
		} else {
		    LineWarning("free is not preceeded by an alloc! (0x%llx)", ptr);
		}
	    }
	    ppd->size     = size;
	    ppd->n_alloc += AllocInc[atype];
	    ppd->n_free  += FreeInc[atype];
	    ppd->in_use   = InUse[atype];
	}
	if (InUse[atype]) {
	    ppd->alloc_name = pe->key;
	}

    }
    dump_name_data(pld->fout, &(pld->name_map));

    return(0);
}

static void init_attr_arrays()
{
    InUse[LALLOC] = TRUE;
    InUse[SALLOC] = TRUE;
    InUse[LFREE]  = FALSE;
    InUse[SFREE]  = FALSE;

    AllocInc[LALLOC] = 1;
    AllocInc[SALLOC] = 1;
    AllocInc[LFREE]  = 0;
    AllocInc[SFREE]  = 0;

    FreeInc[LALLOC] = 0;
    FreeInc[SALLOC] = 0;
    FreeInc[LFREE]  = 1;
    FreeInc[SFREE]  = 1;
}

static void init_leak_data(LEAKDATA *pld)
{
    pld->fin = fopen(pld->fin_name, "r");
    if (pld->fin == NULL) {
        Error("Could not open input file '%s'", pld->fin_name);
    }
    SDFxTLMapInit(&(pld->ptr_map), PTR_MAP_SIZE, NULL);
    SDFTLMapInit(&(pld->name_map), NAME_MAP_SIZE, NULL);
    init_attr_arrays();
}

static int name_data_sortfn(const void *e1_in, const void *e2_in)
{
    NAME_DATA *pnd1, *pnd2;
    long long   t1, t2;

    pnd1 = *((NAME_DATA **) e1_in);
    pnd2 = *((NAME_DATA **) e2_in);

    t1 = pnd1->used_bytes;
    t2 = pnd2->used_bytes;

    if (t1 < t2) {
        return(1);
    } else if (t1 > t2) {
        return(-1);
    } else {
        return(0);
    }
}

static void dump_name_data(FILE *fout, SDFTLMap_t *pm)
{
    int               i, nnames;
    NAME_DATA        *pnd;
    SDFTLMapEntry_t  *pe;

    nnames = 0;
    SDFTLMapEnum(pm);
    while (pe = SDFTLMapNextEnum(pm)) {
	pnd = (NAME_DATA *) pe->contents;
	pnd->key = pe->key;
	if (pnd->alloc_flag) {
	    NameDataSortArray[nnames] = pnd;
	    nnames++;
	    if (nnames >= NAME_MAP_SIZE) {
	        Error("Too many NAME_DATA entries (%d max)", NAME_MAP_SIZE);
	    }
	} else {
	    /* make sure that no stats were collected for the free stuff */

	    if ((pnd->used_bytes != 0) ||
	        (pnd->n_alloc != 0)    ||
	        (pnd->n_free != 0)     ||
	        (pnd->sum_alloc_bytes != 0))
	    {
	        Warning("Name data for '%s' should all be zero!", pe->key);
	    }
	}
    }

    qsort((void *) NameDataSortArray, (size_t) nnames, 
	  (size_t) sizeof(NAME_DATA *), name_data_sortfn);

    for (i=0; i<nnames; i++) {
        pnd = NameDataSortArray[i];
	(void) fprintf(fout, "%37s: %10lldB, %7lld alloc, %7lld free, %8lld B/alloc\n", pnd->key, pnd->used_bytes, pnd->n_alloc, pnd->n_free, (long long) ((double) pnd->sum_alloc_bytes)/pnd->n_alloc);
    }

    (void) fprintf(fout, "---------------------------------------------------\n");
}

/************************************************************************/

static char *skip_white(char *sin)
{
    char *s;

    for (s=sin; (*s == ' ') || (*s == '\t'); s++);

    return(s);
}

static char *skip_not_white(char *sin)
{
    char *s;

    for (s=sin; (*s != ' ') && (*s != '\t') && (*s != '\0'); s++);

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
   (void) fprintf(stderr, "\n%s\n", Line);

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
	Line = NULL;
    }
    return(Line);
}

static void GetLineString(char *s, FILE *f)
{
    size_t    n;

    n = 1024;
    Line = NULL;
    while (getline(&Line, &n, f) != -1) {
        LineNum++;
	if (strstr(Line, s)) {
	    return;
	}
	Line = NULL;
    }
    LineWarning("Reached end of file!");
}


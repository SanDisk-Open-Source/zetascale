//----------------------------------------------------------------------------
// ZetaScale
// Copyright (c) 2016, SanDisk Corp. and/or all its affiliates.
//
// This program is free software; you can redistribute it and/or modify it under
// the terms of the GNU Lesser General Public License version 2.1 as published by the Free
// Software Foundation;
//
// This program is distributed in the hope that it will be useful, but WITHOUT
// ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
// FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License v2.1 for more details.
//
// A copy of the GNU Lesser General Public License v2.1 is provided with this package and
// can also be found at: http://opensource.org/licenses/LGPL-2.1
// You should have received a copy of the GNU Lesser General Public License along with
// this program; if not, write to the Free Software Foundation, Inc., 59 Temple
// Place, Suite 330, Boston, MA 02111-1307 USA.
//----------------------------------------------------------------------------

/******************************************************************
 *
 * stopleak -- A program to find SDF memory leaks.
 *
 * Brian O'Krafka   10/22/08
 *
 ******************************************************************/

#ifndef _STOPLEAK_H
#define _STOPLEAK_H

typedef int BOOLEAN;

#define FALSE  0
#define TRUE   1

#define LINE_LEN   10000

#define STRINGLEN  100
typedef char  STRING[STRINGLEN];

#define PTR_MAP_SIZE     1000000
#define NAME_MAP_SIZE    1000

typedef struct {
    BOOLEAN     in_use;
    BOOLEAN     is_shmem;
    long long   size;
    long long   n_alloc;     
    long long   n_free;     
    char       *alloc_name;
} PTR_DATA;

typedef struct {
    BOOLEAN     alloc_flag;
    char       *key;
    long long   used_bytes;
    long long   sum_alloc_bytes;     
    long long   n_alloc;     
    long long   n_free;     
} NAME_DATA;

typedef enum {
    LALLOC = 0,
    SALLOC,
    LFREE,
    SFREE,
    N_ACCESS_TYPES,
} ACCESS_TYPE;

/* these must be kept in sync with above #define's */
#ifndef _MAIN_C
    extern char *AccessTypeStrings[] = {
#else
    char *AccessTypeStrings[] = {
	"Lalloc",
	"Salloc",
	"Lfree",
	"Sfree",
    };
#endif // _MAIN_C

#ifndef _MAIN_C
    extern int InUse[N_ACCESS_TYPES];
    extern int AllocInc[N_ACCESS_TYPES];
    extern int FreeInc[N_ACCESS_TYPES];
#else
    int InUse[N_ACCESS_TYPES];
    int AllocInc[N_ACCESS_TYPES];
    int FreeInc[N_ACCESS_TYPES];
#endif // _MAIN_C

typedef struct {
   char          *fin_name;
   FILE          *fin;
   FILE          *fout;
   SDFxTLMap_t    ptr_map; 
   SDFTLMap_t     name_map; 
} LEAKDATA;


#endif   // _STOPLEAK_H

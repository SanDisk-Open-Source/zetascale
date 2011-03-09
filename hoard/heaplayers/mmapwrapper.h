// -*- C++ -*-

/*

  Heap Layers: An Extensible Memory Allocation Infrastructure
  
  Copyright (C) 2000-2005 by Emery Berger
  http://www.cs.umass.edu/~emery
  emery@cs.umass.edu
  
  This program is free software; you can redistribute it and/or modify
  it under the terms of the GNU General Public License as published by
  the Free Software Foundation; either version 2 of the License, or
  (at your option) any later version.
  
  This program is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU General Public License for more details.
  
  You should have received a copy of the GNU General Public License
  along with this program; if not, write to the Free Software
  Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA

*/

#ifndef _MMAPWRAPPER_H_
#define _MMAPWRAPPER_H_

#if defined(_WIN32)
#include <windows.h>
#else
// UNIX
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>
#include <map>
#endif

#if HL_EXECUTABLE_HEAP
#define HL_MMAP_PROTECTION_MASK (PROT_READ | PROT_WRITE | PROT_EXEC)
#else
#define HL_MMAP_PROTECTION_MASK (PROT_READ | PROT_WRITE)
#endif

#if !defined(MAP_ANONYMOUS) && defined(MAP_ANON)
#define MAP_ANONYMOUS MAP_ANON
#endif

namespace HL {

class MmapWrapper {
public:

#if defined(_WIN32) 
  
  // Microsoft Windows has 4K pages aligned to a 64K boundary.
  enum { Size = 4 * 1024 };
  enum { Alignment = 64 * 1024 };

  static void * map (size_t sz) {
    void * ptr;
#if HL_EXECUTABLE_HEAP
    const int permflags = PAGE_EXECUTE_READWRITE;
#else
    const int permflags = PAGE_READWRITE;
#endif
    ptr = VirtualAlloc (NULL, sz, MEM_RESERVE | MEM_COMMIT | MEM_TOP_DOWN, permflags);
    return  ptr;
  }
  
  static void unmap (void * ptr, size_t) {
    VirtualFree (ptr, 0, MEM_RELEASE);
  }

#else

#if defined(__SVR4)
  // Solaris aligns 8K pages to a 64K boundary.
  enum { Size = 8 * 1024 };
  enum { Alignment = 64 * 1024 };
#else
  // Linux and most other operating systems align memory to a 4K boundary.
  enum { Size = 4 * 1024 };
  enum { Alignment = 4 * 1024 };
#endif

  static void * map (size_t sz) {

    if (sz == 0) {
      return NULL;
    }

    void * ptr;

//#define PHYSMEM_MAP

#ifdef PHYSMEM_MAP
    

#else
#if defined(MAP_ALIGN) && defined(MAP_ANON)
    // Request memory aligned to the Alignment value above.
    ptr = mmap ((char *) Alignment, sz, HL_MMAP_PROTECTION_MASK, MAP_PRIVATE | MAP_ALIGN | MAP_ANON, -1, 0);
#elif !defined(MAP_ANONYMOUS)
    static int fd = ::open ("/dev/zero", O_RDWR);
    ptr = mmap (NULL, sz, HL_MMAP_PROTECTION_MASK, MAP_PRIVATE, fd, 0);
#else
    ptr = mmap (0, sz, HL_MMAP_PROTECTION_MASK, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
#endif

    if (ptr == MAP_FAILED) {
      return NULL;
    } else {
      return ptr;
    }
  }

  static void unmap (void * ptr, size_t sz) {
    munmap (reinterpret_cast<char *>(ptr), sz);
  }
   
#endif
#endif

#ifdef PHYSMEM_MAP
private:
    void *physmem_base = NULL;
    void *physmem_list = NULL;
#endif

};

}

#endif

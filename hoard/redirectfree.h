// -*- C++ -*-

/*

  Heap Layers: An Extensible Memory Allocation Infrastructure
  
  Copyright (C) 2000-2003 by Emery Berger
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

#ifndef _REDIRECTFREE_H_
#define _REDIRECTFREE_H_

#include "fred.h"
#include "guard.h"

namespace Hoard {

/**
 * @class RedirectFree
 * @brief Routes free calls to the Superblock's owner heap.
 * @note  We also lock the heap on calls to malloc.
 */

template <class Heap,
	  typename SuperblockType_>
class RedirectFree {
public:

  enum { Alignment = (int) Heap::Alignment };

  typedef SuperblockType_ SuperblockType;

  inline void * malloc (size_t sz) {
    return _theHeap.malloc (sz);
  }

  size_t getSize (void * ptr) {
    return Heap::getSize (ptr);
  }

  SuperblockType * getSuperblock (void * ptr) {
    return Heap::getSuperblock (ptr);
  }

  /// Free the given object, obeying the required locking protocol.
  static inline void free (void * ptr) {
    // Get the superblock header.
    SuperblockType * s = reinterpret_cast<SuperblockType *>(Heap::getSuperblock (ptr));

    assert (s->isValidSuperblock());

    // Find out who the owner is.

    typedef BaseHoardManager<SuperblockType> * baseHeapType;
    baseHeapType owner;

    s->lock();

    // By acquiring the lock on the superblock (above),
    // we prevent it from moving up to a higher heap.
    // This eventually pins it down in one heap,
    // so this loop is guaranteed to terminate.
    // (It should generally take no more than two iterations.)

    for (;;) {
      owner = reinterpret_cast<baseHeapType>(s->getOwner());
      assert (owner != NULL);
      assert (owner->isValid());
      // Lock the owner. If ownership changed between these two lines,
      // we'll detect it and try again.
      owner->lock();
      if (owner == reinterpret_cast<baseHeapType>(s->getOwner())) {
	owner->free (ptr);
	owner->unlock();
	s->unlock();
	return;
      }
      owner->unlock();

      // Sleep a little.
      HL::Fred::yield();
    }
  }

private:

  Heap _theHeap;

};

}

#endif

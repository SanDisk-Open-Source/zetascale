// -*- C++ -*-

/*

  Heap Layers: An Extensible Memory Allocation Infrastructure
  
  Copyright (C) 2000-2007 by Emery Berger
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

#ifndef _HOARDMANAGER_H_
#define _HOARDMANAGER_H_

#include <stdlib.h>
#include <new>

// Hoard-specific Heap Layers
#include "sassert.h"
#include "statistics.h"
#include "emptyclass.h"
#include "array.h"
#include "manageonesuperblock.h"


// Generic Heap Layers
#include "bins.h"
#include "basehoardmanager.h"
#include "emptyhoardmanager.h"
#include "guard.h"
#include "hldefines.h"

using namespace HL;

/**
 *
 * @class HoardManager
 * @brief Manages superblocks by emptiness, returning them to the parent heap when empty enough.
 * @author Emery Berger <http://www.cs.umass.edu/~emery>
 *
 **/

namespace Hoard {

template <class SourceHeap,
	  class ParentHeap,
	  class SuperblockType_,
	  int EmptinessClasses,
	  class LockType,
          class thresholdFunctionClass,
          class HeapType>
class HoardManager : public BaseHoardManager<SuperblockType_>,
                     public thresholdFunctionClass
{
public:

  HoardManager (void)
    : _magic (MAGIC_NUMBER)
  {}

  virtual ~HoardManager (void) {}

  typedef SuperblockType_ SuperblockType;

  /// This heap guarantees only double-word alignment.
  enum { Alignment = sizeof(double) };


  MALLOC_FUNCTION INLINE void * malloc (size_t sz)
  {
    Check<HoardManager, sanityCheck> check (this);
    const int binIndex = binType::getSizeClass(sz);
    size_t realSize = binType::getClassSize (binIndex);
    assert (realSize >= sz);

    // Iterate until we succeed in allocating memory.
    void * ptr = getObject (binIndex, realSize);
    if (ptr) {
      return ptr;
    } else {
      return slowPathMalloc (realSize);
    }
  }


  /// Put a superblock on this heap.
  NO_INLINE void put (SuperblockType * s, size_t sz) {
    HL::Guard<LockType> l (_theLock);

    assert (s->getOwner() != this);
    Check<HoardManager, sanityCheck> check (this);

    const int binIndex = binType::getSizeClass(sz);

    // Check to see whether this superblock puts us over.
    Statistics& stats = _stats(binIndex);
    int a = stats.getAllocated() + s->getTotalObjects();
    int u = stats.getInUse() + (s->getTotalObjects() - s->getObjectsFree());

    if (thresholdFunctionClass::function (u, a, sz)) {
      // We've crossed the threshold function,
      // so we move this superblock up to the parent.
      _ph.put (reinterpret_cast<typename ParentHeap::SuperblockType *>(s), sz);
    } else {
      unlocked_put (s, sz);
    }
  }


  /// Get an empty (or nearly-empty) superblock.
  NO_INLINE SuperblockType * get (size_t sz, HeapType * dest) {
    HL::Guard<LockType> l (_theLock);
    Check<HoardManager, sanityCheck> check (this);
    const int binIndex = binType::getSizeClass (sz);
    SuperblockType * s = _otherBins(binIndex).get();
    if (s) {
      assert (s->isValidSuperblock());
      
      // Update the statistics, removing objects in use and allocated for s.
      decStatsSuperblock (s, binIndex);
      s->setOwner (dest);
    }
    // printf ("getting sb %x (size %d) on %x\n", (void *) s, sz, (void *) this);
    return s;
  }

  /// Return one object to its superblock and update stats.
  INLINE void free (void * ptr) {
    Check<HoardManager, sanityCheck> check (this);

    // Get the corresponding superblock.
    SuperblockType * s = SuperHeap::getSuperblock (ptr);
 
    assert (s->getOwner() == this);

    // Find out which bin it belongs to.
    // Note that we assume that all pointers have been correctly
    // normalized at this point.
    assert (s->normalize (ptr) == ptr);

    const size_t sz = s->getObjectSize ();
    const int binIndex = binType::getSizeClass (sz);

    // Free the object.
    _otherBins(binIndex).free (ptr);


    // Update statistics.
    Statistics& stats = _stats(binIndex);
    int u = stats.getInUse();
    const int a = stats.getAllocated();
    // FIX ME    assert (u > 0);
    if (u > 0)
      u--;
    stats.setInUse (u);

    // Free up a superblock if we've crossed the emptiness threshold.

    if (thresholdFunctionClass::function (u, a, sz)) {

      slowPathFree (binIndex, u, a);

    }
  }

  INLINE void lock (void) {
    _theLock.lock();
  }

  INLINE void unlock (void) {
    _theLock.unlock();
  }

private:

  typedef BaseHoardManager<SuperblockType_> SuperHeap;

  enum { SuperblockSize = sizeof(SuperblockType_) };

  /// Ensure that the superblock size is a power of two.
  HL::sassert<((SuperblockSize & (SuperblockSize-1)) == 0)> verifyPowerOfTwo;

  enum { MAGIC_NUMBER = 0xfeeddadd };

  /// A magic number used for debugging.
  const unsigned long _magic;

  inline int isValid (void) const {
    return (_magic == MAGIC_NUMBER);
  }

  sassert<sizeof(typename SuperblockType::Header) % sizeof(double) == 0>
    verifyHeaderRightSize;


  /// The type of the bin manager.
  typedef HL::bins<typename SuperblockType::Header, SuperblockSize> binType;

  /// How many bins do we need to maintain?
  enum { NumBins = binType::NUM_BINS };

  NO_INLINE void slowPathFree (int binIndex, int u, int a) {
    // We've crossed the threshold.
    // Remove a superblock and give it to the 'parent heap.'
    Check<HoardManager, sanityCheck> check (this);
    
    //	printf ("HoardManager: this = %x, getting a superblock\n", this);
    
    SuperblockType * sb = _otherBins(binIndex).get ();
    
    // We should always get one.
    assert (sb);
    if (sb) {

      const size_t sz = binType::getClassSize (binIndex);
      Statistics& stats = _stats(binIndex);
      int totalObjects = sb->getTotalObjects();
      stats.setInUse (u - (totalObjects - sb->getObjectsFree()));
      stats.setAllocated (a - totalObjects);

      // Give it to the parent heap.
      ///////// NOTE: We change the superblock type here!
      ///////// THIS HAD BETTER BE SAFE!
      _ph.put (reinterpret_cast<typename ParentHeap::SuperblockType *>(sb), sz);
      assert (sb->isValidSuperblock());

    }
  }


  NO_INLINE void unlocked_put (SuperblockType * s, size_t sz) {
    if (!s || !s->isValidSuperblock()) {
      return;
    }

    Check<HoardManager, sanityCheck> check (this);

    const int binIndex = binType::getSizeClass(sz);

    // Now put it on this heap.
    s->setOwner (reinterpret_cast<HeapType *>(this));
    _otherBins(binIndex).put (s);

    // Update the heap statistics with the allocated and in use stats
    // for the superblock.

    addStatsSuperblock (s, binIndex);
    assert (s->isValidSuperblock());

  }

  void addStatsSuperblock (SuperblockType * s, int binIndex) {
    Statistics& stats = _stats(binIndex);
    
    int a = stats.getAllocated();
    int u = stats.getInUse();
    int totalObjects = s->getTotalObjects();
    stats.setInUse (u + (totalObjects - s->getObjectsFree()));
    stats.setAllocated (a + totalObjects);
  }


  void decStatsSuperblock (SuperblockType * s, int binIndex) {
    Statistics& stats = _stats(binIndex);
    
    int a = stats.getAllocated();
    int u = stats.getInUse();
    int totalObjects = s->getTotalObjects();
    stats.setInUse (u - (totalObjects - s->getObjectsFree()));
    stats.setAllocated (a - totalObjects);
  }

  MALLOC_FUNCTION NO_INLINE void * slowPathMalloc (size_t sz) {
    const int binIndex = binType::getSizeClass (sz);
    size_t realSize = binType::getClassSize (binIndex);
    assert (realSize >= sz);
    for (;;) {
      Check<HoardManager, sanityCheck> check (this);
      void * ptr = getObject (binIndex, realSize);
      if (ptr) {
	return ptr;
      } else {
	Check<HoardManager, sanityCheck> check (this);
	// Return null if we can't allocate another superblock.
	if (!getAnotherSuperblock (realSize)) {
	  //	  fprintf (stderr, "HoardManager::malloc - no memory.\n");
	  return 0;
	}
      }
    }
  }

  /// Get one object of a particular size.
  MALLOC_FUNCTION INLINE void * getObject (int binIndex, size_t sz) {
    Check<HoardManager, sanityCheck> check (this);
    void * ptr = _otherBins(binIndex).malloc (sz);
    if (ptr) {
      // We got one. Update stats.
      int u = _stats(binIndex).getInUse();
      _stats(binIndex).setInUse (u+1);
    }
    return ptr;
  }

  friend class sanityCheck;

  class sanityCheck {
  public:
    inline static void precondition (HoardManager * h) {
      checkInvariant(h);
    }
    inline static void postcondition (HoardManager * h) {
      checkInvariant(h);
    }
  private:
    inline static void checkInvariant (HoardManager * h) {
      assert (h->isValid());
    }
  };

public:

  NO_INLINE void * getAnotherSuperblock (size_t sz) {

    // NB: This function should be on the slow path.

    SuperblockType * sb = NULL;

    // Try the parent heap.
    // NOTE: We change the superblock type here!
    sb = reinterpret_cast<SuperblockType *>(_ph.get (sz, reinterpret_cast<ParentHeap *>(this)));

    if (sb) {
      if (!sb->isValidSuperblock()) {
	// As above - drop any invalid superblocks.
	sb = NULL;
      }

    } else {
      // Nothing - get memory from the source.
      void * ptr = _sourceHeap.malloc (SuperblockSize);
      if (!ptr) {
	return 0;
      }
      sb = new (ptr) SuperblockType (sz);
    }

    // Put the superblock into its appropriate bin.
    if (sb) {
      unlocked_put (sb, sz);
    }
    return sb;
  }

private:

  LockType _theLock;

  /// Usage statistics for each bin.
  Array<NumBins, Statistics> _stats;

  typedef SuperblockType * SuperblockTypePointer;

  typedef EmptyClass<SuperblockType, EmptinessClasses> OrganizedByEmptiness;

  typedef ManageOneSuperblock<OrganizedByEmptiness> BinManager;

  /// Bins that hold superblocks for each size class.
  Array<NumBins, BinManager> _otherBins;

  /// The parent heap.
  ParentHeap _ph;

  /// Where memory comes from.
  SourceHeap _sourceHeap;

};

}

#endif

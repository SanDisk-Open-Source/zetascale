// -*- C++ -*-

#ifndef _EMPTYCLASS_H_
#define _EMPTYCLASS_H_

#include <stdio.h> // for stderr
#include "check.h"
#include "array.h"
#include "hldefines.h"

/**
 * @class EmptyClass
 * @brief Maintains superblocks organized by emptiness.
 */

namespace Hoard {

template <class SuperblockType_,
	  int EmptinessClasses>
class EmptyClass {

  enum { SuperblockSize = sizeof(SuperblockType_) };

public:

  typedef SuperblockType_ SuperblockType;

  EmptyClass (void)
  {
    for (int i = 0; i <= EmptinessClasses + 1; i++) {
      _available(i) = 0;
    }
  }

  void dumpStats (void) {
    for (int i = 0; i <= EmptinessClasses + 1; i++) {
      SuperblockType * s = _available(i);
      if (s) {
	fprintf (stderr, "EmptyClass: emptiness class = %d\n", i);
	while (s) {
	  s->dumpStats();
	  s = s->getNext();
	}
      }
    }
  }


  SuperblockType * getEmpty (void) {
    Check<EmptyClass, MyChecker> check (this);
    SuperblockType * s = _available(0);
    if (s && 
	(s->getObjectsFree() == s->getTotalObjects())) {
      // Got an empty one. Remove it.
      _available(0) = s->getNext();
      if (_available(0)) {
	_available(0)->setPrev (0);
      }
      s->setPrev (0);
      s->setNext (0);
      return s;
    }
    return 0;
  }

  SuperblockType * get (void) {
    Check<EmptyClass, MyChecker> check (this);
    // Return as empty a superblock as possible
    // by iterating from the emptiest to the fullest available class.
    for (int n = 0; n < EmptinessClasses + 1; n++) {
      SuperblockType * s = _available(n);
      while (s) {
	assert (s->isValidSuperblock());
	// Got one. Remove it.
	_available(n) = s->getNext();
	if (_available(n)) {
	  _available(n)->setPrev (0);
	}
	s->setPrev (0);
	s->setNext (0);

#ifndef NDEBUG
	// Verify that this superblock is *gone* from the lists.
	for (int z = 0; z < EmptinessClasses + 1; z++) {
	  SuperblockType * p = _available(z);
	  while (p) {
	    assert (p != s);
	    p = p->getNext();
	  }
	}
#endif

	// Ensure that we return a superblock that is as free as
	// possible.
	int cl = getFullness (s);
	if (cl > n) {
	  put (s);
	  SuperblockType * sNew = _available(n);
	  assert (s != sNew);
	  s = sNew;
	} else {
	  return s;
	}
      }
    }
    return 0;
  }

  void put (SuperblockType * s) {
    Check<EmptyClass, MyChecker> check (this);

#ifndef NDEBUG
    // Check to verify that this superblock is not already on one of the lists.
    for (int n = 0; n <= EmptinessClasses + 1; n++) {
      SuperblockType * p = _available(n);
      while (p) {
	assert (p != s);
	p = p->getNext();
      }
    }
#endif

    // Put on the appropriate available list.
    int cl = getFullness (s);

    //    printf ("put %x, cl = %d\n", s, cl);
    s->setPrev (0);
    s->setNext (_available(cl));
    if (_available(cl)) {
      _available(cl)->setPrev (s);
    }
    _available(cl) = s;
  }

  INLINE MALLOC_FUNCTION void * malloc (size_t sz) {
    // Malloc from the fullest superblock first.
    for (int i = EmptinessClasses; i >= 0; i--) {
      SuperblockType * s = _available(i);
      // printf ("i\n");
      if (s) {
	int oldCl = getFullness (s);
	void * ptr = s->malloc (sz);
	int newCl = getFullness (s);
	if (ptr) {
	  if (oldCl != newCl) {
	    transfer (s, oldCl, newCl);
	  }
	  return ptr;
	}
      }
    }
    return NULL;
  }

  INLINE void free (void * ptr) {
    Check<EmptyClass, MyChecker> check (this);
    SuperblockType * s = getSuperblock (ptr);
    int oldCl = getFullness (s);
    s->free (ptr);
    int newCl = getFullness (s);

    if (oldCl != newCl) {
      // Transfer.
      transfer (s, oldCl, newCl);
    }
  }

  /// Find the superblock (by bit-masking) that holds a given pointer.
  static INLINE SuperblockType * getSuperblock (void * ptr) {
    return SuperblockType::getSuperblock (ptr);
  }

private:

  void transfer (SuperblockType * s, int oldCl, int newCl)
  {
    SuperblockType * prev = s->getPrev();
    SuperblockType * next = s->getNext();
    if (prev) { prev->setNext (next); }
    if (next) { next->setPrev (prev); }
    if (s == _available(oldCl)) {
      assert (prev == 0);
      _available(oldCl) = next;
    }
    s->setNext (_available(newCl));
    s->setPrev (0);
    if (_available(newCl)) { _available(newCl)->setPrev (s); }
    _available(newCl) = s;
  }

  static INLINE int getFullness (SuperblockType * s) {
    // Completely full = EmptinessClasses + 1
    // Completely empty (all available) = 0
    int total = s->getTotalObjects();
    int free = s->getObjectsFree();
    if (total == free) {
      return 0;
    } else {
      return 1 + (EmptinessClasses * (total - free)) / total;
    }
  }

  /// Forward declarations for the sanity checker.
  /// @sa Check
  class MyChecker;
  friend class MyChecker;

  /// Precondition and postcondition checking.
  class MyChecker {
  public:
#ifndef NDEBUG
    static void precondition (EmptyClass * e) {
      e->sanityCheckPre();
    }
    static void postcondition (EmptyClass * e) {
      e->sanityCheck();
    }
#else
    static void precondition (EmptyClass *) {}
    static void postcondition (EmptyClass *) {}
#endif
  };

  void sanityCheckPre (void) { sanityCheck(); }

  void sanityCheck (void) {
    for (int i = 0; i <= EmptinessClasses + 1; i++) {
      SuperblockType * s = _available(i);
      while (s) {
	assert (getFullness(s) == i);
	s = s->getNext();
      }
    }
  }

  /// The bins of superblocks, by emptiness class.
  /// @note index 0 = completely empty, EmptinessClasses + 1 = full
  Array<EmptinessClasses + 2, SuperblockType *> _available;

};

}


#endif

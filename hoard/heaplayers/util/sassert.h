#ifndef _SASSERT_H_
#define _SASSERT_H_

/**
 * @class  sassert
 * @brief  Implements compile-time assertion checking.
 * @author Emery Berger <http://www.cs.umass.edu/~emery>
 *
 * @code
 *   sassert<(1+1 == 2)> CheckOnePlusOneIsTwo;
 * @endcode
 *
 * This code is part of the Hoard distribution and is governed by its license.
 * <http://www.hoard.org>
 *
 */

namespace HL {

  template <int assertion>
    class sassert;

  template<> class sassert<1> {
  public:
    enum { VALUE = 1 };
  };

}

#endif

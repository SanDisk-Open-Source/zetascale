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


#ifndef SDF_ERR_H
#define SDF_ERR_H



/* Define the error base at some convenient number outside
   of unix errors range. Currently arbitrarily setup at 1<<16.
   Salt to taste later.
*/
#define SDF_ERR_BASE 1<<16
#define ERR_MAX      (SDF_ERR_BASE + 1000)
/* This can be made more useful as need arises  */
#define ERR_DEF(err_name, err_num)    \
    int err_name = (err_num);

ERR_DEF(SDF_ERR_FAILED_AGENT_RENDEZVOUS, (SDF_ERR_BASE)+2)

#endif  /* SDF_ERR_H */

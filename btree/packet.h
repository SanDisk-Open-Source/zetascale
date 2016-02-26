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



typedef struct rec_packet_structure	rec_packet_t;
typedef struct rec_packet_trx_structure	rec_packet_trx_t;
typedef struct stats_packet_structure	stats_packet_t;
typedef struct stats_packet_node_struct	stats_packet_node_t;

struct rec_packet_trx_structure	{
	char	**newnodes,
		**oldnodes;
	uint	newcount,
		oldcount;
};
struct stats_packet_node_struct {
	char	*data;
	uint	datalen;
};

rec_packet_t		*recovery_packet_open( uint);
stats_packet_t		*stats_packet_open( uint);
rec_packet_trx_t	*recovery_packet_get_trx( rec_packet_t *);
stats_packet_node_t	*stats_packet_get_node( stats_packet_t *);
void			recovery_packet_free_trx( rec_packet_trx_t *),
			stats_packet_free_node( stats_packet_node_t *),
			recovery_packet_close( rec_packet_t *),
			stats_packet_close( stats_packet_t *),
			recovery_packet_delete( uint),
			stats_packet_delete( uint);

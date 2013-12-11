

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



typedef struct rec_packet_structure	rec_packet_t;
typedef struct rec_packet_trx_structure	rec_packet_trx_t;

struct rec_packet_trx_structure	{
	char	**newnodes,
		**oldnodes;
	uint	newcount,
		oldcount;
};

rec_packet_t		*recovery_packet_open( uint);
rec_packet_trx_t	*recovery_packet_get_trx( rec_packet_t *);
void			recovery_packet_free_trx( rec_packet_trx_t *),
			recovery_packet_close( rec_packet_t *),
			recovery_packet_delete( uint);

#include <sys/socket.h>
#include <sys/ioctl.h>
#include <netinet/in.h>
#include <errno.h>
#include <pthread.h>
#include <search.h>
#include <time.h>
#include <net/if.h>
#include <netdb.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>

#include "platform/types.h"
#include "platform/assert.h"
#include "platform/string.h"
#include "platform/shmem.h"
#include "platform/stdlib.h"
#include "platform/stdio.h"
#include "platform/time.h"

#include "sdf_msg_oob.h"

#define MSG_OOB_MAX_BUFSIZE 1024
#define ETH_NAME "eth0"
#define LISTENQ MAX_NODES-1

typedef const struct sockaddr SA;
PLAT_LOG_CAT_LOCAL(LOG_CAT, "sdf/sdfmsg/tests/mpilog");

static int sdf_msg_oob_get_serv_sock(uint16_t port, const char *transport,
        int qlen);
static int sdf_msg_oob_get_connect_sock(const char *host, uint16_t port,
        const char *transport);
//static unsigned short  portbase = 0;
/**
 * @brief allocate & bind a server socket using TCP or UDP
 *
 * It is an internal function for convenience, get a socket if we want to
 * establish a server
 *
 * @param service <IN> service associated with the desired port or just
 * port itself
 * @param trasport <IN> transport protocol to use ("tcp" or "udp")
 * @param qlen <IN> maximum server request queue length
 * @retval s successfully returned and 0 if error occurred
 */
static int sdf_msg_oob_get_serv_sock(uint16_t port, const char *transport,
        int qlen) {
    //struct servent  *pse;   /* pointer to service information entry */
    /*struct protoent *ppe;  pointer to protocol information entry*/
    struct sockaddr_in sin; /* an Internet endpoint address     */
    int listenfd, type; /* socket descriptor and socket type    */

    memset(&sin, 0, sizeof(sin));
    sin.sin_family = AF_INET;
    sin.sin_addr.s_addr = INADDR_ANY;
    sin.sin_port = htons(port);

/*     Map protocol name to protocol number
    if (strcmp(transport, "tcp") == 0 && (ppe = getprotobyname(transport)) == 0) {
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                "\ncan't get \"%s\" protocol entry\n", transport);
        return 0;
    }*/
    /* Use protocol to choose a socket type */
    if (strcmp(transport, "udp") == 0)
        type = SOCK_DGRAM;
    else
        type = SOCK_STREAM;

    /* Allocate a socket */
    listenfd = socket(AF_INET, type, 0);

    if (listenfd < 0) {
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                "\ncan't create socket\n");
        return 0;
    }

    if(type == SOCK_DGRAM)
    {
        int on = 1;
        setsockopt(listenfd, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on));
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                "\nSet resue mark\n");
    }

    /* Bind the socket */
    if (bind(listenfd, (struct sockaddr *) &sin, sizeof(sin)) < 0) {

        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                "\ncan't bind to %d port\n", port);
        return 0;
    }

    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
            "\nbind to %d port\n", port);

    if (type == SOCK_STREAM && listen(listenfd, qlen) < 0) {
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                "\ncan't listen on %d port\n", port);
        return 0;
    }
    return listenfd;
}

/**
 * @brief allocate & connect a socket using TCP or UDP
 *
 * It is an internal function for convenience, get a socket if we want to
 * connect to a server
 *
 * @param host <IN> name of host to which connection is desired
 * @param service <IN> service associated with the desired port or just
 * port itself
 * @param trasport <IN> transport protocol to use ("tcp" or "udp")
 * @retval s successfully returned and 0 if error occurred
 */
static int sdf_msg_oob_get_connect_sock(const char *host, uint16_t port,
        const char *transport) {
    struct hostent *phe; /* pointer to host information entry    */
    //struct servent  *pse;   /* pointer to service information entry */
//    struct protoent *ppe; /* pointer to protocol information entry*/
    struct sockaddr_in sin; /* an Internet endpoint address     */
    int s, type; /* socket descriptor and socket type    */

    memset(&sin, 0, sizeof(sin));
    sin.sin_family = AF_INET;
    sin.sin_port = htons(port);

    /* Map host name to IP address, allowing for dotted decimal */
    if ((phe = gethostbyname(host)))
        memcpy(&sin.sin_addr, phe->h_addr, phe->h_length);
    else if ((sin.sin_addr.s_addr = inet_addr(host)) == INADDR_NONE) {
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                "\ncan't get \"%s\" host entry\n", host);
        return 0;
    }

    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
            "\nbegin to get %s sock to connect ip %s, port %d\n", transport, host, port);
/*    //Map transport protocol name to protocol number
    if ((ppe = getprotobyname(transport)) == 0) {
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                "\ncan't get \"%s\" protocol entry\n", transport);
        return 0;
    }*/

    /* Use protocol to choose a socket type */
    if (strcmp(transport, "udp") == 0)
        type = SOCK_DGRAM;
    else
        type = SOCK_STREAM;

    /* Allocate a socket */
//    s = socket(PF_INET, type, ppe->p_proto);
    s = socket(AF_INET, type, 0);
    if (s < 0) {
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                "\ncan't create socket\n");
        return 0;
    }

    if(type == SOCK_DGRAM && bind(s, (struct sockaddr *) &sin, sizeof(sin)) < 0)
    {
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                "\nUDP sender can't bind to %d\n", port);
        return 0;
    }
    /* Connect the socket */
    if (type == SOCK_STREAM
            && connect(s, (struct sockaddr *) &sin, sizeof(sin)) < 0) {
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                "\ncan't connect to %s.%d\n", host, port);
        return 0;
    }
    return s;
}

/**
 * @brief get self ip address whose host name usually is "eth0"
 *
 * mainly used by node with rank 0 get self ip address to
 * send to other nodes
 *
 * @retval a uint32_t value present ip address, 0 if fails
 */
uint32_t sdf_msg_oob_get_self_ipaddr() {
    int sock;
    struct sockaddr_in sin;
    struct ifreq ifr;

    sock = socket(AF_INET, SOCK_DGRAM, 0);
    if (sock == 0) {
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                "get socket error\n");
        return 0;
    }

    strncpy(ifr.ifr_name, ETH_NAME, IFNAMSIZ);
    ifr.ifr_name[IFNAMSIZ - 1] = 0;

    if (ioctl(sock, SIOCGIFADDR, &ifr) < 0) {
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                "ioctl error\n");
        return 0;
    }
    memcpy(&sin, &ifr.ifr_addr, sizeof(sin));

    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
            "\nGet self ip address, %s\n", inet_ntoa(sin.sin_addr));

    return sin.sin_addr.s_addr;
}

/**
 * @brief construct identity information
 *
 * construct identity information according to ip address and mpi rank value
 * @param ipaddr <IN> ip address
 * @param rank <IN> mpi rank value
 * @param id_info <OUT> identity information
 * @retval 0 if success, otherwise larger than 0
 */
uint32_t sdf_msg_oob_create_id_info(uint32_t ipaddr, uint32_t rank,
        sdf_msg_oob_identity_t* id_info) {
    if (!id_info) {
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                "A null value param input\n");
        return OOB_PARAM_ERROR;
    }

    id_info->mpi_rank_id = rank;
    id_info->ip_address = ipaddr;

    return OOB_SUCCESS;
}

/*!
 * @brief allocate & bind a server socket using TCP
 *
 * use sdf_msg_oob_get_serv_sock with param tcp
 */
int sdf_msg_oob_create_tcp_server(uint16_t port, int qlen) {
    return sdf_msg_oob_get_serv_sock(port, "tcp", qlen);
}

/*!
 * @brief allocate & connect a socket using TCP or UDP
 *
 * use sdf_msg_oob_get_connect_sock with param tcp
 */

int sdf_msg_oob_create_tcp_client(const char* host, uint16_t port) {
    return sdf_msg_oob_get_connect_sock(host, port, "tcp");
}

/*!
 * @brief allocate & bind a receiver socket using UDP
 *
 * use sdf_msg_oob_get_serv_sock
 */
int sdf_msg_oob_create_udp_receiver(uint16_t port, int qlen) {
    return sdf_msg_oob_get_serv_sock(port, "udp", qlen);
}

/*!
 * @brief allocate a sender socket using UDP
 *
 * use sdf_msg_oob_get_connect_sock
 */
int sdf_msg_oob_create_udp_sender(const char* host, uint16_t port) {
    return sdf_msg_oob_get_connect_sock(host, port, "udp");
}

/**
 * @brief init sever information
 */
uint32_t sdf_msg_oob_init_server(sdf_msg_oob_server_t* server_info,
        uint32_t rank, int total_rank) {
    uint32_t servaddr;
    servaddr = sdf_msg_oob_get_self_ipaddr();
    server_info->server_id.nid.ip_address = servaddr;
    server_info->server_id.nid.mpi_rank_id = rank;

    // it will be modified in further check, if it is not node 0
    server_info->server_id.port = LISTEN_PORT_BASE;
    server_info->clients[0].nid.ip_address = servaddr;
    server_info->clients[0].nid.mpi_rank_id = rank;
    server_info->cnt_total = (uint32_t) total_rank;
    server_info->connected_clients = 1;
    // it will be modified in further check, if it is not node 0
    server_info->clients[0].port = LISTEN_PORT_BASE; // it will be modified in further check
    return OOB_SUCCESS;
}

/**
 * @brief node 0 receive information from other nodes and then send the contactor list
 *
 * The node 0 acts as a information collection server. It receives client id reports
 * and construct a contactor-list
 *
 * @param server_info <IN><OUT> server information
 *
 * @retval OOB_SUCCESS if ok, larger than OOB_SUCCESS otherwise
 */
uint32_t sdf_msg_oob_collect_client_information_and_send_list(
        sdf_msg_oob_server_t* server_info) {
    int i, maxi, maxfd, listenfd, connfd, sockfd;
    int nready, client[FD_SETSIZE];
    int waitforreps_client[FD_SETSIZE];
    ssize_t n;
    fd_set rset, allset;

    char buf[MSG_OOB_MAX_BUFSIZE];
    socklen_t clilen;
    struct sockaddr_in cliaddr, servaddr;

    listenfd = socket(AF_INET, SOCK_STREAM, 0);

    bzero(&servaddr, sizeof(servaddr));
    servaddr.sin_family = AF_INET;
    servaddr.sin_addr.s_addr = htonl(INADDR_ANY);
    servaddr.sin_port = htons(SERV_PORT);

    bind(listenfd, (SA*) &servaddr, sizeof(servaddr));

    listen(listenfd, LISTENQ);

    maxfd = listenfd;
    maxi = -1;

    for (i = 0; i < FD_SETSIZE; i++) {
        client[i] = -1;
        waitforreps_client[i] = -1;
    }
    FD_ZERO(&allset);
    FD_SET(listenfd, &allset);

    plat_log_msg(
            PLAT_LOG_ID_INITIAL,
            LOG_CAT,
            PLAT_LOG_LEVEL_TRACE,
            "\n$$Inside sdf_msg_oob_collect_client_information_and_send_list and before circle\n");

    for (;;) {
        rset = allset;
        nready = select(maxfd + 1, &rset, NULL, NULL, NULL);

        if (FD_ISSET(listenfd, &rset)) {
            clilen = sizeof(cliaddr);
            connfd = accept(listenfd, (struct sockaddr*) &cliaddr, &clilen);

            for (i = 0; i < FD_SETSIZE; i++)
                if (client[i] < 0) {
                    client[i] = connfd;
                    break;
                }

            if (i == FD_SETSIZE) {
                plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT,
                        PLAT_LOG_LEVEL_TRACE, "\ntoo many clients\n");
                continue;
            }
            FD_SET(connfd, &allset);
            if (connfd > maxfd)
                maxfd = connfd;
            if (i > maxi)
                maxi = i;
            if (--nready <= 0)
                continue;
        }

        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                "\nSome socket connected\n");

        for (i = 0; i <= maxi; i++) {
            if ((sockfd = client[i]) < 0)
                continue;
            if (FD_ISSET(sockfd, &rset)) {
                n = read(sockfd, buf, MSG_OOB_MAX_BUFSIZE);
                if (n == 0) {
                    /*                    close(sockfd);
                     FD_CLR(sockfd, &allset);
                     client[i] = -1;*/
                    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT,
                            PLAT_LOG_LEVEL_TRACE, "\n$$Client send over\n");
                } else if (n < 0) {
                    close(sockfd);
                    FD_CLR(sockfd, &allset);
                    client[i] = -1;

                    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT,
                            PLAT_LOG_LEVEL_TRACE, "\n$$Error occurred\n");
                } else {
                    /*
                     * process the MSG, the MSG ought to have a header and a message
                     * type with SDF_MSG_OOB_IDENTITY. Then, the indentity information will
                     * be added to the contactor list.
                     */
                    sdf_msg_oob_header_t* header = (sdf_msg_oob_header_t*) buf;
                    if (header->msgtype != CLIENT_REPORT) {
                        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT,
                                PLAT_LOG_LEVEL_TRACE,
                                "\nGet error msg type %d\n", header->msgtype);
                        continue;
                    }
                    sdf_msg_oob_identity_t* id_info;
                    id_info = (sdf_msg_oob_identity_t*) (buf
                            + sizeof(sdf_msg_oob_header_t));
                    if (id_info->mpi_rank_id >= server_info->cnt_total) {
                        plat_log_msg(
                                PLAT_LOG_ID_INITIAL,
                                LOG_CAT,
                                PLAT_LOG_LEVEL_TRACE,
                                "\nGet error rank value %d from clients, server cnt_total is %d\n",
                                id_info->mpi_rank_id, server_info->cnt_total);
                        continue;
                    } else {
                        server_info->clients[server_info->connected_clients].nid.mpi_rank_id
                                = id_info->mpi_rank_id;
                        server_info->clients[server_info->connected_clients].nid.ip_address
                                = id_info->ip_address;

                        int i = 0;
                        int repeatcnt = 0;
                        //choose a listening port for it
                        for (i = 0; i < server_info->connected_clients; i++) {
                            if (server_info->clients[i].nid.ip_address
                                    == id_info->ip_address)
                                repeatcnt++;
                        }
                        server_info->clients[server_info->connected_clients].port
                                = LISTEN_PORT_BASE + repeatcnt;

                        server_info->connected_clients++;
                    }
                    for (i = 0; i < FD_SETSIZE; i++)
                        if (waitforreps_client[i] < 0) {
                            waitforreps_client[i] = sockfd;
                            plat_log_msg(
                                    PLAT_LOG_ID_INITIAL,
                                    LOG_CAT,
                                    PLAT_LOG_LEVEL_TRACE,
                                    "\nGet a client connected, i:%d, sockfd:%d\n",
                                    i, sockfd);
                            break;
                        }
                }
            }
        }
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                "\n$$After get the list\n");
        // if information collection is completed
        if (server_info->connected_clients == server_info->cnt_total) {
            plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                    "\nClients information collection complete, then send constractor list\n");

            char contactor_buf[MSG_OOB_MAX_BUFSIZE];
            sdf_msg_oob_header_t* header;
            header = (sdf_msg_oob_header_t*) contactor_buf;
            header->version = OOB_VERSION;
            header->msgtype = CONTACTORS_SHARING;
            header->msglen = sizeof(sdf_msg_oob_header_t)
                    + server_info->connected_clients
                            * sizeof(sdf_msg_oob_comm_node_t);

            memcpy(contactor_buf + sizeof(sdf_msg_oob_header_t),
                    server_info->clients, server_info->connected_clients
                            * sizeof(sdf_msg_oob_comm_node_t));

            for (i = 0; i < FD_SETSIZE; i++)
                if (waitforreps_client[i] > 0) {
                    int nwritten = write(waitforreps_client[i], contactor_buf,
                            header->msglen);
                    if (nwritten != header->msglen) {
                        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT,
                                PLAT_LOG_LEVEL_TRACE,
                                "\nSend contactor list error\n");
                        return OOB_SOCK_ERROR;
                    } else {
                        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT,
                                PLAT_LOG_LEVEL_TRACE,
                                "\nSend contactor list successfully\n");
                    }
                }
            break;
        }

    }
    close(listenfd);
    for (i = 0; i < FD_SETSIZE; i++)
        if (waitforreps_client[i] > 0) {
            close(waitforreps_client[i]);
        }
    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
            "\n$$collect list and send to others successfully\n");
    return OOB_SUCCESS;
}

/**
 * @brief nodes 1~n send id_report to node 0 and get contactor list form node 0
 *
 * @param client_info <IN> client id information
 * @param client_info <IN> server destination information
 *
 * @retval OOB_SUCCESS if ok, larger than OOB_SUCCESS otherwise
 */
uint32_t sdf_msg_oob_send_id_report_and_get_list(
        sdf_msg_oob_identity_t* client_info,
        sdf_msg_oob_server_dsp_t* server_info, sdf_msg_oob_server_t* self) {
    int sockfd, i;
    struct in_addr inaddr;
    char buf[MSG_OOB_MAX_BUFSIZE];
    inaddr.s_addr = server_info->ipaddr;
    sdf_msg_oob_header_t* header;
    sdf_msg_oob_identity_t* id_info;
    header = (sdf_msg_oob_header_t *) buf;
    header->version = OOB_VERSION;
    header->msgtype = CLIENT_REPORT;
    header->msglen = sizeof(sdf_msg_oob_header_t)
            + sizeof(sdf_msg_oob_identity_t);

    id_info = (sdf_msg_oob_identity_t *) (buf + sizeof(sdf_msg_oob_header_t));
    id_info->ip_address = client_info->ip_address;
    id_info->mpi_rank_id = client_info->mpi_rank_id;

    //create a client socket
    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
            "\nIn client. Before create tcp client\n");

    int trycount = 500;
    while (trycount) {
        sockfd = sdf_msg_oob_create_tcp_client(inet_ntoa(inaddr), SERV_PORT);
        if (sockfd <= 0) {
            plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                    "\nGet error sock\n");
        } else {
            plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                    "\nGet sock successfully, sock: %d\n", sockfd);
            break;
        }
        usleep(100000 / trycount);
        trycount--;
    }

    if (!trycount) {
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                "\nExceed max try time, failed to connect server\n");
        return OOB_SOCK_ERROR;
    }
    //send a report to server
    int nwritten = write(sockfd, buf, header->msglen);
    if (nwritten <= 0) {
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                "\nSend client report message failed\n");
        return OOB_SOCK_ERROR;
    }
    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
            "\nSend client report message successfully\n");

    //receive the list of all contactors
    int nrecv = read(sockfd, buf, MSG_OOB_MAX_BUFSIZE);
    if (nrecv <= 0) {
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                "\nReceive contactor list failed\n");
        return OOB_SOCK_ERROR;
    }
    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
            "\nReceive contactor list successfully\n");

    sdf_msg_oob_header_t* recv_header = (sdf_msg_oob_header_t*) buf;
    plat_assert(recv_header->version == OOB_VERSION);
    plat_assert(recv_header->msgtype == CONTACTORS_SHARING);
    int len = recv_header->msglen;
    plat_assert(len == nrecv);

    int nodenum = (len - sizeof(sdf_msg_oob_header_t))
            / sizeof(sdf_msg_oob_comm_node_t);
    sdf_msg_oob_comm_node_t* pIterator = (sdf_msg_oob_comm_node_t*) (buf
            + sizeof(sdf_msg_oob_header_t));
    self->connected_clients = 0;
    for (i = 0; i < nodenum; i++) {
        memcpy(&(self->clients[i]), pIterator, sizeof(sdf_msg_oob_comm_node_t));
        self->connected_clients++;
        pIterator++;
    }

    // find self in the list, and re-initialize serv_id
    int find_debug = 0;
    ;
    for (i = 0; i < self->connected_clients; i++) {
        if (self->server_id.nid.mpi_rank_id == self->clients[i].nid.mpi_rank_id) {
            self->server_id.nid.ip_address = self->clients[i].nid.ip_address;
            self->server_id.port = self->clients[i].port;
            find_debug++;
            break;
        }
    }

    if (find_debug != 1) {
        plat_log_msg(
                PLAT_LOG_ID_INITIAL,
                LOG_CAT,
                PLAT_LOG_LEVEL_TRACE,
                "\nError, can't find self information, rank %d, find_debug %d\n",
                self->server_id.nid.mpi_rank_id, find_debug);
    }

    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
            "\nsend info and get list successfully, with clients %d\n",
            self->connected_clients);

    close(sockfd);
    return OOB_SUCCESS;
}

/*
 * @ brief search node according to rank
 */
sdf_msg_oob_comm_node_t* sdf_msg_oob_search_node_by_rank(uint32_t rank,
        sdf_msg_oob_server_t* self) {
    int i;
    for (i = 0; i < self->connected_clients; i++) {
        if (self->clients[i].nid.mpi_rank_id == rank)
            return &self->clients[i];
    }
    return NULL;
}

/*
 * @ brief search node according to ip address and port
 */
sdf_msg_oob_comm_node_t* sdf_msg_oob_search_node_by_ipandport(uint32_t ipaddr,
        uint16_t port, sdf_msg_oob_server_t* self) {
    int i;
    for (i = 0; i < self->connected_clients; i++) {
        if (self->clients[i].nid.ip_address == ipaddr && self->clients[i].port
                == port)
            return &self->clients[i];
    }
    return NULL;
}

/*!
 * @brief send message to a certain node indicated by rank value
 *
 * @param msg_buf<IN> the buf pointer
 * @param msg_len<IN> the length of buf prepare to send
 * @param rank<IN> the rank value indicating node
 * @param self<IN> the information of node itself
 *
 * @retval OOB_SUCCESS if all is ok, otherwise OOB_SOCK_ERROR or
 * OOB_PARAM_ERROR
 */
uint32_t sdf_msg_oob_send_msg(char* msg_buf, int msg_len, uint32_t rank,
        sdf_msg_oob_server_t * self) {
    int sendsock;
    int sendlen;
    uint32_t host_ip, dest_ip;
    uint16_t host_port, dest_port;
    struct sockaddr_in destaddr;
    sdf_msg_oob_comm_node_t* dest;
    sdf_msg_oob_comm_node_t* host;
    dest = sdf_msg_oob_search_node_by_rank(rank, self);
    if (dest == NULL) {
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                "Can't find node with rank %d\n", rank);
        return OOB_PARAM_ERROR;
    }

    host = &self->server_id;
    host_ip = host->nid.ip_address;
    host_port = host->port;
    dest_ip = dest->nid.ip_address;
    dest_port = dest->port;

    struct in_addr tmp_addr;
    tmp_addr.s_addr = host_ip;
    sendsock = sdf_msg_oob_create_udp_sender(inet_ntoa(tmp_addr), host_port);
    if (!sendsock) {
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                "Get sendsock error, ip is %s, port is %d\n", inet_ntoa(
                        tmp_addr), host_port);
        return OOB_SOCK_ERROR;
    }

    bzero(&destaddr, sizeof(struct sockaddr_in));
    destaddr.sin_family = AF_INET;
    destaddr.sin_port = htons(dest_port);
    destaddr.sin_addr.s_addr = dest_ip;

    tmp_addr.s_addr = dest_ip;
    sendlen = sendto(sendsock, msg_buf, msg_len, 0, (SA*) &destaddr,
            sizeof(struct sockaddr_in));
    if (sendlen != msg_len) {
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                "Send msg error, ip is %s, port is %d\n", inet_ntoa(tmp_addr),
                dest_port);
        close(sendsock);
        return OOB_SOCK_ERROR;
    }

    close(sendsock);
    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
            "\nSend msg %s\n, ip is %s, port is %d, rank is %d\n", msg_buf,
            inet_ntoa(tmp_addr), dest_port, rank);
    return OOB_SUCCESS;
}

/*
 * @brief receive msg and check its rank
 *
 * @param recv_buf<OUT> received data buf
 * @param recv_len<OUT> received data length
 * @param rank<OUT> which node the data comes from
 * @param self<IN> the information of node itself
 *
 * @retval OOB_SUCCESS if all is ok, otherwise OOB_SOCK_ERROR or
 * OOB_PARAM_ERROR
 */
uint32_t sdf_msg_oob_recv_msg(char* recv_buf, int* recv_len, uint32_t* rank,
        sdf_msg_oob_server_t * self) {
    int recvsock;
    struct sockaddr_in sender_addr;
    uint32_t host_ip, from_ip;
    uint16_t host_port, from_port;
    struct in_addr tmp_addr;
    sdf_msg_oob_comm_node_t* host;
    sdf_msg_oob_comm_node_t* from;
    host = &self->server_id;
    host_port = self->server_id.port;
    host_ip = self->server_id.nid.ip_address;

    recvsock = sdf_msg_oob_create_udp_receiver(host_port, 0);
    if (!recvsock) {
        tmp_addr.s_addr = host_ip;
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                "\nGet recvsock error, ip is %s, port is %d\n",
                inet_ntoa(tmp_addr), host_port);
        return OOB_SOCK_ERROR;
    }

    bzero(&sender_addr, sizeof(struct sockaddr_in));

    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
            "\nwaiting to receiving msg\n");

    socklen_t socklen = sizeof(struct sockaddr_in);
    *recv_len = recvfrom(recvsock, (void *) recv_buf, MSG_OOB_MAX_BUFSIZE, 0,
            (struct sockaddr*) &sender_addr, &socklen);
    from_ip = sender_addr.sin_addr.s_addr;
    from_port = ntohs(sender_addr.sin_port);
    if (*recv_len <= 0) {
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                "\nRecv msg error, ip is %s, port is %d, retval is %d\n", inet_ntoa(
                        sender_addr.sin_addr), from_port, *recv_len);
        close(recvsock);
        return OOB_SOCK_ERROR;
    }

    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
            "\nRecv msg %s, ip is %s, port is %d\n", recv_buf, inet_ntoa(
                    sender_addr.sin_addr), from_port);
    //find sender node rank according to ip and port
    from = sdf_msg_oob_search_node_by_ipandport(from_ip, from_port, self);
    if (from == NULL) {
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                "\nSender is not on the list, ip is %s, port is %d\n",
                inet_ntoa(sender_addr.sin_addr), from_port);
        close(recvsock);
        return OOB_SOCK_ERROR;
    }
    *rank = from->nid.mpi_rank_id;
    close(recvsock);
    return OOB_SUCCESS;
}

/*!
 * @brief get server information now it is reading from some files
 *
 * @param server_info <IN><OUT> server information
 *
 * @retval OOB_SUCCESS if all is ok, otherwise OOB_SOCK_ERROR or
 * OOB_PARAM_ERROR
 */
uint32_t sdf_msg_oob_get_server_info(sdf_msg_oob_server_dsp_t * server_info) {
    uint32_t ipaddr;
    ipaddr = sdf_msg_oob_get_self_ipaddr();
    server_info->ipaddr = ipaddr;
    server_info->port = LISTEN_PORT_BASE;
    server_info->rank = 0;
    server_info->reserved = 0;
    return OOB_SUCCESS;
}

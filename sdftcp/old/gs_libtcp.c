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

/*
 * File:   libtcp.c
 * Author: gshaw
 *
 * Created on September 22, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: $
 */

/*
 * Implementation of Schooner cluster TCP messaging.
 */

#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>     // Import sleep
// #include <sys/types.h>
#include <string.h>     // Import memset
#include <errno.h>      // Import EFAULT, et al
#include <netdb.h>      // Import struct hostent
// #include <sys/socket.h>
// #include <netinet/in.h>
#include <arpa/inet.h>
#include <inttypes.h>   // Import PRI... format strings

#include "platform/logging.h"
#include "platform/shmem.h"
#include "platform/fcntl.h"
#include "platform/string.h"
#include "fth/fth.h"
#include "fth/fthMbox.h"
#include "platform/errno.h"

#include "tcp.h"
#include "tcp_impl.h"

#define XXX_NOTYET 0


/*
 * Mark instances of "magic" constants to be reviewed later.
 */
#define XXX_MAGIC_CONSTANT(n) (n)

#define ELEMENTS(var) (sizeof (var) / sizeof (*var))

#define eprintf(fmt, ...) \
    fprintf(stderr, fmt, ## __VA_ARGS__)

#define explain_error(fmt, ...)                                                \
    ({                                                                         \
        if (mcp->mcc_flags & TCP_EXPLAIN) {                                    \
            fprintf(stderr, fmt, ## __VA_ARGS__);                              \
        }                                                                      \
    })                                                                         \


/*
 * Macros for printing `size_t'
 *
 * Things that should have been defined in inttypes.h, but weren't
 */
#define __PRISIZE_PREFIX __PRIPTR_PREFIX
#define PRIuSIZE __PRISIZE_PREFIX "u"
#define PRIxSIZE __PRISIZE_PREFIX "x"
#define PRIXSIZE __PRISIZE_PREFIX "X"
#define PRIdSIZE __PRISIZE_PREFIX "d"

#define HTON_COPY(lval, rval)                                                  \
    ({                                                                         \
        if (sizeof (lval) != sizeof (rval)) {                                  \
            eprintf("HTON_COPY: size mismatch\\n");                            \
            plat_abort();                                                      \
        } else if (sizeof (rval) == sizeof (short)) {                          \
            (lval) = htons(rval);                                              \
        } else if (sizeof (rval) == sizeof (long)) {                           \
            (lval) = htonl(rval);                                              \
        } else {                                                               \
            eprintf("HTON_COPY: size=%lu, must be %lu or %lu\\n",              \
                sizeof (rval), sizeof (short), sizeof (long));                 \
            plat_abort();                                                      \
        }                                                                      \
    })                                                                         \
  
#define NTOH_COPY(lval, rval)                                                  \
    ({                                                                         \
        if (sizeof (lval) != sizeof (rval)) {                                  \
            eprintf("NTOH_COPY: size mismatch\\n");                            \
            plat_abort();                                                      \
        } else if (sizeof (rval) == sizeof (short)) {                          \
            (lval) = ntohs(rval);                                              \
        } else if (sizeof (rval) == sizeof (long)) {                           \
            (lval) = ntohl(rval);                                              \
        } else {                                                               \
            eprintf("NTOH_COPY: size=%lu, must be %lu or %lu\\n",              \
                sizeof (rval), sizeof (short), sizeof (long));                 \
            plat_abort();                                                      \
        }                                                                      \
    })                                                                         \
  
static const int listen_backlog = 10;

/**
 * @brief Convert ssize_t to int, check for possible overflow.
 */

static int
ssize_to_int(ssize_t sz)
{
    if (sz > __INT_MAX__) {
        eprintf("%s: overflow: size=%" PRIdSIZE " > INT_MAX (%d)\n",
            __func__, sz, __INT_MAX__);
        plat_abort();
    }
    return ((int)sz);
}

/*
 * @brief Wrapper for malloc() that explains errors.
 * Explain any errors if debug flags are enabled.
 * @param func  <IN> name of calling function
 * @param desc  <IN> description of the type of object being allocated
 * @param size  <IN> total size to be allocated
 * @param flags <IN> control whether to explain failure on stderr
 * @return pointer to newly-allocated memory; NULL on failure
 */
static void *
mc_malloc(const char *func, const char *desc, size_t size, int flags)
{
    void *mem;

    mem = malloc(size);
    if (mem == NULL) {
        if (flags & TCP_EXPLAIN) {
            eprintf("%s: Unable to allocate %s (%" PRIuSIZE " bytes);"
                " out of memory\n", func, desc, size);
        }
    }
    return (mem);
}

/*
 * @brief Duplicate a region of memory to allocated memory.
 * Explain any errors if debug flags are enabled.
 * @param func  <IN> name of calling function
 * @param desc  <IN> description of the type of object being allocated
 * @param size  <IN> total size of region to be duplicated
 * @param flags <IN> control whether to explain failure on stderr
 * @return pointer to newly-allocated copy; NULL on failure
 */
static void *
mc_memdup(const char *func, const char *desc, const void *src, size_t size, int flags)
{
    void *dst;

    dst = mc_malloc(func, desc, size, flags);
    if (dst != NULL) {
        memcpy(dst, src, size);
    }
    return (dst);
}

#if XXX_NOTYET

static char *
decode_addr(struct sockaddr *addr)
{
    struct sockaddr_in *ipv4_addr;
    char *addr_str;

    ipv4_addr = (struct sockaddr_in *)addr;
    addr_str = inet_ntoa(ipv4_addr->sin_addr);
    if (addr_str == NULL) {
        /*
         * Never actually return NULL, because this is a decode function
         * which should always return something printable.
         */
        return ("<NULL>");
    }
    return (addr_str);
}
#endif /* XXX_NOTYET */

static char *
decode_endpoint(struct sockaddr *addr)
{
    struct sockaddr_in *ipv4_addr;
    char *addr_str;
    static char buf[64];

    ipv4_addr = (struct sockaddr_in *)addr;
    addr_str = inet_ntoa(ipv4_addr->sin_addr);
    if (addr_str == NULL) {
        /*
         * Never actually return NULL, because this is a decode function,
         * which should always return something printable.
         */
        return ("<NULL>");
    }
    (void) snprintf(buf, sizeof (buf), "%s:%d", addr_str, ntohs(ipv4_addr->sin_port));
    return (buf);
}

static char *
decode_fdset(const fd_set *fds)
{
    static char dcode[128];
    char *p;
    int fd;
    size_t len;

    p = dcode;
    for (fd = 0; fd < FD_SETSIZE; ++fd) {
        if (FD_ISSET(fd, fds)) {
            if (p > dcode) {
                *p++ = ',';
            }
            len = snprintf(p, dcode + sizeof (dcode) - p, "%d", fd);
            p += len;
        }
    }
    *p = '\0';
    return (dcode);
}

static int
smallest_fd(const fd_set *fds, int nfds)
{
    int fd;

    for (fd = 0; fd < nfds; ++fd) {
        if (FD_ISSET(fd, fds)) {
            return (fd);
        }
    }
    return (-1);
}

static void
mc_print_node(const mc_config_t *mcp, int nno)
{
    mc_node_t *nodep;

    if (nno < 0 || nno >= mcp->mcc_size) {
        return;
    }
    eprintf("Node %d:\n", nno);
    nodep = mcp->mcc_nodes[nno];
    if (nodep == NULL) {
        eprintf("    <NULL>\n");
        return;
    }
    eprintf("    host     = %s\n", nodep->mcn_host);
    eprintf("    port     = %u\n", nodep->mcn_port);
    eprintf("    endpoint = %s\n", decode_endpoint(&nodep->mcn_addr));
    eprintf("    sockfd   = %d\n", nodep->mcn_sockfd);
    eprintf("    connfd   = %d\n", nodep->mcn_connfd);
}

static void
mc_print_all(const mc_config_t *mcp)
{
    int nno;

    eprintf("Number of nodes: %d\n", mcp->mcc_size);
    eprintf("My node: %d\n", mcp->mcc_self);
    for (nno = 0; nno < mcp->mcc_size; ++nno) {
        mc_print_node(mcp, nno);
    }
}

static void
syscall_error(mc_node_t *nodep, const char *name)
{
    nodep->mcn_syscall = name;
    nodep->mcn_err = errno;

    if (nodep->mcn_flags & TCP_DEBUG) {
        perror(name);
    }
}

#if XXX_NOTYET

/*
 * Compare two connection endpoints {ip_address, port}
 */
static int
compare_endpoint(struct sockaddr *addr1, struct sockaddr *addr2)
{
    struct sockaddr_in *ipv4_addr1;
    struct sockaddr_in *ipv4_addr2;
    int cmp;

    ipv4_addr1 = (struct sockaddr_in *)addr1;
    ipv4_addr2 = (struct sockaddr_in *)addr2;
    cmp = memcmp(&ipv4_addr1->sin_addr, &ipv4_addr2->sin_addr,
            sizeof (ipv4_addr1->sin_addr));
    if (cmp) {
        return (cmp);
    }

    return (ipv4_addr1->sin_port - ipv4_addr2->sin_port);
}
#endif /* XXX_NOTYET */

static int
mc_init_server(const mc_config_t *mcp)
{
    mc_node_t *nodep;
    int sockfd;
    int ret;
    int yes = 1;
    struct sockaddr_in my_addr;

    nodep = mcp->mcc_nodes[mcp->mcc_self];
    sockfd = socket(PF_INET, SOCK_STREAM, 0);
    nodep->mcn_sockfd = sockfd;
    if (sockfd < 0) {
        int err = errno;
        syscall_error(nodep, "socket");
        return (-err);
    }

    ret = setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(int));
    if (ret == -1) {
        int err = errno;
        syscall_error(nodep, "setsockopt");
        (void) close(sockfd);
        return (-err);
    }

    my_addr.sin_family = AF_INET;
    my_addr.sin_port = htons(nodep->mcn_port);
    my_addr.sin_addr.s_addr = htonl(INADDR_ANY); // fill with my IP
    memset(my_addr.sin_zero, 0, sizeof (my_addr.sin_zero));

    ret = bind(sockfd, (struct sockaddr *)&my_addr, sizeof (my_addr));
    if (ret == -1) {
        int err = errno;
        syscall_error(nodep, "bind");
        (void) close(sockfd);
        return (-err);
    }

    ret = listen(sockfd, listen_backlog);
    if (ret == -1) {
        int err = errno;
        syscall_error(nodep, "listen");
        (void) close(sockfd);
        return (-err);
    }

    memcpy(&nodep->mcn_addr, &my_addr, sizeof (nodep->mcn_addr));
    return (0);
}

static int
mc_init_client(const mc_config_t *mcp, int nno)
{
    mc_node_t *nodep;
    struct hostent *hent;
    struct sockaddr_in server_addr;
    struct sockaddr peer_addr;
    socklen_t peer_addr_size;
    int sockfd;
    int ret;
    char rank_msg[32];
    size_t rank_msg_len;

    nodep = mcp->mcc_nodes[nno];
    hent = gethostbyname(nodep->mcn_host);
    if (hent == NULL) {
        nodep->mcn_syscall = "gethostbyname";
        nodep->mcn_err = h_errno;
        return (-1);
    }

    sockfd = socket(PF_INET, SOCK_STREAM, 0);
    nodep->mcn_sockfd = sockfd;
    if (sockfd < 0) {
        int err = errno;
        syscall_error(nodep, "socket");
        return (-err);
    }

    server_addr.sin_family = AF_INET;
    server_addr.sin_addr = *((struct in_addr *)hent->h_addr);
    server_addr.sin_port = htons(nodep->mcn_port);
    memset(server_addr.sin_zero, 0, sizeof (server_addr.sin_zero));

    for (;;) {
        eprintf("connect(node=%u, %s)\n", nno,
            decode_endpoint((struct sockaddr *)&server_addr));
        ret = connect(sockfd, (struct sockaddr *)&server_addr, sizeof (server_addr));
        if (ret == -1) {
            if (errno == ECONNREFUSED) {
                eprintf("Trying to connect to node %u\n", nno);
                (void) sleep(2);
                continue;
            } else {
                int err = errno;
                syscall_error(nodep, "connect");
                (void) close(sockfd);
                return (-err);
            }
        }
        break;
    }

    peer_addr_size = sizeof (peer_addr);
    ret = getpeername(sockfd, &peer_addr, &peer_addr_size);
    if (ret < 0) {
        int err = errno;
        syscall_error(nodep, "getpeername");
        return (-err);
    }
    eprintf("connected to %s\n", decode_endpoint(&peer_addr));

    memcpy(&nodep->mcn_addr, &server_addr, sizeof (nodep->mcn_addr));

    /*
     * Initial handshake.
     * Send my rank to the server.
     */
    rank_msg_len = snprintf(rank_msg, sizeof (rank_msg), "rank=%03d\n", mcp->mcc_self);
    ret = write(sockfd, rank_msg, rank_msg_len + 1);
    return (0);
}

/*
 * Accept connections from all my neighbors. Take connection requests,
 * as they arrive.
 *
 * When all n-1 neighbors are accounted for, then we are done listening.
 */

static int
mc_start_server(const mc_config_t *mcp)
{
    mc_node_t *nodep;
    mc_node_t *client_nodep;
    int sockfd;
    int connfd;
    struct sockaddr client_addr;
    struct sockaddr peer_addr;
    socklen_t client_addr_size;
    socklen_t peer_addr_size;
    int client_nno;
    int count;
    int ret;
    char rank_msg[32];
    ssize_t read_len;
    ssize_t expect_len;
    long n;
    char *end;

    nodep = mcp->mcc_nodes[mcp->mcc_self];
    count = mcp->mcc_size - 1;
    while (count != 0) {
        sockfd = nodep->mcn_sockfd;
        client_addr_size = sizeof (client_addr);
        connfd = accept(sockfd, &client_addr, &client_addr_size);
        if (connfd == -1) {
            int err = errno;
            syscall_error(nodep, "accept");
            return (-err);
        }
        peer_addr_size = sizeof (peer_addr);
        ret = getpeername(connfd, &peer_addr, &peer_addr_size);
        if (ret < 0) {
            int err = errno;
            syscall_error(nodep, "getpeername");
            return (-err);
        }

        eprintf("client addr = %s\n", decode_endpoint(&client_addr));
        eprintf("peer   addr = %s\n", decode_endpoint(&peer_addr));

        expect_len = sizeof ("rank=nnn\n");
        read_len = read(connfd, rank_msg, expect_len);
        if (read_len < 0) {
            int err = errno;
            syscall_error(nodep, "read");
            return (-err);
        }
        if (read_len != expect_len) {
            if (mcp->mcc_flags & TCP_EXPLAIN) {
                eprintf("%s: read: expect length=%" PRIdSIZE
                    ", got %" PRIdSIZE "\n",
                    __func__, expect_len, read_len);
                eprintf("  = [%s]\n", rank_msg);
            }
            return (-EINVAL);
        }
        if (memcmp(rank_msg, "rank=", 5) != 0) {
            if (mcp->mcc_flags & TCP_EXPLAIN) {
                eprintf("%s: expect handshake of the form, 'rank='\n",
                    __func__);
            }
            return (-EINVAL);
        }
        n = strtol(rank_msg + 5, &end, 10);
        if (*end != '\n') {
            if (mcp->mcc_flags & TCP_EXPLAIN) {
                eprintf("%s: client rank='%s'; must be numeric\n",
                    __func__, rank_msg + 5);
            }
            return (-EINVAL);
        }
        if (n < 0 || n >= mcp->mcc_size) {
            if (mcp->mcc_flags & TCP_EXPLAIN) {
                eprintf("%s: client rank=%ld, but there are %d nodes total\n",
                    __func__, n, mcp->mcc_size);
            }
            return (-ERANGE);
        }
        client_nno = (int)n;

        if (mcp->mcc_flags & TCP_DEBUG) {
            eprintf("%s: server: got connection from node %d @ %s\n",
                __func__, client_nno, decode_endpoint(&peer_addr));
        }
        client_nodep = mcp->mcc_nodes[client_nno];
        client_nodep->mcn_addr = client_addr;
        client_nodep->mcn_connfd = connfd;
        --count;
    }

    /*
     * XXX Do not need to listen for connections, anymore.
     * XXX close(mcp->mcc_nodes[mcp->mcc_self]->mcn_sockfd);
     */
    return (0);
}

/**
 * @brief 
 *
 * XXX Count unique IP addresses, instead.
 * XXX because hostnames can be different aliases for the same IP address.
 */
static int
count_unique_hosts(const mc_config_t *mcp)
{
    int unique_hosts;
    int n, i, j, diff;

    unique_hosts = 0;
    n = mcp->mcc_size;
    for (i = 0; i < n; ++i) {
        diff = 0;
        for (j = 0; j < i; ++j) {
            diff = strcmp(mcp->mcc_nodes[i]->mcn_host, mcp->mcc_nodes[j]->mcn_host);
            if (diff == 0) {
                break;
            }
        }
        if (diff != 0) {
            ++unique_hosts;
        }
    }
    return (unique_hosts);
}

/**
 * @brief Read config file and create config data structure
 * and per node data structures.
 */
static int
tcp_read_config(mc_config_t **mc_config_ref, FILE *cf, const char *conf_fname, int flags)
{
    char buf[XXX_MAGIC_CONSTANT(1024)];
    struct hostent *hent;
    void *mem;
    size_t size;
    char *host;			// receives copy of "official" hostname
    size_t host_size;
    mc_config_t *mcp;
    mc_node_t *nodepv[XXX_MAGIC_CONSTANT(64)];
    mc_node_t *nodep;
    int nodes;

    mem = malloc(sizeof (mc_config_t));
    if (mem == NULL) {
        if (flags & TCP_EXPLAIN) {
            eprintf("%s: Cannot alloc mc_config_t (%lu bytes)\n",
                __func__, sizeof (mc_config_t));
        }
        return (-ENOMEM);
    }
    mcp = (mc_config_t *)mem;
    *mc_config_ref = mcp;
    memset(mcp, 0, sizeof (mc_config_t));
    mcp->mcc_magic = MCC_MAGIC;
    mcp->mcc_flags = flags;

    nodes = 0;
    while (fgets(buf, sizeof (buf), cf)) {
        char *s;

        // Scan for begin comment
        for (s = buf; *s != '\0' && *s != '#'; ++s) {
            // Empty
        }
        if (*s == '#') {
            if (s == buf) {
                // This line is pure comment -- ignore it.
                continue;
            }
            *s = '\0';
        }
        while (s > buf && (s[-1] == ' ' || s[-1] == '\t' || s[-1] == '\n')) {
            --s;
            *s = '\0';
        }
        if (s == buf) {
            // This line is nothing more than comments and/or whitespace.
            continue;
        }
        hent = gethostbyname(buf);
        if (hent == NULL) {
            int err;

            err = h_errno;
            if (flags & TCP_EXPLAIN) {
                eprintf("%s: gethostbyname(%s) failed; %d %s\n",
                    __func__, buf, err, hstrerror(err));
            }
	    // XXX Record errors in mc_config (top level) rather than per node.
	    // XXX Record node number (if applicable).
	    // XXX Record error type = { syscall, host, internal }
	    // XXX mcp->mcc_errno = err;
            return (-1024 + err);
        }
        host_size = strlen(hent->h_name) + 1;
        mem = malloc(sizeof (mc_node_t) + host_size);
        if (mem == NULL) {
            if (flags & TCP_EXPLAIN) {
                eprintf("%s: Could not alloc node info (%lu bytes)\n",
                    __func__, sizeof (mc_node_t) + host_size);
            }
            return (-ENOMEM);
        }
        nodep = (mc_node_t *)mem;
        memset(nodep, 0, sizeof (mc_node_t));
        nodep->mcn_magic = MCN_MAGIC;
        host = (char *)mem + sizeof (mc_node_t);
        strncpy(host, hent->h_name, host_size);
        nodep->mcn_host = host;
        nodep->mcn_port = 3000 + nodes;
        nodep->mcn_sockfd = -1;
        nodep->mcn_connfd = -1;
        nodepv[nodes] = nodep;
        ++nodes;
    }

    if (nodes == 0) {
        eprintf("No entries in configuration file, '%s'.\n", conf_fname);
	return (-ENOENT);
    }

    size = nodes * sizeof (mc_node_t *);
    mem = mc_memdup(__func__, "node pointers", nodepv, size, flags);
    if (mem == NULL) {
        return (-ENOMEM);
    }
    mcp->mcc_nodes = (mc_node_t **)mem;
    mcp->mcc_size = nodes;
    mcp->mcc_hosts = count_unique_hosts(mcp);
    return (0);
}

/**
 *
 * Manage file open and close, here; call tcp_read_config() to parse stream.
 * That way tcp_read_config() can bail out quickly on most errors, leaving
 * us to cleanup FILE handle(s) and memory.
 */

static int
tcp_config_init(mc_config_t **mc_config_ref, const char *conf_fname, int flags)
{
    FILE *cf;
    int ret_cf, ret, err;

    cf = fopen(conf_fname, "r");
    if (cf == NULL) {
        err = errno;
        if (flags & TCP_EXPLAIN) {
            eprintf("%s: fopen(%s, \"r\") failed; %d %s\n",
                __func__, conf_fname, err, plat_strerror(err));
        }
        return (-err);
    }

    ret_cf = tcp_read_config(mc_config_ref, cf, conf_fname, flags);

    ret = fclose(cf);
    if (ret != 0) {
        err = errno;
        if (flags & TCP_EXPLAIN) {
            eprintf("%s: fclose(%s) failed; %d %s\n",
                __func__, conf_fname, err, plat_strerror(err));
        }
        return (-err);
    }
    return (ret_cf);
}

/**
 * @brief Initialize
 * @param tcpref -- Set to a new TCP *.
 *    On early failure *tcpref is NULL, but *tcpref can be non-NULL, even
 *    on failure, in order to retain information about any errors
 *    that were encountered.
 *
 * @param flags  -- control debugging trace messages and failure messages.
 * @return 0 on success -errno on failure.
 * *tcpref is an opaque descriptor for a new TCP connection.
 */
int
tcp_init(TCP **tcpref, int flags)
{
    mc_config_t *mcp;
    int nno;
    int ret;
    const char *env_str;
    const char *default_conf_file = "test1.conf";
    const char *config_fname;
    const char *rank_str;

    env_str = getenv("TCP_CONFIG_FILE");
    if (env_str == NULL) {
        if (flags & TCP_EXPLAIN) {
            eprintf("%s: environment variable was not set, TCP_CONFIG_FILE... use hardcoded default\n",
                __func__);
        }
        env_str = default_conf_file;
//        return (-EINVAL);
    }
    printf("%s: default is single node lab02.schoonerinfotech.net, using local file %s!\n",
	   __func__, env_str);
    config_fname = env_str;

    ret = tcp_config_init(&mcp, config_fname, flags);
    *tcpref = (TCP *)mcp;

    if (ret < 0) {
        return (ret);
    }

    env_str = getenv("TCP_RANK");
    if (env_str == NULL) {
        eprintf("%s: environment variable was not set, TCP_RANK\n", __func__);
        return (-EINVAL);
    }

    rank_str = env_str;
    mcp->mcc_self = atoi(rank_str);

    if (mcp->mcc_self < 0 || mcp->mcc_self >= mcp->mcc_size) {
        if (flags & TCP_EXPLAIN) {
            eprintf("TCP_RANK out of range.\n");
            eprintf("Config has %d nodes, TCP_RANK=%d\n",
                mcp->mcc_size, mcp->mcc_self);
        }
        return (-ERANGE);
    }

    /*
     * Establish myself as a server.
     */
    ret = mc_init_server(mcp);
    if (ret < 0) {
        return (ret);
    }

    /*
     * Establish my role as a client of all of my peers in this "cluster".
     */
    for (nno = 0; nno < mcp->mcc_size; ++nno) {
        if (nno == mcp->mcc_self) {
            continue;
        }
        ret = mc_init_client(mcp, nno);
        if (ret < 0) {
            return (ret);
        }
    }

    ret = mc_start_server(mcp);
    if (ret < 0) {
        return (ret);
    }

    if (flags & TCP_DEBUG) {
        mc_print_all(mcp);
    }

    return (0);
}

int
tcp_node_count(TCP *tcp)
{
    mc_config_t *mcp;

    mcp = (mc_config_t *)tcp;
    if (mcp->mcc_magic != MCC_MAGIC) {
        return (-EFAULT);
    }
    return (mcp->mcc_size);
}

int
tcp_me(TCP *tcp)
{
    mc_config_t *mcp;

    mcp = (mc_config_t *)tcp;
    if (mcp->mcc_magic != MCC_MAGIC) {
        return (-EFAULT);
    }

    return (mcp->mcc_self);
}

int
tcp_send_fd(TCP *tcp, int nno)
{
    mc_config_t *mcp;

    mcp = (mc_config_t *)tcp;
    if (mcp->mcc_magic != MCC_MAGIC) {
        explain_error("%s: bad magic\n", __func__);
        return (-EFAULT);
    }

    if (nno < 0 || nno >= mcp->mcc_size) {
        explain_error("%s: nno=%d\n", __func__, nno);
        return (-ENOENT);
    }

    return (mcp->mcc_nodes[nno]->mcn_sockfd);
}

int
tcp_recv_fd(TCP *tcp, int nno)
{
    mc_config_t *mcp;

    mcp = (mc_config_t *)tcp;
    if (mcp->mcc_magic != MCC_MAGIC) {
        explain_error("%s: bad magic\n", __func__);
        return (-EFAULT);
    }

    if (nno < 0 || nno >= mcp->mcc_size) {
        explain_error("%s: nno=%d\n", __func__, nno);
        return (-ENOENT);
    }

    return (mcp->mcc_nodes[nno]->mcn_connfd);
}

/**
 *
 */

/*
 * Send a message.
 */
int
tcp_send(TCP *tcp, SMSG *smsg, SFUNC *func, void *arg)
{
    mc_config_t *mcp;
    mc_node_t *nodep;
    preamble_t preamble;
    int count;
    size_t size;
    ssize_t wsize;
    int i;
    RANK srank;
    int fd;

    mcp = (mc_config_t *)tcp;
    if (mcp->mcc_magic != MCC_MAGIC) {
        return (-EFAULT);
    }
    srank = mcp->mcc_self;
    nodep = mcp->mcc_nodes[srank];

    /*
     * Add up the sizes of all scatter/gather entries.
     */
    size = 0;
    for (i = 0; i < MSG_SGE; ++i) {
        if (smsg->sgv[i].iov_base == NULL) {
            break;
        }
        size += smsg->sgv[i].iov_len;
    }
    count = i;

    /*
     * Send all data over the wire in network byte order.
     */
    HTON_COPY(preamble.stag, smsg->stag);
    HTON_COPY(preamble.dtag, smsg->dtag);
    HTON_COPY(preamble.srank, srank);
    HTON_COPY(preamble.size, size);

    fd = tcp_send_fd(tcp, smsg->drank);
    if (fd < 0) {
        return (fd);
    }

    /*
     * Write preamble.
     */
    wsize = write(fd, &preamble, sizeof (preamble));
    if (wsize < 0) {
        int err = errno;
        syscall_error(nodep, "write");
        return (-err);
    }
    if (wsize != sizeof (preamble)) {
        explain_error("write of preamble: expect %lu" PRIuSIZE " bytes, got %lu" PRIdSIZE "\n",
            (size_t)(sizeof (preamble)), wsize);
        return (-1);
    }

    wsize = writev(fd, &smsg->sgv[0], count);
    if (wsize < 0) {
        int err = errno;
        syscall_error(nodep, "writev");
        return (-err);
    }

    if (func != NULL) {
        (*func)(smsg, arg);
    }
    return (ssize_to_int(wsize));
}

/**
 * @brief Receive a message; do not consult queue of already received messages.
 * @param tcp <IN>
 * @param rmsg <IN>
 * @param timeout
 * @return 0 on success, -errno on failure.
 */
int
tcp_recv_nq(TCP *tcp, RMSG *rmsg, long timeout)
{
    fd_set read_fds;
    int max_fd;
    mc_config_t *mcp;
    mc_node_t *nodep;
    int nno;
    struct timeval tv;
    struct timeval *tvp;
    int fd;
    preamble_t preamble;
    void *iobuf;
    size_t size;
    ssize_t read_len;
    int ret;

    mcp = (mc_config_t *)tcp;
    if (mcp->mcc_magic != MCC_MAGIC) {
        return (-EFAULT);
    }
    /*
     * build read_fds from all other nodes
     */
    FD_ZERO(&read_fds);
    max_fd = 0;
    for (nno = 0; nno < mcp->mcc_size; ++nno) {
        int rfd;

        if (nno == mcp->mcc_self) {
            continue;
        }
        nodep = mcp->mcc_nodes[nno];
        rfd = nodep->mcn_connfd;
        FD_SET(rfd, &read_fds);
        if (rfd > max_fd) {
            max_fd = rfd;
        }
    }

eprintf("%s: read_fds = {%s}\n", __func__, decode_fdset(&read_fds));

    if (timeout == -1L) {
        tvp = NULL;
    } else {
        long sec;

        sec = timeout / 1000000;
        tv.tv_sec = sec;
        tv.tv_usec = timeout - (sec * 1000000);
        tvp = &tv;
    }

    nodep = mcp->mcc_nodes[mcp->mcc_self];

    ret = select(max_fd + 1, &read_fds, NULL, NULL, tvp);
    if (ret < 0) {
        int err = errno;
        syscall_error(nodep, "select");
        return (-err);
    }

    fd = smallest_fd(&read_fds, max_fd + 1);

    read_len = read(fd, &preamble, sizeof (preamble));
    if (read_len < 0) {
        int err = errno;
        syscall_error(nodep, "read");
        return (-err);
    }

    if (read_len != sizeof (preamble)) {
        explain_error("read of preamble: expect length=%" PRIuSIZE ", got %" PRIdSIZE "\n",
            (size_t)(sizeof (preamble)), read_len);
        return (-EINVAL);
    }

    NTOH_COPY(size, preamble.size);

    iobuf = mc_malloc(__func__, "iobuf", size, mcp->mcc_flags);
    if (iobuf == NULL) {
        return (-ENOMEM);
    }
    read_len = read(fd, iobuf, size);
    if (read_len < 0) {
        int err = errno;
        free(iobuf);
        syscall_error(nodep, "read");
        return (-err);
    }

    NTOH_COPY(rmsg->stag, preamble.stag);
    NTOH_COPY(rmsg->dtag, preamble.dtag);
    NTOH_COPY(rmsg->srank, preamble.srank);
    rmsg->buf = iobuf;
    rmsg->len = size;
    return (0);
}

/**
 * @brief Receive a message.  Consult queue of already received messages.
 * @param tcp <IN>
 * @param rmsg <IN>
 * @param timeout
 * @return 0 on success, -errno on failure.
 */
int
tcp_recv(TCP *tcp, RMSG *rmsg, long timeout)
{
    mc_config_t *mcp;
    MSGQ *dq_rmsg;

    mcp = (mc_config_t *)tcp;
    if (mcp->mcc_magic != MCC_MAGIC) {
        return (-EFAULT);
    }

    dq_rmsg = mcp->mcc_rcvq;
    if (dq_rmsg) {
        mcp->mcc_rcvq = dq_rmsg->mq_next;
        *rmsg = dq_rmsg->mq_rmsg;
        return (0);
    }

    return (tcp_recv_nq(tcp, rmsg, timeout));
}


/**
 * @brief >>>
 */

int
tcp_recv_tag(TCP *tcp, RMSG *rmsg, TAG tag, long timeout)
{
    mc_config_t *mcp;
    MSGQ *dq_rmsg, *prev_rmsg, *new_msgent;
    int ret;

    mcp = (mc_config_t *)tcp;
    if (mcp->mcc_magic != MCC_MAGIC) {
        return (-EFAULT);
    }

    dq_rmsg = mcp->mcc_rcvq;
    if (dq_rmsg) {
        prev_rmsg = NULL;
        while (dq_rmsg != NULL) {
            if (dq_rmsg->mq_rmsg.dtag == tag) {
                if (prev_rmsg) {
                    prev_rmsg->mq_next = dq_rmsg->mq_next;
                } else {
                    mcp->mcc_rcvq = dq_rmsg->mq_next;
                }
                *rmsg = dq_rmsg->mq_rmsg;
                free(dq_rmsg);
                return (0);
            }
            prev_rmsg = dq_rmsg;
            dq_rmsg = dq_rmsg->mq_next;
        }
    }

    /*
     * No message matching @param{tag} was found.
     * Read fresh messages from the network.
     */
    for (;;) {
        ret = tcp_recv_nq(tcp, rmsg, timeout);
        if (ret < 0) {
            return (ret);
        }
        if (rmsg->dtag == tag) {
            // This messages matches the tag of interest.  Deliver it.
            return (ret);
        }

        /*
         * We received a new message, but it does not have the tag of interest.
         * So, place it on the queue of received messages.
         */
        new_msgent = (MSGQ *)malloc(sizeof (MSGQ));
        new_msgent->mq_next = NULL;
        new_msgent->mq_rmsg = *rmsg;

        dq_rmsg = mcp->mcc_rcvq;
        if (dq_rmsg) {
            /*
             * We must maintain queued messages in order.
             * So, move to end of list.
             */
            while (dq_rmsg->mq_next != NULL) {
                dq_rmsg = dq_rmsg->mq_next;
            }
            dq_rmsg->mq_next = new_msgent;
        } else {
            mcp->mcc_rcvq = new_msgent;
        }
    }
}

/**
 * @brief Shutdown and destroy a single node.
 *
 * Free all file descriptors.
 * Release all allocated memory.
 *
 * Note: mcn_host is allocated along with the mc_node_t itself,
 * in a single malloc().  Do not try to free it separately.
 */
static int
mc_free_node(mc_node_t *nodep)
{
    if (nodep->mcn_sockfd >= 0) {
        (void) close(nodep->mcn_sockfd);
    }
    if (nodep->mcn_connfd >= 0) {
        (void) close(nodep->mcn_connfd);
    }
    free(nodep);
    return (0);
}

/**
 * @brief Free all nodes, then free top-level config data.
 */
static int
mc_free_all(mc_config_t *mcp)
{
    MSGQ *qmsg, *next_qmsg;
    int nno;

    for (nno = 0; nno < mcp->mcc_size; ++nno) {
        mc_node_t *nodep;
        int ret;

        nodep = mcp->mcc_nodes[nno];
        ret = mc_free_node(nodep);
    }

    free(mcp->mcc_nodes);

    /*
     * Free any queued messages.  They just get lost.
     */
    qmsg = mcp->mcc_rcvq;
    while (qmsg) {
        next_qmsg = qmsg->mq_next;
        free(qmsg);
        qmsg = next_qmsg;
    }

    free(mcp);
    return (0);
}

/**
 * @brief Shutdown a TCP connection.
 * Release resources (open file descriptors, memory).
 *
 * @param tcp <IN>
 * @return 0 on success, -errno on failure
 */
int
tcp_exit(TCP *tcp)
{
    mc_config_t *mcp;

    mcp = (mc_config_t *)tcp;
    if (mcp->mcc_magic != MCC_MAGIC) {
        return (-EFAULT);
    }
    return (mc_free_all(mcp));
}

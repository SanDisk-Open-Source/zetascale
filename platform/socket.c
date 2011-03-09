/*
 * File:   $HeadURL: svn://svn.schoonerinfotech.net/schooner-trunk/trunk/sdf/platform/socket.c $
 * Author: drew
 *
 * Created on January 25, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: socket.c 631 2008-03-17 22:58:28Z tomr $
 */

/*
 * Thin wrappers for unix functions to accomodate interception for
 * 1) Blocking behavior in user-scheduled threads
 * 2) The simulated cluster environment
 *
 * Simulated interface implementation stages:
 * 1.  Namespace translation. 
 * 
 * 2.  File descriptor tracking/remapping: close on termination,
 *     hide process namespaces from each other.
 *
 * 3.  Random delay & failure injection (can make socket pairs be socket
 *     quads end 1 middle 1 middle 2 end 2 where ends are exposed
 *     and middle has a copy).  SOCK_STREAM can have data broken on 
 *     any byte boundary.  SOCK_DGRAM can usually (except AF_UNIX)
 *     drop packets.  
 *
 * 4.  More interesting failures - ECONNRESET, etc.
 *     
 */

#include <sys/socket.h>
#include <sys/types.h>

#define PLATFORM_SOCKET_C 1

#include "platform/socket.h"

int
plat_accept(int s, struct sockaddr *addr,
    socklen_t *addrlen) {
    return (accept(s, addr, addrlen));
}

int
plat_bind(int s, const struct sockaddr *my_addr, socklen_t addrlen) {
    return (bind(s, my_addr, addrlen));
}

int
plat_connect(int s, const struct sockaddr *serv_addr, socklen_t
  addrlen) {
  return (connect(s, serv_addr, addrlen));
}

int
plat_listen(int s, int backlog) {
    return (listen(s, backlog));
}

int
plat_recv(int s, void *buf, size_t len, int flags) {
   return (recv(s, buf, len, flags));
}

int
plat_recvfrom(int s, void *buf, size_t len, int flags, struct sockaddr
    *from, socklen_t *fromlen) {
    return (recvfrom(s, buf, len, flags, from, fromlen));
}

int
plat_recvmsg(int s, struct msghdr *msg, int flags) {
    return (recvmsg(s, msg, flags));
}

int
plat_send(int s, const void *msg, size_t len, int flags) {
    return (send(s, msg, len, flags));
}

int
plat_sendmsg(int s, const struct msghdr *msg, int flags) {
    return (sendmsg(s, msg, flags));
}

int
plat_sendto(int s, const void *msg, size_t len, int flags, const struct
    sockaddr *to, socklen_t tolen) {
    return (sendto(s, msg, len, flags, to, tolen));
}

int
plat_socket(int domain, int type, int protocol) {
    return (socket(domain, type, protocol));
}

int
plat_socketpair(int domain, int type, int protocol, int sv[2]) {
    return (socketpair(domain, type, protocol, sv));
}

int 
plat_shutdown(int s, int how) {
    return (shutdown(s, how));
}

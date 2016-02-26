/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

#ifndef PLATFORM_SOCKET_H
#define PLATFORM_SOCKET_H 1

/*
 * File:   platform/socket.h
 * Author: drew
 *
 * Created on January 25, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: socket.h 208 2008-02-08 01:12:24Z drew $
 */

/*
 * Thin wrappers for unix socket functions to accomodate interception for
 * 1) Blocking behavior in user-scheduled threads
 * 2) The simulated cluster environment
 */

#include <sys/cdefs.h>

/*
 * Hide functions which have been (or should be) replaced, producing 
 * compile time warnings which become errors with -Werror.  Where
 * multiple 'C' source in platform must use a system function 
 * multple defined() clauses can be used.
 */
#ifndef PLATFORM_SOCKET_C
#define accept accept_use_plat
#define bind bind_use_plat
#define connect connect_use_plat
#define listen listen_use_plat
#define recv recv_use_plat
#define recvfrom recvfrom_use_plat
#define recvmsg recvmsg_use_plat
#define send send_use_plat
#define sendmsg sendmsg_use_plat
#define sendto sendto_use_plat
#define socket socket_use_plat
#define socketpair socketpair_use_plat
#define shutdown shutdown_use_plat
#endif 

#include <sys/socket.h>

/*
 * Undef to avoid collisions with with struct field names and local 
 * functions.
 */
#ifndef PLATFORM_SOCKET_C
#undef accept
#undef bind
#undef connect
#undef listen
#undef recv
#undef recvfrom
#undef recvmsg
#undef send
#undef sendmsg
#undef sendto
#undef socket
#undef socketpair
#undef shutdown
#endif

#include <sys/types.h>


__BEGIN_DECLS

int plat_accept(int s, struct sockaddr *addr, socklen_t *addrlen);

int plat_bind(int s, const struct sockaddr *my_addr, socklen_t addrlen);

int plat_connect(int s, const struct sockaddr *serv_addr, socklen_t
  addrlen);

int plat_listen(int s, int backlog);

int plat_recv(int s, void *buf, size_t len, int flags);

int plat_recvfrom(int s, void *buf, size_t len, int flags, struct sockaddr
    *from, socklen_t *fromlen);

int plat_recvmsg(int s, struct msghdr *msg, int flags);

int plat_send(int s, const void *msg, size_t len, int flags);

int plat_sendmsg(int s, const struct msghdr *msg, int flags);

int plat_sendto(int s, const void *msg, size_t len, int flags, const struct
    sockaddr *to, socklen_t tolen);

int plat_socket(int domain, int type, int protocol);

int plat_socketpair(int domain, int type, int protocol, int sv[2]);

int plat_shutdown(int s, int how);

__END_DECLS

#endif /* ndef PLATFORM_SOCKET_H */

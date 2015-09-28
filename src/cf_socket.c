/*
 * A good, basic C client for the Aerospike protocol
 * Creates a library which is linkable into a variety of systems
 *
 * First attempt is a very simple non-threaded blocking interface
 * currently coded to C99 - in our tree, GCC 4.2 and 4.3 are used
 *
 * Brian Bulkowski, 2009
 * All rights reserved
 */

#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include "citrusleaf/cf_byte_order.h"
#include "citrusleaf/cf_clock.h"
#include "citrusleaf/cf_errno.h"
#include "citrusleaf/cf_log_internal.h"

#include "citrusleaf/cf_socket.h"


#ifndef CF_WINDOWS
//====================================================================
// Linux
//

#include <fcntl.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/epoll.h>
#include <sys/socket.h>

#define STACK_LIMIT (16 * 1024)

// #define DEBUG_TIME


#ifdef DEBUG_TIME
#include <pthread.h>
static void debug_time_printf(const char* desc, int try, int busy, uint64_t start, uint64_t end, uint64_t deadline)
{
	cf_info("%s|%zu|%d|%d|%lu|%lu|%lu",
		desc, (uint64_t)pthread_self(),
		try,
		busy,
		start,
		end,
		deadline
	);
}
#endif


int
cf_socket_create_nb()
{
	// Create the socket.
	int fd = socket(AF_INET, SOCK_STREAM, 0);

	if (-1 == fd) {
		cf_warn("could not allocate socket, errno %d", errno);
		return -1;
	}

    // Make the socket nonblocking.
	int flags = fcntl(fd, F_GETFL, 0);

    if (flags < 0) {
		cf_warn("could not read socket flags");
		close(fd);
        return -1;
	}

    if (fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0) {
		cf_warn("could not set socket nonblocking");
		close(fd);
        return -1;
	}

	int f = 1;
	setsockopt(fd, SOL_TCP, TCP_NODELAY, &f, sizeof(f));

	return fd;
}

//
// There is a conflict even among various versions of
// linux, because it's common to compile kernels - or set ulimits -
// where the FD_SETSIZE is much greater than a compiled version.
// Thus, we can compute the required size of the fdset and use a reasonable size
// the other option is using epoll, which is a little scary for this kind of
// "I just want a timeout" usage.
//
// The reality is 8 bits per byte, but this calculation is a little more general
//
static inline size_t
cf_fdset_size(int fd)
{
	// Roundup fd in increments of FD_SETSIZE and convert to bytes.
	return ((fd / FD_SETSIZE) + 1) * FD_SETSIZE / 8;
}

// From glibc-2.15 (ubuntu 12.0.4+), the FD_* functions has a check on the 
// number of fds passed. According to the man page of FD_SET, the behavior
// is undefined for fd > 1024. But this is enforced from glibc-2.15
// https://sourceware.org/bugzilla/show_bug.cgi?id=10352
//
// So, manipulate the base and the offset of the fd in it
// 
static inline void
cf_fd_set(int fd, fd_set *fdset)
{
	FD_SET(fd%FD_SETSIZE, &fdset[fd/FD_SETSIZE]);
}

static inline int
cf_fd_isset(int fd, fd_set *fdset)
{
	return FD_ISSET(fd%FD_SETSIZE, &fdset[fd/FD_SETSIZE]);
}

//
// Network socket helpers
// Often, you know the amount you want to read, and you have a timeout.
// Do the simple of wrapping in a select so we can read or write with timout
//
// There are two timeouts: the total deadline for the transaction,
// and the maximum time before making progress on a connection which
// we consider a failure so we can flip over to another node that might be healthier.
//
// Return the error number, not the number of bytes.
//
// there are two timeouts: the timeout for this attempt, and the deadline
// for the entire transaction.
//
int
cf_socket_read_timeout(int fd, uint8_t *buf, size_t buf_len, uint64_t trans_deadline, int attempt_ms)
{
#ifdef DEBUG_TIME
	uint64_t start = cf_getms();
#endif
	struct timeval tv;
	size_t pos = 0;

	int flags;
	if (-1 == (flags = fcntl(fd, F_GETFL, 0)))
		flags = 0;
	if (! (flags & O_NONBLOCK)) {
		if (-1 == fcntl(fd, F_SETFL, flags | O_NONBLOCK)) {
			return(EBADF);
		}
	}

	// between the transaction deadline and the attempt_ms, find the lesser
	// and create a deadline for this attempt
	uint64_t deadline = attempt_ms + cf_getms();
    if ((trans_deadline != 0) && (trans_deadline < deadline))
		deadline = trans_deadline;

	// Setup fdset. This looks weird, but there's no guarantee that compiled
	// FD_SETSIZE has much to do with the machine we're running on.
	size_t rset_size = cf_fdset_size(fd);
	fd_set* rset = (fd_set*)(rset_size > STACK_LIMIT ? malloc(rset_size) : alloca(rset_size));

	if (!rset) {
		cf_warn("fdset allocation error: %d", rset_size);
		return -1;
	}

	int rv = 0;
	int try = 0;
#ifdef DEBUG_TIME
	int select_busy = 0;
#endif

	do {
		uint64_t now = cf_getms();
		if (now > deadline) {
#ifdef DEBUG_TIME
			debug_time_printf("socket readselect timeout", try, select_busy, start, now, deadline);
#endif
			rv = ETIMEDOUT;
			goto Out;
        }

		uint64_t ms_left = deadline - now;
		tv.tv_sec = ms_left / 1000;
		tv.tv_usec = (ms_left % 1000) * 1000;

		memset((void*)rset, 0, rset_size);
		cf_fd_set(fd, rset);
		rv = select(fd +1, rset /*readfd*/, 0 /*writefd*/, 0 /*oobfd*/, &tv);

		// we only have one fd, so we know it's ours?
		if ((rv > 0) && cf_fd_isset(fd, rset)) {

			int r_bytes = (int)read(fd, buf + pos, buf_len - pos );

			if (r_bytes > 0) {
				pos += r_bytes;
            }
			else if (r_bytes == 0) {
				// We believe this means that the server has closed this socket.
				rv = EBADF;
				goto Out;
			}
            else if (errno != ETIMEDOUT
                     // It's apparently possible that select() returns successfully yet
                     // the socket is not ready for reading.
                     && errno != EWOULDBLOCK && errno != EINPROGRESS && errno != EAGAIN) {
#ifdef DEBUG_TIME
    			debug_time_printf("socket read timeout", try, select_busy, start, now, deadline);
#endif
				rv = errno;
				goto Out;
            }
		}
		else if (rv == 0) {
#ifdef DEBUG_TIME
			select_busy++;
#endif
        }
        else {
			if (rv == -1)  {
				rv = errno;
				goto Out;
			}
        }

        try++;

	} while( pos < buf_len );

	rv = 0; // success!

Out:
	if (rset_size > STACK_LIMIT) {
		free(rset);
	}
	return( rv );
}


int
cf_socket_write_timeout(int fd, uint8_t *buf, size_t buf_len, uint64_t trans_deadline, int attempt_ms)
{
#ifdef DEBUG_TIME
	uint64_t start = cf_getms();
#endif
	struct timeval tv;
	size_t pos = 0;

	int flags;
	if (-1 == (flags = fcntl(fd, F_GETFL, 0)))
		flags = 0;
	if (! (flags & O_NONBLOCK)) {
		if (-1 == fcntl(fd, F_SETFL, flags | O_NONBLOCK)) {
			return(ENOENT);
		}
	}

	// between the transaction deadline and the attempt_ms, find the lesser
	// and create a deadline for this attempt
	uint64_t deadline = attempt_ms + cf_getms();
    if ((trans_deadline != 0) && (trans_deadline < deadline))
		deadline = trans_deadline;

	// Setup fdset. This looks weird, but there's no guarantee that compiled
	// FD_SETSIZE has much to do with the machine we're running on.
	size_t wset_size = cf_fdset_size(fd);
	fd_set* wset = (fd_set*)(wset_size > STACK_LIMIT ? malloc(wset_size) : alloca(wset_size));

	if (!wset) {
		cf_warn("fdset allocation error: %d", wset_size);
		return -1;
	}

	int rv = 0;
	int try = 0;
#ifdef DEBUG_TIME
	int select_busy = 0;
#endif

	do {
		uint64_t now = cf_getms();
		if (now > deadline) {
#ifdef DEBUG_TIME
			debug_time_printf("socket writeselect timeout", try, select_busy, start, now, deadline);
#endif
			rv = ETIMEDOUT;
			goto Out;
        }

		uint64_t ms_left = deadline - now;
		tv.tv_sec = ms_left / 1000;
		tv.tv_usec = (ms_left % 1000) * 1000;

		memset((void*)wset, 0, wset_size);
		cf_fd_set(fd, wset);

		rv = select(fd +1, 0 /*readfd*/, wset /*writefd*/, 0/*oobfd*/, &tv);

		// we only have one fd, so we know it's ours, but select seems confused sometimes - do the safest thing
		if ((rv > 0) && cf_fd_isset(fd, wset)) {

			int r_bytes = (int)write(fd, buf + pos, buf_len - pos );

			if (r_bytes > 0) {
				pos += r_bytes;
				if (pos >= buf_len)	{ rv = 0; goto Out; } // done happily
            }
			else if (r_bytes == 0) {
				// We shouldn't see 0 returned unless we try to write 0 bytes, which we don't.
				rv = EBADF;
				goto Out;
			}
            else if (errno != ETIMEDOUT
                     && errno != EWOULDBLOCK && errno != EINPROGRESS && errno != EAGAIN) {
#ifdef DEBUG_TIME
    			debug_time_printf("socket write timeout", try, select_busy, start, now, deadline);
#endif
                rv = errno;
                goto Out;
			}
		}
        else if (rv == 0) {
#ifdef DEBUG_TIME
        	select_busy++;
#endif
    	}
        else {
			if (rv == -1)	{ rv = errno; goto Out; }
        }

		try++;

	} while( pos < buf_len );
    rv = 0;

Out:
	if (wset_size > STACK_LIMIT) {
		free(wset);
	}
	return( rv );
}


//
// These FOREVER calls are only called in the 'getmany' case, which is used
// for application level highly variable queries
//
//


int
cf_socket_read_forever(int fd, uint8_t *buf, size_t buf_len)
{
	// one way is to make sure the fd is blocking, and block
	int flags;
	if (-1 == (flags = fcntl(fd, F_GETFL, 0)))
		flags = 0;
	if (flags & O_NONBLOCK) {
		if (-1 == fcntl(fd, F_SETFL, flags & ~O_NONBLOCK)) {
			return(ENOENT);
		}
	}

	size_t pos = 0;
	do {
		int r_bytes = read(fd, buf + pos, buf_len - pos );
		if (r_bytes < 0) { // don't combine these into one line! think about it!
			if (errno != ETIMEDOUT) {
				return(errno);
			}
		}
		else if (r_bytes == 0) {
			// blocking read returns 0 bytes socket timedout at server side
			// is closed
			return EBADF;
		}
		else {
			pos += r_bytes;
		}
	} while (pos < buf_len);

	return(0);
}


int
cf_socket_write_forever(int fd, uint8_t *buf, size_t buf_len)
{
	// one way is to make sure the fd is blocking, and block
	int flags;
	if (-1 == (flags = fcntl(fd, F_GETFL, 0)))
		flags = 0;
	if (flags & O_NONBLOCK) {
		if (-1 == fcntl(fd, F_SETFL, flags & ~O_NONBLOCK)) {
			return(ENOENT);
		}
	}

	size_t pos = 0;
	do {
		int r_bytes = write(fd, buf + pos, buf_len - pos );
		if (r_bytes < 0) { // don't combine these into one line! think about it!
			if (errno != ETIMEDOUT) {
				return(errno);
			}
		}
		else {
			pos += r_bytes;
		}
	} while (pos < buf_len);

	if (-1 == fcntl(fd, F_SETFL, flags & O_NONBLOCK)) {
		return(ENOENT);
	}

	return(0);
}

void
cf_print_sockaddr_in(char *prefix, struct sockaddr_in *sa_in)
{
	char str[INET_ADDRSTRLEN];
	inet_ntop(AF_INET, &(sa_in->sin_addr), str, INET_ADDRSTRLEN);
	cf_error("%s %s:%d", prefix, str, (int)ntohs(sa_in->sin_port));
}


#else // CF_WINDOWS
//====================================================================
// Windows
//

#include <WinSock2.h>
#include <Ws2tcpip.h>


int
cf_socket_create_nb()
{
	// Create the socket.
	SOCKET fd = socket(AF_INET, SOCK_STREAM, 0);

	if (-1 == fd) {
		cf_warn("could not allocate socket, errno %d", errno);
		return -1;
	}

    // Make the socket nonblocking.
	u_long iMode = 1;

	if (NO_ERROR != ioctlsocket(fd, FIONBIO, &iMode)) {
		cf_info("could not connect nonblocking socket %d, errno %d", fd, errno);
		return -1;
	}

	int f = 1;
	setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, (char*)&f, sizeof(f));

	return (int)fd;
}


#endif // CF_WINDOWS


int
cf_socket_start_connect_nb(int fd, struct sockaddr_in* sa)
{
	if (0 != connect(fd, (struct sockaddr*)sa, sizeof(*sa))) {
        if (! IS_CONNECTING()) {
        	if (errno == ECONNREFUSED) {
        		cf_debug("host refused socket connection");
        	}
        	else {
        		cf_info("could not connect nonblocking socket %d, errno %d", fd, errno);
        	}

        	return -1;
		}
	}

	return 0;
}


int
cf_socket_create_and_connect_nb(struct sockaddr_in* sa)
{
	// Create the socket.
	int fd = cf_socket_create_nb();

	// Initiate non-blocking connect.
	if (0 != cf_socket_start_connect_nb(fd, sa)) {
		cf_close(fd);
		return -1;
	}

	return fd;
}

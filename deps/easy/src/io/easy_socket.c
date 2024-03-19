#include "io/easy_socket.h"
#include "io/easy_io.h"
#include "util/easy_inet.h"
#include "util/easy_string.h"
#include <sys/ioctl.h>
#include <netinet/tcp.h>
#include <sys/sendfile.h>
#include <sys/un.h>

static ssize_t easy_socket_chain_writev(easy_connection_t *conn, easy_list_t *l, struct iovec *iovs, int cnt, int *again);
static ssize_t easy_socket_sendfile(easy_connection_t *conn, easy_file_buf_t *fb, int *again);

int easy_unix_domain_listen(const char* path, int backlog)
{
    int fd = -1;
    if ((fd = socket(AF_UNIX, SOCK_STREAM, 0)) < 0) {
        easy_error_log("create socket error.\n");
        goto error_exit;
    }

    easy_socket_non_blocking(fd);
    struct sockaddr_un      addr;
    socklen_t               len = sizeof(addr);
    memset(&addr, 0, len);
    addr.sun_family = AF_UNIX;
    snprintf(addr.sun_path, UNIX_PATH_MAX, "%s", path);
    if (unlink(path) < 0) {
        if (ENOENT != errno) {
            easy_error_log("remove old unix domain socket fail: %s", path);
            goto error_exit;
        }
    } else {
        easy_info_log("success cleanup old unix domain socket: %s, will create new one on server startup", path);
    }
    if (bind(fd, (struct sockaddr *)&addr, len) < 0) {
        easy_error_log("bind socket error: %d\n", errno);
        goto error_exit;
    }
    if (listen(fd, backlog > 0 ? backlog : 1024) < 0) {
        easy_error_log("listen error. %d\n", errno);
        goto error_exit;
    }

    return fd;

error_exit:
    if (fd >= 0)
        close(fd);

    return -1;
}

/**
 * 打开监听端口
 */
int easy_socket_listen(int udp, easy_addr_t *address, int *flags, int backlog)
{
    int v, fd = -1;

    if ((fd = socket(address->family, (udp ? SOCK_DGRAM : SOCK_STREAM), 0)) < 0) {
        easy_error_log("create socket error.\n");
        goto error_exit;
    }

    easy_socket_non_blocking(fd);

    if (udp == 0) {
        if ((*flags & EASY_FLAGS_DEFERACCEPT)) {
            easy_ignore(easy_socket_set_tcpopt(fd, TCP_DEFER_ACCEPT, 1));
            easy_ignore(easy_socket_set_tcpopt(fd, TCP_SYNCNT, 2));
        }
    }

    // set reuseport, reuseaddr
    v = (flags && (*flags & EASY_FLAGS_SREUSEPORT)) ? 1 : 0;

    if (v) {
        easy_ignore(easy_socket_set_opt(fd, SO_REUSEPORT, 1));
    } else {
        if (easy_socket_set_opt(fd, SO_REUSEPORT, 1) == 0) {
            easy_ignore(easy_socket_set_opt(fd, SO_REUSEPORT, 0));
            if (flags) {
                if ((AF_INET == address->family || AF_INET6 == address->family) &&
                    (*flags & EASY_FLAGS_NOLISTEN)) {
                    udp = 2;
                    *flags = EASY_FLAGS_REUSEPORT;
                }
            }
        }
    }

    if (easy_socket_set_opt(fd, SO_REUSEADDR, 1) < 0) {
        easy_error_log("SO_REUSEADDR error: %d, fd=%d\n", errno, fd);
        goto error_exit;
    }

    if (address->family == AF_INET6) {
        struct sockaddr_in6     addr;
        memset(&addr, 0, sizeof(struct sockaddr_in6));
        addr.sin6_family = AF_INET6;
        addr.sin6_port = address->port;
        memcpy(&addr.sin6_addr, address->u.addr6, sizeof(address->u.addr6));

        if (bind(fd, (struct sockaddr *)&addr, sizeof(struct sockaddr_in6)) < 0) {
            easy_error_log("bind socket error: %d\n", errno);
            goto error_exit;
        }

    } else {
        struct sockaddr_in      addr;
        socklen_t               len = sizeof(addr);
        memset(&addr, 0, sizeof(struct sockaddr_in));
        memcpy(&addr, address, sizeof(struct sockaddr_in));

        if (bind(fd, (struct sockaddr *)&addr, sizeof(struct sockaddr_in)) < 0) {
            easy_error_log("bind socket error: %d\n", errno);
            goto error_exit;
        }

        if (address->port == 0 && getsockname(fd, (struct sockaddr *)&addr, &len) == 0) {
            memcpy(address, &addr, sizeof(uint64_t));
        }
    }

    if (udp == 0 && listen(fd, backlog > 0 ? backlog : 1024) < 0) {
        easy_error_log("listen error. %d\n", errno);
        goto error_exit;
    }

    return fd;

error_exit:
    if (fd >= 0) {
        close(fd);
    }
    return -1;
}

char* easy_socket_err_reason(int error_no)
{
    char *err_reason;

    switch (error_no) {
    case EBADF:
        /* 9 - Bad file number */
        err_reason = " Wrong socket fd, or socket may have been closed.";
        break;
    case EPIPE:
        /* 32 - Broken pipe */
        err_reason = " Socket may have been closed, or write on socket"
                     " with error.";
        break;
    case ECONNRESET:
        /* 104 - Connection reset by peer */
        err_reason = " Connection reset by peer, due to keepalive or others.";
        break;
    case ETIMEDOUT:
        /* 110 - Connection timed out */
        err_reason = " Destination host does not exist, or been shut down.";
        break;
    case ECONNREFUSED:
        /* 111 - Connection refused */
        err_reason = " No listener on destination IP/PORT, or connect request"
                     " rejected by firewall/iptables. Use 'iptalbe -L -n' or"
                     " 'netstat -ntpl' to check it.";
        break;
    case ENETDOWN:
        /* 100 - Network is down */
    case ENETUNREACH:
        /* 101 - Network is unreachable */
    case EHOSTUNREACH:
        /* 113 - No route to host */
    default:
        err_reason = "";
        break;
    }

    return err_reason;
}


/**
 * 从socket读入数据到buf中
 */
ssize_t easy_socket_read(easy_connection_t *conn, char *buf, size_t size, int *pending)
{
    ssize_t n;
    EASY_SOCKET_IO_TIME_GUARD(ev_read_count, ev_read_time, size);

    do {
        n = recv(conn->fd, buf, size, 0);
    } while (n == -1 && errno == EINTR);

    if (n < 0) {
        // n = ((errno == EAGAIN) ? EASY_AGAIN : EASY_ERROR);
        if (errno == EAGAIN) {
            n = EASY_AGAIN;
        } else {
            n = EASY_ERROR;
            easy_info_log("Failed to read socket, fd(%d), conn(%p), errno(%d), strerror(%s).%s",
                conn->fd, conn, errno, strerror(errno), easy_socket_err_reason(errno));
        }
    }

    // if (EASY_ERROR == n) {
    //   SYS_ERROR("read error: fd=%d errno=%d", c->fd, errno);
    // }
    return n;
}

/**
 * 把buf_chain_t上的内容通过writev写到socket上
 */
#define EASY_SOCKET_RET_CHECK(ret, size, again) if (ret<0) return ret; else size += ret; if (again) return size;
ssize_t easy_socket_write(easy_connection_t *conn, easy_list_t *l)
{
    return easy_socket_tcpwrite(conn, l);
}

ssize_t easy_socket_tcpwrite(easy_connection_t *conn, easy_list_t *l)
{
    easy_buf_t              *b, *b1;
    easy_file_buf_t         *fb;
    struct          iovec   iovs[EASY_IOV_MAX];
    ssize_t                 sended, size, wbyte, ret;
    int                     cnt, again;

    EASY_SOCKET_IO_TIME_GUARD(ev_write_count, ev_write_time, wbyte);
    wbyte = cnt = sended = again = 0;

    // foreach
    easy_list_for_each_entry_safe(b, b1, l, node) {
        // sendfile
        if ((b->flags & EASY_BUF_FILE)) {
            // 先writev出去
            if (cnt > 0) {
                ret = easy_socket_chain_writev(conn, l, iovs, cnt, &again);
                EASY_SOCKET_RET_CHECK(ret, wbyte, again);
                cnt = 0;
            }

            fb = (easy_file_buf_t *)b;
            sended += fb->count;
            ret = easy_socket_sendfile(conn, fb, &again);
            EASY_SOCKET_RET_CHECK(ret, wbyte, again);
        } else {
            size = b->last - b->pos;
            iovs[cnt].iov_base = b->pos;
            iovs[cnt].iov_len = size;
            cnt++;
            sended += size;
        }

        // 跳出
        if (cnt >= EASY_IOV_MAX || sended >= EASY_IOV_SIZE) {
            break;
        }
    }

    // writev
    if (cnt > 0) {
        ret = easy_socket_chain_writev(conn, l, iovs, cnt, &again);
        EASY_SOCKET_RET_CHECK(ret, wbyte, again);
    }

    return wbyte;
}

/**
 * writev
 */
static ssize_t easy_socket_chain_writev(easy_connection_t *conn, easy_list_t *l, struct iovec *iovs, int cnt, int *again)
{
    int fd = conn->fd;
    ssize_t ret, sended, size;
    easy_buf_t *b, *b1;
    char btmp[64];

    {
        int retry = 0;
        EASY_TIME_GUARD();
        do {
            retry++;
            if (cnt == 1) {
                ret = send(fd, iovs[0].iov_base, iovs[0].iov_len, 0);
            } else {
                ret = writev(fd, iovs, cnt);
            }
        } while (ret == -1 && errno == EINTR);
    }

    // 结果处理
    if (ret >= 0) {
        sended = ret;

        easy_list_for_each_entry_safe(b, b1, l, node) {
            size = b->last - b->pos;
            if (easy_log_level >= EASY_LOG_TRACE) {
                easy_trace_log("fd: %d write: %d,%d => %s",
                    fd, size, sended, easy_string_tohex(b->pos, size, btmp, 64));
            }

            b->pos += sended;
            sended -= size;

            if (sended >= 0) {
                cnt--;
                easy_buf_destroy(b);
            }

            if (sended <= 0) {
                break;
            }
        }
        *again = (cnt > 0);
    } else {
        // ret = ((errno == EAGAIN) ? EASY_AGAIN : EASY_ERROR);
        // if (EASY_ERROR == ret) {
        //    SYS_ERROR("write error: fd=%d errno=%d", fd, errno);
        // }
        if (errno == EAGAIN) {
            ret = EASY_AGAIN;
        } else {
            ret = EASY_ERROR;
            easy_info_log("Failed to write socket, fd(%d), conn(%p), errno(%d), strerror(%s).%s",
                      fd, conn, errno, strerror(errno), easy_socket_err_reason(errno));
        }
    }

    return ret;
}

/**
 * sendfile
 */
static ssize_t easy_socket_sendfile(easy_connection_t *conn, easy_file_buf_t *fb, int *again)
{
    ssize_t ret;

    do {
        ret = sendfile(conn->fd, fb->fd, (off_t *)&fb->offset, fb->count);
    } while (ret == -1 && errno == EINTR);

    // 结果处理
    if (ret >= 0) {
        easy_debug_log("sendfile: %d, fd: %d\n", ret, conn->fd);

        if (ret < fb->count) {
            fb->count -= ret;
            *again = 1;
        } else {
            easy_buf_destroy((easy_buf_t *)fb);
        }
    } else {
        ret = ((errno == EAGAIN) ? EASY_AGAIN : EASY_ERROR);
    }

    return ret;
}

// 非阻塞
int easy_socket_non_blocking(int fd)
{
    int                     flags = 1;
    return ioctl(fd, FIONBIO, &flags);
}

// TCP
int easy_socket_set_tcpopt(int fd, int option, int value)
{
    int ret = setsockopt(fd, IPPROTO_TCP, option, (const void *) &value, sizeof(value));
    if (ret < 0) {
        easy_warn_log("IPPROTO_TCP fd: %d, errno: %d, option: %d, value: %d", fd, errno, option, value);
    }
    return ret;
}

// SOCKET
int easy_socket_set_opt(int fd, int option, int value)
{
    int ret = setsockopt(fd, SOL_SOCKET, option, (void *)&value, sizeof(value));
    if (ret < 0) {
        easy_warn_log("SOL_SOCKET fd: %d, errno: %d, option: %d, value: %d", fd, errno, option, value);
    }
    return ret;
}

// check ipv6
int easy_socket_support_ipv6()
{
    int fd = socket(AF_INET6, SOCK_STREAM, 0);

    if (fd == -1) {
        return 0;
    }

    close(fd);
    return 1;
}

// udp send
ssize_t easy_socket_usend(easy_connection_t *conn, easy_list_t *l)
{
    struct sockaddr_storage addr;
    memset(&addr, 0, sizeof(addr));
    easy_inet_etoa(&conn->addr, &addr);
    return easy_socket_udpwrite(conn, (struct sockaddr *)&addr, l);
}

ssize_t easy_socket_udpwrite(easy_connection_t *conn, struct sockaddr *addr, easy_list_t *l)
{
    easy_buf_t              *b, *b1;
    struct msghdr           msg;
    ssize_t                 cnt = 0, ret = 0;
    struct iovec            iovs[EASY_IOV_MAX];
    socklen_t               addr_len = sizeof(struct sockaddr_storage);
    int                     fd = conn->fd;

    // foreach
    easy_list_for_each_entry(b, l, node) {
        iovs[cnt].iov_base = b->pos;
        iovs[cnt].iov_len = b->last - b->pos;

        if (++cnt >= EASY_IOV_MAX) {
            break;
        }
    }

    if (cnt > 1) {
        memset(&msg, 0, sizeof(msg));
        msg.msg_name = (struct sockaddr *)addr;
        msg.msg_namelen = addr_len;
        msg.msg_iov = (struct iovec *)iovs;
        msg.msg_iovlen = cnt;

        ret = sendmsg(fd, &msg, 0);
    } else if (cnt == 1) {
        ret = sendto(fd, iovs[0].iov_base, iovs[0].iov_len, 0, (struct sockaddr *)addr, addr_len);
    }

    easy_list_for_each_entry_safe(b, b1, l, node) {
        easy_buf_destroy(b);

        if (--cnt <= 0) {
            break;
        }
    }
    return ret;
}

int easy_socket_error(int fd)
{
    int err = 0;
    socklen_t len = sizeof(err);

    if (getsockopt(fd, SOL_SOCKET, SO_ERROR, (void *) &err, &len) == -1) {
        easy_info_log("Got socket error by getsockopt, err(%d), fd(%d), errno(%d), strerror(%s).",
                err, fd, errno, strerror(errno));
        return -1;
    }

    return err;
}

int easy_socket_set_linger(int fd, int t)
{
    struct linger so_linger;
    so_linger.l_onoff = (t < 0 ? 0 : 1);
    so_linger.l_linger = (t <= 0 ? 0 : t);
    return setsockopt(fd, SOL_SOCKET, SO_LINGER, &so_linger, sizeof(so_linger));
}

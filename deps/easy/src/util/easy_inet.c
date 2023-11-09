#include "util/easy_inet.h"
#include "util/easy_string.h"
#include <easy_atomic.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <netdb.h>
#include <arpa/inet.h>      // inet_addr
#include <sys/ioctl.h>
#include <linux/if.h>

/**
 * 把sockaddr_in转成string
 */
char *easy_inet_addr_to_str(easy_addr_t *addr, char *buffer, int len)
{
    unsigned char           *b;

    if (addr->family == AF_INET6) {
        char                    tmp[INET6_ADDRSTRLEN];

        if (inet_ntop(AF_INET6, addr->u.addr6, tmp, INET6_ADDRSTRLEN) != NULL) {
            if (addr->port) {
                lnprintf(buffer, len, "[%s]:%d", tmp, ntohs(addr->port));
            } else {
                lnprintf(buffer, len, "%s", tmp);
            }
        }
    } else {
        b = (unsigned char *) &addr->u.addr;

        if (addr->port)
            lnprintf(buffer, len, "%d.%d.%d.%d:%d", b[0], b[1], b[2], b[3], ntohs(addr->port));
        else
            lnprintf(buffer, len, "%d.%d.%d.%d", b[0], b[1], b[2], b[3]);
    }

    return buffer;
}

/**
 * 把str转成addr(用uint64_t表示,IPV4)
 */
easy_addr_t easy_inet_str_to_addr(const char *host, int port)
{
    easy_addr_t             address;
    char                    *p, buffer[64];
    int                     len = -1, ipv6 = 0;

    memset(&address, 0, sizeof(easy_addr_t));

    if (host) {
        if (*host == '[' && (p = strchr(host, ']')) != NULL) {
            host ++;
            len = p - host;
            p = (*(p + 1) == ':') ? (p + 2) : NULL;
            ipv6 = 0x10000;
        } else if ((p = strchr(host, ':')) != NULL && (p == strrchr(host, ':'))) {
            len = p - host;
            p ++;
        }

        if (len > 63)
            return address;

        if (len >= 0) {
            memcpy(buffer, host, len);
            buffer[len] = '\0';
            host = buffer;

            if (!port && p) port = atoi(p);
        }
    }

    // parse host
    easy_inet_parse_host(&address, host, (port | ipv6));

    return address;
}

/**
 * 把端口改变一下
 */
easy_addr_t easy_inet_add_port(easy_addr_t *addr, int diff)
{
    easy_addr_t             ret;

    memcpy(&ret, addr, sizeof(easy_addr_t));
    ret.port = ntohs(ntohs(addr->port) + diff);
    return ret;
}

/**
 * 是IP地址, 如: 192.168.1.2
 */
int easy_inet_is_ipaddr(const char *host)
{
    unsigned char           c, *p;

    p = (unsigned char *)host;

    while ((c = (*p++)) != '\0') {
        if ((c != '.') && (c < '0' || c > '9')) {
            return 0;
        }
    }

    return 1;
}

/**
 * 解析host
 */
int easy_inet_parse_host(easy_addr_t *addr, const char *host, int port)
{
    int                     family = AF_INET;

    memset(addr, 0, sizeof(easy_addr_t));

    if (host && host[0]) {
        int                     rc;

        if (easy_inet_is_ipaddr(host)) {
            if ((rc = inet_addr(host)) == INADDR_NONE) {
                return EASY_ERROR;
            }

            addr->u.addr = rc;
        } else if (inet_pton(AF_INET6, host, addr->u.addr6) > 0) {
            family = AF_INET6;
        } else {
            // FIXME: gethostbyname会阻塞
            char                    buffer[1024];
            struct  hostent         h, *hp;

            if (gethostbyname_r(host, &h, buffer, 1024, &hp, &rc) || hp == NULL)
                return EASY_ERROR;

            if (hp->h_addrtype == AF_INET6) {
                family = AF_INET6;
                memcpy(addr->u.addr6, hp->h_addr, sizeof(addr->u.addr6));
            } else {
                addr->u.addr = *((uint32_t *)(hp->h_addr));
            }
        }
    } else if ((port & 0x10000)) {
        family = AF_INET6;
    } else {
        addr->u.addr = htonl(INADDR_ANY);
    }

    addr->family = family;
    addr->port = htons((port & 0xffff));

    return EASY_OK;
}

/**
 * 得到本机所有IP
 */
int easy_inet_hostaddr(uint64_t *address, int size, int local)
{
    int                     fd, ret, n;
    struct ifconf           ifc;
    struct ifreq            *ifr;

    ret = 0;

    if ((fd = socket(AF_INET, SOCK_DGRAM, 0)) < 0)
        return 0;

    ifc.ifc_len = sizeof(struct ifreq) * easy_max(size, 16);
    ifc.ifc_buf = (char *) malloc(ifc.ifc_len);

    if (ioctl(fd, SIOCGIFCONF, (char *)&ifc) < 0)
        goto out;

    ifr = ifc.ifc_req;

    for (n = 0; n < ifc.ifc_len; n += sizeof(struct ifreq)) {
        if (local || strncmp(ifr->ifr_name, "lo", 2)) {
            memcpy(&address[ret++], &(ifr->ifr_addr), sizeof(uint64_t));
        }

        ifr++;
    }

out:
    easy_free(ifc.ifc_buf);
    close(fd);
    return ret;
}

/**
 * 根据默认路由,得到本机IP
 */
int easy_inet_myip(easy_addr_t *addr)
{
    int             fd;
    socklen_t       addrlen = sizeof(easy_addr_t);

    memset(addr, 0, addrlen);
    addr->family = AF_INET;
    addr->port = 17152;
    addr->u.addr = 1481263425;

    if ((fd = socket(AF_INET, SOCK_DGRAM, 0)) < 0)
        goto error_exit;

    if (connect(fd, (struct sockaddr *) addr, addrlen) < 0)
        goto error_exit;

    if (getsockname(fd, (struct sockaddr *) addr, &addrlen) < 0)
        goto error_exit;

    addr->port = 0;
    close(fd);
    return EASY_OK;

error_exit:
    addr->port = 0;
    addr->u.addr = 0;

    if (fd >= 0) close(fd);

    return EASY_ERROR;
}

/**
 *get fd addr
 */
easy_addr_t easy_inet_getpeername(int s)
{
    socklen_t               len;
    struct sockaddr_storage addr;
    easy_addr_t             ret;

    len = sizeof(addr);
    memset(&ret, 0, sizeof(easy_addr_t));

    if (getpeername(s, (struct sockaddr *) &addr, &len) == 0) {
        easy_inet_atoe(&addr, &ret);
    }

    return ret;
}

/**
 *
 */
void easy_inet_atoe(void *a, easy_addr_t *e)
{
    struct sockaddr_storage *addr = (struct sockaddr_storage *) a;
    memset(e, 0, sizeof(easy_addr_t));

    if (addr->ss_family == AF_UNIX) {
        e->family = AF_UNIX;
    } else if (addr->ss_family == AF_INET) {
        struct sockaddr_in      *s = (struct sockaddr_in *)a;
        e->family = AF_INET;
        e->port = s->sin_port;
        e->u.addr = s->sin_addr.s_addr;
    } else {
        struct sockaddr_in6     *s = (struct sockaddr_in6 *)a;
        e->family = AF_INET6;
        e->port = s->sin6_port;
        memcpy(e->u.addr6, &s->sin6_addr, sizeof(e->u.addr6));
    }
}

void easy_inet_etoa(easy_addr_t *e, void *a)
{
    if (e->family == AF_INET6) {
        struct sockaddr_in6     *s = (struct sockaddr_in6 *)a;
        s->sin6_family = AF_INET6;
        s->sin6_port = e->port;
        memcpy(&s->sin6_addr, e->u.addr6, sizeof(e->u.addr6));
    } else if (e->family == AF_UNIX) {
        struct sockaddr_un *s = (struct sockaddr_un *)a;
        s->sun_family = AF_UNIX;
        snprintf(s->sun_path, UNIX_PATH_MAX, "%s", e->u.unix_path);
    } else {
        struct sockaddr_in      *s = (struct sockaddr_in *)a;
        s->sin_family = AF_INET;
        s->sin_port = e->port;
        s->sin_addr.s_addr = e->u.addr;
    }
}


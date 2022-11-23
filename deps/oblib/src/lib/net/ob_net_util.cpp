/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX LIB
#include "lib/oblog/ob_log.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/net/ob_net_util.h"
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <errno.h>

using namespace oceanbase::common;

namespace oceanbase {
namespace obsys {

int ObNetUtil::get_local_addr_ipv6(const char *dev_name, char *ipv6, int len)
{
  int ret = -1;
  struct ifaddrs *ifa = nullptr, *ifa_tmp = nullptr;

  if (len < INET6_ADDRSTRLEN || getifaddrs(&ifa) == -1) {
  } else {
    ifa_tmp = ifa;
    while (ifa_tmp) {
      if (ifa_tmp->ifa_addr && ifa_tmp->ifa_addr->sa_family == AF_INET6 &&
                               !strcmp(ifa_tmp->ifa_name, dev_name)) {
        struct sockaddr_in6 *in6 = (struct sockaddr_in6 *) ifa_tmp->ifa_addr;
        if (IN6_IS_ADDR_LOOPBACK(&in6->sin6_addr)
            || IN6_IS_ADDR_LINKLOCAL(&in6->sin6_addr)
            || IN6_IS_ADDR_SITELOCAL(&in6->sin6_addr)
            || IN6_IS_ADDR_V4MAPPED(&in6->sin6_addr)) {
          // filter ipv6 local, site-local etc.
        } else if (!inet_ntop(AF_INET6, &in6->sin6_addr, ipv6, len)) { // use ipv6 global
          ret = 0;
          break;
        }
      }
      ifa_tmp = ifa_tmp->ifa_next;
    } // while
  }
  return ret;
}

uint32_t ObNetUtil::get_local_addr_ipv4(const char *dev_name)
{
    uint32_t ret_addr = 0;
    int ret = OB_SUCCESS;
    int fd = -1;
    int interface = 0;
    struct ifreq buf[16];
    struct ifconf ifc;

    if ((fd = socket(AF_INET, SOCK_DGRAM, 0)) < 0) {
        LOG_ERROR("syscall socket failed", K(errno), KP(dev_name));
    } else {
        ifc.ifc_len = sizeof(buf);
        ifc.ifc_buf = (caddr_t)buf;

        if (0 != ioctl(fd, SIOCGIFCONF, (char *)&ifc)) {
            LOG_WARN("syscall ioctl(SIOCGIFCONF) failed", K(errno), K(fd), KP(dev_name));
        } else {
            interface = static_cast<int>(ifc.ifc_len / sizeof(struct ifreq));
            while (interface-- > 0 && OB_SUCC(ret)) {
                if (0 != ioctl(fd, SIOCGIFFLAGS, (char *)&buf[interface])) {
                    continue;
                }
                if (!(buf[interface].ifr_flags & IFF_UP)) {
                    continue;
                }
                if (NULL != dev_name && strcmp(dev_name, buf[interface].ifr_name)) {
                    continue;
                }
                if (!(ioctl(fd, SIOCGIFADDR, (char *)&buf[interface]))) {
                    ret = -1;
                    ret_addr = ((struct sockaddr_in *) (&buf[interface].ifr_addr))->sin_addr.s_addr;
                }
            }
        }
        close(fd);
    }

    return ret_addr;
}

std::string ObNetUtil::addr_to_string(uint64_t ipport)
{
    char str[32];
    uint32_t ip = (uint32_t)(ipport & 0xffffffff);
    int port = (int)((ipport >> 32 ) & 0xffff);
    unsigned char *bytes = (unsigned char *) &ip;
    if (port > 0) {
        snprintf(str, sizeof(str), "%d.%d.%d.%d:%d", bytes[0], bytes[1], bytes[2], bytes[3], port);
    } else {
        snprintf(str, sizeof(str), "%d.%d.%d.%d", bytes[0], bytes[1], bytes[2], bytes[3]);
    }
    return str;
}

uint64_t ObNetUtil::ip_to_addr(uint32_t ip, int port)
{
    uint64_t ipport = port;
    ipport <<= 32;
    ipport |= ip;
    return ipport;
}

}
}

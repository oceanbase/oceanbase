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
  int ret = OB_ERROR;
  int level = -1; // 0: loopback; 1: linklocal; 2: sitelocal; 3: v4mapped; 4: global
  struct ifaddrs *ifa = nullptr, *ifa_tmp = nullptr;

  if (nullptr == dev_name) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("devname can not be NULL", K(ret));
  } else if (len < INET6_ADDRSTRLEN) {
    ret = OB_BUF_NOT_ENOUGH;
    LOG_WARN("the buffer size cannot be less than INET6_ADDRSTRLEN",
             "INET6_ADDRSTRLEN", INET6_ADDRSTRLEN, K(len), K(ret));
  } else if (-1 == getifaddrs(&ifa)) {
    ret = OB_ERR_SYS;
    LOG_WARN("call getifaddrs fail", K(errno), K(ret));
  } else {
    ifa_tmp = ifa;
    while (ifa_tmp) {
      if (ifa_tmp->ifa_addr &&
          ifa_tmp->ifa_addr->sa_family == AF_INET6 &&
          0 == strcmp(ifa_tmp->ifa_name, dev_name)) {
        struct sockaddr_in6 *in6 = (struct sockaddr_in6 *) ifa_tmp->ifa_addr;
        int cur_level = -1;
        if (IN6_IS_ADDR_LOOPBACK(&in6->sin6_addr)) {
          cur_level = 0;
        } else if (IN6_IS_ADDR_LINKLOCAL(&in6->sin6_addr)) {
          cur_level = 1;
        } else if (IN6_IS_ADDR_SITELOCAL(&in6->sin6_addr)) {
          cur_level = 2;
        } else if (IN6_IS_ADDR_V4MAPPED(&in6->sin6_addr)) {
          cur_level = 3;
        } else {
          cur_level = 4;
        }
        if (cur_level > level) {
          if (nullptr == inet_ntop(AF_INET6, &in6->sin6_addr, ipv6, len)) {
            ret = OB_ERR_SYS;
            LOG_WARN("call inet_ntop fail", K(errno), K(ret));
          } else {
            level =  cur_level;
            ret = OB_SUCCESS;
          }
        }
      } // if end
      ifa_tmp = ifa_tmp->ifa_next;
    } // while end
  }

  if (nullptr != ifa) {
    freeifaddrs(ifa);
  }

  return ret;
}

uint32_t ObNetUtil::get_local_addr_ipv4(const char *dev_name)
{
  int ret = OB_SUCCESS;
  uint32_t ret_addr = 0;
  struct ifaddrs *ifa = nullptr, *ifa_tmp = nullptr;

  if (nullptr == dev_name) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("devname can not be NULL", K(ret));
  } else if (-1 == getifaddrs(&ifa)) {
    ret = OB_ERR_SYS;
    LOG_WARN("call getifaddrs fail", K(errno), K(ret));
  } else {
    ifa_tmp = ifa;
    bool has_found = false;
    while (nullptr != ifa_tmp && !has_found) {
      if (ifa_tmp->ifa_addr &&
          ifa_tmp->ifa_addr->sa_family == AF_INET &&
          0 == strcmp(ifa_tmp->ifa_name, dev_name)) {
        has_found = true;
        struct sockaddr_in *in = (struct sockaddr_in *) ifa_tmp->ifa_addr;
        ret_addr = in->sin_addr.s_addr;
      } // if end
      ifa_tmp = ifa_tmp->ifa_next;
    } // while end
  }

  if (nullptr != ifa) {
    freeifaddrs(ifa);
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

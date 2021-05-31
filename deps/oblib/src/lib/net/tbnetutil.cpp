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

#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include "lib/net/tbnetutil.h"

namespace oceanbase {
namespace obsys {

int CNetUtil::getLocalAddr6(const char* dev_name, char* ipv6, int len)
{
  int ret = -1;
  struct ifaddrs *ifa = nullptr, *ifa_tmp = nullptr;

  if (len < INET6_ADDRSTRLEN || getifaddrs(&ifa) == -1) {
  } else {
    ifa_tmp = ifa;
    while (ifa_tmp) {
      if (ifa_tmp->ifa_addr && ifa_tmp->ifa_addr->sa_family == AF_INET6 && !strcmp(ifa_tmp->ifa_name, dev_name)) {
        struct sockaddr_in6* in6 = (struct sockaddr_in6*)ifa_tmp->ifa_addr;
        if (IN6_IS_ADDR_LOOPBACK(&in6->sin6_addr) || IN6_IS_ADDR_LINKLOCAL(&in6->sin6_addr) ||
            IN6_IS_ADDR_SITELOCAL(&in6->sin6_addr) || IN6_IS_ADDR_V4MAPPED(&in6->sin6_addr)) {
          // filter ipv6 local, site-local etc.
        } else if (!inet_ntop(AF_INET6, &in6->sin6_addr, ipv6, len)) {  // use ipv6 global
          ret = 0;
          break;
        }
      }
      ifa_tmp = ifa_tmp->ifa_next;
    }  // while
  }
  return ret;
}

uint32_t CNetUtil::getLocalAddr(const char* dev_name)
{
  uint32_t ret = 0;
  int fd, intf;
  struct ifreq buf[16];
  struct ifconf ifc;

  if ((fd = socket(AF_INET, SOCK_DGRAM, 0)) >= 0) {
    ifc.ifc_len = sizeof(buf);
    ifc.ifc_buf = (caddr_t)buf;
    if (ioctl(fd, SIOCGIFCONF, (char*)&ifc) == 0) {
      intf = static_cast<int>(ifc.ifc_len / sizeof(struct ifreq));
      while (intf-- > 0) {
        if (ioctl(fd, SIOCGIFFLAGS, (char*)&buf[intf]) != 0)
          continue;
        if (!(buf[intf].ifr_flags & IFF_UP))
          continue;
        if (dev_name != NULL && strcmp(dev_name, buf[intf].ifr_name))
          continue;
        if (!(ioctl(fd, SIOCGIFADDR, (char*)&buf[intf]))) {
          ret = ((struct sockaddr_in*)(&buf[intf].ifr_addr))->sin_addr.s_addr;
          break;
        }
      }
    }
    close(fd);
  }
  return ret;
}

bool CNetUtil::isLocalAddr(uint32_t ip, bool loopSkip)
{
  int fd, intrface;
  struct ifreq buf[16];
  struct ifconf ifc;

  if ((fd = socket(AF_INET, SOCK_DGRAM, 0)) < 0) {
    return false;
  }

  ifc.ifc_len = sizeof(buf);
  ifc.ifc_buf = (caddr_t)buf;
  if (ioctl(fd, SIOCGIFCONF, (char*)&ifc) != 0) {
    close(fd);
    return false;
  }

  intrface = static_cast<int>(ifc.ifc_len / sizeof(struct ifreq));
  while (intrface-- > 0) {
    if (ioctl(fd, SIOCGIFFLAGS, (char*)&buf[intrface]) != 0) {
      continue;
    }
    if (loopSkip && buf[intrface].ifr_flags & IFF_LOOPBACK)
      continue;
    if (!(buf[intrface].ifr_flags & IFF_UP))
      continue;
    if (ioctl(fd, SIOCGIFADDR, (char*)&buf[intrface]) != 0) {
      continue;
    }
    if (((struct sockaddr_in*)(&buf[intrface].ifr_addr))->sin_addr.s_addr == ip) {
      close(fd);
      return true;
    }
  }
  close(fd);
  return false;
}

/**
 * 10.0.100.89 => 1499725834
 */
uint32_t CNetUtil::getAddr(const char* ip)
{
  if (ip == NULL)
    return 0;
  int x = inet_addr(ip);
  if (x == (int)INADDR_NONE) {
    struct hostent* hp;
    if ((hp = gethostbyname(ip)) == NULL) {
      return 0;
    }
    x = ((struct in_addr*)hp->h_addr)->s_addr;
  }
  return x;
}

std::string CNetUtil::addrToString(uint64_t ipport)
{
  char str[32];
  uint32_t ip = (uint32_t)(ipport & 0xffffffff);
  int port = (int)((ipport >> 32) & 0xffff);
  unsigned char* bytes = (unsigned char*)&ip;
  if (port > 0) {
    sprintf(str, "%d.%d.%d.%d:%d", bytes[0], bytes[1], bytes[2], bytes[3], port);
  } else {
    sprintf(str, "%d.%d.%d.%d", bytes[0], bytes[1], bytes[2], bytes[3]);
  }
  return str;
}

uint64_t CNetUtil::strToAddr(const char* ip, int port)
{
  uint32_t nip = 0;
  const char* p = strchr(ip, ':');
  if (p != NULL && p > ip) {
    int64_t len = p - ip;
    if (len > 64)
      len = 64;
    char tmp[128];
    strncpy(tmp, ip, len);
    tmp[len] = '\0';
    nip = getAddr(tmp);
    port = atoi(p + 1);
  } else {
    nip = getAddr(ip);
  }
  if (nip == 0) {
    return 0;
  }
  uint64_t ipport = port;
  ipport <<= 32;
  ipport |= nip;
  return ipport;
}

uint64_t CNetUtil::ipToAddr(uint32_t ip, int port)
{
  uint64_t ipport = port;
  ipport <<= 32;
  ipport |= ip;
  return ipport;
}
}  // namespace obsys
}  // namespace oceanbase

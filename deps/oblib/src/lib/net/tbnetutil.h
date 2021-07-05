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

#ifndef TBSYS_NETUTIL_H_
#define TBSYS_NETUTIL_H_

#include <stdint.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netinet/tcp.h>
#include <ifaddrs.h>  // ifaddrs
#include <unistd.h>
#include <sys/ioctl.h>
#include <netdb.h>
#include <sys/time.h>
#include <net/if.h>
#include <inttypes.h>
#include <sys/types.h>
#include <linux/unistd.h>
#include <string>

// using namespace std;

namespace oceanbase {
namespace obsys {
struct ipaddr_less {
  bool operator()(const uint64_t a, const uint64_t b) const
  {
    uint64_t a1 = ((a & 0xFF) << 24) | ((a & 0xFF00) << 8) | ((a & 0xFF0000) >> 8) | ((a & 0xFF000000) >> 24);
    a1 <<= 32;
    a1 |= ((a >> 32) & 0xffff);
    uint64_t b1 = ((b & 0xFF) << 24) | ((b & 0xFF00) << 8) | ((b & 0xFF0000) >> 8) | ((b & 0xFF000000) >> 24);
    b1 <<= 32;
    b1 |= ((b >> 32) & 0xffff);
    return (a1 < b1);
  }
};

class CNetUtil {
public:
  static int getLocalAddr6(const char* dev_name, char* ipv6, int len);
  static uint32_t getLocalAddr(const char* dev_name);
  static bool isLocalAddr(uint32_t ip, bool loopSkip = true);
  static uint32_t getAddr(const char* ip);
  static std::string addrToString(uint64_t ipport);
  static uint64_t strToAddr(const char* ip, int port);
  static uint64_t ipToAddr(uint32_t ip, int port);
};

}  // namespace obsys
}  // namespace oceanbase

#endif

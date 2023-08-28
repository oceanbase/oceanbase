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

#ifndef OCEANBASE_NET_UTIL_H_
#define OCEANBASE_NET_UTIL_H_

#include <stdint.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netinet/tcp.h>
#include <ifaddrs.h>
#include <unistd.h>
#include <sys/ioctl.h>
#include <netdb.h>
#include <sys/time.h>
#include <net/if.h>
#include <inttypes.h>
#include <sys/types.h>
#include <linux/unistd.h>
#include <string>

namespace oceanbase
{
namespace obsys
{

class ObNetUtil
{
public:
  static int get_local_addr_ipv6(const char *dev_name, char *ipv6, int len, bool *is_linklocal = nullptr);
  static uint32_t get_local_addr_ipv4(const char *dev_name);
  static std::string addr_to_string(uint64_t ipport);
  static uint64_t ip_to_addr(uint32_t ip, int port);
  // get ipv4 by hostname, no need free the returned value
  static char *get_addr_by_hostname(const char *hostname);
  static int get_ifname_by_addr(const char *local_ip, char *if_name, uint64_t if_name_len);
};
}  // namespace obsys
}  // namespace oceanbase
#endif

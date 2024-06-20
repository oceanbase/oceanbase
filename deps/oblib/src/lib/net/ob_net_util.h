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

#include "lib/string/ob_string.h"
#include "lib/net/ob_addr.h"

namespace oceanbase
{
namespace obsys
{

class ObNetUtil
{
private:
  static const uint32_t FAKE_PORT = 0;
  static int get_int_value(const common::ObString &str, int64_t &value);
  static bool calc_ip(const common::ObString &host_ip, common::ObAddr &addr);
  static bool calc_ip_mask(const common::ObString &host_name, common::ObAddr &host, common::ObAddr &mask);
  static bool is_ip_match(const common::ObString &client_ip, common::ObString host_name);
  static bool is_wild_match(const common::ObString &client_ip, const common::ObString &host_name);
public:
  static int get_local_addr_ipv6(const char *dev_name, char *ipv6, int len, bool *is_linklocal = nullptr);
  static int get_local_addr_ipv4(const char *dev_name, uint32_t &addr);
  static std::string addr_to_string(uint64_t ipport);
  static uint64_t ip_to_addr(uint32_t ip, int port);
  // get ipv4 by hostname, no need free the returned value
  static char *get_addr_by_hostname(const char *hostname);
  static int get_ifname_by_addr(const char *local_ip, char *if_name, uint64_t if_name_len, bool& has_found);
  static struct sockaddr_storage* make_unix_sockaddr_any(bool is_ipv6, int port, struct sockaddr_storage *sock_addr);
  static struct sockaddr_storage* make_unix_sockaddr(bool is_ipv6, const void *ip, int port, struct sockaddr_storage *sock_addr);
  static void sockaddr_to_addr(struct sockaddr_storage *sock_addr, bool &is_ipv6, void *ip, int &port);
  static bool straddr_to_addr(const char *ip_str, bool &is_ipv6, void *ip);
  static bool is_support_ipv6();
  static char *sockaddr_to_str(struct sockaddr_storage *sock_addr, char *buf, int len);
  static char *sockfd_to_str(int fd, char *buf, int len);
  static int sockaddr_compare(struct sockaddr_storage *left, struct sockaddr_storage *right);
  static bool is_valid_sockaddr(struct sockaddr_storage *sock_addr);

  static bool is_match(const common::ObString &client_ip, const common::ObString &host_name);
  static bool is_in_white_list(const common::ObString &client_ip, common::ObString &orig_ip_white_list);
};
}  // namespace obsys
}  // namespace oceanbase

extern "C" {
  struct sockaddr_storage* make_unix_sockaddr_any_c(bool is_ipv6, int port, struct sockaddr_storage *a);
  struct sockaddr_storage* make_unix_sockaddr_c(bool is_ipv6, void *ip, int port, struct sockaddr_storage *sock_addr);
  void sockaddr_to_addr_c(struct sockaddr_storage *sock_addr, bool *is_ipv6, void *ip, int *port);
  bool straddr_to_addr_c(const char *ip_str, bool *is_ipv6, void *ip);
  bool is_support_ipv6_c();
  char *sockaddr_to_str_c(struct sockaddr_storage *sock_addr, char *buf, int len);
  char *sockfd_to_str_c(int fd, char *buf, int len);
  int sockaddr_compare_c(struct sockaddr_storage *left, struct sockaddr_storage *right);
  bool is_valid_sockaddr_c(struct sockaddr_storage *sock_addr);
} /* extern "C" */
#endif

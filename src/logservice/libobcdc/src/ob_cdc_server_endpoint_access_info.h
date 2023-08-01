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
 *
 * ObCDCServerEndpiontAccessInfo define
 */

#ifndef OCEANBASE_LIBOBCDC_SERVER_ENDPOINT_ACCESS_INFO_H__
#define OCEANBASE_LIBOBCDC_SERVER_ENDPOINT_ACCESS_INFO_H__

#include "lib/net/ob_addr.h"
#include "lib/container/ob_array_serialization.h"

namespace oceanbase
{
namespace libobcdc
{
static const int64_t MAX_HOSTNAME_LEN = 100;
static const int64_t MAX_HOSTNAME_BUFFER_LEN = MAX_HOSTNAME_LEN + 1;
// Endpoint format: hostname:port
// hostname may be IP, or domain, or SLB addr
class ObCDCEndpoint
{
public:
  ObCDCEndpoint() : host_(), port_(0), is_domain_(false) { reset(); }
  ~ObCDCEndpoint() { reset(); }
public:
  int init(const char *tenant_endpoint);
  ObCDCEndpoint &operator=(const ObCDCEndpoint &other);
  void reset() { MEMSET(host_, '\0', MAX_HOSTNAME_BUFFER_LEN); port_ = 0; is_domain_ = false; }
  OB_INLINE const char *get_host() const { return host_; }
  OB_INLINE int32_t get_port() const { return port_; }
  ObAddr get_addr() const;
  bool is_valid() const;
  TO_STRING_KV(K_(host), K_(port), K_(is_domain), "addr", get_addr());
private:
  int check_domain_or_addr_();

private:
  char    host_[MAX_HOSTNAME_BUFFER_LEN];
  int64_t port_;
  bool    is_domain_; // is domain or IP Addr
};

class ObAccessInfo
{
public:
  ObAccessInfo() : user_(), password_() { reset(); }
  ~ObAccessInfo() { reset(); }
public:
  int init(const char *user, const char *password);
  void reset();
public:
  const char *get_user() const { return user_; }
  const char *get_password() const { return password_; }
  // TODO support get base64 encoded password
  const char *get_base64_encoded_password() const { return nullptr; }
  TO_STRING_KV(K_(user), K_(password));
private:
  char user_[common::OB_MAX_USER_NAME_BUF_LENGTH];
  // TODO use base64 encode to print info.
  char password_[common::OB_MAX_PASSWORD_BUF_LENGTH];
};

typedef common::ObArray<ObCDCEndpoint> ObCDCEndpointList;

} // namespace libobcdc
} // namespace oceanbase
#endif

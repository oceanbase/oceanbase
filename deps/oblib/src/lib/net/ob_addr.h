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

#ifndef _OCEABASE_LIB_NET_OB_ADDR_H_
#define _OCEABASE_LIB_NET_OB_ADDR_H_

#include "lib/ob_define.h"
#include "lib/string/ob_string.h"
#include "lib/utility/ob_unify_serialize.h"
#include "lib/container/ob_se_array.h"
#include "lib/container/ob_array_serialization.h"
#include "lib/ob_name_id_def.h"
namespace oceanbase {
namespace common {

class ObAddr {
  OB_UNIS_VERSION(1);

public:
  static constexpr int IPV6_LEN = 16;
  enum VER { IPV4 = 4, IPV6 = 6 };

  ObAddr() : version_(IPV4), ip_(), port_(0)
  {
    // memset(&ip_, 0, sizeof(ip_));
  }

  ObAddr(VER version, const char* ip, const int32_t port) : version_(IPV4), ip_(), port_(0)
  {
    // memset(&ip_, 0, sizeof(ip_));
    if (version == IPV4) {
      IGNORE_RETURN set_ipv4_addr(ip, port);
    } else if (version == IPV6) {
      IGNORE_RETURN set_ipv6_addr(ip, port);
    }
  }

  // TODO: delete, should not be used
  explicit ObAddr(const int64_t ipv4_server_id) : version_(IPV4), ip_(), port_(0)
  {
    ip_.v4_ = static_cast<int32_t>(0x00000000ffffffff & (ipv4_server_id >> 32));
    port_ = static_cast<int32_t>(0x00000000ffffffff & ipv4_server_id);
  }

  explicit ObAddr(const int32_t ip, const int32_t port) : version_(IPV4), ip_(), port_(port)
  {
    ip_.v4_ = ip;
  }

  void reset()
  {
    port_ = 0;
    memset(&ip_, 0, sizeof(ip_));
  }

  bool using_ipv4() const
  {
    return IPV4 == version_;
  }
  bool using_ipv6() const
  {
    return IPV6 == version_;
  }
  int64_t to_string(char* buffer, const int64_t size) const;
  bool ip_to_string(char* buffer, const int32_t size) const;
  int ip_port_to_string(char* buffer, const int32_t size) const;
  int addr_to_buffer(char* buffer, const int32_t size, int32_t& ret_len) const;
  int to_yson(char* buf, const int64_t buf_len, int64_t& pos) const;

  bool set_ip_addr(const char* ip, const int32_t port);
  bool set_ip_addr(const ObString& ip, const int32_t port);
  bool set_ipv4_addr(const int32_t ip, const int32_t port);
  bool set_ipv6_addr(const void* buff, const int32_t port);
  bool set_ipv6_addr(const uint64_t ipv6_high, const uint64_t ipv6_low, const int32_t port);

  int parse_from_cstring(const char* ip_str);
  int parse_from_string(const ObString& str);
  // TODO: delete
  int64_t get_ipv4_server_id() const;
  void set_ipv4_server_id(const int64_t ipv4_server_id);
  ObAddr& as_mask(const int64_t mask_bits);
  ObAddr& as_subnet(const ObAddr& mask);

  int64_t hash() const;
  ObAddr& operator=(const ObAddr& rv);
  bool operator!=(const ObAddr& rv) const;
  bool operator==(const ObAddr& rv) const;
  bool operator<(const ObAddr& rv) const;
  bool operator>(const ObAddr& rv) const;
  bool is_equal_except_port(const ObAddr& rv) const;
  inline int32_t get_version() const
  {
    return version_;
  }
  inline int32_t get_port() const
  {
    return port_;
  }
  inline uint32_t get_ipv4() const
  {
    return ip_.v4_;
  }
  int get_ipv6(void* buff, const int32_t size) const;
  uint64_t get_ipv6_high() const;
  uint64_t get_ipv6_low() const;
  void set_port(int32_t port);
  void set_max();
  bool is_valid() const;
  int compare(const ObAddr& rv) const;

  void reset_ipv4_10(int ip = 10);

private:
  int convert_ipv4_addr(const char* ip);
  int convert_ipv6_addr(const char* ip);
  bool set_ipv4_addr(const char* ip, const int32_t port);
  bool set_ipv6_addr(const char* ip, const int32_t port);

private:
  VER version_;
  union {
    uint32_t v4_;
    uint8_t v6_[IPV6_LEN];
  } ip_;
  int32_t port_;
};  // end of class ObAddr

typedef ObSEArray<ObAddr, 3> ObAddrArray;
typedef ObSArray<ObAddr> ObAddrSArray;
typedef ObIArray<ObAddr> ObAddrIArray;

inline bool ObAddr::is_valid() const
{
  bool valid = false;
  if (port_ <= 0) {
  } else if (IPV4 == version_) {
    valid = (0 != ip_.v4_);
  } else if (IPV6 == version_) {
    int pos = 0;
    for (; !valid && pos < IPV6_LEN; pos++) {
      valid = (0 != ip_.v6_[pos]);
    }
  }
  return valid;
}

inline bool ObAddr::set_ipv4_addr(const int32_t ip, const int32_t port)
{
  version_ = IPV4;
  ip_.v4_ = ip;
  port_ = port;
  return true;
}

inline int64_t ObAddr::hash() const
{
  int64_t code = 0;

  if (IPV4 == version_) {
    code += (port_ + ip_.v4_);
  } else if (IPV6 == version_) {
    int pos = 0;
    code += port_;
    for (; pos < IPV6_LEN; pos++) {
      code += ip_.v6_[pos];
    }
  }

  return code;
}

inline ObAddr& ObAddr::operator=(const ObAddr& rv)
{
  this->version_ = rv.version_;
  this->port_ = rv.port_;
  memcpy(&ip_, &rv.ip_, sizeof(ip_));
  return *this;
}

inline bool ObAddr::operator!=(const ObAddr& rv) const
{
  return !(*this == rv);
}

inline bool ObAddr::operator==(const ObAddr& rv) const
{
  return version_ == rv.version_ && port_ == rv.port_ && (0 == memcmp(&ip_, &rv.ip_, sizeof(ip_)));
}

inline int ObAddr::compare(const ObAddr& rv) const
{
  int compare_ret = 0;

  if (&rv == this) {
    compare_ret = 0;
  } else if (rv.version_ != version_) {
    compare_ret = rv.version_ < version_ ? 1 : -1;
  } else if (rv.port_ != port_) {
    compare_ret = rv.port_ < port_ ? 1 : -1;
  } else {
    compare_ret = memcmp(&rv.ip_, &ip_, sizeof(ip_));
  }

  return compare_ret;
}

}  // end of namespace common
}  // end of namespace oceanbase

#endif /* _OCEABASE_LIB_NET_OB_ADDR_H_ */

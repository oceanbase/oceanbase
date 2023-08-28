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
#include "util/easy_inet.h"
#include "lib/utility/ob_unify_serialize.h"
#include "lib/utility/ob_print_utils.h"

struct easy_addr_t;

namespace oceanbase
{
namespace obrpc
{
class ObBatchPacket;
class ObBatchP;
};
namespace common
{
class ObString;
static constexpr char UNIX_SOCKET_CLIENT_IP[] = "unix:";
class ObAddr
{
  OB_UNIS_VERSION(1);

public:
  static constexpr int IPV6_LEN = 16;
  enum VER {
    IPV4 = 4, IPV6 = 6, UNIX = 1
  };

  ObAddr()
      : version_(IPV4), ip_(), port_(0)
  {
    //memset(&ip_, 0, sizeof(ip_));
  }

  ObAddr(VER version, const char *ip, const int32_t port)
      : version_(IPV4), ip_(), port_(0)
  {
    //memset(&ip_, 0, sizeof(ip_));
    if (version == IPV4) {
      IGNORE_RETURN set_ipv4_addr(ip, port);
    } else if (version == IPV6) {
      IGNORE_RETURN set_ipv6_addr(ip, port);
    }
  }

  // TODO: delete, should not be used
  explicit ObAddr(const int64_t ipv4_server_id)
      : version_(IPV4), ip_(), port_(0)
  {
    ip_.v4_  = static_cast<int32_t>(0x00000000ffffffff & (ipv4_server_id >> 32));
    port_ = static_cast<int32_t>(0x00000000ffffffff & ipv4_server_id);
  }

  explicit ObAddr(const int32_t ip, const int32_t port)
      : version_(IPV4), ip_(), port_(port)
  {
    ip_.v4_  = ip;
  }

  explicit ObAddr(const easy_addr_t& addr);

  explicit ObAddr(const sockaddr &addr);

  void reset()
  {
    port_ = 0;
    //memset(&ip_, 0, sizeof (ip_));
  }

  bool using_ipv4() const { return IPV4 == version_; }
  bool using_ipv6() const { return IPV6 == version_; }
  bool using_unix() const { return UNIX == version_; }
  int64_t to_string(char *buffer, const int64_t size) const;
  bool ip_to_string(char *buffer, const int32_t size) const;
  int ip_port_to_string(char *buffer, const int32_t size) const;
  int addr_to_buffer(char *buffer, const int32_t size, int32_t &ret_len) const;
  int to_yson(char *buf, const int64_t buf_len, int64_t &pos) const;

  bool set_ip_addr(const char *ip, const int32_t port);
  bool set_ip_addr(const ObString &ip, const int32_t port);
  bool set_ipv4_addr(const int32_t ip, const int32_t port);
  bool set_ipv6_addr(const void *buff, const int32_t port);
  bool set_ipv6_addr(const uint64_t ipv6_high, const uint64_t ipv6_low, const int32_t port);
  bool set_unix_addr(const char *unix_path);

  int parse_from_cstring(const char *ip_str);
  int parse_from_string(const ObString &str);
  ObAddr &as_mask(const int64_t mask_bits);
  ObAddr &as_subnet(const ObAddr &mask);

  int64_t hash() const;
  int hash(uint64_t &code) const;
  ObAddr &operator=(const ObAddr &rv);
  bool operator !=(const ObAddr &rv) const;
  bool operator ==(const ObAddr &rv) const;
  bool operator < (const ObAddr &rv) const;
  bool operator > (const ObAddr &rv) const;
  bool is_equal_except_port(const ObAddr &rv) const;
  inline int32_t get_version() const { return version_; }
  inline int32_t get_port() const { return port_; }
  inline uint32_t get_ipv4() const { return ip_.v4_; }
  inline const char* get_unix_path() const { return ip_.unix_path_; }
  int get_ipv6(void *buff, const int32_t size) const;
  uint64_t get_ipv6_high() const;
  uint64_t get_ipv6_low() const;
  void set_port(int32_t port);
  void set_max();
  bool is_valid() const;
  int compare(const ObAddr &rv) const;
private:
  // depercate:
  friend class  ObProposalID;
  friend class oceanbase::obrpc::ObBatchPacket;
  friend class oceanbase::obrpc::ObBatchP;
  int64_t get_ipv4_server_id() const;
  void set_ipv4_server_id(const int64_t ipv4_server_id);

private:
  int convert_ipv4_addr(const char *ip);
  int convert_ipv6_addr(const char *ip);
  bool set_ipv4_addr(const char *ip, const int32_t port);
  bool set_ipv6_addr(const char *ip, const int32_t port);

private:
  VER version_;
  union
  {
    uint32_t v4_;
    uint8_t v6_[IPV6_LEN];
    char unix_path_[IPV6_LEN];
  } ip_;
  int32_t port_;
}; // end of class ObAddr

inline bool ObAddr::is_valid() const
{
  bool valid = false;
  if (UNIX == version_) {
    size_t len = STRLEN(ip_.unix_path_);
    if (len > 0 && len < UNIX_PATH_MAX) {
        valid = true;
    }
  } else if (OB_UNLIKELY(port_ <= 0)) {
  } else if (IPV4 == version_) {
    valid = (0 != ip_.v4_);
  } else if (IPV6 == version_) {
    int pos = 0;
    for(; !valid && pos < IPV6_LEN; pos++) {
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

inline int ObAddr::hash(uint64_t &code) const
{
  code = hash();
  return OB_SUCCESS;
}

inline ObAddr &ObAddr::operator=(const ObAddr &rv) {
  this->version_ = rv.version_;
  this->port_ = rv.port_;
  memcpy(&ip_, &rv.ip_, sizeof(ip_));
  return *this;
}

inline bool ObAddr::operator !=(const ObAddr &rv) const
{
  return !(*this == rv);
}

inline bool ObAddr::operator ==(const ObAddr &rv) const
{
  return version_ == rv.version_ && port_ == rv.port_ && (0 == memcmp(&ip_, &rv.ip_, sizeof(ip_)));
}

inline int ObAddr::compare(const ObAddr &rv) const
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

//for ofs proxy service,server addr and seq present a life cycle for one server
class ObAddrWithSeq
{
  OB_UNIS_VERSION(1);
public:
  ObAddrWithSeq()
	  : server_addr_(),
	    server_seq_(OB_INVALID_SVR_SEQ){}
  ObAddrWithSeq(const ObAddr &server_addr,
            const int64_t server_seq)
      : server_addr_(server_addr),
	server_seq_(server_seq){}
  ~ObAddrWithSeq() {}
public:
  void reset() {
    server_addr_.reset();
    server_seq_ = OB_INVALID_SVR_SEQ;
  }
  bool operator == (const ObAddrWithSeq &other) const {
    return server_addr_ == other.server_addr_ && server_seq_ == other.server_seq_;
  }
  bool operator!=(const ObAddrWithSeq &other) const { return !operator==(other); }

  int64_t hash() const {
    return murmurhash(&server_addr_, sizeof(server_addr_), server_seq_);
  }
  int hash(uint64_t &hash_val) const {
    hash_val = hash();
    return OB_SUCCESS;
  }

  bool is_valid() const {
    return server_addr_.is_valid() && server_seq_ >= 0;
  }

  ObAddrWithSeq &operator=(const ObAddrWithSeq &other) {
    this->server_addr_ = other.server_addr_;
    this->server_seq_ = other.server_seq_;
    return *this;
  }
  void set_addr_seq(const ObAddr &svr_addr, const int64_t svr_seq) {
    server_addr_ = svr_addr;
    server_seq_ = svr_seq;
  }

  void set_addr(const ObAddr &svr_addr) {
    server_addr_ = svr_addr;
  }

  void set_seq(const int64_t svr_seq) {
    server_seq_ = svr_seq;
  }

  const ObAddr &get_addr() const { return server_addr_; }
  const int64_t &get_seq() const { return server_seq_; }
  DECLARE_TO_STRING;
private:
  ObAddr server_addr_;
  int64_t server_seq_;
};

} // end of namespace common
} // end of namespace oceanbase

#endif /* _OCEABASE_LIB_NET_OB_ADDR_H_ */

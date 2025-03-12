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


#include "ob_addr.h"
#include "lib/utility/utility.h"
#include "lib/net/ob_net_util.h"

namespace oceanbase
{
namespace common
{

// --------------------------------------------------------
// class ObAddr implements
// --------------------------------------------------------
ObAddr::ObAddr(const easy_addr_t& addr)
{
  if (addr.family == AF_INET) {
    version_ = IPV4;
    ip_.v4_ = addr.u.addr;
  } else if (addr.family == AF_INET6) {
    version_ = IPV6;
    MEMCPY(ip_.v6_, addr.u.addr6, sizeof(ip_.v6_));
  } else if (addr.family == AF_UNIX) {
    version_ = UNIX;
    MEMCPY(ip_.unix_path_, addr.u.unix_path, sizeof(ip_.unix_path_));
  }
  port_ = addr.port;
}

void ObAddr::from_sockaddr(struct sockaddr_storage *sock_addr)
{
  reset();
  bool is_ipv6 = false;
  oceanbase::obsys::ObNetUtil::sockaddr_to_addr(sock_addr, is_ipv6, &ip_, port_);
  version_ = !is_ipv6 ? IPV4 : IPV6;
}

struct sockaddr_storage *ObAddr::to_sockaddr(struct sockaddr_storage *sock_addr) const
{
  sockaddr_storage *sret = NULL;
  if (IPV4 == version_ || IPV6 == version_) {
    sret = oceanbase::obsys::ObNetUtil::make_unix_sockaddr(IPV6 == version_, &ip_, port_, sock_addr);
  }
  return sret;
}

int ObAddr::convert_ipv4_addr(const char *ip)
{
  int ret = OB_SUCCESS;
  bool is_ipv6 = false;
  if (!oceanbase::obsys::ObNetUtil::straddr_to_addr(ip, is_ipv6, &ip_)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    reset_v4_extraneous();
  }
  return ret;
}

int ObAddr::convert_ipv6_addr(const char *ip)
{
  int ret = OB_SUCCESS;
  bool is_ipv6 = false;
  if (!oceanbase::obsys::ObNetUtil::straddr_to_addr(ip, is_ipv6, &ip_)) {
    ret = OB_INVALID_ARGUMENT;
  }
  return ret;
}

int ObAddr::parse_from_string(const ObString &str)
{
  int ret = OB_SUCCESS;
  char buf[MAX_IP_PORT_LENGTH] = "";
  int port = 0;

  if (str.ptr() != NULL) {
    int64_t data_len = MIN(str.length(), sizeof (buf) - 1);
    MEMCPY(buf, str.ptr(), data_len);
    buf[data_len] = '\0';
    char *pport = strrchr(buf, ':');
    if (NULL != pport) {
      *(pport++) = '\0';
      char *end = NULL;
      port = static_cast<int>(strtol(pport, &end, 10));
      if (NULL == end || end - pport != static_cast<int64_t>(strlen(pport))) {
        ret = OB_INVALID_ARGUMENT;
      }
    } else {
      ret = OB_INVALID_ARGUMENT;
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
  }

  if (OB_SUCC(ret)) {
    char *p = buf;
    if ('[' == buf[0]) p = buf + 1;
    if (']' == buf[strlen(buf) - 1]) buf[strlen(buf) - 1] = '\0';
    if (NULL == strchr(p, ':') ?
        !set_ipv4_addr(p, port) : !set_ipv6_addr(p, port)) {
      ret = OB_INVALID_ARGUMENT;
    }
  }
  return ret;
}

int ObAddr::parse_from_cstring(const char *ipport)
{
  return parse_from_string(ObString::make_string(ipport));
}

bool ObAddr::set_ipv6_addr(const void *buff, const int32_t port)
{
  bool ret = false;
  in6_addr in6;
  char ipv6[INET6_ADDRSTRLEN] = { '\0' };
  memcpy(in6.s6_addr, buff, sizeof(in6.s6_addr));
  if (nullptr != inet_ntop(AF_INET6, &in6, ipv6, sizeof(ipv6))) {
    memcpy(ip_.v6_, in6.s6_addr, sizeof(ip_.v6_));
    version_ = IPV6;
    port_ = port;
    ret = true;
  }
  return ret;
}

bool ObAddr::set_ipv6_addr(const uint64_t ipv6_high, const uint64_t ipv6_low, const int32_t port)
{
  bool ret = true;
  uint64_t *high = reinterpret_cast<uint64_t *>(&ip_.v6_[0]);
  uint64_t *low = reinterpret_cast<uint64_t *>(&ip_.v6_[8]);
  *high = ipv6_high;
  *low = ipv6_low;
  version_ = IPV6;
  port_= port;
  return ret;
}

bool ObAddr::set_unix_addr(const char *unix_path)
{
  bool ret = false;
  version_ = UNIX;
  if (nullptr != unix_path) {
    int n = snprintf(ip_.unix_path_, UNIX_PATH_MAX, "%s", unix_path);
    if (n < UNIX_PATH_MAX && n >= 0) {
      ret = true;
    }
  }
  return ret;
}

int64_t ObAddr::inner_to_string(char *buffer, const int64_t size) const
{
  int64_t pos = 0;
  if (nullptr != buffer && size > 0) {
    //databuff_printf(buffer, size, pos, "version=%d ", version_);
#ifdef ENABLE_DEBUG_LOG
    if (size < MAX_IP_PORT_LENGTH && (version_ == IPV4 || version_ == IPV6)) {
      LOG_ERROR_RET(OB_BUF_NOT_ENOUGH, "buffer size is not enough for an ip string",
                    K(size));
    }
#endif
    if (version_ == IPV4) {
      if (port_ > 0) {
        databuff_printf(buffer, size, pos, "\"%d.%d.%d.%d:%d\"",
                        (ip_.v4_ >> 24) & 0xFF,
                        (ip_.v4_ >> 16) & 0xFF,
                        (ip_.v4_ >> 8) & 0xFF,
                        (ip_.v4_) & 0xFF, port_);
      } else {
        databuff_printf(buffer, size, pos, "\"%d.%d.%d.%d:%d\"", 0,0,0,0,0);
      }
    } else if (version_ == IPV6) {
      if (port_ > 0) {
        databuff_printf(buffer, size, pos, "\"[%x:%x:%x:%x:%x:%x:%x:%x]:%d\"",
                 (((uint16_t) (ip_.v6_[0])) << 8) | ((uint16_t)(ip_.v6_[1])),
                 (((uint16_t) (ip_.v6_[2])) << 8) | ((uint16_t)(ip_.v6_[3])),
                 (((uint16_t) (ip_.v6_[4])) << 8) | ((uint16_t)(ip_.v6_[5])),
                 (((uint16_t) (ip_.v6_[6])) << 8) | ((uint16_t)(ip_.v6_[7])),
                 (((uint16_t) (ip_.v6_[8])) << 8) | ((uint16_t)(ip_.v6_[9])),
                 (((uint16_t) (ip_.v6_[10])) << 8) | ((uint16_t)(ip_.v6_[11])),
                 (((uint16_t) (ip_.v6_[12])) << 8) | ((uint16_t)(ip_.v6_[13])),
                 (((uint16_t) (ip_.v6_[14])) << 8) | ((uint16_t)(ip_.v6_[15])),
                 port_);
      } else {
        databuff_printf(buffer, size, pos, "\"%x:%x:%x:%x:%x:%x:%x:%x\"",
                 (((uint16_t) (ip_.v6_[0])) << 8) | ((uint16_t)(ip_.v6_[1])),
                 (((uint16_t) (ip_.v6_[2])) << 8) | ((uint16_t)(ip_.v6_[3])),
                 (((uint16_t) (ip_.v6_[4])) << 8) | ((uint16_t)(ip_.v6_[5])),
                 (((uint16_t) (ip_.v6_[6])) << 8) | ((uint16_t)(ip_.v6_[7])),
                 (((uint16_t) (ip_.v6_[8])) << 8) | ((uint16_t)(ip_.v6_[9])),
                 (((uint16_t) (ip_.v6_[10])) << 8) | ((uint16_t)(ip_.v6_[11])),
                 (((uint16_t) (ip_.v6_[12])) << 8) | ((uint16_t)(ip_.v6_[13])),
                 (((uint16_t) (ip_.v6_[14])) << 8) | ((uint16_t)(ip_.v6_[15])));
      }
    } else if (version_ == UNIX) {
      databuff_printf(buffer, size, pos, "\"unix:%s\"", ip_.unix_path_);
    }
  }
  return pos;
}

bool ObAddr::inner_ip_to_string(char *buffer, const int32_t size) const
{
  bool res = false;
  if (nullptr != buffer && size > 0) {
#ifdef ENABLE_DEBUG_LOG
    if (size < MAX_IP_ADDR_LENGTH && (version_ == IPV4 || version_ == IPV6)) {
      LOG_ERROR_RET(OB_BUF_NOT_ENOUGH, "buffer size is not enough for an ip string", K(size));
    }
#endif
    if (version_ == IPV4) {
      snprintf(buffer, size, "%d.%d.%d.%d",
               (ip_.v4_ >> 24) & 0XFF,
               (ip_.v4_ >> 16) & 0xFF,
               (ip_.v4_ >> 8) & 0xFF,
               (ip_.v4_) & 0xFF);
    } else if (version_ == IPV6) {
      snprintf(buffer, size, "%x:%x:%x:%x:%x:%x:%x:%x",
                (((uint16_t) (ip_.v6_[0])) << 8) | ((uint16_t)(ip_.v6_[1])),
                (((uint16_t) (ip_.v6_[2])) << 8) | ((uint16_t)(ip_.v6_[3])),
                (((uint16_t) (ip_.v6_[4])) << 8) | ((uint16_t)(ip_.v6_[5])),
                (((uint16_t) (ip_.v6_[6])) << 8) | ((uint16_t)(ip_.v6_[7])),
                (((uint16_t) (ip_.v6_[8])) << 8) | ((uint16_t)(ip_.v6_[9])),
                (((uint16_t) (ip_.v6_[10])) << 8) | ((uint16_t)(ip_.v6_[11])),
                (((uint16_t) (ip_.v6_[12])) << 8) | ((uint16_t)(ip_.v6_[13])),
                (((uint16_t) (ip_.v6_[14])) << 8) | ((uint16_t)(ip_.v6_[15])));
    } else if (version_ == UNIX) {
      snprintf(buffer, size, "unix:%s", ip_.unix_path_);
    }
    res = true;
  }
  return res;
}

int ObAddr::inner_ip_port_to_string(char *buffer, const int32_t size) const
{
  int ret = OB_SUCCESS;
  int ret_len = 0;
#ifdef ENABLE_DEBUG_LOG
  if (size < MAX_IP_PORT_LENGTH) {
    LOG_ERROR_RET(OB_BUF_NOT_ENOUGH, "buffer size is not enough for an ip string",
                  K(size));
  }
#endif
  if (NULL == buffer || size <= 0) {
    ret = OB_INVALID_ARGUMENT;
  } else if (version_ == IPV6) {
    ret_len = snprintf(buffer, size, "[%x:%x:%x:%x:%x:%x:%x:%x]:%d",
        (((uint16_t) (ip_.v6_[0])) << 8) | ((uint16_t)(ip_.v6_[1])),
        (((uint16_t) (ip_.v6_[2])) << 8) | ((uint16_t)(ip_.v6_[3])),
        (((uint16_t) (ip_.v6_[4])) << 8) | ((uint16_t)(ip_.v6_[5])),
        (((uint16_t) (ip_.v6_[6])) << 8) | ((uint16_t)(ip_.v6_[7])),
        (((uint16_t) (ip_.v6_[8])) << 8) | ((uint16_t)(ip_.v6_[9])),
        (((uint16_t) (ip_.v6_[10])) << 8) | ((uint16_t)(ip_.v6_[11])),
        (((uint16_t) (ip_.v6_[12])) << 8) | ((uint16_t)(ip_.v6_[13])),
        (((uint16_t) (ip_.v6_[14])) << 8) | ((uint16_t)(ip_.v6_[15])),
        port_);
  } else {
    ret_len = snprintf(buffer, size, "%d.%d.%d.%d:%d",
                       (ip_.v4_ >> 24) & 0XFF,
                       (ip_.v4_ >> 16) & 0xFF,
                       (ip_.v4_ >> 8) & 0xFF,
                       (ip_.v4_) & 0xFF,
                       port_);

  }
  if (OB_FAIL(ret)) {
  } else if (ret_len < 0) {
    ret = OB_ERR_SYS;
  } else if (ret_len >= size) {
    ret = OB_SIZE_OVERFLOW;
  }
  return ret;
}

int ObAddr::inner_addr_to_buffer(char *buffer, const int32_t size, int32_t &ret_len) const
{
  int ret = OB_SUCCESS;
#ifdef ENABLE_DEBUG_LOG
  if (size < MAX_IP_PORT_LENGTH) {
    LOG_ERROR_RET(OB_BUF_NOT_ENOUGH, "buffer size is not enough for an ip string",
                  K(size));
  }
#endif
  if (NULL == buffer || size <= 0) {
    ret = OB_INVALID_ARGUMENT;
  } else if (version_ == IPV6) {
    ret_len = snprintf(buffer, size, "[%x:%x:%x:%x:%x:%x:%x:%x]:%d",
        (((uint16_t) (ip_.v6_[0])) << 8) | ((uint16_t)(ip_.v6_[1])),
        (((uint16_t) (ip_.v6_[2])) << 8) | ((uint16_t)(ip_.v6_[3])),
        (((uint16_t) (ip_.v6_[4])) << 8) | ((uint16_t)(ip_.v6_[5])),
        (((uint16_t) (ip_.v6_[6])) << 8) | ((uint16_t)(ip_.v6_[7])),
        (((uint16_t) (ip_.v6_[8])) << 8) | ((uint16_t)(ip_.v6_[9])),
        (((uint16_t) (ip_.v6_[10])) << 8) | ((uint16_t)(ip_.v6_[11])),
        (((uint16_t) (ip_.v6_[12])) << 8) | ((uint16_t)(ip_.v6_[13])),
        (((uint16_t) (ip_.v6_[14])) << 8) | ((uint16_t)(ip_.v6_[15])),
        port_);
  } else if (version_ == IPV4){
    ret_len = snprintf(buffer, size, "%d.%d.%d.%d:%d",
                           (ip_.v4_ >> 24) & 0XFF,
                           (ip_.v4_ >> 16) & 0xFF,
                           (ip_.v4_ >> 8) & 0xFF,
                           (ip_.v4_) & 0xFF,
                           port_);

  }
  if (ret_len < 0) {
    ret = OB_ERR_SYS;
  } else if (ret_len >= size) {
    ret = OB_SIZE_OVERFLOW;
  }
  return ret;
}

int ObAddr::to_yson(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (IPV4 == version_) {
    ret = oceanbase::yson::databuff_encode_elements(buf, buf_len, pos,
                                                    OB_ID(ip), ip_.v4_,
                                                    OB_ID(port), port_);
  } else if (IPV6 == version_) {
    char ip[MAX_IP_PORT_LENGTH + 1] = { '\0' };
    if (!ip_to_string(ip, sizeof(ip))) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      ret = oceanbase::yson::databuff_encode_elements(buf, buf_len, pos,
                                                      OB_ID(ip), (const char *)ip,
                                                      OB_ID(port), port_);
    }
  }

  return ret;
}

bool ObAddr::set_ip_addr(const char *ip, const int32_t port)
{
  bool ret = false;
  if (OB_ISNULL(ip)) {
    // null ptr
  } else {
    // simply distinguish v4 & v6 with colon
    const char *colonp = strchr(ip, ':');
    if (nullptr != colonp) {
      ret = set_ipv6_addr(ip, port);
    } else {
      ret = set_ipv4_addr(ip, port);
    }
  }
  return ret;
}

bool ObAddr::set_ipv6_addr(const char *ip, const int32_t port)
{
  bool ret = true;
  if (NULL == ip || port < 0) {
    ret = false;
  } else {
    version_ = IPV6;
    port_ = port;
    ret = (OB_SUCCESS == convert_ipv6_addr(ip));
  }
  return ret;
}

bool ObAddr::set_ipv4_addr(const char *ip, const int32_t port)
{
  bool ret = true;
  if (NULL == ip || port < 0) {
    ret = false;
  } else {
    version_ = IPV4;
    port_ = port;
    ret = (OB_SUCCESS == convert_ipv4_addr(ip));
  }
  return ret;
}

bool ObAddr::set_ip_addr(const ObString &ip, const int32_t port)
{
  bool ret = true;
  char ip_buf[MAX_IP_PORT_LENGTH] = "";
  if (ip.length() >= MAX_IP_PORT_LENGTH) {
    ret = false;
  } else {
    // ObString may be not terminated by '\0'
    MEMCPY(ip_buf, ip.ptr(), ip.length());
    ret = set_ip_addr(ip_buf, port);
  }
  return ret;
}

int64_t ObAddr::get_ipv4_server_id() const
{
  int64_t server_id = 0;
  if (IPV4 == version_) {
    server_id = ip_.v4_;
    server_id <<= 32;
    server_id |= port_;
  } else {
    LOG_ERROR_RET(OB_ERROR, "this is ipv6 addr", K(*this));
  }
  return server_id;
}

void ObAddr::set_ipv4_server_id(const int64_t ipv4_server_id)
{
  version_ = IPV4;
  ip_.v4_ = static_cast<int32_t>(0x00000000ffffffff & (ipv4_server_id >> 32));
  reset_v4_extraneous();
  port_ = static_cast<int32_t>(0x00000000ffffffff & ipv4_server_id);
}

bool ObAddr::as_mask(int64_t mask_bits, int32_t version)
{
  int ret = false;
  const int64_t MAX_IPV4_MASK_BITS = 32;
  const int64_t MAX_IPV6_MASK_BITS = 128;
  version_ = static_cast<VER>(version);
  if (OB_UNLIKELY(mask_bits < 0)) {
  } else if (version_ == IPV4 && mask_bits <= MAX_IPV4_MASK_BITS) {
    ip_.v4_ = static_cast<uint32_t>(((static_cast<uint64_t>(1) << mask_bits) - 1) << (32 - mask_bits));
    ret = true;
  } else if (version_ == IPV6 && mask_bits <= MAX_IPV6_MASK_BITS) {
    int bytes = 0;
    while (bytes < IPV6_LEN && mask_bits >= 8) {
      ip_.v6_[bytes++] = 0xff;
      mask_bits -= 8;
    }
    if (bytes < IPV6_LEN && mask_bits > 0) {
      ip_.v6_[bytes] = static_cast<uint8_t>(((static_cast<uint8_t>(1) << mask_bits) - 1) << (8 - mask_bits));
    }
    ret = true;
  }
  return ret;
}

ObAddr &ObAddr::as_subnet(const ObAddr &mask)
{
  int bytes = 0;
  while (bytes < IPV6_LEN) {
    ip_.v6_[bytes] &= mask.ip_.v6_[bytes];
    bytes++;
  }
  return (*this);
}

bool ObAddr::operator <(const ObAddr &rv) const
{
  return compare_refactored(rv) < 0;
}

bool ObAddr::operator >(const ObAddr &rv) const
{
  return compare_refactored(rv) > 0;
}

bool ObAddr::is_equal_except_port(const ObAddr &rv) const
{
  return version_ == rv.version_
      && 0 == MEMCMP(&ip_, &rv.ip_, sizeof (rv.ip_));
}

int ObAddr::get_ipv6(void *buff, const int32_t size) const
{
  int ret = OB_SUCCESS;
  if (nullptr != buff && size >= IPV6_LEN) {
    memcpy(buff, ip_.v6_, IPV6_LEN);
  } else {
    ret = OB_SIZE_OVERFLOW;
  }
  return ret;
}

uint64_t ObAddr::get_ipv6_high() const
{
  const uint64_t *p = reinterpret_cast<const uint64_t *>(&ip_.v6_[0]);
  return *p;
}

uint64_t ObAddr::get_ipv6_low() const
{
  const uint64_t *p = reinterpret_cast<const uint64_t *>(&ip_.v6_[8]);
  return *p;
}

void ObAddr::set_max()
{
  port_ = UINT32_MAX;
  memset(&ip_, 0xff, sizeof (ip_));
}

void ObAddr::set_port(int32_t port)
{
  port_ = port;
}

OB_DEF_SERIALIZE(ObAddr)
{
  int ret = OB_SUCCESS;
  uint32_t ip_1 = *reinterpret_cast<const uint32_t *>(&ip_.v6_[0]);
  uint32_t ip_2 = *reinterpret_cast<const uint32_t *>(&ip_.v6_[4]);
  uint32_t ip_3 = *reinterpret_cast<const uint32_t *>(&ip_.v6_[8]);
  uint32_t ip_4 = *reinterpret_cast<const uint32_t *>(&ip_.v6_[12]);
  LST_DO_CODE(OB_UNIS_ENCODE, version_, ip_1, ip_2, ip_3, ip_4, port_);
  return ret;
}

OB_DEF_DESERIALIZE(ObAddr)
{
  int ret = OB_SUCCESS;
  uint32_t ip_1 = 0, ip_2 = 0, ip_3 = 0, ip_4 = 0;
  LST_DO_CODE(OB_UNIS_DECODE, version_, ip_1, ip_2, ip_3, ip_4, port_);
  *reinterpret_cast<uint32_t *>(&ip_.v6_[0]) = ip_1;
  *reinterpret_cast<uint32_t *>(&ip_.v6_[4]) = ip_2;
  *reinterpret_cast<uint32_t *>(&ip_.v6_[8]) = ip_3;
  *reinterpret_cast<uint32_t *>(&ip_.v6_[12]) = ip_4;
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObAddr)
{
  int64_t len = 0;
  uint32_t ip_1 = *reinterpret_cast<const uint32_t *>(&ip_.v6_[0]);
  uint32_t ip_2 = *reinterpret_cast<const uint32_t *>(&ip_.v6_[4]);
  uint32_t ip_3 = *reinterpret_cast<const uint32_t *>(&ip_.v6_[8]);
  uint32_t ip_4 = *reinterpret_cast<const uint32_t *>(&ip_.v6_[12]);
  LST_DO_CODE(OB_UNIS_ADD_LEN, version_, ip_1, ip_2, ip_3, ip_4, port_);
  return len;
}

OB_SERIALIZE_MEMBER(ObAddrWithSeq,
                    server_addr_,
                    server_seq_);

DEF_TO_STRING(ObAddrWithSeq)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(server_addr), K_(server_seq));
  J_OBJ_END();
  return pos;
}
} // end namespace common
} // end namespace oceanbase

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

#ifndef _OB_2_0_PROTOCOL_STRUCT_H_
#define _OB_2_0_PROTOCOL_STRUCT_H_

#include "rpc/obmysql/ob_mysql_packet.h"

namespace oceanbase
{
namespace obmysql
{

// used for proxy and observer to negotiate new features
union Ob20ProtocolFlags
{
  Ob20ProtocolFlags() : flags_(0) {}
  explicit Ob20ProtocolFlags(uint32_t flag) : flags_(flag) {}

  bool is_extra_info_exist() const { return 1 == st_flags_.OB_EXTRA_INFO_EXIST; }
  bool is_last_packet() const { return 1 == st_flags_.OB_IS_LAST_PACKET; }
  bool is_proxy_reroute() const { return 1 == st_flags_.OB_IS_PROXY_REROUTE; }
  bool is_new_extra_info() const { return 1 == st_flags_.OB_IS_NEW_EXTRA_INFO; }
  bool is_weak_read() const { return 1 == st_flags_.OB_IS_WEAK_READ; }
  bool txn_free_route() const { return 1 == st_flags_.OB_TXN_FREE_ROUTE; }
  bool proxy_switch_route() const { return 1 == st_flags_.OB_PROXY_SWITCH_ROUTE; }

  uint32_t flags_;
  struct Protocol20Flags
  {
    uint32_t OB_EXTRA_INFO_EXIST:                       1;
    uint32_t OB_IS_LAST_PACKET:                         1;
    uint32_t OB_IS_PROXY_REROUTE:                       1;
    uint32_t OB_IS_NEW_EXTRA_INFO:                      1;
    uint32_t OB_IS_WEAK_READ:                           1;
    uint32_t OB_TXN_FREE_ROUTE:                         1;
    uint32_t OB_PROXY_SWITCH_ROUTE:                     1;
    uint32_t OB_FLAG_RESERVED_NOT_USE:                 25;
  } st_flags_;
};

class Ob20ProtocolHeader
{
public:
  ObMySQLCompressedPacketHeader cp_hdr_;

  uint16_t magic_num_;
  uint16_t header_checksum_;
  uint32_t connection_id_;
  uint32_t request_id_;
  uint8_t pkt_seq_;
  uint32_t payload_len_;
  Ob20ProtocolFlags flag_;
  uint16_t version_;
  uint16_t reserved_;

public:
  Ob20ProtocolHeader()
    : cp_hdr_(), magic_num_(0), header_checksum_(0),
      connection_id_(0), request_id_(0), pkt_seq_(0), payload_len_(0),
      flag_(0), version_(0), reserved_(0) {}

  ~Ob20ProtocolHeader() {}

  TO_STRING_KV("ob 20 protocol header", cp_hdr_,
               K_(magic_num),
               K_(header_checksum),
               K_(connection_id),
               K_(request_id),
               K_(pkt_seq),
               K_(payload_len),
               K_(version),
               K_(flag_.flags),
               K_(reserved));
};


class Ob20Packet : public rpc::ObPacket
{
public:
  Ob20Packet() : hdr_(), cdata_(NULL) {}
  virtual ~Ob20Packet() {}

  inline uint32_t get_comp_len() const { return hdr_.cp_hdr_.comp_len_; }
  inline uint8_t get_comp_seq() const { return hdr_.cp_hdr_.comp_seq_; }
  inline uint32_t get_uncomp_len() const { return hdr_.cp_hdr_.uncomp_len; }
  inline const char *get_cdata() const { return cdata_; }
  inline Ob20ProtocolFlags get_flags() const { return hdr_.flag_; }
  inline uint32_t get_payload_len() const { return hdr_.payload_len_; }
  inline uint32_t get_mysql_packet_len() {
    uint32_t len = hdr_.payload_len_;
    if (hdr_.flag_.is_extra_info_exist()) {
      len = (uint32_t)(len - extra_info_.extra_len_ - common::OB20_PROTOCOL_EXTRA_INFO_LENGTH);
    }
    return len;
  }
  inline uint8_t get_seq() const { return hdr_.pkt_seq_; }
  inline uint32_t get_request_id() const { return hdr_.request_id_; }
  inline void set_header(const Ob20ProtocolHeader &header) { hdr_ = header; }
  inline void set_content(
      const char *content,
      const Ob20ProtocolHeader &header,
      const Ob20ExtraInfo extra_info)
  {
    set_header(header);
    extra_info_ = extra_info;
    cdata_ = content;
  }
  const Ob20ExtraInfo &get_extra_info() const { return extra_info_; }

  VIRTUAL_TO_STRING_KV("header", hdr_,
                       K_(extra_info));

protected:
  Ob20ProtocolHeader hdr_;
  Ob20ExtraInfo extra_info_;
  const char *cdata_;
};

} // end of namespace obmysql
} // end of namespace oceanbase

#endif /* _OB_2_0_PROTOCOL_STRUCT_H_ */

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

#ifndef _OMPK_SSL_REQUEST_H_
#define _OMPK_SSL_REQUEST_H_

#include "lib/string/ob_string.h"
#include "rpc/obmysql/ob_mysql_packet.h"
#include "lib/container/ob_se_array.h"

namespace oceanbase
{
namespace obmysql
{

using common::ObString;

class OMPKSSLRequest : public ObMySQLPacket
{
public:
  OMPKSSLRequest()
    : capability_(), max_packet_size_(0), character_set_(0) {}

  virtual ~OMPKSSLRequest() { }

  // reset hand shake response
  void reset();

  // decode hand shake response
  virtual int decode();
  // serialize hand shake response data into buffer
  virtual int serialize(char *buffer, const int64_t length, int64_t &pos) const;
  virtual int64_t get_serialize_size() const { return HANDSHAKE_RESPONSE_MIN_SIZE; }

  inline const ObMySQLCapabilityFlags &get_capability_flags() const { return capability_; }
  inline uint32_t get_max_packet_size() const { return max_packet_size_; }
  inline uint8_t get_char_set() const { return character_set_; }

  inline void set_capability_flags(const ObMySQLCapabilityFlags &cap) { capability_ = cap; }
  inline void set_max_packet_size(const uint32_t max_size) { max_packet_size_ = max_size; }
  inline void set_character_set(const uint8_t char_set) { character_set_ = char_set; }

  VIRTUAL_TO_STRING_KV("header", hdr_, K_(capability_.capability), K_(max_packet_size),
                       K_(character_set));
private:
  uint64_t get_connect_attrs_len() const;

private:
  // capability flags of the client as defined in Protocol::CapabilityFlags.
  ObMySQLCapabilityFlags capability_;
  // max size of a command packet that the client wants to send to the server.
  uint32_t max_packet_size_;
  // connection's default character set as defined in Protocol::CharacterSet.
  uint8_t character_set_;
}; // end of class OMPKSSLRequest

} // end of namespace obmysql
} // end of namespace oceanbase

#endif /* _OMPK_SSL_REQUEST_H_ */

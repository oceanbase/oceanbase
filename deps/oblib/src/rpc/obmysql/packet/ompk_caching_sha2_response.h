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

#ifndef _OMPK_CACHING_SHA2_RESPONSE_H_
#define _OMPK_CACHING_SHA2_RESPONSE_H_

#include "rpc/obmysql/ob_mysql_packet.h"

namespace oceanbase
{
namespace obmysql
{

// Helper packet class for sending caching_sha2_password authentication response
class OMPKCachingSha2Response : public ObMySQLPacket
{
public:
  explicit OMPKCachingSha2Response(uint8_t response_byte)
      : response_byte_(response_byte)
  {}
  virtual ~OMPKCachingSha2Response() {}

  virtual int serialize(char *buffer, const int64_t length, int64_t &pos) const;
  virtual int64_t get_serialize_size() const;
  virtual ObMySQLPacketType get_mysql_packet_type() { return ObMySQLPacketType::PKT_STR; }

  VIRTUAL_TO_STRING_KV("header", hdr_, K_(response_byte));

private:
  DISALLOW_COPY_AND_ASSIGN(OMPKCachingSha2Response);
  uint8_t response_byte_;
};

} // end namespace obmysql
} // end namespace oceanbase

#endif /* _OMPK_CACHING_SHA2_RESPONSE_H_ */

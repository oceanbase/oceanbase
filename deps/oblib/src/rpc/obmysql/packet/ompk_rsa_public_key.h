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

#ifndef _OMPK_RSA_PUBLIC_KEY_H_
#define _OMPK_RSA_PUBLIC_KEY_H_

#include "rpc/obmysql/ob_mysql_packet.h"

namespace oceanbase
{
namespace obmysql
{

// Helper packet class for sending RSA public key
class OMPKRsaPublicKey : public ObMySQLPacket
{
public:
  OMPKRsaPublicKey(const char *public_key, int64_t key_len)
      : public_key_(public_key), key_len_(key_len)
  {}
  virtual ~OMPKRsaPublicKey() {}

  virtual int serialize(char *buffer, const int64_t length, int64_t &pos) const;
  virtual int64_t get_serialize_size() const;
  virtual ObMySQLPacketType get_mysql_packet_type() { return ObMySQLPacketType::PKT_STR; }

  VIRTUAL_TO_STRING_KV("header", hdr_, KP_(public_key), K_(key_len));

private:
  DISALLOW_COPY_AND_ASSIGN(OMPKRsaPublicKey);
  const char *public_key_;
  int64_t key_len_;
};

} // end namespace obmysql
} // end namespace oceanbase

#endif /* _OMPK_RSA_PUBLIC_KEY_H_ */

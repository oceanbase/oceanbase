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

#ifndef _OMPK_STRING_H_
#define _OMPK_STRING_H_

#include "rpc/obmysql/ob_mysql_packet.h"
#include "lib/string/ob_string.h"

namespace oceanbase
{
namespace obmysql
{

class OMPKString
    : public ObMySQLPacket
{
public:
  explicit OMPKString(const common::ObString &str)
      : str_(str)
  {}

  virtual int64_t get_serialize_size() const { return str_.length(); }
  virtual ~OMPKString() {};
  virtual int serialize(char *buffer, const int64_t length, int64_t &pos) const
  {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(buffer) || length < pos) {
      ret = OB_INVALID_ARGUMENT;
    } else if (str_.length() > length - pos) {
      ret = OB_SIZE_OVERFLOW;
    } else if (!str_.empty()) {
      MEMCPY(buffer + pos, str_.ptr(), str_.length());
      pos += str_.length();
    }
    return ret;
  }
  inline ObMySQLPacketType get_mysql_packet_type() { return ObMySQLPacketType::PKT_STR; }

  VIRTUAL_TO_STRING_KV("header", hdr_, K_(str));

private:
  DISALLOW_COPY_AND_ASSIGN(OMPKString);
  const common::ObString &str_;
}; // end of class OMPKString

} // end of namespace obmysql
} // end of namespace oceanbase


#endif /* _OMPK_STRING_H_ */

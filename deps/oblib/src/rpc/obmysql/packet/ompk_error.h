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

#ifndef _OMPK_ERROR_H_
#define _OMPK_ERROR_H_

#include "lib/string/ob_string.h"
#include "rpc/obmysql/ob_mysql_packet.h"

namespace oceanbase
{
namespace obmysql
{

class OMPKError : public ObMySQLPacket
{
public:
  static const uint64_t SQLSTATE_SIZE = 5;
  static const uint8_t MARKER = '#';

public:
  OMPKError();
  virtual ~OMPKError();

  virtual int serialize(char *buffer, int64_t len, int64_t &pos) const;
  virtual int64_t get_serialize_size() const;
  virtual int decode();

  int set_message(const common::ObString &message);
  inline void set_errcode(uint16_t code) { errcode_ = code; }
  int set_sqlstate(const char *sqlstate);

  inline uint8_t get_field_count() const { return field_count_; }
  inline uint16_t get_err_code() const { return errcode_; }
  inline common::ObString get_sql_state() const { return sqlstate_; }
  inline common::ObString get_message() const { return message_; }
  virtual int64_t to_string(char *buf, const int64_t buf_len) const;
  inline ObMySQLPacketType get_mysql_packet_type() { return ObMySQLPacketType::PKT_ERR; }

private:

private:
  DISALLOW_COPY_AND_ASSIGN(OMPKError);
  uint8_t field_count_;     // always 0xff
  uint16_t errcode_;
  common::ObString sqlstate_;
  common::ObString message_;
};

} // end of namespace obmysql
} // end of namespace oceanbase


#endif /* _OMPK_ERROR_H_ */

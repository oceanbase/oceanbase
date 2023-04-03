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

#ifndef _OMPK_EOF_H_
#define _OMPK_EOF_H_

#include "rpc/obmysql/ob_mysql_packet.h"
#include "lib/ob_define.h"

namespace oceanbase
{
namespace obmysql
{

// In the MySQL client/server protocol, EOF and OK packets serve the same purpose,
// to mark the end of a query execution result. Due to changes in MySQL 5.7 in the
// OK packet (such as session state tracking), and to avoid repeating the changes
// in the EOF packet, the EOF packet is deprecated as of MySQL 5.7.5.
class OMPKEOF : public ObMySQLPacket
{
public:
  OMPKEOF();
  virtual ~OMPKEOF();

  virtual int serialize(char *buffer, int64_t len, int64_t &pos) const;
  virtual int64_t get_serialize_size() const;
  virtual int decode();

  inline void set_warning_count(const uint16_t count) { warning_count_ = count; }
  inline void set_server_status(const ObServerStatusFlags status) { server_status_ = status; }

  inline uint8_t get_field_count() const { return field_count_; }
  inline uint16_t get_warning_count() const { return warning_count_; }
  inline ObServerStatusFlags  get_server_status() const { return server_status_; }
  inline ObMySQLPacketType get_mysql_packet_type() { return ObMySQLPacketType::PKT_EOF; }
  virtual int64_t to_string(char *buf, const int64_t buf_len) const;
private:
  DISALLOW_COPY_AND_ASSIGN(OMPKEOF);

  uint8_t field_count_;    // always 0xfe
  uint16_t warning_count_;
  ObServerStatusFlags server_status_;
};

} // end of namespace obmysql
} // end of namespace oceanbase

#endif /* _OMPK_EOF_H_ */

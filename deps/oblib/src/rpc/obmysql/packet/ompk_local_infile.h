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

#ifndef _OMPK_LOCAL_INFILE_H_
#define _OMPK_LOCAL_INFILE_H_

#include "rpc/obmysql/ob_mysql_packet.h"
#include "lib/ob_define.h"

namespace oceanbase
{
namespace obmysql
{

// In the MySQL client/server protocol, server send a `local infile` message to client
// to specific the file name to load.
// format:
// int<1>      | packet type  | 0xFB: LOCAL INFILE
// string<EOF> | filename     | the path to the file the client shall send
// The notation is "string<EOF>" Strings whose length will be calculated by the packet remaining length.
class OMPKLocalInfile : public ObMySQLRawPacket
{
public:
  OMPKLocalInfile();
  virtual ~OMPKLocalInfile();

  virtual int serialize(char *buffer, int64_t len, int64_t &pos) const override;
  virtual int64_t get_serialize_size() const override;

  virtual int64_t to_string(char *buf, const int64_t buf_len) const override;

  void set_filename(const ObString &filename);

  inline ObMySQLPacketType get_mysql_packet_type() override { return ObMySQLPacketType::PKT_FILENAME; }

private:
  DISALLOW_COPY_AND_ASSIGN(OMPKLocalInfile);
  int8_t   packet_type_;
  ObString filename_;
};

} // end of namespace obmysql
} // end of namespace oceanbase

#endif /* _OMPK_LOCAL_INFILE_H_ */

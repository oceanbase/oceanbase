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

#ifndef _OB_2_0_PROTOCOL_PROCESSOR_H_
#define _OB_2_0_PROTOCOL_PROCESSOR_H_

#include "rpc/obmysql/ob_mysql_protocol_processor.h"

namespace oceanbase {
namespace rpc {
class ObPacket;
}
namespace obmysql {
class Ob20ProtocolHeader;

class Ob20ProtocolProcessor : public ObMysqlProtocolProcessor {
public:
  Ob20ProtocolProcessor(ObMySQLHandler& handler) : ObMysqlProtocolProcessor(handler)
  {}
  virtual ~Ob20ProtocolProcessor()
  {}

  virtual int decode(easy_message_t* m, rpc::ObPacket*& pkt);
  virtual int process(easy_request_t* r, bool& need_decode_more);

protected:
  int do_header_checksum(char* origin_start, const Ob20ProtocolHeader& hdr);
  int do_body_checksum(easy_message_t& m, const Ob20ProtocolHeader& hdr);
  int decode_ob20_body(easy_message_t& m, const Ob20ProtocolHeader& hdr, rpc::ObPacket*& pkt);
  int process_ob20_packet(easy_connection_t& c, easy_pool_t& pool, void*& ipacket, bool& need_decode_more);

private:
  DISALLOW_COPY_AND_ASSIGN(Ob20ProtocolProcessor);
};

}  // end of namespace obmysql
}  // end of namespace oceanbase

#endif /* _OB_2_0_PROTOCOL_PROCESSOR_H_ */

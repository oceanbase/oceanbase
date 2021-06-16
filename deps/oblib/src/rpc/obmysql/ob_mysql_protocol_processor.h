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

#ifndef _OB_MYSQL_PROTOCOL_PROCESSOR_H_
#define _OB_MYSQL_PROTOCOL_PROCESSOR_H_

#include "lib/ob_define.h"
#include "rpc/obmysql/ob_mysql_packet.h"
#include "rpc/obmysql/ob_virtual_cs_protocol_processor.h"

namespace oceanbase {
namespace rpc {
class ObPacket;
}
namespace obmysql {
class ObMysqlPktContext;

class ObMysqlProtocolProcessor : public ObVirtualCSProtocolProcessor {
public:
  ObMysqlProtocolProcessor(ObMySQLHandler& handler) : ObVirtualCSProtocolProcessor(handler)
  {}
  virtual ~ObMysqlProtocolProcessor()
  {}

  virtual int decode(easy_message_t* m, rpc::ObPacket*& pkt) override;
  virtual int process(easy_request_t* r, bool& is_going_on) override;

protected:
  int decode_hsr_body(easy_message_t& m, const uint32_t pktlen, const uint8_t pktseq, rpc::ObPacket*& pkt);
  int decode_sslr_body(easy_message_t& m, const uint32_t pktlen, const uint8_t pktseq, rpc::ObPacket*& pkt);

  int decode_body(easy_message_t& m, const uint32_t pktlen, const uint8_t pktseq, rpc::ObPacket*& pkt);

  int decode_header(char*& buf, uint32_t& buf_size, uint32_t& pktlen, uint8_t& pktseq);

  int check_mysql_packet_len(const uint32_t packet_len) const;

  int read_header(ObMysqlPktContext& context, const char* start, const int64_t len, int64_t& pos);

  int read_body(ObMysqlPktContext& context, easy_pool_t& pool, const char* start, const int64_t len, void*& ipacket,
      bool& need_decode_more, int64_t& pos);

  int process_fragment_mysql_packet(easy_connection_t& c, easy_pool_t& pool, const char* start, const int64_t len,
      void*& ipacket, bool& need_decode_more);

  int process_one_mysql_packet(ObMysqlPktContext& context, easy_pool_t& pool, const int64_t actual_data_len,
      void*& ipacket, bool& need_decode_more);

  int process_mysql_packet(easy_connection_t& c, easy_pool_t& pool, void*& ipacket, bool& need_decode_more);

private:
  DISALLOW_COPY_AND_ASSIGN(ObMysqlProtocolProcessor);
};

inline int ObMysqlProtocolProcessor::check_mysql_packet_len(const uint32_t packet_len) const
{
  INIT_SUCC(ret);
  if (packet_len > OB_MYSQL_MAX_PAYLOAD_LENGTH) {  // packet len can not > 2^24 - 1
    ret = common::OB_INVALID_ARGUMENT;
    COMMON_LOG(ERROR, "invalid packet len", K(packet_len), K(ret));
  }
  return ret;
}

}  // end of namespace obmysql
}  // end of namespace oceanbase

#endif /* _OB_MYSQL_PROTOCOL_PROCESSOR_H_ */

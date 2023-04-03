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

#ifndef _OB_MYSQL_COMPRESS_PROTOCOL_PROCESSOR_H_
#define _OB_MYSQL_COMPRESS_PROTOCOL_PROCESSOR_H_

#include "rpc/obmysql/ob_mysql_protocol_processor.h"
#include "rpc/obmysql/ob_mysql_request_utils.h"

namespace oceanbase
{
namespace rpc
{
class ObPacket;
}
namespace obmysql
{
class ObCompressedPktContext;

class ObMysqlCompressProtocolProcessor : public ObMysqlProtocolProcessor
{
public:
   ObMysqlCompressProtocolProcessor()
    : ObMysqlProtocolProcessor() {}
  virtual ~ObMysqlCompressProtocolProcessor() {}

  virtual int do_decode(observer::ObSMConnection& conn, ObICSMemPool& pool, const char*& start, const char* end, rpc::ObPacket*& pkt, int64_t& next_read_bytes);
  virtual int do_splice(observer::ObSMConnection& conn, ObICSMemPool& pool, void*& pkt, bool& need_decode_more);

private:
  int decode_compressed_body(ObICSMemPool& pool, const char*& buf, const uint32_t comp_pktlen,
                             const uint8_t comp_pktseq,
                             const uint32_t pktlen_before_compress,
                             rpc::ObPacket *&pkt);

  int decode_compressed_packet(const char *comp_buf, const uint32_t comp_pktlen,
                               const uint32_t pktlen_before_compress, char *&pkt_body,
                               const uint32_t pkt_body_size);

  int process_compressed_packet(ObCompressedPktContext& context, ObMysqlPktContext &mysql_pkt_context,
                                obmysql::ObPacketRecordWrapper &pkt_rec_wrapper, ObICSMemPool& pool,
                                void *&ipacket, bool &need_decode_more);

private:
  DISALLOW_COPY_AND_ASSIGN(ObMysqlCompressProtocolProcessor);
};

} // end of namespace obmysql
} // end of namespace oceanbase

#endif /* _OB_MYSQL_COMPRESS_PROTOCOL_PROCESSOR_H_ */

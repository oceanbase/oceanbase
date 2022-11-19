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

#ifndef _OB_RPC_PROTOCOL_PROCESSOR_H_
#define _OB_RPC_PROTOCOL_PROCESSOR_H_

#include "rpc/obrpc/ob_rpc_compress_struct.h"
#include "rpc/obrpc/ob_rpc_packet.h"
#include "rpc/obrpc/ob_virtual_rpc_protocol_processor.h"

namespace oceanbase
{
namespace common
{
class ObTimeGuard;
}
namespace obrpc
{

class ObRpcProtocolProcessor: public ObVirtualRpcProtocolProcessor
{
public :
  ObRpcProtocolProcessor() {}
  virtual ~ObRpcProtocolProcessor() {}

  virtual int encode(easy_request_t *req, ObRpcPacket *pkt);
  virtual int decode(easy_message_t *ms, ObRpcPacket *&pkt);
private :
  /*
   *@param [out] is_demand_data_enough:  true  if  length of received data is enough to decode a packet, or false
   *@param [out]  preceding_data_len: data len need skip when decode packet, when beginnign with a cmdPacket, preceding_data_len is not zero
   *@param [out]  decode_data_len: len of data to decode
   */
  int resolve_packet_type(common::ObTimeGuard &timeguard,
                          easy_message_t *ms,
                          bool &is_demand_data_enough,
                          int64_t &preceding_data_len,
                          int64_t &decode_data_len);
  int init_compress_ctx(easy_connection_t *conn,
                        ObRpcCompressMode mode,
                        int16_t block_size,
                        int32_t ring_buffer_size);
  int init_decompress_ctx(easy_connection_t *conn,
                          ObRpcCompressMode mode,
                          int16_t block_size,
                          int32_t ring_buffer_size);
  int init_ctx(easy_connection_t *conn,
               ObRpcCompressMode mode,
               int16_t block_size,
               int32_t ring_buffer_size,
               bool is_compress);
};

}//end of namespace obrpc
}//end of namespace oceanbase
#endif

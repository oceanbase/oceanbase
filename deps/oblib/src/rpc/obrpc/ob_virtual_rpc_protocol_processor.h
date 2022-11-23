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

#ifndef _OB_VIRTUAL_RPC_PROTOCOL_PROCESSOR_H_
#define _OB_VIRTUAL_RPC_PROTOCOL_PROCESSOR_H_

#include "lib/ob_define.h"
#include "rpc/frame/ob_req_deliver.h"
#include "rpc/obrpc/ob_rpc_packet.h"
#include "rpc/obrpc/ob_rpc_compress_struct.h"

namespace oceanbase
{
namespace common
{
class ObStreamCompressor;
class ObTimeGuard;
};

namespace obrpc
{

class ObVirtualRpcProtocolProcessor
{
public:
  ObVirtualRpcProtocolProcessor(){}
  virtual ~ObVirtualRpcProtocolProcessor() {}

  virtual int encode(easy_request_t *req, ObRpcPacket *pkt) = 0;
  virtual int decode(easy_message_t *m, ObRpcPacket *&pkt) = 0;

protected:
  /*
   *@param [in] timeguard:
   *@param [in] req:
   *@param [in] pkt: packet data to send
   *@param [out] full_size: total data len need to send after compressing
   * */
  int encode_compressed_rpc_packet(common::ObTimeGuard &timeguard,
                                   easy_request_t *req, ObRpcPacket *&pkt, int32_t &full_size);

  /*this func is to encode rpc packet as  without compressing
   *@param [in] timeguard:
   *@param [in] req:
   *@param [in] pkt: packet data to send
   *@param [out] full_size: total data len need to send
   */
  int encode_raw_rpc_packet(common::ObTimeGuard &timeguard,
                            easy_request_t *req,
                            ObRpcPacket *&pkt,
                            int32_t &full_size);

  /*this func is to  compress data beginning at segment , and then  set it to
   * send_list of easy buf
   *@param [in] timeguard:
   *@param [in] segment: data to compress
   *@param [in] segment_size: size of data to compress
   *@param [in] header_size: size of packet header outsize
   *@param [in] header_buf: buf to place packet header
   *@param [out] compressed_size: data len of compressed data
   */
  int encode_segment(easy_request_t *req,
                     ObRpcCompressCCtx &compress_ctx,
                     char *segment, int64_t segment_size,
                     int64_t header_size, char *&header_buf,
                     int64_t &compressed_size);

  /*
   *@param [in] preceding_data_len: cmd_packet锁占的内存大小,
   ms->input->pos + preceding_data_len 即为压缩后的packet的起始位置
   *@param [in] decode_data_len: 压缩后数据的总大小
   * */
  int decode_compressed_packet_data(common::ObTimeGuard &timeguard,
                                    easy_message_t *ms,
                                    const int64_t preceding_data_len,
                                    const int64_t decode_data_len,
                                    ObRpcPacket *&pkt);
  /*
   * this func is to decode data after decompressed as a net rpc packet
   *@param [in] data: 开始的sizeof(ObRpcPacket)个字节用于分配ObRpcPacket, 接下来的数据是一个完整的net packet
   *@param [in] data_len: data的长度
   */
  int decode_net_rpc_packet(common::ObTimeGuard &timeguard,
                            char *data, int64_t data_len, ObRpcPacket *&pkt);

  /*
   * this func is to decode original data  as a net rpc packet, data is not compressed
   */
  int decode_raw_net_rpc_packet(common::ObTimeGuard &timeguard,
                                easy_message_t *m, const int64_t preceding_data_len, ObRpcPacket *&pkt);

  char *easy_alloc(easy_pool_t *pool, int64_t size) const;
  inline int set_next_read_len(easy_message_t *m,
                               const int64_t fallback_len,
                               const int64_t next_read_len);

  int reset_compress_ctx(easy_message_t *m,
                         ObRpcCompressMode mode,
                         bool &need_send_cmd_packet,
                         bool &is_still_need_compress);
  /*
   * this func is to decompress segment data
   *@param [in] ctx : ctx of decompress
   *@param [in] data + pos: data ptr to decompress
   *@param [in] net_packet_buf: buf to place decompressed data, used with
   *@param [in] net_packet_buf_size: size of net buf
   *@param [in] net_packet_buf_pos: pos to place decompressed data
   *@param [in] compressed_size: size of data  to decompress
   *@param [in] original_size: size of original data  before compressing
   */
  int decode_segment(ObRpcCompressDCtx &ctx,
                     char *data,
                     char *net_packet_buf,
                     int64_t net_packet_buf_size,
                     bool is_data_compressed,
                     int16_t compressed_size,
                     int16_t original_size,
                     int64_t &pos,
                     int64_t &net_packet_buf_pos);
protected:
  //const int16_t COMPRESS_BLOCK_SIZE = 1024;
  const int64_t MAX_COMPRESS_DATA_SIZE = (1UL << 32) - 1024;// 4G - 1K
  const int16_t COMPRESS_BLOCK_SIZE = (1 << 15) - 1024;// 31K
  const int32_t COMPRESS_RING_BUFFER_SIZE = 1 << 17;//128K
  const int64_t ENCODE_SEGMENT_COST_TIME = 3 * 1000;//3ms
  const int64_t DECODE_SEGMENT_COST_TIME = 3 * 1000;//3ms
};


} // end of namespace obmysql
} // end of namespace oceanbase

#endif /* _OB_VIRTUAL_RPC_PROTOCOL_PROCESSOR_H_ */

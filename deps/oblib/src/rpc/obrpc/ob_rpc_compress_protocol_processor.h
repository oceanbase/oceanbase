/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef _OB_RPC_COMPRESS_PROTOCOL_PROCESSOR_H_
#define _OB_RPC_COMPRESS_PROTOCOL_PROCESSOR_H_


#include "io/easy_io_struct.h"
#include "rpc/obrpc/ob_rpc_compress_struct.h"
#include "rpc/obrpc/ob_virtual_rpc_protocol_processor.h"

namespace oceanbase
{
namespace common
{
class ObStreamCompressor;
};

namespace obrpc
{
class ObRpcCompressProtocolProcessor: public ObVirtualRpcProtocolProcessor
{
public:
  ObRpcCompressProtocolProcessor() {}
  virtual ~ObRpcCompressProtocolProcessor() {}

  virtual int encode(easy_request_t *req, ObRpcPacket *pkt);
  virtual int decode(easy_message_t *ms, ObRpcPacket *&pkt);
private:
  int reset_compress_ctx_mode(easy_connection_t *conn,
                              ObRpcCompressMode mode,
                              ObRpcPacket *&pkt,
                              ObCmdPacketInCompress::CmdType &cmd_type,
                              bool &is_still_need_compress);

  int reset_decompress_ctx_mode(easy_connection_t *easy_conn,
                                ObRpcCompressMode mode);

  int reset_decompress_ctx_ctx(easy_connection_t *easy_conn);
};

}//end of namespace obrpc
}//end of namespace oceanbase
#endif

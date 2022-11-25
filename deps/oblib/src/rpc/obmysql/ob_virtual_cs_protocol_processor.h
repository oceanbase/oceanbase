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

#ifndef _OB_VIRTUAL_CS_PROTOCOL_PROCESSOR_H_
#define _OB_VIRTUAL_CS_PROTOCOL_PROCESSOR_H_

#include "lib/ob_define.h"
#include "rpc/frame/ob_req_handler.h"
#include "rpc/frame/ob_req_deliver.h"
#include "rpc/obmysql/ob_i_cs_mem_pool.h"

namespace oceanbase
{
namespace rpc
{
class ObPacket;
}
namespace observer
{
struct ObSMConnection;
}
namespace obmysql
{
class ObMySQLHandler;
class ObVirtualCSProtocolProcessor
{
public:
  ObVirtualCSProtocolProcessor() {}
  virtual ~ObVirtualCSProtocolProcessor() {}

  int decode(easy_message_t *m, rpc::ObPacket *&pkt) { return easy_decode(m, pkt); }
  int process(easy_request_t *r, bool &need_read_more) { return easy_process(r, need_read_more); }

  virtual int easy_decode(easy_message_t *m, rpc::ObPacket *&pkt);
  virtual int easy_process(easy_request_t *r, bool &need_read_more);

  virtual int do_decode(observer::ObSMConnection& conn, ObICSMemPool& pool, const char*& start, const char* end, rpc::ObPacket*& pkt, int64_t& next_read_bytes) = 0;
  virtual int do_splice(observer::ObSMConnection& conn, ObICSMemPool& pool, void*& pkt, bool& need_decode_more) = 0;
};

} // end of namespace obmysql
} // end of namespace oceanbase

#endif /* _OB_VIRTUAL_CS_PROTOCOL_PROCESSOR_H_ */

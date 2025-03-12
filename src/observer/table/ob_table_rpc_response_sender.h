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

#ifndef _OB_TABLE_RPC_RESPONSE_SENDER_H
#define _OB_TABLE_RPC_RESPONSE_SENDER_H 1
#include "rpc/ob_request.h"
#include "rpc/obrpc/ob_rpc_packet.h"
#include "rpc/frame/ob_req_processor.h"
#include "rpc/obmysql/ob_mysql_request_utils.h"
#include "rpc/obrpc/ob_rpc_result_code.h"
#include "lib/oblog/ob_warning_buffer.h"
#include "ob_table_rpc_processor_util.h"
namespace oceanbase
{
namespace obrpc
{
// this class is copied from ObRpcProcessor
class ObTableRpcResponseSender
{
public:
  ObTableRpcResponseSender(rpc::ObRequest *req, table::ObITableResult *result, const int exec_ret_code = common::OB_SUCCESS)
      :req_(req),
       result_(result),
       exec_ret_code_(exec_ret_code),
       pcode_(ObRpcPacketCode::OB_INVALID_RPC_CODE),
       using_buffer_(NULL)
  {
    if (OB_NOT_NULL(req_)) {
      const ObRpcPacket *rpc_pkt = &reinterpret_cast<const ObRpcPacket&>(req_->get_packet());
      pcode_ = rpc_pkt->get_pcode();
    }
  }
  ObTableRpcResponseSender()
      : req_(nullptr),
        result_(nullptr),
        exec_ret_code_(common::OB_SUCCESS),
        pcode_(ObRpcPacketCode::OB_INVALID_RPC_CODE),
        using_buffer_(nullptr)
  {
  }
  virtual ~ObTableRpcResponseSender() = default;
  int response(const int cb_param);
  OB_INLINE void set_pcode(ObRpcPacketCode pcode) { pcode_ = pcode; }
  OB_INLINE void set_req(rpc::ObRequest *req)
  {
    req_ = req;
    if (OB_NOT_NULL(req_)) {
      const ObRpcPacket *rpc_pkt = &reinterpret_cast<const ObRpcPacket&>(req_->get_packet());
      pcode_ = rpc_pkt->get_pcode();
    }
  }
  OB_INLINE const rpc::ObRequest* get_req() const { return req_; }
  OB_INLINE void set_result(table::ObITableResult *result) { result_ = result; }
private:
  int serialize();
  int do_response(ObRpcPacket *response_pkt, bool require_rerouting);
  char *easy_alloc(int64_t size) const;
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObTableRpcResponseSender);
private:
  rpc::ObRequest *req_;
  table::ObITableResult *result_;
  const int exec_ret_code_; // processor执行的返回码
  ObRpcPacketCode pcode_;
  common::ObDataBuffer *using_buffer_;
};

} // end namespace obrpc
} // end namespace oceanbase

#endif /* _OB_TABLE_RPC_RESPONSE_SENDER_H */

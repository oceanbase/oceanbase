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

#ifndef OB_DTL_RPC_CHANNEL_H
#define OB_DTL_RPC_CHANNEL_H

#include <stdint.h>
#include <functional>
#include "lib/queue/ob_fixed_queue.h"
#include "lib/queue/ob_link_queue.h"
#include "lib/time/ob_time_utility.h"
#include "lib/utility/ob_print_utils.h"
#include "sql/dtl/ob_dtl_channel.h"
#include "sql/dtl/ob_dtl_linked_buffer.h"
#include "share/ob_scanner.h"
#include "observer/ob_server_struct.h"
#include "sql/dtl/ob_dtl_rpc_proxy.h"
#include "sql/dtl/ob_dtl_basic_channel.h"

namespace oceanbase {

// forward declarations
namespace common {
class ObNewRow;
class ObScanner;
class ObThreadCond;
}  // common

namespace sql {
namespace dtl {

// Rpc channel is "rpc version" of channel. As the name explained,
// this kind of channel will do exchange between two tasks by using
// rpc calls.
class ObDtlRpcChannel
    : public ObDtlBasicChannel
{
  friend class ObDtlBcastService;
  class SendMsgCB : public obrpc::ObDtlRpcProxy::AsyncCB<obrpc::OB_DTL_SEND>
  {
  public:
    explicit SendMsgCB(SendMsgResponse &response, const common::ObCurTraceId::TraceId trace_id, const int64_t timeout_ts)
        : response_(response),
          timeout_ts_(timeout_ts)
    {
      trace_id_.set(trace_id);
    }
    virtual int process() override;
    virtual void on_invalid() override;
    virtual void on_timeout() override;
    virtual rpc::frame::ObReqTransport::AsyncCB *clone(const rpc::frame::SPAlloc &alloc) const override;
    virtual void set_args(const AsyncCB::Request &arg) override { UNUSED(arg); }
  private:
    SendMsgResponse &response_;
    common::ObCurTraceId::TraceId trace_id_;
    int64_t timeout_ts_;
  };

  class SendBCMsgCB : public obrpc::ObDtlRpcProxy::AsyncCB<obrpc::OB_DTL_BC_SEND>
  {
  public:
    explicit SendBCMsgCB(const common::ObCurTraceId::TraceId trace_id) : responses_()
    {
      trace_id_.set(trace_id);
    }
    virtual int process() override;
    virtual void on_invalid() override;
    virtual void on_timeout() override;
    virtual void destroy() override;
    virtual rpc::frame::ObReqTransport::AsyncCB *clone(const rpc::frame::SPAlloc &alloc) const override;
    virtual void set_args(const AsyncCB::Request &arg) override { UNUSED(arg); }
    int assign_resp(const ObIArray<SendMsgResponse *> &resps) {
      return responses_.assign(resps);
    }
  private:
    common::ObSEArray<SendMsgResponse *, 4> responses_;
    common::ObCurTraceId::TraceId trace_id_;
  };

public:
  explicit ObDtlRpcChannel(const uint64_t tenant_id,
     const uint64_t id, const common::ObAddr &peer, DtlChannelType type);
  explicit ObDtlRpcChannel(const uint64_t tenant_id, const uint64_t id, const common::ObAddr &peer,
                           const int64_t hash_val, DtlChannelType type);
  virtual ~ObDtlRpcChannel();

  virtual int init() override;
  virtual void destroy();

  virtual int feedup(ObDtlLinkedBuffer *&buffer) override;
  virtual int send_message(ObDtlLinkedBuffer *&buf);

  bool recv_sqc_fin_res() { return recv_sqc_fin_res_; }
private:
  bool recv_sqc_fin_res_;
};

}  // dtl
}  // sql
}  // oceanbase

#endif /* OB_DTL_CHANNEL_H */

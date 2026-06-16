/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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

class ObDtlRpcChannel;

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

  class SendBatchMsgCB : public obrpc::ObDtlRpcProxy::AsyncCB<obrpc::OB_DTL_BATCH_SEND>
  {
  public:
    explicit SendBatchMsgCB(const common::ObCurTraceId::TraceId trace_id) : responses_()
    {
      trace_id_.set(trace_id);
    }
  public:
    virtual int process() override;
    virtual void on_invalid() override;
    virtual void on_timeout() override;
    virtual void destroy() override;
    virtual rpc::frame::ObReqTransport::AsyncCB *clone(const rpc::frame::SPAlloc &alloc) const override;
    virtual void set_args(const AsyncCB::Request &arg) override { UNUSED(arg); }
    int assign_resp(const ObIArray<SendMsgResponse *> &resps) {
      return responses_.assign(resps);
    }
    ObIArray<SendMsgResponse *> &get_responses() { return responses_; }
  private:
    ObTMArray<SendMsgResponse *> responses_;
    ObCurTraceId::TraceId trace_id_;
  };

public:
  // Bounded wait policy for blocked sibling channels in batch_flush_server_group().
  // See that function for the full rationale.
  static constexpr int64_t BATCH_FLUSH_PRIORITY_WAIT_CNT = 2;
  static constexpr int64_t BATCH_FLUSH_PRIORITY_WAIT_US  = 1 * 1000;  // 1ms
  static constexpr int64_t BATCH_FLUSH_NORMAL_WAIT_US    = 50;        // 50us
  static constexpr int64_t BATCH_FLUSH_WAIT_BUDGET_US    = 10 * 1000; // 10ms
  explicit ObDtlRpcChannel(const uint64_t tenant_id,
     const uint64_t id, const common::ObAddr &peer, DtlChannelType type);
  explicit ObDtlRpcChannel(const uint64_t tenant_id, const uint64_t id, const common::ObAddr &peer,
                           const int64_t hash_val, DtlChannelType type);
  virtual ~ObDtlRpcChannel();

  virtual int init(ObDtlFlowControl *dfc = nullptr) override;
  virtual void destroy();

  virtual int feedup(ObDtlLinkedBuffer *&buffer) override;
  virtual int send_message(ObDtlLinkedBuffer *&buf);

  bool recv_sqc_fin_res() { return recv_sqc_fin_res_; }

  int pop_write_buffer(ObDtlLinkedBuffer *&buffer);
  // Override: called when defer_flush_ is true and send_list_ has pending buffers.
  // Triggers batch flush when enough buffers have accumulated in the server group.
  virtual int on_buffer_pending() override;

  // Seal the current write_buffer_ and push it to send_list_.
  // Used by batch send to collect partially-filled buffers from sibling channels.
  int seal_write_buffer();
  // Batch flush all pending buffers from all channels in the server group.
  // Best-effort aggregation: collects whatever buffers are available and sends them.
  int batch_flush_server_group();
  static constexpr int64_t MAX_BATCH_SIZE = 2LL * 1024 * 1024 * 1024;  // 2GB soft limit

private:
  int do_single_send(ObDtlLinkedBuffer *&buf,
                     const common::ObCurTraceId::TraceId &trace_id);
  bool recv_sqc_fin_res_;
};

}  // dtl
}  // sql
}  // namespace oceanbase

#endif /* OB_DTL_CHANNEL_H */

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

#ifndef OB_PX_RPC_PROCESSOR_H
#define OB_PX_RPC_PROCESSOR_H

#include "rpc/obrpc/ob_rpc_processor.h"
#include "share/interrupt/ob_global_interrupt_call.h"
#include "sql/engine/px/ob_px_rpc_proxy.h"
#include "sql/engine/ob_des_exec_context.h"
#include "sql/engine/ob_physical_plan.h"


namespace oceanbase {
namespace sql {

class ObPxSqcHandler;

class ObInitSqcP
    : public obrpc::ObRpcProcessor<obrpc::ObPxRpcProxy::ObRpc<obrpc::OB_PX_ASYNC_INIT_SQC>>
{
public:
  ObInitSqcP(const observer::ObGlobalContext &gctx)
    : exec_ctx_(CURRENT_CONTEXT->get_arena_allocator(), gctx.session_mgr_),
      phy_plan_(),
      unregister_interrupt_(false)
  {}
  virtual ~ObInitSqcP() = default;
  virtual int init() final;
  virtual void destroy() final;
  virtual int process() final;
  virtual int after_process(int error_code) final;
private:
  int pre_setup_op_input(ObPxSqcHandler &sqc_handler);
  int startup_normal_sqc(ObPxSqcHandler &sqc_handler);
private:
  sql::ObDesExecContext exec_ctx_;
  sql::ObPhysicalPlan phy_plan_;
  bool unregister_interrupt_;
};


class ObInitTaskP
    : public obrpc::ObRpcProcessor<obrpc::ObPxRpcProxy::ObRpc<obrpc::OB_PX_INIT_TASK> >
{
public:
  ObInitTaskP(const observer::ObGlobalContext &gctx)
    : exec_ctx_(CURRENT_CONTEXT->get_arena_allocator(), gctx.session_mgr_),
      phy_plan_()
  {}
  virtual ~ObInitTaskP() = default;
  virtual int init() final;
  virtual int process() final;
  virtual int after_process(int error_code) final;
private:
  sql::ObDesExecContext exec_ctx_;
  sql::ObPhysicalPlan phy_plan_;
  //observer::ObVirtualTableIteratorFactory vt_iter_factory_;
  //share::schema::ObSchemaGetterGuard schema_guard_;

};


class ObInitFastSqcP
    : public obrpc::ObRpcProcessor<obrpc::ObPxRpcProxy::ObRpc<obrpc::OB_PX_FAST_INIT_SQC> >
{
public:
  ObInitFastSqcP(const observer::ObGlobalContext &gctx)
    : exec_ctx_(CURRENT_CONTEXT->get_arena_allocator(), gctx.session_mgr_),
      phy_plan_()
  {}
  virtual ~ObInitFastSqcP() = default;
  virtual int init() final;
  virtual void destroy() final;
  virtual int process() final;
private:
  int startup_normal_sqc(ObPxSqcHandler &sqc_handler);
private:
  sql::ObDesExecContext exec_ctx_;
  sql::ObPhysicalPlan phy_plan_;
};


class ObFastInitSqcReportQCMessageCall
{
public:
  ObFastInitSqcReportQCMessageCall(ObPxSqcMeta *sqc,
      int err,
      int64_t timeout_ts,
      bool need_set_not_alive) : sqc_(sqc), err_(err),
      need_interrupt_(false), timeout_ts_(timeout_ts),
      need_set_not_alive_(need_set_not_alive)
  {
    need_interrupt_ = true;
  }
  ~ObFastInitSqcReportQCMessageCall() = default;
  void operator() (hash::HashMapPair<ObInterruptibleTaskID,
      ObInterruptCheckerNode *> &entry);
  int mock_sqc_finish_msg();
public:
  ObPxSqcMeta *sqc_;
  int err_;
  bool need_interrupt_;
  int64_t timeout_ts_;
  bool need_set_not_alive_;
};

class ObDealWithRpcTimeoutCall
{
public:
  ObDealWithRpcTimeoutCall(common::ObAddr addr,
      ObQueryRetryInfo *retry_info,
      int64_t timeout_ts,
      common::ObCurTraceId::TraceId &trace_id) : addr_(addr), retry_info_(retry_info),
      timeout_ts_(timeout_ts), trace_id_(trace_id), ret_(common::OB_TIMEOUT) {}
  ~ObDealWithRpcTimeoutCall() = default;
  void operator() (hash::HashMapPair<ObInterruptibleTaskID,
      ObInterruptCheckerNode *> &entry);
  void deal_with_rpc_timeout_err();
public:
  common::ObAddr addr_;
  ObQueryRetryInfo *retry_info_;
  int64_t timeout_ts_;
  common::ObCurTraceId::TraceId trace_id_;
  int ret_;
};

class ObFastInitSqcCB
      : public obrpc::ObPxRpcProxy::AsyncCB<obrpc::OB_PX_FAST_INIT_SQC>
{
public:
    ObFastInitSqcCB(const common::ObAddr &server,
                    const common::ObCurTraceId::TraceId &trace_id,
                    ObQueryRetryInfo *retry_info,
                    int64_t timeout_ts,
                    ObInterruptibleTaskID tid,
                    ObPxSqcMeta *sqc)
        : addr_(server), retry_info_(retry_info),
          timeout_ts_(timeout_ts), interrupt_id_(tid),
          sqc_(sqc)
  {
    trace_id_.set(trace_id);
  }
  virtual ~ObFastInitSqcCB() {}
public:
  virtual int process();
  virtual void on_invalid() {}
  virtual void on_timeout();
  rpc::frame::ObReqTransport::AsyncCB *clone(
      const rpc::frame::SPAlloc &alloc) const
  {
    void *buf = alloc(sizeof(*this));
    rpc::frame::ObReqTransport::AsyncCB *newcb = NULL;
    if (NULL != buf) {
      newcb = new (buf) ObFastInitSqcCB(addr_, trace_id_, retry_info_,
          timeout_ts_, interrupt_id_, sqc_);
    }
    return newcb;
  }
  virtual void set_args(const Request &arg) { UNUSED(arg); }
  int deal_with_rpc_timeout_err_safely();
  void interrupt_qc(int err);
  void log_warn_sqc_fail(int ret);
private:
  common::ObAddr addr_;
  ObQueryRetryInfo *retry_info_;
  int64_t timeout_ts_;
  ObInterruptibleTaskID interrupt_id_;
  ObPxSqcMeta *sqc_;
  common::ObCurTraceId::TraceId trace_id_;
  DISALLOW_COPY_AND_ASSIGN(ObFastInitSqcCB);
};

class ObPxTenantTargetMonitorP
    : public obrpc::ObRpcProcessor<obrpc::ObPxRpcProxy::ObRpc<obrpc::OB_PX_TARGET_REQUEST> >
{
public:
  ObPxTenantTargetMonitorP(const observer::ObGlobalContext &global_ctx) : 
      global_ctx_(global_ctx) { (void)global_ctx_; }
  virtual ~ObPxTenantTargetMonitorP() = default;
  virtual int init() final;
  virtual void destroy() final;
  virtual int process() final;
private:
  const observer::ObGlobalContext &global_ctx_;
};

class ObPxCleanDtlIntermResP
    : public obrpc::ObRpcProcessor<obrpc::ObPxRpcProxy::ObRpc<obrpc::OB_CLEAN_DTL_INTERM_RESULT> >
{
public:
  ObPxCleanDtlIntermResP(const observer::ObGlobalContext &gctx)
  {}
  virtual ~ObPxCleanDtlIntermResP() = default;
  virtual int init() final { return OB_SUCCESS; }
  virtual void destroy() final {}
  virtual int process() final;
};

}  // sql
}  // oceanbase

#endif /* OB_PX_RPC_PROCESSOR_H */

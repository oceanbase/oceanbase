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

#ifndef OB_PX_SQC_ASYNC_PROXY_H_
#define OB_PX_SQC_ASYNC_PROXY_H_

#include "lib/allocator/ob_mod_define.h"
#include "lib/container/ob_array.h"
#include "lib/lock/ob_thread_cond.h"
#include "lib/profile/ob_trace_id.h"
#include "lib/utility/ob_print_utils.h"
#include "share/ob_define.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/ob_physical_plan.h"
#include "sql/engine/ob_physical_plan_ctx.h"
#include "sql/engine/px/ob_dfo.h"
#include "sql/engine/px/ob_px_rpc_proxy.h"
#include "sql/session/ob_sql_session_info.h"

namespace oceanbase {

using namespace common;

namespace sql {

class ObSqcAsyncCB : public obrpc::ObPxRpcProxy::AsyncCB<obrpc::OB_PX_ASYNC_INIT_SQC> {
public:
  ObSqcAsyncCB(ObThreadCond& cond, const ObCurTraceId::TraceId trace_id) : cond_(cond), trace_id_(trace_id)
  {
    reset();
  }
  ~ObSqcAsyncCB(){};
  virtual int process() override;
  virtual void on_invalid() override;
  virtual void on_timeout() override;
  const ObPxRpcInitSqcResponse& get_result() const
  {
    return result_;
  }
  virtual rpc::frame::ObReqTransport::AsyncCB* clone(const rpc::frame::SPAlloc& alloc) const override;
  void set_args(const AsyncCB::Request& arg) override
  {
    UNUSED(arg);
  }
  void reset()
  {
    is_processed_ = false;
    is_timeout_ = false;
    is_invalid_ = false;
    is_visited_ = false;
    need_retry_ = false;
  }
  void set_visited(bool value)
  {
    is_visited_ = value;
  }
  bool is_visited() const
  {
    return is_visited_;
  }
  bool is_timeout() const
  {
    return is_timeout_;
  }
  void set_invalid(bool value)
  {
    is_invalid_ = value;
  }
  bool is_invalid() const
  {
    return is_invalid_;
  }
  bool is_processed() const
  {
    return is_processed_;
  }
  void set_retry(bool value)
  {
    need_retry_ = value;
  }
  bool need_retry() const
  {
    return need_retry_;
  }
  const obrpc::ObRpcResultCode get_ret_code() const
  {
    return rcode_;
  }
  const common::ObAddr& get_dst() const
  {
    return dst_;
  }
  int64_t get_timeout() const
  {
    return timeout_;
  }
  // to string
  TO_STRING_KV("dst", get_dst(), "timeout", get_timeout(), "ret_code", get_ret_code(), "result", get_result(),
      "is_visited", is_visited(), "is_timeout", is_timeout(), "is_processed", is_processed(), "is_invalid",
      is_invalid());

private:
  bool is_processed_;
  bool is_timeout_;
  bool is_invalid_;
  bool is_visited_;
  bool need_retry_;
  ObThreadCond& cond_;
  ObCurTraceId::TraceId trace_id_;
};

class ObPxSqcAsyncProxy {
public:
  ObPxSqcAsyncProxy(obrpc::ObPxRpcProxy& proxy, ObDfo& dfo, ObExecContext& exec_ctx, ObPhysicalPlanCtx* phy_plan_ctx,
      ObSQLSessionInfo* session, const ObPhysicalPlan* phy_plan, ObArray<ObPxSqcMeta*>& sqcs)
      : proxy_(proxy),
        dfo_(dfo),
        exec_ctx_(exec_ctx),
        phy_plan_ctx_(phy_plan_ctx),
        session_(session),
        phy_plan_(phy_plan),
        sqcs_(sqcs),
        allocator_(ObModIds::OB_SQL_PX_ASYNC_SQC_RPC),
        return_cb_count_(0),
        error_index_(0)
  {
    cond_.init(common::ObWaitEventIds::DEFAULT_COND_WAIT);
  }

  ~ObPxSqcAsyncProxy()
  {
    destroy();
  }
  // asynchronously request all sqc rpc tasks
  int launch_all_rpc_request();
  // synchronously wait for all asynchronous sqc rpc tasks to return results; if there is an
  // internal error that can be handled, it will retry.
  int wait_all();

  const ObArray<ObSqcAsyncCB*>& get_callbacks() const
  {
    return callbacks_;
  }

  int get_error_index() const
  {
    return error_index_;
  }

private:
  void destroy();
  // asynchronously request a single sqc rpc task
  int launch_one_rpc_request(int64_t idx, ObSqcAsyncCB* cb);
  bool check_for_retry(ObSqcAsyncCB& callback);
  void fail_process();

private:
  obrpc::ObPxRpcProxy& proxy_;
  ObDfo& dfo_;
  ObExecContext& exec_ctx_;
  ObPhysicalPlanCtx* phy_plan_ctx_;
  ObSQLSessionInfo* session_;
  const ObPhysicalPlan* phy_plan_;
  ObArray<ObPxSqcMeta*>& sqcs_;
  ObArray<const ObPxRpcInitSqcResponse*> results_;
  ObArray<ObSqcAsyncCB*> callbacks_;
  ObArenaAllocator allocator_;
  int64_t return_cb_count_;
  // the index of the first asynchronous sqc request with error
  int64_t error_index_;
  ObThreadCond cond_;
};
}  // namespace sql
}  // namespace oceanbase

#endif

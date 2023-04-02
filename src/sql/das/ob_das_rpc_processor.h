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

#ifndef OBDEV_SRC_SQL_DAS_OB_DAS_RPC_PROCESSOR_H_
#define OBDEV_SRC_SQL_DAS_OB_DAS_RPC_PROCESSOR_H_
#include "ob_das_extra_data.h"
#include "sql/das/ob_das_task.h"
#include "rpc/obrpc/ob_rpc_processor.h"
#include "observer/virtual_table/ob_virtual_table_iterator_factory.h"
#include "sql/das/ob_das_rpc_proxy.h"
#include "sql/das/ob_das_define.h"
#include "sql/das/ob_das_factory.h"
#include "sql/engine/ob_des_exec_context.h"

namespace oceanbase
{
namespace observer
{
struct ObGlobalContext;
}
namespace sql
{
typedef obrpc::ObRpcProcessor<obrpc::ObDASRpcProxy::ObRpc<obrpc::OB_DAS_SYNC_FETCH_RESULT> > ObDASSyncFetchResRpcProcessor;
typedef obrpc::ObRpcProcessor<obrpc::ObDASRpcProxy::ObRpc<obrpc::OB_DAS_ASYNC_ERASE_RESULT> > ObDASAsyncEraseResRpcProcessor;

template<obrpc::ObRpcPacketCode pcode>
class ObDASBaseAccessP : public obrpc::ObRpcProcessor<obrpc::ObDASRpcProxy::ObRpc<pcode>>
{
public:
  typedef obrpc::ObRpcProcessor<obrpc::ObDASRpcProxy::ObRpc<pcode>> RpcProcessor;
  ObDASBaseAccessP(const observer::ObGlobalContext &gctx)
    : das_factory_(CURRENT_CONTEXT->get_arena_allocator()),
      exec_ctx_(CURRENT_CONTEXT->get_arena_allocator(), gctx.session_mgr_),
      frame_info_(CURRENT_CONTEXT->get_arena_allocator()),
      das_remote_info_()
  {
    RpcProcessor::set_preserve_recv_data();
  }
  virtual ~ObDASBaseAccessP() {}
  virtual int init();
  virtual int before_process();
  virtual int process();
  virtual int after_process(int error_code);
  virtual void cleanup() override;
  static ObDASTaskFactory *&get_das_factory()
  {
    RLOCAL(ObDASTaskFactory*, g_das_fatory);
    return g_das_fatory;
  }
protected:
  ObDASTaskFactory das_factory_;
  ObDesExecContext exec_ctx_;
  ObExprFrameInfo frame_info_;
  share::schema::ObSchemaGetterGuard schema_guard_;
  ObDASRemoteInfo das_remote_info_;
};

class ObDASSyncAccessP final : public ObDASBaseAccessP<obrpc::OB_DAS_SYNC_ACCESS> {
 public:
  typedef ObDASBaseAccessP<obrpc::OB_DAS_SYNC_ACCESS> ObDASSyncRpcProcessor;
  ObDASSyncAccessP(const observer::ObGlobalContext &gctx)
      : ObDASSyncRpcProcessor(gctx) {}
  virtual ~ObDASSyncAccessP() {}
  virtual int process();
};

class ObDASAsyncAccessP final : public ObDASBaseAccessP<obrpc::OB_DAS_ASYNC_ACCESS> {
 public:
  typedef ObDASBaseAccessP<obrpc::OB_DAS_ASYNC_ACCESS> ObDASAsyncRpcProcessor;
  ObDASAsyncAccessP(const observer::ObGlobalContext &gctx)
      : ObDASAsyncRpcProcessor(gctx) {}
  virtual ~ObDASAsyncAccessP() {}
  virtual int process();
};

class ObDasAsyncRpcCallBackContext
{
public:
  ObDasAsyncRpcCallBackContext(ObDASRef &das_ref,
                               const common::ObSEArray<ObIDASTaskOp*, 2> &task_ops,
                               int64_t timeout_ts)
      : das_ref_(das_ref), task_ops_(task_ops), alloc_(), timeout_ts_(timeout_ts) {}
  ~ObDasAsyncRpcCallBackContext() = default;
  int init(const ObMemAttr &attr);
  ObDASRef &get_das_ref() { return das_ref_; };
  const common::ObSEArray<ObIDASTaskOp*, 2> &get_task_ops() const { return task_ops_; };
  common::ObArenaAllocator &get_alloc() { return alloc_; };
  int64_t get_timeout_ts() const { return timeout_ts_; }
private:
  ObDASRef &das_ref_;
  const common::ObSEArray<ObIDASTaskOp*, 2> task_ops_;
  common::ObArenaAllocator alloc_;  // used for async rpc result allocation.
  int64_t timeout_ts_;
};

class ObRpcDasAsyncAccessCallBack
      : public obrpc::ObDASRpcProxy::AsyncCB<obrpc::OB_DAS_ASYNC_ACCESS>
{
public:
  ObRpcDasAsyncAccessCallBack(ObDasAsyncRpcCallBackContext *context)
      : context_(context)
  {
    // we need das_factory to allocate task op result on receiving rpc response.
    result_.set_das_factory(&context->get_das_ref().get_das_factory());
  }
  ~ObRpcDasAsyncAccessCallBack() = default;
  void on_timeout() override;
  void on_invalid() override;
  void set_args(const Request &arg);
  oceanbase::rpc::frame::ObReqTransport::AsyncCB *clone(
      const oceanbase::rpc::frame::SPAlloc &alloc) const;
  virtual int process();
  const common::ObSEArray<ObIDASTaskResult*, 2> &get_op_results() const { return result_.get_op_results(); };
  common::ObSEArray<ObIDASTaskResult*, 2> &get_op_results() { return result_.get_op_results(); };
  const sql::ObDASTaskResp &get_task_resp() const { return result_; };
  const common::ObSEArray<ObIDASTaskOp*, 2> &get_task_ops() const { return context_->get_task_ops(); };
  common::ObIAllocator &get_result_alloc() { return context_->get_alloc(); }
  ObDasAsyncRpcCallBackContext *get_async_cb_context() { return context_; };
private:
  ObDasAsyncRpcCallBackContext *context_;
};

class ObDASSyncFetchP : public ObDASSyncFetchResRpcProcessor
{
public:
  ObDASSyncFetchP() {}
  ~ObDASSyncFetchP() {}
  virtual int process() override;
  virtual int after_process(int error_code);
private:
  DISALLOW_COPY_AND_ASSIGN(ObDASSyncFetchP);
};

class ObDASAsyncEraseP : public ObDASAsyncEraseResRpcProcessor
{
public:
  ObDASAsyncEraseP() {}
  ~ObDASAsyncEraseP() {}
  int process();
private:
  DISALLOW_COPY_AND_ASSIGN(ObDASAsyncEraseP);
};
}  // namespace sql
}  // namespace oceanbase
#endif /* OBDEV_SRC_SQL_DAS_OB_DAS_RPC_PROCESSOR_H_ */

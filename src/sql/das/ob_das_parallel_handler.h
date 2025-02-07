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
#ifndef OBDEV_SRC_SQL_DAS_OB_DAS_PARALLEL_HANDLER_H_
#define OBDEV_SRC_SQL_DAS_OB_DAS_PARALLEL_HANDLER_H_
#include "sql/engine/dml/ob_dml_ctx_define.h"
namespace oceanbase
{
namespace sql
{
class ObDASParallelHandler : public rpc::frame::ObReqProcessor
{
public:
  ObDASParallelHandler()
    : task_(nullptr)
  {  }
  ~ObDASParallelHandler() {}
  int init(observer::ObSrvTask *task);
  void reset()
  {
    task_ = nullptr;
  }
protected:
  int run();
  int deep_copy_all_das_tasks(ObDASTaskFactory &das_factory,
                             ObIAllocator &alloc,
                             ObIArray<ObIDASTaskOp*> &src_task_list,
                             ObIArray<ObIDASTaskOp*> &new_task_list,
                             ObDASRemoteInfo &remote_info,
                             ObDasAggregatedTask &das_task_wrapper);
  int deep_copy_das_task(ObDASTaskFactory &das_factory,
                         ObIDASTaskOp *src_op,
                         ObIDASTaskOp *&dst_op,
                         ObIAllocator &alloc);
  int record_status_and_op_result(ObIDASTaskOp *src_op, ObIDASTaskOp *dst_op);
private:
  DISALLOW_COPY_AND_ASSIGN(ObDASParallelHandler);
  observer::ObSrvTask *task_;
}; // end of class ObDASParallelHandler
class ObDASParallelTask : public observer::ObSrvTask
{
public:
  ObDASParallelTask(DASRefCountContext &das_ref_count_ctx)
    : agg_task_(nullptr),
      das_ref_count_ctx_(das_ref_count_ctx),
      trace_id_(),
      handler_()
  {}
  ~ObDASParallelTask() {}
  int init(ObDasAggregatedTask *agg_task, int32_t group_id);
  const ObCurTraceId::TraceId &get_trace_id() const { return trace_id_; }
  void reset()
  {
    agg_task_ = nullptr;
    handler_.reset();
  }
  ObDasAggregatedTask *get_agg_task() const { return agg_task_; }
  rpc::frame::ObReqProcessor &get_processor() { return handler_; }
  DASRefCountContext &get_das_ref_count_ctx() { return das_ref_count_ctx_; }
  TO_STRING_KV(KP(this), KPC(agg_task_));
private:
  ObDasAggregatedTask *agg_task_;
  DASRefCountContext &das_ref_count_ctx_;
  ObCurTraceId::TraceId trace_id_;
  ObDASParallelHandler handler_;
};
class ObDASParallelTaskFactory
{
public:
  static ObDASParallelTask *alloc(DASRefCountContext &ref_count_ctx);
  static void free(ObDASParallelTask *task);
private:
  static int64_t alloc_count_;
  static int64_t free_count_;
};
}  // namespace sql
}  // namespace oceanbase
#endif /* OBDEV_SRC_SQL_DAS_OB_DAS_PARALLEL_HANDLER_H_ */

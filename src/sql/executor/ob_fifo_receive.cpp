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

#define USING_LOG_PREFIX SQL_EXE

#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/ob_physical_plan_ctx.h"
#include "sql/executor/ob_fifo_receive.h"
#include "sql/executor/ob_receive.h"
#include "sql/executor/ob_transmit.h"
#include "sql/executor/ob_job.h"
#include "sql/executor/ob_task_executor_ctx.h"
#include "sql/executor/ob_executor_rpc_impl.h"
#include "sql/executor/ob_distributed_scheduler.h"
#include "share/ob_define.h"
#include "sql/engine/ob_exec_context.h"

using namespace oceanbase::common;

namespace oceanbase {
namespace sql {

ObTaskResultIter::ObTaskResultIter(IterType iter_type) : iter_type_(iter_type)
{}

ObTaskResultIter::~ObTaskResultIter()
{}

ObRootTaskResultIter::ObRootTaskResultIter(
    ObExecContext& exec_ctx, uint64_t exec_id, uint64_t child_op_id, int64_t ts_timeout)
    : ObTaskResultIter(IT_ROOT),
      exec_ctx_(exec_ctx),
      exec_id_(exec_id),
      scheduler_holder_(),
      scheduler_(NULL),
      child_op_id_(child_op_id),
      ts_timeout_(ts_timeout)
{}

ObRootTaskResultIter::~ObRootTaskResultIter()
{
  scheduler_holder_.reset();
}

int ObRootTaskResultIter::get_next_task_result(ObTaskResult& task_result)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(scheduler_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("scheduler is NULL", K(ret));
  } else if (OB_FAIL(scheduler_->pop_task_result_for_root(exec_ctx_, child_op_id_, task_result, ts_timeout_))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("fail to pop task result for root", K(ret));
    }
  }
  return ret;
}

int ObRootTaskResultIter::check_status()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(scheduler_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("scheduler is NULL", K(ret));
  } else if (OB_FAIL(scheduler_->get_schedule_ret())) {
    if (OB_ITER_END != ret) {
      LOG_WARN("schecule error", K(ret));
    } else {
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

int ObRootTaskResultIter::init()
{
  int ret = OB_SUCCESS;
  ObDistributedSchedulerManager* scheduler_manager = ObDistributedSchedulerManager::get_instance();
  if (OB_ISNULL(scheduler_manager)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObDistributedSchedulerManager instance is NULL", K(ret));
  } else if (OB_FAIL(scheduler_manager->get_scheduler(exec_id_, scheduler_holder_))) {
    LOG_WARN("fail to get scheduler holder", K(ret));
  } else if (OB_FAIL(scheduler_holder_.get_scheduler(scheduler_))) {
    LOG_WARN("fail to get scheduler", K(ret));
  } else if (OB_ISNULL(scheduler_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("scheduler is NULL", K(ret));
  }
  return ret;
}

ObDistributedTaskResultIter::ObDistributedTaskResultIter(const ObIArray<ObTaskResultBuf>& task_results)
    : ObTaskResultIter(IT_DISTRIBUTED), task_results_(task_results), cur_idx_(0)
{}

ObDistributedTaskResultIter::~ObDistributedTaskResultIter()
{}

int ObDistributedTaskResultIter::get_next_task_result(ObTaskResult& task_result)
{
  int ret = OB_SUCCESS;
  if (cur_idx_ < 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cur idx is less than 0", K(ret));
  } else if (cur_idx_ >= task_results_.count()) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(task_result.init(
                 task_results_.at(cur_idx_).get_task_location(), task_results_.at(cur_idx_).get_slice_events()))) {
    LOG_WARN("fail to init task result", K(ret));
  } else {
    cur_idx_++;
  }
  return ret;
}

int ObDistributedTaskResultIter::check_status()
{
  return OB_SUCCESS;
}

ObDistributedReceiveInput::ObDistributedReceiveInput() : child_task_results_(), child_job_id_(0)
{}

ObDistributedReceiveInput::~ObDistributedReceiveInput()
{}

void ObDistributedReceiveInput::reset()
{
  child_task_results_.reset();
}

int ObDistributedReceiveInput::init(ObExecContext& exec_ctx, ObTaskInfo& task_info, const ObPhyOperator& phy_op)
{
  int ret = OB_SUCCESS;
  ObIArray<ObTaskResultBuf>& child_task_results = task_info.get_child_task_results();
  for (int64_t i = 0; OB_SUCC(ret) && i < child_task_results.count(); i++) {
    ObTaskResultBuf& task_result = child_task_results.at(i);
    if (task_result.get_task_location().get_job_id() == child_job_id_) {
      if (OB_FAIL(child_task_results_.push_back(task_result))) {
        LOG_WARN("fail to push back child task result", K(ret));
      }
    }
  }
  UNUSED(exec_ctx);
  UNUSED(phy_op);
  return ret;
}

OB_SERIALIZE_MEMBER(ObDistributedReceiveInput, child_task_results_, child_job_id_);

ObIDataSource::ObIDataSource()
    : cur_scanner_(ObModIds::OB_SQL_EXECUTOR), row_iter_(), exec_ctx_(nullptr), use_small_result_(false), inited_(false)
{}

ObIDataSource::~ObIDataSource()
{
  //  destroy();
}

int ObIDataSource::open(ObExecContext& exec_ctx, const ObTaskResult& task_result)
{
  int ret = OB_SUCCESS;
  exec_ctx_ = &exec_ctx;
  const common::ObIArray<ObSliceEvent>* slice_events = task_result.get_slice_events();
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("open twice", K(ret));
  } else if (OB_ISNULL(slice_events)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("slice event is NULL", K(ret), KP(slice_events));
  } else if (1 != slice_events->count()) {
    ret = OB_NOT_IMPLEMENT;
    LOG_WARN("receive should read only 1 slice per task now", K(ret));
  } else if (!cur_scanner_.is_inited() && OB_FAIL(cur_scanner_.init())) {
    LOG_WARN("fail to init scanner", K(ret));
  } else if (slice_events->at(0).has_small_result()) {
    int64_t pos = 0;
    if (OB_FAIL(cur_scanner_.deserialize(
            slice_events->at(0).get_small_result_buf(), slice_events->at(0).get_small_result_len(), pos))) {
      LOG_WARN("fail to deserialize current small result", K(ret));
    } else {
      use_small_result_ = true;
    }
  } else {
    slice_id_ = slice_events->at(0).get_ob_slice_id();
    peer_ = task_result.get_task_location().get_server();
    if (OB_FAIL(fetch_next_scanner())) {
      LOG_WARN("fetch next scanner failed", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    row_iter_ = cur_scanner_.begin();
    inited_ = true;
    if (!cur_scanner_.is_result_accurate()) {
      if (OB_ISNULL(exec_ctx.get_physical_plan_ctx())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("phy plan ctx is NULL", K(ret));
      } else {
        exec_ctx.get_physical_plan_ctx()->set_is_result_accurate(false);
      }
    }
  }
  return ret;
}

int ObIDataSource::get_next_row(ObNewRow& row)
{
  int ret = OB_SUCCESS;
  while (OB_SUCC(ret)) {
    if (OB_SUCC(row_iter_.get_next_row(row))) {
      break;
    } else if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("fail to get next row", K(ret));
    } else if (!use_small_result_) {
      if (OB_FAIL(fetch_next_scanner())) {
        if (OB_ITER_END != ret) {
          LOG_WARN("fetch next scanner failed", K(ret));
        }
      } else {
        row_iter_ = cur_scanner_.begin();
      }
    }
  }
  return ret;
}

int ObIDataSource::close()
{
  cur_scanner_.reset();
  row_iter_.reset();
  exec_ctx_ = NULL;
  slice_id_.reset();
  peer_.reset();
  use_small_result_ = false;
  inited_ = false;
  return OB_SUCCESS;
}

ObStreamDataSource::ObStreamDataSource() : stream_handler_(ObModIds::OB_SQL_EXECUTOR), stream_opened_(false)
{}

ObStreamDataSource::~ObStreamDataSource()
{
  if (inited_) {
    int ret = close();
    if (OB_SUCCESS != ret) {
      LOG_WARN("close data source failed", K(ret));
    }
  }
}

int ObStreamDataSource::fetch_next_scanner()
{
  int ret = OB_SUCCESS;
  ObIntermResultItem* handler_result = stream_handler_.get_result();
  if (OB_ISNULL(exec_ctx_) || OB_ISNULL(handler_result)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("execute ctx/handler result is NULL");
  } else {
    if (!stream_opened_) {
      ObExecutorRpcImpl* exec_rpc = NULL;
      ObSQLSessionInfo* session = exec_ctx_->get_my_session();
      ObPhysicalPlanCtx* plan_ctx = exec_ctx_->get_physical_plan_ctx();
      if (OB_FAIL(ObTaskExecutorCtxUtil::get_task_executor_rpc(*exec_ctx_, exec_rpc))) {
        LOG_WARN("fail to get executor rpc", K(ret));
      } else if (OB_ISNULL(session) || OB_ISNULL(plan_ctx) || OB_ISNULL(exec_rpc)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("some parameter is NULL", K(ret), K(session), K(plan_ctx), K(exec_rpc));
      } else {
        // should reset first to release previous scanner memory
        handler_result->reset();

        ObExecutorRpcCtx rpc_ctx(session->get_rpc_tenant_id(),
            plan_ctx->get_timeout_timestamp(),
            exec_ctx_->get_task_exec_ctx().get_min_cluster_version(),
            &exec_ctx_->get_scheduler_thread_ctx().get_scheduler_retry_info_for_update(),
            exec_ctx_->get_my_session(),
            plan_ctx->is_plain_select_stmt());
        if (OB_FAIL(exec_rpc->task_fetch_interm_result(rpc_ctx, slice_id_, peer_, stream_handler_))) {
          LOG_WARN("fail to fetch task result", K(ret));
        } else if (OB_FAIL(handler_result->to_scanner(cur_scanner_))) {
          LOG_WARN("fail to assign to scanner", K(ret), K(*handler_result));
        }
      }
      stream_opened_ = true;
    } else {
      if (!stream_handler_.get_handle().has_more()) {
        ret = OB_ITER_END;
      } else if (OB_FAIL(stream_handler_.reset_result())) {
        LOG_WARN("reset result failed", K(ret));
      } else if (OB_FAIL(stream_handler_.get_handle().get_more(*handler_result))) {
        LOG_WARN("get more result failed", K(ret));
      } else {
        cur_scanner_.reset();
        if (OB_FAIL(handler_result->to_scanner(cur_scanner_))) {
          LOG_WARN("assign to scanner failed", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObStreamDataSource::close()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObIDataSource::close())) {
    LOG_WARN("close base data source failed", K(ret));
  }
  if (OB_SUCC(ret)) {
    if (stream_handler_.get_handle().has_more()) {
      if (OB_FAIL(stream_handler_.get_handle().abort())) {
        LOG_WARN("fail to abort stream handler", K(ret));
      }
    }
  }
  stream_opened_ = false;
  return ret;
}

ObSpecifyDataSource::ObSpecifyDataSource() : fetch_index_(OB_INVALID_INDEX)
{}

ObSpecifyDataSource::~ObSpecifyDataSource()
{
  if (inited_) {
    int ret = close();
    if (OB_SUCCESS != ret) {
      LOG_WARN("close data source failed", K(ret));
    }
  }
}

int ObSpecifyDataSource::fetch_next_scanner()
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_INDEX == fetch_index_) {  // first fetch
    fetch_index_ = 0;
  } else if (fetch_index_ >= interm_result_item_.total_item_cnt_) {
    ret = OB_ITER_END;
  }
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(exec_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("execute ctx/task result is NULL", K(ret));
  } else {
    ObExecutorRpcImpl* exec_rpc = NULL;
    ObSQLSessionInfo* session = exec_ctx_->get_my_session();
    ObPhysicalPlanCtx* plan_ctx = exec_ctx_->get_physical_plan_ctx();
    if (OB_FAIL(ObTaskExecutorCtxUtil::get_task_executor_rpc(*exec_ctx_, exec_rpc))) {
      LOG_WARN("fail to get executor rpc", K(ret));
    } else if (OB_ISNULL(session) || OB_ISNULL(plan_ctx) || OB_ISNULL(exec_rpc)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("some parameter is NULL", K(ret), K(session), K(plan_ctx), K(exec_rpc));
    } else {
      ObExecutorRpcCtx rpc_ctx(session->get_rpc_tenant_id(),
          plan_ctx->get_timeout_timestamp(),
          exec_ctx_->get_task_exec_ctx().get_min_cluster_version(),
          &exec_ctx_->get_scheduler_thread_ctx().get_scheduler_retry_info_for_update(),
          exec_ctx_->get_my_session(),
          plan_ctx->is_plain_select_stmt());
      int64_t pre_total_cnt = interm_result_item_.total_item_cnt_;
      interm_result_item_.result_item_.reset();
      if (OB_FAIL(exec_rpc->fetch_interm_result_item(rpc_ctx, peer_, slice_id_, fetch_index_, interm_result_item_))) {
        LOG_WARN("fetch interm result item failed", K(ret), K(peer_), K(slice_id_), K(fetch_index_));
      } else {
        fetch_index_ += 1;
        cur_scanner_.reset();
        if (interm_result_item_.total_item_cnt_ < 0 ||
            (pre_total_cnt >= 0 && pre_total_cnt != interm_result_item_.total_item_cnt_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN(
              "invalid interm result item count", K(ret), K(interm_result_item_.total_item_cnt_), K(pre_total_cnt));
        } else if (OB_FAIL(interm_result_item_.result_item_.to_scanner(cur_scanner_))) {
          LOG_WARN("assign to scanner failed", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObSpecifyDataSource::close()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObIDataSource::close())) {
    LOG_WARN("close base data source failed", K(ret));
  } else {
    fetch_index_ = OB_INVALID_INDEX;
    interm_result_item_.total_item_cnt_ = -1;
    interm_result_item_.result_item_.reset();
  }
  return ret;
}

ObAsyncReceive::ObAsyncReceiveCtx::ObAsyncReceiveCtx(ObExecContext& exec_ctx)
    : ObReceiveCtx(exec_ctx),
      task_result_iter_(NULL),
      found_rows_(0),
      affected_rows_(0),
      matched_rows_(0),
      duplicated_rows_(0),
      iter_end_(false)
{}

ObAsyncReceive::ObAsyncReceiveCtx::~ObAsyncReceiveCtx()
{}

void ObAsyncReceive::ObAsyncReceiveCtx::destroy()
{
  if (NULL != task_result_iter_) {
    task_result_iter_->~ObTaskResultIter();
    task_result_iter_ = NULL;
  }
  ObReceiveCtx::destroy();
}

int ObAsyncReceive::ObAsyncReceiveCtx::init_root_iter(uint64_t child_op_id)
{
  int ret = OB_SUCCESS;
  uint64_t exec_id = exec_ctx_.get_execution_id();
  ObPhysicalPlanCtx* plan_ctx = exec_ctx_.get_physical_plan_ctx();
  int64_t ts_timeout = 0;
  void* iter_buf = NULL;
  ObRootTaskResultIter* task_result_iter = NULL;
  if (!OB_ISNULL(task_result_iter_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("task result iter is not null", K(ret));
  } else if (OB_ISNULL(plan_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("plan ctx is NULL", K(ret));
  } else if (FALSE_IT(ts_timeout = plan_ctx->get_timeout_timestamp())) {
  } else if (OB_ISNULL(iter_buf = exec_ctx_.get_allocator().alloc(sizeof(ObRootTaskResultIter)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc root iter buf", K(ret));
  } else if (OB_ISNULL(
                 task_result_iter = new (iter_buf) ObRootTaskResultIter(exec_ctx_, exec_id, child_op_id, ts_timeout))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to new root task result iter object", K(ret));
  } else if (OB_FAIL(task_result_iter->init())) {
    LOG_WARN("fail to init task result iter", K(ret));
    task_result_iter->~ObRootTaskResultIter();
    task_result_iter = NULL;
  } else {
    task_result_iter_ = task_result_iter;
  }
  return ret;
}

int ObAsyncReceive::ObAsyncReceiveCtx::init_distributed_iter(const ObIArray<ObTaskResultBuf>& task_results)
{
  int ret = OB_SUCCESS;
  void* iter_buf = NULL;
  if (!OB_ISNULL(task_result_iter_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("task result iter is not null", K(ret));
  } else if (OB_ISNULL(iter_buf = exec_ctx_.get_allocator().alloc(sizeof(ObDistributedTaskResultIter)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc root iter buf", K(ret));
  } else if (OB_ISNULL(task_result_iter_ = new (iter_buf) ObDistributedTaskResultIter(task_results))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to new distributed task result iter object", K(ret));
  }
  return ret;
}

int ObAsyncReceive::create_data_source(ObExecContext& exec_ctx, ObIDataSource*& data_source) const
{
  int ret = OB_SUCCESS;
  const bool use_specify_ds = GET_MIN_CLUSTER_VERSION() > CLUSTER_VERSION_2000;
  const int64_t buf_size = use_specify_ds ? sizeof(ObSpecifyDataSource) : sizeof(ObStreamDataSource);
  void* buf = exec_ctx.get_allocator().alloc(buf_size);
  if (OB_ISNULL(buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc data source buf", K(ret), K(buf_size));
  } else {
    if (use_specify_ds) {
      data_source = new (buf) ObSpecifyDataSource();
    } else {
      data_source = new (buf) ObStreamDataSource();
    }
  }
  return ret;
}

ObAsyncReceive::ObAsyncReceive(common::ObIAllocator& alloc) : ObReceive(alloc), in_root_job_(false)
{}

ObAsyncReceive::~ObAsyncReceive()
{}

int ObAsyncReceive::create_operator_input(ObExecContext& exec_ctx) const
{
  int ret = OB_SUCCESS;
  ObDistributedReceiveInput* op_input = NULL;
  if (!is_in_root_job()) {
    if (OB_FAIL(
            CREATE_PHY_OP_INPUT(ObDistributedReceiveInput, exec_ctx, get_id(), PHY_DISTRIBUTED_RECEIVE, op_input))) {
      LOG_WARN("fail to create distributed receive input", K(ret));
    } else if (OB_ISNULL(op_input)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("receive input is NULL", K(ret));
    }
  }
  return ret;
}

int ObAsyncReceive::init_op_ctx(ObExecContext& exec_ctx) const
{
  int ret = OB_SUCCESS;
  ObAsyncReceiveCtx* op_ctx = NULL;
  if (OB_FAIL(create_op_ctx(exec_ctx, op_ctx))) {
    LOG_WARN("fail to create receive ctx", K(ret));
  } else if (OB_ISNULL(op_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("receive ctx is NULL", K(ret), K(op_ctx));
  } else if (OB_FAIL(init_cur_row(*op_ctx, true))) {
    LOG_WARN("create current row failed", K(ret));
  } else if (is_in_root_job()) {
    if (1 != get_child_num()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("receive child num should be 1", K(ret), K(get_child_num()));
    } else if (OB_FAIL(op_ctx->init_root_iter(get_child(0)->get_id()))) {
      LOG_WARN("fail to init root iter", K(ret));
    }
  } else {
    ObDistributedReceiveInput* op_input = GET_PHY_OP_INPUT(ObDistributedReceiveInput, exec_ctx, get_id());
    if (OB_ISNULL(op_input)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("receive input is NULL", K(ret), K(op_input));
    } else if (OB_FAIL(op_ctx->init_distributed_iter(op_input->get_child_task_results()))) {
      LOG_WARN("fail to init distributed iter", K(ret));
    }
  }
  return ret;
}

int ObAsyncReceive::inner_close(ObExecContext& exec_ctx) const
{
  int ret = OB_SUCCESS;
  ObAsyncReceiveCtx* op_ctx = GET_PHY_OPERATOR_CTX(ObAsyncReceiveCtx, exec_ctx, get_id());
  ObPhysicalPlanCtx* plan_ctx = exec_ctx.get_physical_plan_ctx();
  if (OB_ISNULL(op_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("receive ctx is NULL", K(ret));
  } else {
    if (OB_ISNULL(op_ctx->task_result_iter_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("task result iter is NULL", K(ret));
    } else {
      op_ctx->task_result_iter_->~ObTaskResultIter();
      op_ctx->task_result_iter_ = NULL;
    }
    if (OB_ISNULL(plan_ctx)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("plan ctx is NULL", K(ret));
    } else {
      plan_ctx->set_found_rows(plan_ctx->get_found_rows() + op_ctx->found_rows_);
    }
  }
  return ret;
}

int ObAsyncReceive::rescan(ObExecContext& exec_ctx) const
{
  UNUSED(exec_ctx);
  int ret = OB_NOT_SUPPORTED;
  LOG_USER_ERROR(OB_NOT_SUPPORTED, "Distributed rescan");
  return ret;
}

int ObAsyncReceive::get_next_task_result(ObAsyncReceiveCtx& op_ctx, ObTaskResult& task_result) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(op_ctx.task_result_iter_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("task result iter is NULL", K(ret));
  } else {
    while (OB_SUCC(ret)) {
      if (OB_FAIL(op_ctx.task_result_iter_->get_next_task_result(task_result))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("fail to get next task result", K(ret));
        }
      } else if (task_result.has_empty_data()) {
        op_ctx.affected_rows_ += task_result.get_affected_rows();
        op_ctx.duplicated_rows_ += task_result.get_duplicated_rows();
        op_ctx.matched_rows_ += task_result.get_matched_rows();
        task_result.reset();
      } else {
        op_ctx.found_rows_ += task_result.get_found_rows();
        break;
      }
    }
  }
  return ret;
}

ObSerialReceive::ObSerialReceiveCtx::ObSerialReceiveCtx(ObExecContext& exec_ctx)
    : ObAsyncReceiveCtx(exec_ctx), data_source_(NULL)
{}

ObSerialReceive::ObSerialReceiveCtx::~ObSerialReceiveCtx()
{}

void ObSerialReceive::ObSerialReceiveCtx::destroy()
{
  if (NULL != data_source_) {
    data_source_->~ObIDataSource();
    data_source_ = NULL;
  }
  ObAsyncReceiveCtx::destroy();
}

ObSerialReceive::ObSerialReceive(common::ObIAllocator& alloc) : ObAsyncReceive(alloc)
{}

ObSerialReceive::~ObSerialReceive()
{}

int ObSerialReceive::inner_open(ObExecContext& exec_ctx) const
{
  int ret = OB_SUCCESS;
  ObSerialReceiveCtx* op_ctx = NULL;
  ObTaskResult task_result;
  ObPhysicalPlanCtx* plan_ctx = exec_ctx.get_physical_plan_ctx();
  if (OB_ISNULL(plan_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("plan_ctx is NULL", K(ret));
  } else if (OB_FAIL(init_op_ctx(exec_ctx))) {
    LOG_WARN("fail to initialize operator context", K(ret));
  } else if (OB_ISNULL(op_ctx = GET_PHY_OPERATOR_CTX(ObSerialReceiveCtx, exec_ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("receive ctx is NULL", K(ret));
  } else if (OB_FAIL(get_next_task_result(*op_ctx, task_result))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("fail to get next task result", K(ret));
    } else {
      if (op_ctx->affected_rows_ > 0) {
        plan_ctx->set_affected_rows(plan_ctx->get_affected_rows() + op_ctx->affected_rows_);
      }
      if (op_ctx->duplicated_rows_ > 0) {
        plan_ctx->set_row_duplicated_count(plan_ctx->get_row_duplicated_count() + op_ctx->duplicated_rows_);
      }
      if (op_ctx->matched_rows_ > 0) {
        plan_ctx->set_row_matched_count(plan_ctx->get_row_matched_count() + op_ctx->matched_rows_);
      }
      ret = OB_SUCCESS;
      op_ctx->iter_end_ = true;
    }
  } else if (OB_FAIL(create_data_source(exec_ctx, op_ctx->data_source_))) {
    LOG_WARN("create data source failed", K(ret));
  } else if (OB_ISNULL(op_ctx->data_source_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("data source is NULL", K(ret));
  } else if (OB_FAIL(op_ctx->data_source_->open(exec_ctx, task_result))) {
    LOG_WARN("fail to open data source", K(ret));
  }
  return ret;
}

int ObSerialReceive::inner_get_next_row(ObExecContext& exec_ctx, const common::ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  ObSerialReceiveCtx* op_ctx = GET_PHY_OPERATOR_CTX(ObSerialReceiveCtx, exec_ctx, get_id());
  if (OB_ISNULL(op_ctx) || OB_ISNULL(op_ctx->task_result_iter_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("receive ctx or task result iter is NULL", K(ret));
  } else if (op_ctx->iter_end_) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(check_status(exec_ctx)) || OB_FAIL(op_ctx->task_result_iter_->check_status())) {
    LOG_WARN("fail to check status", K(ret));
  }
  while (OB_SUCC(ret)) {
    if (OB_SUCC(op_ctx->data_source_->get_next_row(op_ctx->cur_row_))) {
      row = &op_ctx->cur_row_;
      break;
    } else if (OB_ITER_END == ret) {
      ObTaskResult task_result;
      if (OB_FAIL(op_ctx->data_source_->close())) {
        LOG_WARN("fail to close data source", K(ret));
      } else if (OB_FAIL(get_next_task_result(*op_ctx, task_result))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("fail to get next task result", K(ret));
        } else {
          op_ctx->iter_end_ = true;
        }
      } else if (OB_FAIL(op_ctx->data_source_->open(exec_ctx, task_result))) {
        LOG_WARN("fail to open data source", K(ret));
      }
    } else {
      LOG_WARN("fail to get next row", K(ret));
    }
  }
  return ret;
}

int ObSerialReceive::inner_close(ObExecContext& exec_ctx) const
{
  int ret = OB_SUCCESS;
  ObSerialReceiveCtx* op_ctx = GET_PHY_OPERATOR_CTX(ObSerialReceiveCtx, exec_ctx, get_id());
  if (OB_ISNULL(op_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("receive ctx is NULL", K(ret));
  } else {
    if (NULL != op_ctx->data_source_) {
      if (OB_FAIL(op_ctx->data_source_->close())) {
        LOG_WARN("fail to close data source", K(ret));
      }
    }
  }
  if (OB_FAIL(ObAsyncReceive::inner_close(exec_ctx))) {
    LOG_WARN("fail to inner close", K(ret));
  }
  return ret;
}

ObParallelReceive::ObParallelReceiveCtx::ObParallelReceiveCtx(ObExecContext& exec_ctx)
    : ObAsyncReceiveCtx(exec_ctx), data_sources_(), child_rows_(), row_idxs_()
{}

ObParallelReceive::ObParallelReceiveCtx::~ObParallelReceiveCtx()
{}

void ObParallelReceive::ObParallelReceiveCtx::destroy()
{
  data_sources_.~ObSEArray();
  child_rows_.~ObSEArray();
  row_idxs_.~ObSEArray();
  ObAsyncReceiveCtx::destroy();
}

ObParallelReceive::ObParallelReceive(common::ObIAllocator& alloc) : ObAsyncReceive(alloc)
{}

ObParallelReceive::~ObParallelReceive()
{}

int ObParallelReceive::inner_open(ObExecContext& exec_ctx) const
{
  int ret = OB_SUCCESS;
  ObParallelReceiveCtx* op_ctx = NULL;
  if (OB_FAIL(init_op_ctx(exec_ctx))) {
    LOG_WARN("fail to initialize operator context", K(ret));
  } else if (OB_ISNULL(op_ctx = GET_PHY_OPERATOR_CTX(ObParallelReceiveCtx, exec_ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("receive ctx is NULL", K(ret));
  } else {
    ObTaskResult task_result;
    ObIDataSource* data_source = NULL;
    ObNewRow row;
    int64_t row_count = 0;
    while (OB_SUCC(ret)) {
      if (OB_FAIL(get_next_task_result(*op_ctx, task_result))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("fail to get next task result", K(ret));
        } else {
          ret = OB_SUCCESS;
          break;
        }
      } else if (task_result.has_empty_data()) {
        // skip task result with empty data.
      } else if (OB_FAIL(create_data_source(exec_ctx, data_source))) {
        LOG_WARN("fail to create data source", K(ret));
      } else if (OB_ISNULL(data_source)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("data source is NULL", K(ret));
      } else if (OB_FAIL(create_new_row(exec_ctx, op_ctx->cur_row_, row))) {
        LOG_WARN("fail to create new row", K(ret));
      } else if (OB_FAIL(data_source->open(exec_ctx, task_result))) {
        LOG_WARN("fail to open data source", K(ret));
      } else if (OB_FAIL(data_source->get_next_row(row))) {
        LOG_WARN("fail to get next row", K(ret));
      } else if (OB_FAIL(op_ctx->data_sources_.push_back(data_source))) {
        LOG_WARN("fail to push back data source", K(ret));
      } else if (OB_FAIL(op_ctx->child_rows_.push_back(row))) {
        LOG_WARN("fail to push back child row", K(ret));
      } else if (OB_FAIL(op_ctx->row_idxs_.push_back(row_count))) {
        LOG_WARN("fail to push back row idx", K(ret));
      } else {
        row_count++;
        task_result.reset();
        data_source = NULL;
        row.reset();
      }
    }
    if (0 == row_count) {
      op_ctx->iter_end_ = true;
    }
  }
  return ret;
}

int ObParallelReceive::inner_close(ObExecContext& exec_ctx) const
{
  int ret = OB_SUCCESS;
  ObParallelReceiveCtx* op_ctx = GET_PHY_OPERATOR_CTX(ObParallelReceiveCtx, exec_ctx, get_id());
  if (OB_ISNULL(op_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("receive ctx is NULL", K(ret));
  } else {
    for (int64_t i = 0; /*OB_SUCC(ret)*/ i < op_ctx->data_sources_.count(); i++) {
      if (OB_ISNULL(op_ctx->data_sources_.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("data source is NULL", K(ret), K(i));
      } else {
        if (OB_FAIL(op_ctx->data_sources_.at(i)->close())) {
          LOG_WARN("fail to close data source", K(ret), K(i));
        }
        op_ctx->data_sources_.at(i)->~ObIDataSource();
        op_ctx->data_sources_.at(i) = NULL;
      }
    }
  }
  if (OB_FAIL(ObAsyncReceive::inner_close(exec_ctx))) {
    LOG_WARN("fail to inner close", K(ret));
  }
  return ret;
}

int ObParallelReceive::create_new_row(ObExecContext& exec_ctx, const ObNewRow& cur_row, ObNewRow& row) const
{
  int ret = OB_SUCCESS;
  if (NULL == row.cells_) {
    void* cells_buf = exec_ctx.get_allocator().alloc(cur_row.count_ * sizeof(ObObj));
    if (OB_ISNULL(cells_buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc cells buf", K(ret));
    } else {
      row.cells_ = new (cells_buf) ObObj[cur_row.count_];
      row.count_ = cur_row.count_;
      row.projector_ = cur_row.projector_;
      row.projector_size_ = cur_row.projector_size_;
    }
  }
  return ret;
}

ObFifoReceiveV2::ObFifoReceiveV2(common::ObIAllocator& alloc) : ObSerialReceive(alloc)
{}

ObFifoReceiveV2::~ObFifoReceiveV2()
{}

int ObFifoReceiveV2::create_op_ctx(ObExecContext& exec_ctx, ObAsyncReceiveCtx*& op_ctx) const
{
  return CREATE_PHY_OPERATOR_CTX(ObFifoReceiveCtx, exec_ctx, get_id(), get_type(), op_ctx);
}

ObTaskOrderReceive::ObTaskComparer::ObTaskComparer()
{}

ObTaskOrderReceive::ObTaskComparer::~ObTaskComparer()
{}

bool ObTaskOrderReceive::ObTaskComparer::operator()(const ObTaskResult& task1, const ObTaskResult& task2)
{
  return task1.get_task_id() > task2.get_task_id();
}

ObTaskOrderReceive::ObTaskOrderReceiveCtx::ObTaskOrderReceiveCtx(ObExecContext& exec_ctx)
    : ObSerialReceiveCtx(exec_ctx), task_results_(), task_comparer_(), cur_task_id_(0)
{}

ObTaskOrderReceive::ObTaskOrderReceiveCtx::~ObTaskOrderReceiveCtx()
{}

void ObTaskOrderReceive::ObTaskOrderReceiveCtx::destroy()
{
  task_results_.~ObSEArray();
  ObSerialReceiveCtx::destroy();
}

ObTaskOrderReceive::ObTaskOrderReceive(common::ObIAllocator& alloc) : ObSerialReceive(alloc)
{}

ObTaskOrderReceive::~ObTaskOrderReceive()
{}

int ObTaskOrderReceive::create_op_ctx(ObExecContext& exec_ctx, ObAsyncReceiveCtx*& op_ctx) const
{
  return CREATE_PHY_OPERATOR_CTX(ObTaskOrderReceiveCtx, exec_ctx, get_id(), get_type(), op_ctx);
}
int ObTaskOrderReceive::get_next_task_result(ObAsyncReceiveCtx& receive_ctx, ObTaskResult& task_result) const
{
  int ret = OB_SUCCESS;
  ObTaskOrderReceiveCtx& op_ctx = static_cast<ObTaskOrderReceiveCtx&>(receive_ctx);
  if (OB_ISNULL(op_ctx.task_result_iter_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("task result iter is NULL", K(ret));
  } else if (ObTaskResultIter::IT_ROOT == op_ctx.task_result_iter_->get_iter_type()) {
    if (OB_FAIL(get_next_task_result_root(receive_ctx, task_result))) {
      LOG_WARN("fail to get next task result for root", K(ret));
    }
  } else if (ObTaskResultIter::IT_DISTRIBUTED == op_ctx.task_result_iter_->get_iter_type()) {
    if (OB_FAIL(get_next_task_result_distributed(receive_ctx, task_result))) {
      LOG_WARN("fail to get next task result for distributed", K(ret));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid task result iter type", K(ret), "iter_type", op_ctx.task_result_iter_->get_iter_type());
  }
  return ret;
}

int ObTaskOrderReceive::get_next_task_result_root(ObAsyncReceiveCtx& receive_ctx, ObTaskResult& task_result) const
{
  int ret = OB_SUCCESS;
  ObTaskOrderReceiveCtx& op_ctx = static_cast<ObTaskOrderReceiveCtx&>(receive_ctx);
  if (OB_ISNULL(op_ctx.task_result_iter_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("task result iter is NULL", K(ret));
  } else {
    common::ObIArray<ObTaskResult>& task_results = op_ctx.task_results_;
    task_result.reset();
    while (OB_SUCC(ret)) {
      if (task_results.count() > 0 && op_ctx.cur_task_id_ == task_results.at(0).get_task_id()) {
        ObTaskResult* first_task = &task_results.at(0);
        std::pop_heap(first_task, first_task + task_results.count(), op_ctx.task_comparer_);
        if (OB_FAIL(task_results.pop_back(task_result))) {
          LOG_WARN("fail to pop back task result", K(ret));
        }
      } else {
        while (OB_SUCC(ret) && op_ctx.cur_task_id_ != task_result.get_task_id()) {
          if (OB_FAIL(op_ctx.task_result_iter_->get_next_task_result(task_result))) {
            if (OB_ITER_END != ret) {
              LOG_WARN("fail to get next task result", K(ret));
            }
          } else if (op_ctx.cur_task_id_ == task_result.get_task_id()) {
            // nothing.
          } else if (OB_FAIL(task_results.push_back(task_result))) {
            LOG_WARN("fail to push back task result", K(ret));
          } else {
            ObTaskResult* first_task = &task_results.at(0);
            std::push_heap(first_task, first_task + task_results.count(), op_ctx.task_comparer_);
            task_result.reset();
          }
        }
      }
      if (OB_SUCC(ret)) {
        // now we get the task result with cur_task_id.
        if (op_ctx.cur_task_id_ != task_result.get_task_id()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN(
              "task id is not expected", K(ret), "expect", op_ctx.cur_task_id_, "actual", task_result.get_task_id());
        } else {
          op_ctx.cur_task_id_++;
          if (task_result.has_empty_data()) {
            task_result.reset();
          } else {
            break;
          }
        }
      }
    }
  }
  return ret;
}

int ObTaskOrderReceive::get_next_task_result_distributed(
    ObAsyncReceiveCtx& receive_ctx, ObTaskResult& task_result) const
{
  int ret = OB_SUCCESS;
  ObTaskOrderReceiveCtx& op_ctx = static_cast<ObTaskOrderReceiveCtx&>(receive_ctx);
  common::ObIArray<ObTaskResult>& task_results = op_ctx.task_results_;
  if (0 == task_results.count()) {
    if (OB_ISNULL(op_ctx.task_result_iter_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("task result iter is NULL", K(ret));
    } else {
      while (OB_SUCC(ret)) {
        if (OB_FAIL(op_ctx.task_result_iter_->get_next_task_result(task_result))) {
          if (OB_ITER_END != ret) {
            LOG_WARN("fail to get next task result", K(ret));
          }
        } else if (OB_FAIL(task_results.push_back(task_result))) {
          LOG_WARN("fail to push back task result", K(ret));
        } else {
          ObTaskResult* first_task = &task_results.at(0);
          std::push_heap(first_task, first_task + task_results.count(), op_ctx.task_comparer_);
          task_result.reset();
        }
      }
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      }
    }
  }
  if (OB_SUCC(ret)) {
    task_result.reset();
    while (task_results.count() > 0) {
      ObTaskResult* first_task = &task_results.at(0);
      std::pop_heap(first_task, first_task + task_results.count(), op_ctx.task_comparer_);
      if (OB_FAIL(task_results.pop_back(task_result))) {
        LOG_WARN("fail to pop back task result", K(ret));
      } else if (task_result.has_empty_data()) {
        task_result.reset();
      } else {
        break;
      }
    }
    if (!task_result.is_valid()) {
      ret = OB_ITER_END;
    }
  }
  return ret;
}

ObMergeSortReceive::ObRowComparer::ObRowComparer() : columns_(NULL), rows_(NULL), ret_(OB_SUCCESS)
{}

ObMergeSortReceive::ObRowComparer::~ObRowComparer()
{}

void ObMergeSortReceive::ObRowComparer::init(
    const common::ObIArray<ObSortColumn>& columns, const common::ObIArray<ObNewRow>& rows)
{
  columns_ = &columns;
  rows_ = &rows;
}

bool ObMergeSortReceive::ObRowComparer::operator()(int64_t row_idx1, int64_t row_idx2)
{
  int& ret = ret_;
  int cmp = 0;
  bool cmp_ret = false;
  if (OB_FAIL(ret)) {
    // do nothing if we already have an error,
    // so we can finish the sort process ASAP.
  } else if (OB_ISNULL(columns_) || OB_ISNULL(rows_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("columns or rows is NULL", K(ret), K_(columns), K_(rows));
  } else if (OB_UNLIKELY(!(0 <= row_idx1 && row_idx1 < rows_->count() && 0 <= row_idx2 && row_idx2 < rows_->count()))) {
    ret = OB_INDEX_OUT_OF_RANGE;
    LOG_WARN("row idx out of range", K(ret), K(row_idx1), K(row_idx2), K(rows_->count()));
  } else {
    const ObNewRow& row1 = rows_->at(row_idx1);
    const ObNewRow& row2 = rows_->at(row_idx2);
    for (int64_t i = 0; OB_SUCC(ret) && 0 == cmp && i < columns_->count(); i++) {
      int64_t col_idx = columns_->at(i).index_;
      if (OB_UNLIKELY(!(0 <= col_idx && col_idx < row1.count_ && col_idx < row2.count_))) {
        ret = OB_INDEX_OUT_OF_RANGE;
        LOG_WARN("row idx out of range", K(ret), K(col_idx), K(row1.count_), K(row2.count_));
      } else {
        cmp = row1.cells_[col_idx].compare(row2.cells_[col_idx], columns_->at(i).cs_type_);
        if (cmp < 0) {
          cmp_ret = !columns_->at(i).is_ascending();
        } else if (cmp > 0) {
          cmp_ret = columns_->at(i).is_ascending();
        }
      }
    }
  }
  return cmp_ret;
}

ObMergeSortReceive::ObMergeSortReceiveCtx::ObMergeSortReceiveCtx(ObExecContext& exec_ctx)
    : ObParallelReceiveCtx(exec_ctx), row_comparer_(), last_row_idx_(-1)
{}

ObMergeSortReceive::ObMergeSortReceiveCtx::~ObMergeSortReceiveCtx()
{}

ObMergeSortReceive::ObMergeSortReceive(common::ObIAllocator& alloc) : ObParallelReceive(alloc), ObSortableTrait(alloc)
{}

ObMergeSortReceive::~ObMergeSortReceive()
{}

int ObMergeSortReceive::create_op_ctx(ObExecContext& exec_ctx, ObAsyncReceiveCtx*& op_ctx) const
{
  int ret = OB_SUCCESS;
  ObMergeSortReceiveCtx* receive_ctx = NULL;
  if (OB_FAIL(CREATE_PHY_OPERATOR_CTX(ObMergeSortReceiveCtx, exec_ctx, get_id(), get_type(), receive_ctx))) {
    LOG_WARN("fail to create merge sort receive ctx", K(ret));
  } else if (OB_ISNULL(receive_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to create merge sort receive ctx", K(ret));
  } else if (sort_columns_.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sort_column could not be empty", K(ret));
  } else {
    receive_ctx->row_comparer_.init(sort_columns_, receive_ctx->child_rows_);
    op_ctx = receive_ctx;
  }
  return ret;
}

int ObMergeSortReceive::inner_get_next_row(ObExecContext& exec_ctx, const common::ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  ObMergeSortReceiveCtx* op_ctx = GET_PHY_OPERATOR_CTX(ObMergeSortReceiveCtx, exec_ctx, get_id());
  if (OB_ISNULL(op_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("receive ctx is NULL", K(ret));
  } else if (op_ctx->iter_end_) {
    ret = OB_ITER_END;
  } else if (0 == sort_columns_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sort columns count is 0", K(ret));
  } else {
    ObIArray<ObIDataSource*>& data_sources = op_ctx->data_sources_;
    ObIArray<ObNewRow>& child_rows = op_ctx->child_rows_;
    ObIArray<int64_t>& row_idxs = op_ctx->row_idxs_;
    int64_t& last_row_idx = op_ctx->last_row_idx_;
    if (0 <= last_row_idx && last_row_idx < data_sources.count() && last_row_idx < child_rows.count()) {
      if (OB_ISNULL(data_sources.at(last_row_idx))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("data source is NULL", K(ret), K(last_row_idx));
      } else if (OB_FAIL(data_sources.at(last_row_idx)->get_next_row(child_rows.at(last_row_idx)))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("fail to get next row", K(ret));
        } else if (last_row_idx != row_idxs.at(row_idxs.count() - 1)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("last pop idx is not equal to the last row idx", K(ret));
        } else {
          row_idxs.pop_back();
          if (row_idxs.count() > 0) {
            ret = OB_SUCCESS;
          } else {
            op_ctx->iter_end_ = true;
          }
        }
      } else {
        int64_t* first_row_idx = &row_idxs.at(0);
        std::push_heap(first_row_idx, first_row_idx + row_idxs.count(), op_ctx->row_comparer_);
        if (OB_FAIL(op_ctx->row_comparer_.get_ret())) {
          LOG_WARN("fail to compare row", K(ret));
        }
      }
    } else {
      int64_t* first_row_idx = &row_idxs.at(0);
      std::make_heap(first_row_idx, first_row_idx + row_idxs.count(), op_ctx->row_comparer_);
      if (OB_FAIL(op_ctx->row_comparer_.get_ret())) {
        LOG_WARN("fail to compare row", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      int64_t* first_row_idx = &row_idxs.at(0);
      std::pop_heap(first_row_idx, first_row_idx + row_idxs.count(), op_ctx->row_comparer_);
      last_row_idx = row_idxs.at(row_idxs.count() - 1);
      if (OB_FAIL(op_ctx->row_comparer_.get_ret())) {
        LOG_WARN("fail to compare", K(ret));
      } else if (OB_UNLIKELY(!(0 <= last_row_idx && last_row_idx < child_rows.count()))) {
        ret = OB_INDEX_OUT_OF_RANGE;
        LOG_WARN("last row idx is out of range", K(ret), K(last_row_idx), K(child_rows.count()));
      } else {
        row = &child_rows.at(last_row_idx);
      }
    }
  }
  return ret;
}

OB_SERIALIZE_MEMBER((ObMergeSortReceive, ObParallelReceive), sort_columns_);

//
//
/////////////////////  ObFifoReceiveCtx ///////////////////////
//
//

ObFifoReceive::ObFifoReceiveCtx::ObFifoReceiveCtx(ObExecContext& ctx)
    : ObReceiveCtx(ctx),
      found_rows_(0),
      stream_handler_(ObModIds::OB_SQL_EXECUTOR),
      old_stream_handler_(ObModIds::OB_SQL_EXECUTOR),
      cur_scanner_(ObModIds::OB_SQL_EXECUTOR_SMALL_INTERM_RESULT_SCANNER),
      cur_scanner_iter_(),
      scheduler_holder_(),
      iter_has_started_(false),
      iter_end_(false),
      allocator_(ObModIds::OB_SQL_EXECUTOR),
      waiting_finish_tasks_(allocator_),
      last_pull_task_id_(OB_INVALID_ID),
      affected_row_(0),
      row_i_(NULL),
      cur_merge_handle_(NULL)
{}

ObFifoReceive::ObFifoReceiveCtx::~ObFifoReceiveCtx()
{}

//
//
/////////////////////  ObFifoReceiveInput ///////////////////////
//
//

ObPhyOperatorType ObFifoReceiveInput::get_phy_op_type() const
{
  return PHY_FIFO_RECEIVE;
}

OB_SERIALIZE_MEMBER((ObFifoReceiveInput, ObReceiveInput));

//
//
/////////////////////  ObFifoReceive ///////////////////////
//
//

ObFifoReceive::ObFifoReceive(common::ObIAllocator& alloc) : ObReceive(alloc), ObSortableTrait(alloc)
{}

ObFifoReceive::~ObFifoReceive()
{}

int ObFifoReceive::inner_open(ObExecContext& ctx) const
{
  // Implementation process:
  // 1. According to task id, reduce function, child task location you can get the data list to be requested
  //    child_task_list = reduce_function(task_id);
  //    foreach (child_task_list as slice) {
  //      svr = task_locs_.get_loc(slice);
  //    }
  // 2. Read the result streaming in get_next_row
  //
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_op_ctx(ctx))) {
    LOG_WARN("initialize operator context failed", K(ret));
  } else {
    if (ctx.get_physical_plan_ctx() != NULL && ctx.get_physical_plan_ctx()->get_phy_plan() != NULL &&
        ctx.get_physical_plan_ctx()->get_phy_plan()->get_stmt_type() == stmt::T_INSERT) {
      if (OB_FAIL(deal_with_insert(ctx))) {
        LOG_WARN("deal with insert", K(ret), K(ctx.get_physical_plan_ctx()->get_phy_plan()->get_stmt_type()));
      }
    }
  }
  return ret;
}

int ObFifoReceive::inner_get_next_row(ObExecContext& ctx, const common::ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  // NG_TRACE(fifo_recv_next_begin);
  ObFifoReceiveCtx* fifo_receive_ctx = GET_PHY_OPERATOR_CTX(ObFifoReceiveCtx, ctx, get_id());
  ObFifoReceiveInput* fifo_receive_input = GET_PHY_OP_INPUT(ObFifoReceiveInput, ctx, get_id());
  ObDistributedScheduler* scheduler = NULL;
  ObTaskExecutorCtx* task_exec_ctx = ctx.get_task_executor_ctx();
  if (OB_ISNULL(fifo_receive_ctx) || OB_ISNULL(fifo_receive_input)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx or input is NULL", K(ret), "op_id", get_id(), K(fifo_receive_ctx), K(fifo_receive_input));
  } else if (fifo_receive_ctx->iter_end_) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(check_status(ctx))) {
    LOG_WARN("check physical plan status failed", K(ret));
  } else if (OB_FAIL(fifo_receive_ctx->scheduler_holder_.get_scheduler(scheduler))) {
    LOG_WARN("fail to get scheduler from scheduler holder", K(ret));
  } else if (OB_FAIL(check_schedule_status(scheduler))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("check schecule status failed", K(ret));
    }
  } else if (OB_ISNULL(task_exec_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("task exec ctx is NULL", K(ret));
  } else if (fifo_receive_ctx && is_merge_sort_ && task_exec_ctx->min_cluster_version_is_valid() &&
             task_exec_ctx->get_min_cluster_version() >= CLUSTER_VERSION_140) {  // merge sort
    if (OB_FAIL(merge_sort_result(ctx, fifo_receive_ctx, fifo_receive_input))) {
      if (OB_ITER_END == ret) {
        fifo_receive_ctx->iter_end_ = true;
      } else {
        LOG_WARN("merge sort failed", K(ret));
      }
    } else if (OB_ISNULL(fifo_receive_ctx->row_i_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("row_i is null", K(ret));
    } else {  // if get a row
      for (int64_t i = 0; i < fifo_receive_ctx->row_i_->count_ && i < fifo_receive_ctx->cur_row_.count_; ++i) {
        fifo_receive_ctx->cur_row_.cells_[i] = fifo_receive_ctx->row_i_->cells_[i];
      }
      row = &fifo_receive_ctx->cur_row_;
      // LOG_DEBUG("MERGE SORT GET A ROW SUCC", K(*row), K(*fifo_receive_ctx->row_i_));
    }
  } else {
    while (true) {
      if (!fifo_receive_ctx->iter_has_started_) {
        if (OB_FAIL(fetch_more_result(ctx, fifo_receive_ctx, fifo_receive_input))) {
          if (OB_UNLIKELY(OB_ITER_END != ret)) {
            LOG_WARN("fail to fetch more result", K(ret));
          }
          break;
        }
      } else if (OB_FAIL(fifo_receive_ctx->cur_scanner_iter_.get_next_row(fifo_receive_ctx->cur_row_))) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("fail to get next row from receive interm result", K(ret));
          break;
        } else if (fifo_receive_ctx->iter_has_started_) {
          if (OB_FAIL(fetch_more_result(ctx, fifo_receive_ctx, fifo_receive_input))) {
            if (OB_UNLIKELY(OB_ITER_END != ret)) {
              LOG_WARN("fail to fetch more result", K(ret));
            }
            break;
          }
        } else {
          int pre_ret = ret;
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("unexpected case, BUG!", K(ret), K(pre_ret), K(fifo_receive_ctx->iter_has_started_));
          break;
        }
      } else {
        break;
      }
    }
    if (OB_SUCC(ret)) {
      row = &fifo_receive_ctx->cur_row_;
    }
  }
  if (OB_ITER_END == ret) {
    if (OB_ISNULL(fifo_receive_ctx)) {
      LOG_ERROR("ret is OB_ITER_END, but fifo_receive_ctx is NULL", K(ret));
    } else {
      fifo_receive_ctx->iter_end_ = true;
    }
  }
  // NG_TRACE(fifo_recv_next_end);
  return ret;
}

struct ObFifoReceive::MergeRowComparer {
  explicit MergeRowComparer(const ObIArray<ObSortColumn>& sort_columns, int* err)
      : sort_columns_(sort_columns), err_(err)
  {}
  bool operator()(const ObSortRow& sr1, const ObSortRow& sr2)
  {
    bool ret = false;
    int cmp = 0;
    if (OB_UNLIKELY(OB_SUCCESS != *err_)) {
      // do nothing if we already have an error,
      // so we can finish the sort process ASAP.
    } else if (OB_ISNULL(sr1.row_) || OB_ISNULL(sr2.row_)) {
      *err_ = OB_BAD_NULL_ERROR;
      LOG_WARN("row is null", "ret", *err_);
    } else {
      ObNewRow* r1 = sr1.row_;
      ObNewRow* r2 = sr2.row_;
      for (int64_t i = 0; OB_SUCCESS == *err_ && 0 == cmp && i < sort_columns_.count(); ++i) {
        int64_t idx = sort_columns_.at(i).index_;
        if (idx >= r1->get_count() || idx >= r2->get_count()) {
          *err_ = OB_ERR_UNEXPECTED;
          LOG_WARN("idx is invalid", K(ret), K(idx), K(r1->get_count()), K(r2->get_count()));
        } else {
          cmp = r1->cells_[idx].compare(r2->cells_[idx], sort_columns_.at(i).cs_type_);
          if (cmp < 0) {
            ret = !sort_columns_.at(i).is_ascending();
          } else if (cmp > 0) {
            ret = sort_columns_.at(i).is_ascending();
          } else {
          }
        }
      }  // end for
    }
    return ret;
  }

private:
  const ObIArray<ObSortColumn>& sort_columns_;
  int* err_;
};

int ObFifoReceive::pop_a_row_from_heap(ObFifoReceiveCtx& fifo_receive_ctx, ObSortRow& row) const
{
  int ret = OB_SUCCESS;
  const ObIArray<ObSortColumn>& sort_column = get_sort_columns();
  int err = OB_SUCCESS;
  int64_t size = fifo_receive_ctx.heap_sort_rows_.count();
  if (size > 0) {
    MergeRowComparer rowcomparer(sort_column, &err);
    ObSortRow* first_row = &fifo_receive_ctx.heap_sort_rows_.at(0);
    std::pop_heap(first_row, first_row + size, rowcomparer);
    if (OB_FAIL(err)) {
      LOG_WARN("failed to std::pop_heap", K(ret));
    } else if (OB_FAIL(fifo_receive_ctx.heap_sort_rows_.pop_back(row))) {
      LOG_WARN("failed to pop row", K(ret));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("array size is 0", K(ret));
  }

  return ret;
}

int ObFifoReceive::push_a_row_into_heap(ObFifoReceiveCtx& fifo_receive_ctx, ObSortRow& row) const
{
  int ret = OB_SUCCESS;
  const ObIArray<ObSortColumn>& sort_column = get_sort_columns();
  if (OB_FAIL(fifo_receive_ctx.heap_sort_rows_.push_back(row))) {
    LOG_WARN("push back row failed", K(ret));
  } else {
    int err = OB_SUCCESS;
    MergeRowComparer rowcomparer(sort_column, &err);
    ObSortRow* first_row = &fifo_receive_ctx.heap_sort_rows_.at(0);
    std::push_heap(first_row, first_row + fifo_receive_ctx.heap_sort_rows_.count(), rowcomparer);
    if (OB_FAIL(err)) {
      LOG_WARN("failed to std::push_heap", K(ret));
    }
  }
  return ret;
}

int ObFifoReceive::more_get_scanner(ObExecContext& ctx, ObFifoReceiveCtx& fifo_receive_ctx,
    MergeSortHandle& sort_handler, const ObFifoReceiveInput& fifo_receive_input) const
{
  int ret = OB_SUCCESS;
  UNUSED(fifo_receive_input);
  UNUSED(fifo_receive_ctx);
  ObPhysicalPlanCtx* plan_ctx = NULL;
  if (OB_ISNULL(plan_ctx = ctx.get_physical_plan_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get physical plan ctx", K(ret));
  } else {
    FetchIntermResultStreamHandle& stream_handler = sort_handler.stream_handler_;
    ObScanner& cur_scanner = sort_handler.curhandler_scanner_;
    if (stream_handler.get_handle().has_more()) {
      if (OB_FAIL(stream_handler.reset_result())) {
      } else if (OB_ISNULL(stream_handler.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("succ to alloc result, but result is NULL", K(ret));
      } else if (OB_FAIL(stream_handler.get_handle().get_more(*stream_handler.get_result()))) {
        LOG_WARN("fail get more result", K(ret));
      } else if (FALSE_IT(cur_scanner.reset())) {
      } else if (!cur_scanner.is_inited() && OB_FAIL(cur_scanner.init())) {
        LOG_WARN("fail to init current scanner", K(ret));
      } else if (OB_FAIL(stream_handler.get_result()->to_scanner(cur_scanner))) {
        LOG_WARN("fail to assign to scanner", K(ret), K(*stream_handler.get_result()));
      } else {
        sort_handler.row_iter_ = cur_scanner.begin();
        plan_ctx->set_last_insert_id_session(cur_scanner.get_last_insert_id_session());
      }
    } else {  // has no more scanners, cur handler is end
      ret = OB_ITER_END;
    }
  }
  return ret;
}

int ObFifoReceive::get_row_from_heap(ObFifoReceiveCtx& fifo_receive_ctx) const
{
  int ret = OB_SUCCESS;
  ObSortRow sort_row;
  if (OB_FAIL(pop_a_row_from_heap(fifo_receive_ctx, sort_row))) {  // get a row from heap
    LOG_WARN("pop a row from heap failed", K(ret));
  } else if (sort_row.pos_ < 0 || sort_row.pos_ >= fifo_receive_ctx.merge_handles_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sort pos is invalid", K(sort_row.pos_), K(fifo_receive_ctx.merge_handles_.count()));
  } else {
    fifo_receive_ctx.cur_merge_handle_ = fifo_receive_ctx.merge_handles_.at(sort_row.pos_);
    fifo_receive_ctx.row_i_ = sort_row.row_;
  }
  return ret;
}

int ObFifoReceive::get_row_from_scanner(ObExecContext& ctx, ObFifoReceiveCtx& fifo_receive_ctx,
    MergeSortHandle& sort_handler, const ObFifoReceiveInput& fifo_receive_input) const
{
  int ret = OB_SUCCESS;
  ObNewRow* row = fifo_receive_ctx.row_i_;
  ObSortRow sort_row;
  if (OB_ISNULL(row)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("row is null", K(ret));
  } else if (OB_FAIL(sort_handler.row_iter_.get_next_row(*row))) {  // get a new row from cur scanner
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("fail to get next row from cur stream handle", K(ret));
    } else {
      ret = OB_SUCCESS;
      // get a new scanner from cur stream handler
      if (OB_FAIL(more_get_scanner(ctx, fifo_receive_ctx, sort_handler, fifo_receive_input))) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("get scanner failed", K(ret));
        } else if (fifo_receive_ctx.heap_sort_rows_.count() <= 0) {
          // do nothing, iter_end
        } else {
          ret = OB_SUCCESS;  // iter_end -> succ, pop a row
          if (OB_FAIL(get_row_from_heap(fifo_receive_ctx))) {
            LOG_WARN("get row from heap failed", K(ret));
          }
        }
      } else {  // get a new scanner
        sort_handler.row_iter_ = sort_handler.curhandler_scanner_.begin();
        if (OB_FAIL(get_row_from_scanner(ctx, fifo_receive_ctx, sort_handler, fifo_receive_input))) {
          LOG_WARN("get row from scanner failed", K(ret));
        }
      }
    }
  } else if (0 == fifo_receive_ctx.heap_sort_rows_.count()) {
    // now,only one partition has data, no need push row into heap, return row
    fifo_receive_ctx.row_i_ = row;
  } else {
    sort_row.row_ = row;
    sort_row.pos_ = sort_handler.merge_id_;
    if (OB_FAIL(push_a_row_into_heap(fifo_receive_ctx, sort_row))) {  // push cur row
      LOG_WARN("push a row into heap failed", K(ret));
    } else if (OB_FAIL(get_row_from_heap(fifo_receive_ctx))) {  // pop heap_top row into -> fifo_ctx.row_i_
      LOG_WARN("pop a row from heap failed", K(ret));
    } else {
      // do nothing
    }
  }
  return ret;
}

int ObFifoReceive::create_new_cur_row(ObExecContext& ctx, const ObNewRow& cur_row, ObNewRow*& row_i) const
{
  int ret = OB_SUCCESS;
  int64_t obj_size = cur_row.count_ * sizeof(ObObj);
  void* ptr = NULL;
  if (OB_ISNULL(row_i = static_cast<ObNewRow*>(ctx.get_allocator().alloc(sizeof(ObNewRow))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory failed", K(ret));
  } else if (OB_ISNULL(ptr = ctx.get_allocator().alloc(obj_size))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory failed", K(ret), K(obj_size));
  } else {
    row_i->cells_ = new (ptr) ObObj[cur_row.count_];
    row_i->count_ = cur_row.count_;
    row_i->projector_ = cur_row.projector_;
    row_i->projector_size_ = cur_row.projector_size_;
  }
  return ret;
}

int ObFifoReceive::init_row_heap(ObExecContext& ctx, ObFifoReceiveCtx& fifo_receive_ctx) const
{
  int ret = OB_SUCCESS;
  ObSortRow sort_row;
  ObNewRow& cur_row = fifo_receive_ctx.child_row_;
  for (int64_t i = 0; OB_SUCC(ret) && i < fifo_receive_ctx.merge_handles_.count(); ++i) {
    MergeSortHandle* cur_handler = fifo_receive_ctx.merge_handles_.at(i);
    ObNewRow* row_i = NULL;
    if (OB_FAIL(create_new_cur_row(ctx, cur_row, row_i))) {
      LOG_WARN("alloc cur row failed", K(ret));
    } else if (OB_ISNULL(cur_handler) || OB_ISNULL(row_i)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("cur hande or row_i is null", K(ret), K(i), K(row_i));
    } else if (OB_FAIL(cur_handler->row_iter_.get_next_row(*row_i))) {  // get a row
      if (OB_ITER_END == ret) {
        // no row
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("get next row failed", K(ret));
      }
    } else {  // set sort_row
      sort_row.row_ = row_i;
      sort_row.pos_ = i;
      if (OB_FAIL(push_a_row_into_heap(fifo_receive_ctx, sort_row))) {  // push back sort_row into heap_k
        LOG_WARN("push a row into heap failed", K(ret));
      }
    }
  }
  return ret;
}

int ObFifoReceive::fetch_a_new_scanner(ObExecContext& ctx, ObFifoReceiveCtx& fifo_receive_ctx,
    MergeSortHandle& sort_handler, const ObFifoReceiveInput& fifo_receive_input) const
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx* plan_ctx = NULL;
  if (OB_ISNULL(plan_ctx = ctx.get_physical_plan_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get physical plan ctx", K(ret));
  } else {
    // extract common, get a task_loc
    ObDistributedScheduler* scheduler = NULL;
    uint64_t job_id = fifo_receive_input.get_child_job_id();
    uint64_t child_op_id = fifo_receive_input.get_child_op_id();
    int64_t ts_timeout = plan_ctx->get_timeout_timestamp();
    if (OB_FAIL(fifo_receive_ctx.scheduler_holder_.get_scheduler(scheduler))) {
      LOG_WARN("fail to get scheduler from scheduler holder", K(ret), K(job_id));
    } else if (OB_ISNULL(scheduler)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("scheduler is null", K(ret), K(job_id));
    } else {
      ObTaskResult task_result;
      bool result_scanner_is_empty = true;
      while (OB_SUCC(ret) && result_scanner_is_empty) {
        task_result.reset();
        if (OB_FAIL(scheduler->pop_task_result_for_root(ctx, child_op_id, task_result, ts_timeout))) {
          if (OB_ITER_END != ret) {
            LOG_WARN("fail to pop task loc for root", K(ret), K(job_id), K(task_result), K(ts_timeout));
          }
        } else if (OB_FAIL(deal_with_task(ctx,
                       fifo_receive_ctx,
                       sort_handler,
                       task_result,
                       result_scanner_is_empty,
                       fifo_receive_input))) {
        } else {
          // return scanner iter
        }
      }
    }
  }
  return ret;
}

int ObFifoReceive::merge_sort_result(
    ObExecContext& ctx, ObFifoReceiveCtx* fifo_receive_ctx, const ObFifoReceiveInput* fifo_receive_input) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(fifo_receive_ctx) || OB_ISNULL(fifo_receive_input)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fifo_receive_ctx or fifo_receive_input is null", K(ret), K(fifo_receive_input));
  } else if (fifo_receive_ctx->iter_has_started_) {  // not first
    if (OB_ISNULL(fifo_receive_ctx->cur_merge_handle_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("cur merge_handle is null", K(ret));
    } else if (OB_FAIL(get_row_from_scanner(
                   ctx, *fifo_receive_ctx, *fifo_receive_ctx->cur_merge_handle_, *fifo_receive_input))) {
      // from handle get a row
      if (OB_ITER_END == ret) {
        // no row, iter end
      } else {
        LOG_WARN("get a row from scanner failed", K(ret));
      }
    }
  } else {  // first fetch data from all partitions
    fifo_receive_ctx->iter_has_started_ = true;
    while (OB_SUCC(ret)) {
      MergeSortHandle* sort_handler = NULL;
      if (OB_ISNULL(sort_handler = static_cast<MergeSortHandle*>(ctx.get_allocator().alloc(sizeof(MergeSortHandle))))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("alloca memory for sort handle failed", K(ret), K(sizeof(MergeSortHandle)));
      } else {
        new (sort_handler) MergeSortHandle;
        if (OB_FAIL(fetch_a_new_scanner(ctx, *fifo_receive_ctx, *sort_handler, *fifo_receive_input))) {
          if (OB_ITER_END == ret) {  // =0 -> iter end, no data
            if (fifo_receive_ctx->merge_handles_.count() > 0) {
              ret = OB_SUCCESS;
            }
          } else {
            LOG_WARN("first fetch scanners failed", K(ret));
          }
          sort_handler->~MergeSortHandle();
          break;  //  no more handlers,break
        } else if (OB_FAIL(fifo_receive_ctx->merge_handles_.push_back(sort_handler))) {
          sort_handler->~MergeSortHandle();  // free
          LOG_WARN("push back sort handle failed", K(ret));
        } else {
          sort_handler->merge_id_ = fifo_receive_ctx->merge_handles_.count() - 1;
          sort_handler->row_iter_ = sort_handler->curhandler_scanner_.begin();
        }
      }
    }

    if (OB_SUCC(ret) && fifo_receive_ctx->merge_handles_.count() > 0) {
      if (OB_FAIL(init_row_heap(ctx, *fifo_receive_ctx))) {
        LOG_WARN("init row heap failed", K(ret));
      } else if (OB_FAIL(get_row_from_heap(*fifo_receive_ctx))) {  // first get
        LOG_WARN("get row from heap failed", K(ret));
      } else {
        fifo_receive_ctx->iter_has_started_ = true;
      }
    }
  }
  return ret;
}

int ObFifoReceive::deal_with_task(ObExecContext& ctx, ObFifoReceiveCtx& fifo_receive_ctx, MergeSortHandle& sort_handler,
    ObTaskResult& task_result, bool& result_scanner_is_empty, const ObFifoReceiveInput& fifo_receive_input) const
{
  int ret = OB_SUCCESS;
  LOG_DEBUG("Got task result", K_(partition_order_specified), K(task_result));

  ObSQLSessionInfo* session = ctx.get_my_session();
  ObPhysicalPlanCtx* plan_ctx = ctx.get_physical_plan_ctx();
  bool has_local_data = false;
  const ObIArray<ObSliceEvent>* slice_events = task_result.get_slice_events();
  ObScanner& cur_scanner = sort_handler.curhandler_scanner_;
  if (OB_ISNULL(session) || OB_ISNULL(plan_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("session or plan ctx is NULL", K(ret), K(session), K(plan_ctx));
  } else if (OB_ISNULL(slice_events)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("slice event list is NULL", K(ret));
  } else if (OB_UNLIKELY(1 != slice_events->count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("slice events count is not 1", K(ret), K(slice_events->count()));
  } else {
    const ObSliceEvent& slice_event = slice_events->at(0);
    //    if (OB_ISNULL(slice_event)) {
    //      ret = OB_ERR_UNEXPECTED;
    //      LOG_ERROR("slice event is NULL", K(ret), K(slice_events->count()));
    //    } else {
    const ObTaskSmallResult& sr = slice_event.get_small_result();
    if (sr.has_data()) {
      // The intermediate results have been brought back in the task event,
      // so there is no need to go to the remote end to pull the intermediate results
      // Pull to a scanner and point cur sort_handler.row_iter to it
      has_local_data = true;
      if (sr.get_data_len() <= 0) {
        result_scanner_is_empty = true;
      } else {
        int64_t pos = 0;
        result_scanner_is_empty = false;
        cur_scanner.reset();
        if (!cur_scanner.is_inited() && OB_FAIL(cur_scanner.init())) {
          LOG_WARN("fail to init current scanner", K(ret), K(sr));
        } else if (OB_FAIL(cur_scanner.deserialize(sr.get_data_buf(), sr.get_data_len(), pos))) {
          LOG_WARN("fail to deserialize small result scanner", K(ret), K(pos), K(sr));
        } else {
          const ObScanner& sr_data = cur_scanner;
          sort_handler.row_iter_ = sr_data.begin();
          fifo_receive_ctx.found_rows_ += sr_data.get_found_rows();
          plan_ctx->set_last_insert_id_session(sr_data.get_last_insert_id_session());
          if (!(sr_data.is_result_accurate())) {
            plan_ctx->set_is_result_accurate(sr_data.is_result_accurate());
          }
        }
      }
    }
    //    }
  }

  if (OB_SUCC(ret) && !has_local_data) {
    ObExecutorRpcImpl* rpc = NULL;
    ObSliceID slice_id;
    slice_id.set_ob_task_id(task_result.get_task_location().get_ob_task_id());
    slice_id.set_slice_id(fifo_receive_input.get_pull_slice_id());
    if (OB_FAIL(ObTaskExecutorCtxUtil::get_task_executor_rpc(ctx, rpc))) {
      LOG_WARN("fail to get executor rpc", K(ret));
    } else if (OB_ISNULL(rpc)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("succ to get task executor rpc, but rpc is NULL", K(ret));
    } else if (OB_FAIL(sort_handler.stream_handler_.reset_result())) {
      LOG_WARN("fail to reset and init result", K(ret));
    } else if (OB_ISNULL(sort_handler.stream_handler_.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("succ to alloc result, but result is NULL", K(ret));
    } else {
      ObExecutorRpcCtx rpc_ctx(session->get_rpc_tenant_id(),
          plan_ctx->get_timeout_timestamp(),
          ctx.get_task_exec_ctx().get_min_cluster_version(),
          &ctx.get_scheduler_thread_ctx().get_scheduler_retry_info_for_update(),
          ctx.get_my_session(),
          plan_ctx->is_plain_select_stmt());
      if (OB_FAIL(rpc->task_fetch_interm_result(
              rpc_ctx, slice_id, task_result.get_task_location().get_server(), sort_handler.stream_handler_))) {
        LOG_WARN("fail fetch result", K(ret), K(slice_id), K(task_result));
      } else if (FALSE_IT(cur_scanner.reset())) {
      } else if (!cur_scanner.is_inited() && OB_FAIL(cur_scanner.init())) {
        LOG_WARN("fail to init current scanner", K(ret));
      } else if (OB_FAIL(sort_handler.stream_handler_.get_result()->to_scanner(cur_scanner))) {
        LOG_WARN("fail to assign to scanner", K(ret), K(*sort_handler.stream_handler_.get_result()));
      } else {
        result_scanner_is_empty = false;
        sort_handler.row_iter_ = cur_scanner.begin();
        fifo_receive_ctx.found_rows_ += cur_scanner.get_found_rows();
        plan_ctx->set_last_insert_id_session(cur_scanner.get_last_insert_id_session());
        if (!cur_scanner.is_result_accurate()) {
          plan_ctx->set_is_result_accurate(cur_scanner.is_result_accurate());
        }
      }
    }
  }

  return ret;
}

int ObFifoReceive::fetch_more_result(
    ObExecContext& ctx, ObFifoReceiveCtx* fifo_receive_ctx, const ObFifoReceiveInput* fifo_receive_input) const
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx* task_exec_ctx = ctx.get_task_executor_ctx();
  if (OB_ISNULL(task_exec_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("task exec ctx is NULL", K(ret));
  } else {
    if (task_exec_ctx->min_cluster_version_is_valid() &&
        task_exec_ctx->get_min_cluster_version() >= CLUSTER_VERSION_140) {
      if (OB_FAIL(new_fetch_more_result(ctx, fifo_receive_ctx, fifo_receive_input))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("fail to fetch more result by new function", K(ret));
        }
      }
    } else {
      if (OB_FAIL(old_fetch_more_result(ctx, fifo_receive_ctx, fifo_receive_input))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("fail to fetch more result by old function", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObFifoReceive::new_fetch_more_result(
    ObExecContext& ctx, ObFifoReceiveCtx* fifo_receive_ctx, const ObFifoReceiveInput* fifo_receive_input) const
{
  int ret = OB_SUCCESS;
  // NG_TRACE(fetch_more_result_begin);
  ObSQLSessionInfo* session = ctx.get_my_session();
  ObPhysicalPlanCtx* plan_ctx = ctx.get_physical_plan_ctx();
  if (OB_ISNULL(fifo_receive_ctx) || OB_ISNULL(fifo_receive_input)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ctx or input is NULL", K(ret), "op_id", get_id(), K(fifo_receive_ctx), K(fifo_receive_input));
  } else if (OB_ISNULL(session) || OB_ISNULL(plan_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("session or plan ctx is NULL", K(ret), K(session), K(plan_ctx));
  } else {
    FetchIntermResultStreamHandle& stream_handler = fifo_receive_ctx->stream_handler_;
    if (stream_handler.get_handle().has_more()) {
      if (OB_FAIL(stream_handler.reset_result())) {
        LOG_WARN("fail to reset result", K(ret));
      } else if (OB_ISNULL(stream_handler.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("succ to alloc result, but result is NULL", K(ret));
      } else if (OB_FAIL(stream_handler.get_handle().get_more(*stream_handler.get_result()))) {
        LOG_WARN("fail get more result", K(ret));
      } else if (FALSE_IT(fifo_receive_ctx->cur_scanner_.reset())) {
      } else if (!fifo_receive_ctx->cur_scanner_.is_inited() && OB_FAIL(fifo_receive_ctx->cur_scanner_.init())) {
        LOG_WARN("fail to init current scanner", K(ret));
      } else if (OB_FAIL(stream_handler.get_result()->to_scanner(fifo_receive_ctx->cur_scanner_))) {
        LOG_WARN("fail to assign to scanner", K(ret), K(*stream_handler.get_result()));
      } else {
        fifo_receive_ctx->cur_scanner_iter_ = fifo_receive_ctx->cur_scanner_.begin();
        fifo_receive_ctx->iter_has_started_ = true;
        fifo_receive_ctx->found_rows_ += fifo_receive_ctx->cur_scanner_.get_found_rows();
        fifo_receive_ctx->affected_row_ += fifo_receive_ctx->cur_scanner_.get_affected_rows();
        plan_ctx->set_last_insert_id_session(fifo_receive_ctx->cur_scanner_.get_last_insert_id_session());
        if (!fifo_receive_ctx->cur_scanner_.is_result_accurate()) {
          plan_ctx->set_is_result_accurate(fifo_receive_ctx->cur_scanner_.is_result_accurate());
        }
      }
    } else {
      ObDistributedScheduler* scheduler = NULL;
      uint64_t job_id = fifo_receive_input->get_child_job_id();
      uint64_t child_op_id = fifo_receive_input->get_child_op_id();
      int64_t ts_timeout = plan_ctx->get_timeout_timestamp();
      if (OB_FAIL(fifo_receive_ctx->scheduler_holder_.get_scheduler(scheduler))) {
        LOG_WARN("fail to get scheduler from scheduler holder", K(ret), K(job_id));
      } else if (OB_ISNULL(scheduler)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("scheduler is null", K(ret), K(job_id));
      } else {

        ObTaskResult task_result;
        bool result_scanner_is_empty = true;
        while (OB_SUCC(ret) && result_scanner_is_empty) {
          task_result.reset();

          // If you want to pull data in the specified partition order,
          // you only need to ensure the pull order of task_loc here,
          // That is to ensure that the task_id of task_loc is continuous.
          if (partition_order_specified_) {
            // Pull data in the specified partition order
            bool has_got_task = false;
            uint64_t expected_pop_task_id =
                OB_INVALID_ID == fifo_receive_ctx->last_pull_task_id_ ? 0 : fifo_receive_ctx->last_pull_task_id_ + 1;
            if (fifo_receive_ctx->waiting_finish_tasks_.size() > 0) {
              ObList<ObTaskResult, common::ObArenaAllocator>::iterator waiting_task_head_iter =
                  fifo_receive_ctx->waiting_finish_tasks_.begin();
              if (OB_UNLIKELY(fifo_receive_ctx->waiting_finish_tasks_.end() == waiting_task_head_iter)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_ERROR("waiting finish task list size > 0, but begin == end", K(ret));
              } else {
                const ObTaskResult& head_task_result = *waiting_task_head_iter;
                if (head_task_result.get_task_location().get_task_id() == expected_pop_task_id) {
                  if (OB_FAIL(fifo_receive_ctx->waiting_finish_tasks_.pop_front(task_result))) {
                    LOG_WARN("fail to pop front waiting finish task", K(ret), K(head_task_result));
                  } else {
                    has_got_task = true;
                  }
                } else {
                  // The cached task information does not conform to the order, pop in the scheduler
                  has_got_task = false;
                }
              }
            } else {
              // There is no cache task information, pop in the scheduler
              has_got_task = false;
            }

            if (OB_SUCC(ret) && !has_got_task) {
              do {
                task_result.reset();
                if (OB_FAIL(scheduler->pop_task_result_for_root(ctx, child_op_id, task_result, ts_timeout))) {
                  if (OB_UNLIKELY(OB_ITER_END != ret)) {
                    LOG_WARN("fail to pop task loc for root", K(ret), K(job_id), K(task_result), K(ts_timeout));
                  } else if (OB_UNLIKELY(fifo_receive_ctx->waiting_finish_tasks_.size() > 0)) {
                    ret = OB_ERR_UNEXPECTED;
                    LOG_ERROR("waiting task list has still tasks, but pop ret is iter end, bug!",
                        K(ret),
                        K(job_id),
                        K(fifo_receive_ctx->waiting_finish_tasks_.size()));
                  }
                } else {
                  expected_pop_task_id = OB_INVALID_ID == fifo_receive_ctx->last_pull_task_id_
                                             ? 0
                                             : fifo_receive_ctx->last_pull_task_id_ + 1;
                  if (expected_pop_task_id != task_result.get_task_location().get_task_id()) {
                    // Can't pull it yet, cache it, pay attention to sort by task_id
                    ObList<ObTaskResult, common::ObArenaAllocator>::iterator waiting_task_iter =
                        fifo_receive_ctx->waiting_finish_tasks_.begin();
                    for (; OB_SUCC(ret) && waiting_task_iter != fifo_receive_ctx->waiting_finish_tasks_.end();
                         ++waiting_task_iter) {
                      if ((*waiting_task_iter).get_task_location().get_task_id() >
                          task_result.get_task_location().get_task_id()) {
                        // Sort by task_id and insert at this position
                        break;
                      }
                    }
                    if (OB_FAIL(fifo_receive_ctx->waiting_finish_tasks_.insert(waiting_task_iter, task_result))) {
                      LOG_WARN("fail to insert task loc into waiting_finish_tasks_",
                          K(ret),
                          K((*waiting_task_iter)),
                          K(task_result));
                    }
                  } else {
                    has_got_task = true;
                  }
                }
              } while (OB_SUCC(ret) && !has_got_task);
            }

            if (OB_SUCC(ret)) {
              if (OB_UNLIKELY(!has_got_task)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_ERROR("ret is OB_SUCCESS, but has not got task",
                    K(ret),
                    K(job_id),
                    K(task_result),
                    K(fifo_receive_ctx->last_pull_task_id_));
              } else {
                fifo_receive_ctx->last_pull_task_id_ = task_result.get_task_location().get_task_id();
              }
            }
          } else {
            // Pull data in the specified partition order
            if (OB_FAIL(scheduler->pop_task_result_for_root(ctx, child_op_id, task_result, ts_timeout))) {
              if (OB_ITER_END != ret) {
                LOG_WARN("fail to pop task loc for root", K(ret), K(job_id), K(task_result), K(ts_timeout));
              }
            }
          }

          if (OB_FAIL(ret)) {
          } else {
            LOG_DEBUG("Get task result", K_(partition_order_specified), K(task_result));

            bool has_local_data = false;
            const ObIArray<ObSliceEvent>* slice_events = task_result.get_slice_events();
            if (OB_ISNULL(slice_events)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_ERROR("slice event list is NULL", K(ret));
            } else {
              if (OB_UNLIKELY(1 != slice_events->count())) {
                ret = OB_ERR_UNEXPECTED;
                LOG_ERROR("slice events count is not 1", K(ret), K(slice_events->count()));
              } else {
                const ObSliceEvent& slice_event = slice_events->at(0);
                //                if (OB_ISNULL(slice_event)) {
                //                  ret = OB_ERR_UNEXPECTED;
                //                  LOG_ERROR("slice event is NULL", K(ret), K(slice_events->count()));
                //                } else {
                const ObTaskSmallResult& sr = slice_event.get_small_result();
                if (sr.has_data()) {
                  // The intermediate results have been brought back in the task event,
                  // so there is no need to go to the remote end to pull the intermediate results
                  // Pull to a scanner and point cur sort_handler.row_iter to it
                  has_local_data = true;
                  if (sr.get_data_len() <= 0) {
                    result_scanner_is_empty = true;
                    fifo_receive_ctx->found_rows_ += sr.get_found_rows();
                    fifo_receive_ctx->affected_row_ += sr.get_affected_rows();
                    plan_ctx->set_last_insert_id_session(sr.get_last_insert_id());
                  } else {
                    int64_t pos = 0;
                    result_scanner_is_empty = false;
                    fifo_receive_ctx->cur_scanner_.reset();
                    if (!fifo_receive_ctx->cur_scanner_.is_inited() && OB_FAIL(fifo_receive_ctx->cur_scanner_.init())) {
                      LOG_WARN("fail to init current scanner", K(ret), K(sr));
                    } else if (OB_FAIL(fifo_receive_ctx->cur_scanner_.deserialize(
                                   sr.get_data_buf(), sr.get_data_len(), pos))) {
                      LOG_WARN("fail to deserialize small result scanner", K(ret), K(pos), K(sr));
                    } else {
                      const ObScanner& sr_data = fifo_receive_ctx->cur_scanner_;
                      fifo_receive_ctx->cur_scanner_iter_ = sr_data.begin();
                      fifo_receive_ctx->iter_has_started_ = true;
                      fifo_receive_ctx->found_rows_ += sr_data.get_found_rows();
                      fifo_receive_ctx->affected_row_ += sr_data.get_affected_rows();
                      plan_ctx->set_last_insert_id_session(sr_data.get_last_insert_id_session());
                      if (!(sr_data.is_result_accurate())) {
                        plan_ctx->set_is_result_accurate(sr_data.is_result_accurate());
                      }
                    }
                  }
                }
                //                }
              }
            }
            if (OB_SUCC(ret) && !has_local_data) {
              ObExecutorRpcImpl* rpc = NULL;
              ObSliceID slice_id;
              slice_id.set_ob_task_id(task_result.get_task_location().get_ob_task_id());
              slice_id.set_slice_id(fifo_receive_input->get_pull_slice_id());
              stream_handler.reset();
              if (OB_FAIL(ObTaskExecutorCtxUtil::get_task_executor_rpc(ctx, rpc))) {
                LOG_WARN("fail to get executor rpc", K(ret));
              } else if (OB_ISNULL(rpc)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_ERROR("succ to get task executor rpc, but rpc is NULL", K(ret));
              } else if (OB_FAIL(stream_handler.reset_result())) {
                LOG_WARN("fail to reset and init result", K(ret));
              } else if (OB_ISNULL(stream_handler.get_result())) {
                ret = OB_ERR_UNEXPECTED;
                LOG_ERROR("succ to alloc result, but result is NULL", K(ret));
              } else {
                ObExecutorRpcCtx rpc_ctx(session->get_rpc_tenant_id(),
                    plan_ctx->get_timeout_timestamp(),
                    ctx.get_task_exec_ctx().get_min_cluster_version(),
                    &ctx.get_scheduler_thread_ctx().get_scheduler_retry_info_for_update(),
                    ctx.get_my_session(),
                    plan_ctx->is_plain_select_stmt());
                if (OB_FAIL(rpc->task_fetch_interm_result(
                        rpc_ctx, slice_id, task_result.get_task_location().get_server(), stream_handler))) {
                  LOG_WARN("fail fetch result", K(ret), K(slice_id), K(task_result));
                } else if (FALSE_IT(fifo_receive_ctx->cur_scanner_.reset())) {
                } else if (!fifo_receive_ctx->cur_scanner_.is_inited() &&
                           OB_FAIL(fifo_receive_ctx->cur_scanner_.init())) {
                  LOG_WARN("fail to init current scanner", K(ret));
                } else if (OB_FAIL(stream_handler.get_result()->to_scanner(fifo_receive_ctx->cur_scanner_))) {
                  LOG_WARN("fail to assign to scanner", K(ret), K(*stream_handler.get_result()));
                } else {
                  result_scanner_is_empty = false;
                  fifo_receive_ctx->cur_scanner_iter_ = fifo_receive_ctx->cur_scanner_.begin();
                  fifo_receive_ctx->iter_has_started_ = true;
                  fifo_receive_ctx->found_rows_ += fifo_receive_ctx->cur_scanner_.get_found_rows();
                  fifo_receive_ctx->affected_row_ += fifo_receive_ctx->cur_scanner_.get_affected_rows();
                  plan_ctx->set_last_insert_id_session(fifo_receive_ctx->cur_scanner_.get_last_insert_id_session());
                  if (!fifo_receive_ctx->cur_scanner_.is_result_accurate()) {
                    plan_ctx->set_is_result_accurate(fifo_receive_ctx->cur_scanner_.is_result_accurate());
                  }
                }
              }
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObFifoReceive::old_fetch_more_result(
    ObExecContext& ctx, ObFifoReceiveCtx* fifo_receive_ctx, const ObFifoReceiveInput* fifo_receive_input) const
{
  int ret = OB_SUCCESS;
  // NG_TRACE(fetch_more_result_begin);
  ObSQLSessionInfo* session = ctx.get_my_session();
  ObPhysicalPlanCtx* plan_ctx = ctx.get_physical_plan_ctx();
  if (OB_ISNULL(fifo_receive_ctx) || OB_ISNULL(fifo_receive_input)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ctx or input is NULL", K(ret), "op_id", get_id(), K(fifo_receive_ctx), K(fifo_receive_input));
  } else if (OB_ISNULL(session) || OB_ISNULL(plan_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("session or plan ctx is NULL", K(ret), K(session), K(plan_ctx));
  } else {
    FetchResultStreamHandle& old_stream_handler = fifo_receive_ctx->old_stream_handler_;
    if (old_stream_handler.get_handle().has_more()) {
      if (OB_FAIL(old_stream_handler.reset_and_init_result())) {
        LOG_WARN("fail to reset and init result", K(ret));
      } else if (OB_ISNULL(old_stream_handler.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("succ to alloc result, but result is NULL", K(ret));
      } else if (OB_FAIL(old_stream_handler.get_handle().get_more(*old_stream_handler.get_result()))) {
        LOG_WARN("fail get more result", K(ret));
      } else {
        fifo_receive_ctx->cur_scanner_iter_ = old_stream_handler.get_result()->begin();
        fifo_receive_ctx->iter_has_started_ = true;
        fifo_receive_ctx->found_rows_ += old_stream_handler.get_result()->get_found_rows();
        fifo_receive_ctx->affected_row_ += old_stream_handler.get_result()->get_affected_rows();
        plan_ctx->set_last_insert_id_session(old_stream_handler.get_result()->get_last_insert_id_session());
        if (!old_stream_handler.get_result()->is_result_accurate()) {
          plan_ctx->set_is_result_accurate(old_stream_handler.get_result()->is_result_accurate());
        }
      }
    } else {
      ObDistributedScheduler* scheduler = NULL;
      uint64_t job_id = fifo_receive_input->get_child_job_id();
      uint64_t child_op_id = fifo_receive_input->get_child_op_id();
      int64_t ts_timeout = plan_ctx->get_timeout_timestamp();
      if (OB_FAIL(fifo_receive_ctx->scheduler_holder_.get_scheduler(scheduler))) {
        LOG_WARN("fail to get scheduler from scheduler holder", K(ret), K(job_id));
      } else if (OB_ISNULL(scheduler)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("scheduler is null", K(ret), K(job_id));
      } else {
        ObTaskResult task_result;
        bool result_scanner_is_empty = true;
        while (OB_SUCC(ret) && result_scanner_is_empty) {
          task_result.reset();

          // If you want to pull data in the specified partition order,
          // you only need to ensure the pull order of task_loc here,
          // That is to ensure that the task_id of task_loc is continuous.
          if (partition_order_specified_) {
            // Pull data in the specified partition order
            bool has_got_task = false;
            uint64_t expected_pop_task_id =
                OB_INVALID_ID == fifo_receive_ctx->last_pull_task_id_ ? 0 : fifo_receive_ctx->last_pull_task_id_ + 1;
            if (fifo_receive_ctx->waiting_finish_tasks_.size() > 0) {
              ObList<ObTaskResult, common::ObArenaAllocator>::iterator waiting_task_head_iter =
                  fifo_receive_ctx->waiting_finish_tasks_.begin();
              if (OB_UNLIKELY(fifo_receive_ctx->waiting_finish_tasks_.end() == waiting_task_head_iter)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_ERROR("waiting finish task list size > 0, but begin == end", K(ret));
              } else {
                const ObTaskResult& head_task_result = *waiting_task_head_iter;
                if (head_task_result.get_task_location().get_task_id() == expected_pop_task_id) {
                  // The cached task information conforms to the order and is directly taken from the cache
                  if (OB_FAIL(fifo_receive_ctx->waiting_finish_tasks_.pop_front(task_result))) {
                    LOG_WARN("fail to pop front waiting finish task", K(ret), K(head_task_result));
                  } else {
                    has_got_task = true;
                  }
                } else {
                  // The cached task information does not conform to the order, pop in the scheduler
                  has_got_task = false;
                }
              }
            } else {
              // There is no cache task information, pop in the scheduler
              has_got_task = false;
            }

            if (OB_SUCC(ret) && !has_got_task) {
              do {
                task_result.reset();
                if (OB_FAIL(scheduler->pop_task_result_for_root(ctx, child_op_id, task_result, ts_timeout))) {
                  if (OB_UNLIKELY(OB_ITER_END != ret)) {
                    LOG_WARN("fail to pop task loc for root", K(ret), K(job_id), K(task_result), K(ts_timeout));
                  } else if (OB_UNLIKELY(fifo_receive_ctx->waiting_finish_tasks_.size() > 0)) {
                    ret = OB_ERR_UNEXPECTED;
                    LOG_ERROR("waiting task list has still tasks, but pop ret is iter end, bug!",
                        K(ret),
                        K(job_id),
                        K(fifo_receive_ctx->waiting_finish_tasks_.size()));
                  }
                } else {
                  expected_pop_task_id = OB_INVALID_ID == fifo_receive_ctx->last_pull_task_id_
                                             ? 0
                                             : fifo_receive_ctx->last_pull_task_id_ + 1;
                  if (expected_pop_task_id != task_result.get_task_location().get_task_id()) {
                    // Can't pull it yet, cache it, pay attention to sort by task_id
                    ObList<ObTaskResult, common::ObArenaAllocator>::iterator waiting_task_iter =
                        fifo_receive_ctx->waiting_finish_tasks_.begin();
                    for (; OB_SUCC(ret) && waiting_task_iter != fifo_receive_ctx->waiting_finish_tasks_.end();
                         ++waiting_task_iter) {
                      if ((*waiting_task_iter).get_task_location().get_task_id() >
                          task_result.get_task_location().get_task_id()) {
                        // Sort by task_id and insert at this position
                        break;
                      }
                    }
                    if (OB_FAIL(fifo_receive_ctx->waiting_finish_tasks_.insert(waiting_task_iter, task_result))) {
                      LOG_WARN("fail to insert task loc into waiting_finish_tasks_",
                          K(ret),
                          K((*waiting_task_iter)),
                          K(task_result));
                    }
                  } else {
                    has_got_task = true;
                  }
                }
              } while (OB_SUCC(ret) && !has_got_task);
            }

            if (OB_SUCC(ret)) {
              if (OB_UNLIKELY(!has_got_task)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_ERROR("ret is OB_SUCCESS, but has not got task",
                    K(ret),
                    K(job_id),
                    K(task_result),
                    K(fifo_receive_ctx->last_pull_task_id_));
              } else {
                fifo_receive_ctx->last_pull_task_id_ = task_result.get_task_location().get_task_id();
              }
            }
          } else {
            // Pull data in the specified partition order
            if (OB_FAIL(scheduler->pop_task_result_for_root(ctx, child_op_id, task_result, ts_timeout))) {
              if (OB_ITER_END != ret) {
                LOG_WARN("fail to pop task loc for root", K(ret), K(job_id), K(task_result), K(ts_timeout));
              }
            }
          }

          if (OB_FAIL(ret)) {
          } else {
            LOG_DEBUG("Get task result", K_(partition_order_specified), K(task_result));
            ObExecutorRpcImpl* rpc = NULL;
            ObSliceID slice_id;
            slice_id.set_ob_task_id(task_result.get_task_location().get_ob_task_id());
            slice_id.set_slice_id(fifo_receive_input->get_pull_slice_id());
            old_stream_handler.reset();
            if (OB_FAIL(ObTaskExecutorCtxUtil::get_task_executor_rpc(ctx, rpc))) {
              LOG_WARN("fail to get executor rpc", K(ret));
            } else if (OB_ISNULL(rpc)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_ERROR("succ to get task executor rpc, but rpc is NULL", K(ret));
            } else if (OB_FAIL(old_stream_handler.reset_and_init_result())) {
              LOG_WARN("fail to reset and init result", K(ret));
            } else if (OB_ISNULL(old_stream_handler.get_result())) {
              ret = OB_ERR_UNEXPECTED;
              LOG_ERROR("succ to alloc result, but result is NULL", K(ret));
            } else {
              ObExecutorRpcCtx rpc_ctx(session->get_rpc_tenant_id(),
                  plan_ctx->get_timeout_timestamp(),
                  ctx.get_task_exec_ctx().get_min_cluster_version(),
                  &ctx.get_scheduler_thread_ctx().get_scheduler_retry_info_for_update(),
                  ctx.get_my_session(),
                  plan_ctx->is_plain_select_stmt());
              if (OB_FAIL(rpc->task_fetch_result(
                      rpc_ctx, slice_id, task_result.get_task_location().get_server(), old_stream_handler))) {
                LOG_WARN("fail fetch result", K(ret), K(slice_id), K(task_result));
              } else {
                result_scanner_is_empty = false;
                fifo_receive_ctx->cur_scanner_iter_ = old_stream_handler.get_result()->begin();
                fifo_receive_ctx->iter_has_started_ = true;
                fifo_receive_ctx->found_rows_ += old_stream_handler.get_result()->get_found_rows();
                fifo_receive_ctx->affected_row_ += old_stream_handler.get_result()->get_affected_rows();
                plan_ctx->set_last_insert_id_session(old_stream_handler.get_result()->get_last_insert_id_session());
                if (!old_stream_handler.get_result()->is_result_accurate()) {
                  plan_ctx->set_is_result_accurate(old_stream_handler.get_result()->is_result_accurate());
                }
              }
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObFifoReceive::check_schedule_status(ObDistributedScheduler* scheduler) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(scheduler)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("scheculer is NULL", K(ret));
  } else if (OB_FAIL(scheduler->get_schedule_ret())) {
    LOG_WARN("schecule error", K(ret));
  }
  return ret;
}

int ObFifoReceive::inner_close(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  // ObSQLSessionInfo *my_session = NULL;
  ObPhysicalPlanCtx* plan_ctx = ctx.get_physical_plan_ctx();
  ObFifoReceiveCtx* fifo_receive_ctx = GET_PHY_OPERATOR_CTX(ObFifoReceiveCtx, ctx, get_id());
  if (OB_ISNULL(fifo_receive_ctx) || OB_ISNULL(plan_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx is NULL", K(ret), "op_id", get_id(), K(fifo_receive_ctx), K(plan_ctx));
  } else {
    plan_ctx->set_found_rows(plan_ctx->get_found_rows() + fifo_receive_ctx->found_rows_);
    FetchIntermResultStreamHandle& stream_handler = fifo_receive_ctx->stream_handler_;
    if (stream_handler.get_handle().has_more()) {
      if (OB_FAIL(stream_handler.get_handle().abort())) {
        LOG_WARN("abort failed", K(ret));
      }
    }
    FetchResultStreamHandle& old_stream_handler = fifo_receive_ctx->old_stream_handler_;
    if (old_stream_handler.get_handle().has_more()) {
      if (OB_FAIL(old_stream_handler.get_handle().abort())) {
        LOG_WARN("abort failed", K(ret));
      }
    }
  }
  // Release the scheduler's lock here
  if (OB_ISNULL(fifo_receive_ctx)) {
    // It is possible to call close when the context does not have init, so you cannot print ERROR
    LOG_WARN("fifo_receive_ctx is NULL", K(ret));
  } else {
    fifo_receive_ctx->scheduler_holder_.reset();
  }
  return ret;
}

int ObFifoReceive::init_op_ctx(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObPhyOperatorCtx* op_ctx = NULL;
  ObDistributedSchedulerManager* scheduler_manager = ObDistributedSchedulerManager::get_instance();
  if (OB_FAIL(CREATE_PHY_OPERATOR_CTX(ObFifoReceiveCtx, ctx, get_id(), get_type(), op_ctx))) {
    LOG_WARN("create physical operator context failed", K(ret));
  } else if (OB_FAIL(init_cur_row(*op_ctx, true))) {
    LOG_WARN("create current row failed", K(ret));
  } else if (OB_ISNULL(scheduler_manager)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get ObDistributedSchedulerManager instance failed", K(ret));
  } else {
    ObFifoReceiveCtx* receive_ctx = static_cast<ObFifoReceiveCtx*>(op_ctx);
    // this scheduler is allocated and freed in root thread, see callers of
    // alloc_scheduler() and free_scheduler(), so here we can keep this pointer safe.
    if (OB_FAIL(scheduler_manager->get_scheduler(ctx.get_execution_id(), receive_ctx->scheduler_holder_))) {
      LOG_WARN("get scheduler failed", K(ret));
    } else if (OB_ISNULL(child_op_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("child op is null", K(ret));
    } else if (OB_FAIL(receive_ctx->alloc_row_cells(child_op_->get_column_count(), receive_ctx->child_row_))) {
      LOG_WARN("failed to create cur left row ", K(ret));
    } else {
      receive_ctx->child_row_.projector_ = const_cast<int32_t*>(child_op_->get_projector());
      receive_ctx->child_row_.projector_size_ = child_op_->get_projector_size();
    }
  }
  return ret;
}

int ObFifoReceive::create_operator_input(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObIPhyOperatorInput* input = NULL;
  if (OB_FAIL(CREATE_PHY_OP_INPUT(ObFifoReceiveInput, ctx, get_id(), get_type(), input))) {
    LOG_WARN("fail to create fifo receive input", K(ret), "op_id", get_type());
  }
  UNUSED(input);
  return ret;
}

int ObFifoReceive::rescan(ObExecContext& ctx) const
{
  // Do not support distributed rescan operations on remote operators
  UNUSED(ctx);
  int ret = OB_NOT_SUPPORTED;
  LOG_USER_ERROR(OB_NOT_SUPPORTED, "Distributed rescan");
  return ret;
}

int ObFifoReceive::deal_with_insert(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObFifoReceiveCtx* fifo_receive_ctx = GET_PHY_OPERATOR_CTX(ObFifoReceiveCtx, ctx, get_id());
  ObFifoReceiveInput* fifo_receive_input = GET_PHY_OP_INPUT(ObFifoReceiveInput, ctx, get_id());
  ObDistributedScheduler* scheduler = NULL;
  ObPhysicalPlanCtx* plan_ctx = NULL;
  if (OB_ISNULL(fifo_receive_ctx) || OB_ISNULL(fifo_receive_input)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx or input is NULL", K(ret), "op_id", get_id(), K(fifo_receive_ctx), K(fifo_receive_input));
  } else if (OB_FAIL(check_status(ctx))) {
    LOG_WARN("check physical plan status failed", K(ret));
  } else if (OB_FAIL(fifo_receive_ctx->scheduler_holder_.get_scheduler(scheduler))) {
    LOG_WARN("fail to get scheduler from scheduler holder", K(ret));
  } else if (OB_FAIL(check_schedule_status(scheduler))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("check schecule status failed", K(ret));
    }
  } else if (OB_ISNULL(plan_ctx = ctx.get_physical_plan_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error. plan ctx is null", K(ret));
  } else {
    fifo_receive_ctx->iter_has_started_ = true;
    while (OB_SUCC(ret)) {
      if (OB_FAIL(fetch_more_result(ctx, fifo_receive_ctx, fifo_receive_input))) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("fail to fetch more result", K(ret));
        }
        break;
      }
    }
  }
  if (OB_ITER_END == ret) {
    if (OB_ISNULL(fifo_receive_ctx)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("ret is OB_ITER_END, but fifo_receive_ctx is NULL", K(ret));
    } else {
      if (need_set_affected_row_) {
        plan_ctx->set_affected_rows(plan_ctx->get_affected_rows() + fifo_receive_ctx->affected_row_);
      }
      fifo_receive_ctx->iter_end_ = true;
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase

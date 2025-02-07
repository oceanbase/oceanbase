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

#define USING_LOG_PREFIX SQL_DAS
#include "ob_das_ref.h"
#include "sql/das/ob_data_access_service.h"
#include "sql/das/ob_das_rpc_processor.h"

namespace oceanbase
{
using namespace common;
using namespace observer;
namespace sql
{

DASRefCountContext::DASRefCountContext()
  : max_das_task_concurrency_(INT32_MAX),
    das_task_concurrency_limit_(INT32_MAX),
    err_ret_(OB_SUCCESS),
    cond_(),
    need_wait_(false),
    is_inited_(false)
{
}

void DASRefCountContext::inc_concurrency_limit_with_signal()
{
  ObThreadCondGuard guard(cond_);
  if (__sync_add_and_fetch(&das_task_concurrency_limit_, 1) == max_das_task_concurrency_) {
    cond_.signal();
    LOG_TRACE("inc currency with signal", K(get_current_concurrency()));
  }
}

void DASRefCountContext::inc_concurrency_limit()
{
  ATOMIC_INC(&das_task_concurrency_limit_);
}

int DASRefCountContext::dec_concurrency_limit()
{
  int ret = OB_SUCCESS;
  int32_t cur = get_current_concurrency();
  int32_t next = cur - 1;
  if (OB_UNLIKELY(0 == cur)) {
    ret = OB_SIZE_OVERFLOW;
  } else {
    while (ATOMIC_CAS(&das_task_concurrency_limit_, cur, next) != cur) {
      cur = get_current_concurrency();
      next = cur - 1;
      if (OB_UNLIKELY(0 == cur)) {
        ret = OB_SIZE_OVERFLOW;
        break;
      }
    }
  }
  return ret;
}

// not thread safe.
int DASRefCountContext::acquire_task_execution_resource(int64_t timeout_ts)
{
  int ret = OB_SUCCESS;
  OB_ASSERT(get_current_concurrency() >= 0);
  set_need_wait(true);
  if (!is_inited_ && OB_FAIL(cond_.init(ObWaitEventIds::DAS_ASYNC_RPC_LOCK_WAIT))) {
    LOG_WARN("fail to init condition", K(ret));
  } else if (FALSE_IT(is_inited_ = true)) {
    // do nothing
  } else if (OB_FAIL(dec_concurrency_limit())) {
    LOG_WARN("failed to acquire das execution resource", K(ret), K(get_current_concurrency()));
  }
  if (OB_UNLIKELY(OB_SIZE_OVERFLOW == ret)) {
    ret = OB_SUCCESS;
    ObThreadCondGuard guard(cond_);
    if (OB_FAIL(cond_.wait(timeout_ts - ObTimeUtility::current_time()))) {
      LOG_WARN("failed to acquire das task execution resource", K(ret), K(get_current_concurrency()));
    } else if (OB_FAIL(dec_concurrency_limit())) {
      LOG_WARN("failed to acquire das execution resource", K(ret), K(get_current_concurrency()));
    }
  }
  return ret;
}

bool DasRefKey::operator==(const DasRefKey &other) const
{
  return (tablet_loc_ == other.tablet_loc_ && op_type_ == other.op_type_);
}

uint64_t DasRefKey::hash() const
{
  uint64_t hash = 0;
  hash = murmurhash(&tablet_loc_, sizeof(tablet_loc_), hash);
  hash = murmurhash(&op_type_, sizeof(op_type_), hash);
  return hash;
}

ObDASRef::ObDASRef(ObEvalCtx &eval_ctx, ObExecContext &exec_ctx)
  : das_alloc_(exec_ctx.get_allocator()),
    reuse_alloc_(nullptr),
    das_factory_(das_alloc_),
    batched_tasks_(das_alloc_),
    exec_ctx_(exec_ctx),
    eval_ctx_(eval_ctx),
    expr_frame_info_(nullptr),
    wild_datum_info_(eval_ctx),
    del_aggregated_tasks_(das_alloc_),
    aggregated_tasks_(das_alloc_),
    lookup_cnt_(0),
    task_cnt_(0),
    init_mem_used_(exec_ctx.get_allocator().used()),
    task_map_(),
    async_cb_list_(das_alloc_),
    das_ref_count_ctx_(),
    das_parallel_ctx_(),
    flags_(0)
{
}

DASOpResultIter ObDASRef::begin_result_iter()
{
  return DASOpResultIter(batched_tasks_.begin(), wild_datum_info_, enable_rich_format_);
}

ObIDASTaskOp* ObDASRef::find_das_task(const ObDASTabletLoc *tablet_loc, ObDASOpType op_type)
{
  int ret = OB_SUCCESS;
  ObIDASTaskOp *das_task = nullptr;
  lookup_cnt_++;
  if (task_map_.created()) {
    DasRefKey key(tablet_loc, op_type);
    if (OB_FAIL(task_map_.get_refactored(key, das_task))) {
      if (OB_HASH_NOT_EXIST != ret) {
        LOG_WARN("look up from hash map failed", KR(ret), KP(tablet_loc), K(op_type));
      }
    }
  }
  if (OB_SUCC(ret) && NULL != das_task) {
    // found in hash map
  } else if (OB_HASH_NOT_EXIST == ret) {
    // key not found
  } else {
    DASTaskIter task_iter(batched_tasks_.get_header_node()->get_next(), batched_tasks_.get_header_node());
    for (; nullptr == das_task && !task_iter.is_end(); ++task_iter) {
      ObIDASTaskOp *tmp_task = *task_iter;
      if (tmp_task != nullptr &&
          tmp_task->get_tablet_loc() == tablet_loc &&
          tmp_task->get_type() == op_type &&
          !(tmp_task->is_write_buff_full()) &&
          tmp_task->get_agg_task()->start_status_ == DAS_AGG_TASK_UNSTART) {
        das_task = tmp_task;
      }
    }
  }
  if (OB_FAIL(ret) || task_map_.created()) {
    // do nothing
  } else if (lookup_cnt_ > DAS_REF_TASK_LOOKUP_THRESHOLD
             && task_cnt_ > DAS_REF_TASK_SIZE_THRESHOLD
             && OB_FAIL(create_task_map())) {
    LOG_WARN("create task hash map failed", KR(ret));
  }
  return das_task;
}

int ObDASRef::create_task_map()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(task_map_.created())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("task map was already created", KR(ret), K(task_map_.created()));
  } else if (OB_FAIL(task_map_.create(DAS_REF_MAP_BUCKET_SIZE, ObModIds::OB_HASH_BUCKET))) {
    LOG_WARN("create task map failed", KR(ret));
  } else {
    DASTaskIter task_iter(batched_tasks_.get_header_node()->get_next(), batched_tasks_.get_header_node());
    for (; OB_SUCC(ret) && !task_iter.is_end(); ++task_iter) {
      ObIDASTaskOp *task = *task_iter;
      if (OB_ISNULL(task)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("task is null", KR(ret), KP(task));
      } else if (task->is_write_buff_full() ||
          task->get_agg_task()->start_status_ != DAS_AGG_TASK_UNSTART) {
        LOG_TRACE("this task is submitted or write_buffer is full", K(ret));
      } else {
        DasRefKey key(task->get_tablet_loc(), task->get_type());
        if (OB_FAIL(task_map_.set_refactored(key, task))) {
          LOG_WARN("insert into task map failed", KR(ret), K(key), KP(task));
        }
      }
    }
    if (OB_FAIL(ret)) {
      task_map_.destroy();
    }
  }
  return ret;
}

int ObDASRef::add_batched_task(ObIDASTaskOp *das_task)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(batched_tasks_.store_obj(das_task))) {
    LOG_WARN("store das task failed", KR(ret));
  } else if (task_map_.created()) {
    DasRefKey key(das_task->get_tablet_loc(), das_task->get_type());
    if (OB_FAIL(task_map_.set_refactored(key, das_task))) {
      LOG_WARN("insert into task map failed", KR(ret), K(key), KP(das_task));
    }
  }
  if (OB_SUCC(ret)) {
    task_cnt_++;
  }
  return ret;
}

void ObDASRef::print_all_das_task()
{
  DASTaskIter task_iter(batched_tasks_.get_header_node()->get_next(), batched_tasks_.get_header_node());
  int i = 0;
  for (; !task_iter.is_end(); ++task_iter) {
    i++;
    ObIDASTaskOp *tmp_task = task_iter.get_item();
    if (tmp_task != nullptr) {
      LOG_INFO("dump one das task", K(i), K(tmp_task),
               K(tmp_task->get_tablet_id()), K(tmp_task->get_type()));
    }
  }
}
/*
 [header] -> [node1] -> [node2] -> [node3] -> [header]
 */
int ObDASRef::pick_del_task_to_first()
{
  int ret = OB_SUCCESS;
#if !defined(NDEBUG)
//  LOG_DEBUG("print all das_task before sort");
//  print_all_das_task();
#endif
  DasOpNode *head_node = batched_tasks_.get_obj_list().get_header();
  DasOpNode *curr_node = batched_tasks_.get_obj_list().get_first();
  DasOpNode *next_node = curr_node->get_next();
  // if list only have header，then: next_node == head_node == curr_node
  // if list only have one data node，then: next_node == head_node, not need remove delete task
  // if list only have much data node，then: next_node != head_node, need remove delete task
  while(OB_SUCC(ret) && curr_node != head_node) {
    if (OB_ISNULL(curr_node)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("node is null", K(ret));
    } else if (curr_node->get_obj()->get_type() == ObDASOpType::DAS_OP_TABLE_DELETE) {
      if (!(batched_tasks_.get_obj_list().move_to_first(curr_node))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to move delete node to first", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      curr_node = next_node;
      next_node = curr_node->get_next();
    }
  }
#if !defined(NDEBUG)
  LOG_DEBUG("print all das_task after sort");
  print_all_das_task();
#endif
  return ret;
}

bool ObDASRef::is_all_local_task() const
{
  bool bret = false;
  if (has_task()) {
    bret = true;
    DLIST_FOREACH_X(curr, batched_tasks_.get_obj_list(), bret) {
      if (!curr->get_obj()->is_local_task()) {
        bret = false;
      }
    }
  }
  return bret;
}

// In order to solve the problem that tx_desc will be modified concurrently by concurrent threads,
// when the remote das_task is sent, the get_serialize_size and serialize of tx_desc will report an error 4019 due to non-atomicity.
// So we use the SQL main thread to copy a tx_desc_bak_ to remote_das_task for use when submitting the first concurrent task.
// When the savepoint rollback occurs,tx_desc.op_sn_ will change.
// tx_desc_bak_ must be refreshed to ensure that subsequent execution will not report errors.
// The refresh_tx_desc_bak function handles the above logic.
int ObDASRef::parallel_submit_agg_task(ObDasAggregatedTask *agg_task)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session = exec_ctx_.get_my_session();
  agg_task->set_start_status(DAS_AGG_TASK_PARALLEL_EXEC);
  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null ptr", K(ret));
  } else if (OB_FAIL(get_das_parallel_ctx().refresh_tx_desc_bak(get_das_alloc(), session->get_tx_desc()))) {
    LOG_WARN("fail to check and refresh tx_desc", K(ret));
  } else if (OB_FAIL(MTL(ObDataAccessService *)->parallel_submit_das_task(*this, *agg_task))) {
    LOG_WARN("fail to execute parallel_das_task", K(ret));
  } else {
    LOG_TRACE("succeed submit parallel task", K(ret), K(agg_task));
  }
  return ret;
}

int ObDASRef::execute_all_task()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(execute_all_task(del_aggregated_tasks_))) {
    LOG_WARN("fail to execute all delete agg_tasks", K(ret));
  } else if (OB_FAIL(execute_all_task(aggregated_tasks_))) {
    LOG_WARN("fail to execute all agg_tasks", K(ret));
  } else {
    DASTaskIter task_iter = begin_task_iter();
    while (OB_SUCC(ret) && !task_iter.is_end()) {
      int end_ret = OB_SUCCESS;
      ObIDASTaskOp *das_op = nullptr;
      if (OB_ISNULL(das_op = *task_iter)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null ptr", K(ret));
      } else if (OB_FAIL(das_op->record_task_result_to_rtdef())) {
        LOG_WARN("fail to record task result", K(ret), KPC(das_op));
      } else {
        ++task_iter;
      }
    }
  }

  return ret;
}

bool ObDASRef::check_agg_task_can_retry(ObDasAggregatedTask *agg_task)
{
  bool bret = false;
  if (agg_task->get_save_ret() != OB_SUCCESS) {
    if (agg_task->has_parallel_submiitted()) {
      bret = false;
      // parallel submit task can't do retry
    } else if (check_rcode_can_retry(agg_task->get_save_ret())) {
      bret = true;
    }
  } else {
    bret = true;
  }

  return bret;
}

int ObDASRef::retry_all_fail_tasks(common::ObIArray<ObIDASTaskOp *> &failed_tasks)
{
  int ret = OB_SUCCESS;
  for (int i = 0; OB_SUCC(ret) && i < failed_tasks.count(); i++) {
    ObIDASTaskOp *failed_task = failed_tasks.at(i);
    if (!GCONF._enable_partition_level_retry || !failed_task->can_part_retry()) {
      ret = failed_task->errcode_;
      LOG_WARN("can't do task level retry", K(ret), KPC(failed_task));
    } else if (OB_FAIL(MTL(ObDataAccessService *)->retry_das_task(*this, *failed_tasks.at(i)))) {
      LOG_WARN("Failed to retry das task", K(ret));
    }
  }
  return ret;
}

int ObDASRef::execute_all_task(DasAggregatedTaskList &agg_task_list)
{
  int ret = OB_SUCCESS;
  const bool async = GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_4_1_0_0;
  uint32_t finished_cnt = 0;
  while (finished_cnt < agg_task_list.get_size() && OB_SUCC(ret)) {
    finished_cnt = 0;
    // execute tasks follows aggregated task state machine.
    DLIST_FOREACH_X(curr, agg_task_list.get_obj_list(), OB_SUCC(ret)) {
      ObDasAggregatedTask* agg_task = curr->get_obj();
      if (agg_task->has_unstart_tasks() && !agg_task->has_parallel_submiitted()) {
        if (get_parallel_type() == DAS_SERIALIZATION) {
          if (OB_FAIL(MTL(ObDataAccessService *)->execute_das_task(*this, *agg_task, async))) {
            LOG_WARN("failed to execute aggregated das task", K(ret), KPC(agg_task), K(async));
          } else {
            LOG_DEBUG("successfully executing aggregated task", "server", agg_task->server_);
          }
        } else {
          if (OB_FAIL(parallel_submit_agg_task(agg_task))) {
            LOG_WARN("failed to execute aggregated das task", K(ret), KPC(agg_task));
          } else {
            LOG_DEBUG("successfully parallel submit agg_task", KPC(agg_task));
          }
        }
      }
    }

    // wait all existing tasks to be finished
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(wait_tasks_and_process_response())) {
      LOG_WARN("failed to process all async remote tasks", K(ret));
    }
    ret = COVER_SUCC(tmp_ret);
    if (OB_FAIL(ret) && check_rcode_can_retry(ret)) {
      ret = OB_SUCCESS;
    }

    // check das task status.
    DLIST_FOREACH_X(curr, agg_task_list.get_obj_list(), OB_SUCC(ret)) {
      ObDasAggregatedTask* aggregated_task = curr->get_obj();
      if (aggregated_task->has_parallel_submiitted() && aggregated_task->get_save_ret() != OB_SUCCESS) {
        // all parallel_submit task can't retry
        ret = aggregated_task->get_save_ret();
        LOG_WARN("can't retry for this error_ret", K(ret), KPC(aggregated_task));
      } else if (aggregated_task->has_not_execute_task()) {
        // 还有任务没执行完毕
        if (aggregated_task->has_failed_tasks()) {
          // retry all failed tasks.
          common::ObSEArray<ObIDASTaskOp *, 2> failed_tasks;
          int tmp_ret = OB_SUCCESS;
          if (OB_TMP_FAIL(aggregated_task->get_failed_tasks(failed_tasks))) {
            LOG_WARN("failed to get failed tasks", K(ret));
          } else if (failed_tasks.count() == 0) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("failed to get failed tasks");
          } else if (OB_FAIL(retry_all_fail_tasks(failed_tasks))) {
            LOG_WARN("fail to retry das tasks", K(ret), K(failed_tasks));
          }
        }
      } else {
        ++finished_cnt;
        LOG_DEBUG("check finish agg_task print agg_list", K(finished_cnt), K(agg_task_list.get_size()), KPC(aggregated_task));
      }
    }
  }
  return ret;
}

bool ObDASRef::check_rcode_can_retry(int ret)
{
  bool bret = false;
  ObDASRetryCtrl::retry_func retry_func = nullptr;

  int tmp_ret = OB_SUCCESS;
  if (OB_TMP_FAIL(ObQueryRetryCtrl::get_das_retry_func(ret, retry_func))) {
    LOG_WARN("get das retry func failed", KR(tmp_ret), KR(ret));
  } else if (retry_func != nullptr) {
    bret = true;
  }
  return bret;
}

int ObDASRef::wait_all_executing_tasks()
{
  int ret = OB_SUCCESS;
  if (das_ref_count_ctx_.is_need_wait()) {
    ObThreadCondGuard guard(das_ref_count_ctx_.get_cond());
    while (OB_SUCC(ret) &&
        das_ref_count_ctx_.get_current_concurrency() < das_ref_count_ctx_.get_max_das_task_concurrency()) {
      // we cannot use ObCond here because it can not explicitly lock mutex, causing concurrency problem.
      if (OB_FAIL(das_ref_count_ctx_.get_cond().wait())) {
        LOG_WARN("failed to wait all das tasks to be finished.", K(ret));
      }
    }
  }
  return ret;
}

int ObDASRef::wait_tasks_and_process_response()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(wait_all_executing_tasks())) {
    LOG_WARN("fail to wait all executing tasks", K(ret));
  } else if (OB_FAIL(process_remote_task_resp())) {
    LOG_WARN("failed to process remote task resp", K(ret));
  }
  return ret;
}

void ObDASRef::clear_task_map()
{
  if (task_map_.created()) {
    task_map_.clear();
  }
}

int ObDASRef::wait_all_tasks()
{
  // won't implement until das async execution.
  return OB_UNIMPLEMENTED_FEATURE;
}

int ObDASRef::allocate_async_das_cb(ObRpcDasAsyncAccessCallBack *&async_cb,
                                    const common::ObSEArray<ObIDASTaskOp*, 2> &task_ops,
                                    int64_t timeout_ts)
{
  int ret = OB_SUCCESS;
  OB_ASSERT(async_cb == nullptr);
  ObDASTaskFactory &das_factory = get_das_factory();
  if (OB_FAIL(das_factory.create_das_async_cb(task_ops, das_alloc_.get_attr(), *this, async_cb, timeout_ts))) {
    LOG_WARN("failed to create das async cb", K(ret));
  } else if (OB_ISNULL(async_cb)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to allocate async cb obj", K(ret));
  } else if (OB_FAIL(async_cb_list_.store_obj(async_cb))) {
    LOG_WARN("failed to store async cb obj", K(ret));
  }
  return ret;
}

void ObDASRef::remove_async_das_cb(ObRpcDasAsyncAccessCallBack *das_async_cb)
{
  bool removed = false;
  DLIST_FOREACH_X(curr, async_cb_list_.get_obj_list(), !removed) {
    if (curr->get_obj() == das_async_cb) {
      async_cb_list_.get_obj_list().remove(curr);
      removed = true;
      LOG_DEBUG("found remove node", K(das_async_cb));
    }
  }
}

int ObDASRef::process_remote_task_resp()
{
  int ret = OB_SUCCESS;
  int save_ret = OB_SUCCESS;
  DLIST_FOREACH_X(curr, async_cb_list_.get_obj_list(), OB_SUCC(ret)) {
    const sql::ObDASTaskResp &task_resp = curr->get_obj()->get_task_resp();
    const common::ObSEArray<ObIDASTaskOp*, 2> &task_ops = curr->get_obj()->get_task_ops();
    if (OB_UNLIKELY(OB_SUCCESS != task_resp.get_err_code())) {
      LOG_WARN("das async execution failed", K(task_resp));
      for (int i = 0; i < task_ops.count(); i++) {
        get_exec_ctx().get_my_session()->get_trans_result().add_touched_ls(task_ops.at(i)->get_ls_id());
      }
      save_ret = task_resp.get_err_code();
    }
    if (OB_FAIL(MTL(ObDataAccessService *)->process_task_resp(*this, task_resp, task_ops))) {
      LOG_WARN("failed to process das async task resp", K(ret), K(task_resp));
      save_ret = ret;
      ret = OB_SUCCESS;
    } else {
      // if task execute success, error must be success.
      OB_ASSERT(OB_SUCCESS == task_resp.get_err_code());
    }
  }
  async_cb_list_.clear();  // no need to hold async cb anymore. destructor would be called in das factory.
  ret = COVER_SUCC(save_ret);
  return ret;
}


int ObDASRef::move_local_tasks_to_last()
{
  int ret = OB_SUCCESS;
  bool found_local_tasks = false;
  const common::ObAddr &ctrl_addr = MTL(ObDataAccessService *)->get_ctrl_addr();
  DLIST_FOREACH_X(curr, aggregated_tasks_.get_obj_list(), !found_local_tasks) {
    ObDasAggregatedTask* aggregated_task = curr->get_obj();
    if (aggregated_task->server_ == ctrl_addr) {
      if (!aggregated_tasks_.get_obj_list().move_to_last(curr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to move local task to last", KR(ret), KPC(aggregated_task));
      }
      found_local_tasks = true;
    }
  }
  return ret;
}

int ObDASRef::close_all_task()
{
  int ret = OB_SUCCESS;
  int last_end_ret = OB_SUCCESS;
  if (has_task()) {
    FLTSpanGuard(close_das_task);
    ObSQLSessionInfo *session = nullptr;
    int wait_ret = OB_SUCCESS;
    if (get_parallel_type() != DAS_SERIALIZATION) {
      // parallel submit das_task maybe some task is executing, must wait all task end
      // and maybe remote_task need mereg_trans_result
      if (OB_SUCCESS != (wait_ret = wait_tasks_and_process_response())) {
        LOG_WARN("fail to wait all executing tasks", K(wait_ret));
      }
    }
    ret = COVER_SUCC(wait_ret);
    DASTaskIter task_iter = begin_task_iter();
    while (!task_iter.is_end()) {
      int end_ret = OB_SUCCESS;
      if (OB_SUCCESS != (end_ret = MTL(ObDataAccessService*)->end_das_task(*this, **task_iter))) {
        LOG_WARN("execute das task failed", K(end_ret));
      }
      ++task_iter;
      last_end_ret = (last_end_ret == OB_SUCCESS ? end_ret : last_end_ret);
    }

    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
    ret = COVER_SUCC(last_end_ret);

    if (OB_ISNULL(session = exec_ctx_.get_my_session())) {
      ret = COVER_SUCC(OB_NOT_INIT);
      LOG_WARN("session is nullptr", K(ret));
    }
    bool merge_trans_result_fail = (ret != OB_SUCCESS);
    // any fail during merge trans_result,
    // need set trans_result incomplete, in order to
    // indicate some transaction participants info unknown
    if (merge_trans_result_fail && OB_NOT_NULL(session)) {
      LOG_WARN("close all task fail, set trans_result to incomplete", K(ret));
      session->get_trans_result().set_incomplete();
    }
    batched_tasks_.destroy();
    del_aggregated_tasks_.destroy();
    aggregated_tasks_.destroy();
    if (task_map_.created()) {
      task_map_.destroy();
    }
  }
  return ret;
}

int ObDASRef::create_das_task(const ObDASTabletLoc *tablet_loc,
                              ObDASOpType op_type,
                              ObIDASTaskOp *&task_op)
{
  int ret = OB_SUCCESS;
  ObDASTaskFactory &das_factory = get_das_factory();
  ObSQLSessionInfo *session = get_exec_ctx().get_my_session();
  int64_t task_id = 0;
  bool need_das_id = true;
  ObPhysicalPlanCtx *plan_ctx = NULL;
  const ObPhysicalPlan *plan = NULL;
  if (OB_NOT_NULL(plan_ctx = GET_PHY_PLAN_CTX(get_exec_ctx()))
      && OB_NOT_NULL(plan = plan_ctx->get_phy_plan())) {
    need_das_id = !(plan->is_local_plan() && OB_PHY_PLAN_LOCAL == plan->get_location_type());
  }
  if (need_das_id && OB_FAIL(MTL(ObDataAccessService*)->get_das_task_id(task_id))) {
    LOG_WARN("get das task id failed", KR(ret));
  } else if (OB_FAIL(das_factory.create_das_task_op(op_type, task_op))) {
    LOG_WARN("create das task op failed", K(ret), KPC(task_op));
  } else {
    task_op->set_trans_desc(session->get_tx_desc());
    task_op->set_snapshot(&get_exec_ctx().get_das_ctx().get_snapshot());
    task_op->set_write_branch_id(get_exec_ctx().get_das_ctx().get_write_branch_id());
    task_op->set_tenant_id(session->get_effective_tenant_id());
    task_op->set_task_id(task_id);
    task_op->in_stmt_retry_ = session->get_is_in_retry();
    task_op->set_tablet_id(tablet_loc->tablet_id_);
    task_op->set_ls_id(tablet_loc->ls_id_);
    task_op->set_tablet_loc(tablet_loc);
    if (is_do_gts_opt() && OB_FAIL(task_op->init_das_gts_opt_info(session->get_tx_isolation()))) {
      LOG_WARN("fail to init gts opt info", K(ret), K(session->get_tx_isolation()));
    } else if (OB_FAIL(add_aggregated_task(task_op, op_type))) {
      LOG_WARN("failed to add aggregated task", K(ret));
    }
    ObDiagnosticInfo *di = ObLocalDiagnosticInfo::get();
    if (OB_NOT_NULL(di)) {
      task_op->set_plan_line_id(di->get_ash_stat().plan_line_id_);
    }
  }
  return ret;
}

int ObDASRef::find_agg_task(const ObDASTabletLoc *tablet_loc, ObDASOpType op_type, ObDasAggregatedTask *&agg_task)
{
  int ret = OB_SUCCESS;
  bool aggregated = false;
  if (DAS_OP_TABLE_DELETE == op_type) {
    DLIST_FOREACH_X(curr, del_aggregated_tasks_.get_obj_list(), !aggregated && OB_SUCC(ret)) {
      ObDasAggregatedTask* aggregated_task = curr->get_obj();
      if (aggregated_task->server_ == tablet_loc->server_ &&
          aggregated_task->start_status_ == DAS_AGG_TASK_UNSTART) {
        agg_task = aggregated_task;
        aggregated = true;
      }
    }
  } else {
    DLIST_FOREACH_X(curr, aggregated_tasks_.get_obj_list(), !aggregated && OB_SUCC(ret)) {
      ObDasAggregatedTask* aggregated_task = curr->get_obj();
      if (aggregated_task->server_ == tablet_loc->server_ &&
          aggregated_task->start_status_ == DAS_AGG_TASK_UNSTART) {
        agg_task = aggregated_task;
        aggregated = true;
      }
    }
  }
  return ret;
}

int ObDASRef::create_agg_task(ObDASOpType op_type, const ObDASTabletLoc *tablet_loc, ObDasAggregatedTask *&agg_task)
{
  int ret = OB_SUCCESS;
  // create agg_task
  void *buf = das_alloc_.alloc(sizeof(ObDasAggregatedTask));
  ObDasAggregatedTask *tmp_agg_task = nullptr;
  agg_task = nullptr;
  if (OB_ISNULL(buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory for aggregated tasks", KR(ret));
  } else if (FALSE_IT(tmp_agg_task = new(buf) ObDasAggregatedTask(das_alloc_))) {
  } else if (DAS_OP_TABLE_DELETE == op_type &&
      OB_FAIL(del_aggregated_tasks_.store_obj(tmp_agg_task))) {
    LOG_WARN("failed to add aggregated tasks", KR(ret));
  } else if (DAS_OP_TABLE_DELETE != op_type &&
      OB_FAIL(aggregated_tasks_.store_obj(tmp_agg_task))) {
    LOG_WARN("failed to add aggregated tasks", KR(ret));
  } else {
    agg_task = tmp_agg_task;
    agg_task->server_ = tablet_loc->server_;
  }
  return ret;
}

int ObDASRef::add_aggregated_task(ObIDASTaskOp *das_task, ObDASOpType op_type)
{
  int ret = OB_SUCCESS;
  bool aggregated = false;
  ObDasAggregatedTask *agg_task = nullptr;

  if (OB_FAIL(find_agg_task(das_task->tablet_loc_, op_type, agg_task))) {
    LOG_WARN("fail to find agg_task", K(ret), K(op_type));
  } else if (OB_NOT_NULL(agg_task)) {
    if (OB_FAIL(OB_FAIL(agg_task->push_back_task(das_task)))) {
      LOG_WARN("fail to push back das_task", K(ret), KPC(das_task));
    }
  } else {
    // create agg_task
    if (OB_FAIL(create_agg_task(op_type, das_task->tablet_loc_, agg_task))) {
      LOG_WARN("fail to create agg_task", K(ret), K(op_type));
    } else if (OB_FAIL(agg_task->push_back_task(das_task))) {
      LOG_WARN("fail to push back das_task", K(ret), KPC(das_task));
    }
  }

  if (OB_SUCC(ret) && OB_FAIL(add_batched_task(das_task))) {
    LOG_WARN("add batched task failed", KR(ret), KPC(das_task));
  }
  return ret;
}

bool ObDASRef::check_tasks_same_ls_and_is_local(share::ObLSID &ls_id)
{
  ObIDASTaskOp *first_das_op = nullptr;
  bool is_all_same = true;
  DASTaskIter task_iter = begin_task_iter();
  const common::ObAddr &ctrl_addr = MTL(ObDataAccessService *)->get_ctrl_addr();
  if (!has_task()) {
    is_all_same = false;
  }
  while (is_all_same && !task_iter.is_end()) {
    ObIDASTaskOp *das_op = *task_iter;
    if (OB_ISNULL(first_das_op)) {
      first_das_op = das_op;
      if (first_das_op->get_tablet_loc()->server_ != ctrl_addr) {
        is_all_same = false;
      }
    } else if (first_das_op->get_ls_id() != das_op->get_ls_id() ||
        first_das_op->get_tablet_loc()->server_ != das_op->get_tablet_loc()->server_) {
      is_all_same = false;
    }
    ++task_iter;
  }

  if (is_all_same) {
    ls_id = first_das_op->get_ls_id();
  }
  return is_all_same;
}

void ObDASRef::reset()
{
  das_factory_.cleanup();
  batched_tasks_.destroy();
  del_aggregated_tasks_.destroy();
  aggregated_tasks_.destroy();
  lookup_cnt_ = 0;
  task_cnt_ = 0;
  init_mem_used_ = 0;
  das_ref_count_ctx_.reuse();
  das_parallel_ctx_.reset();
  if (task_map_.created()) {
    task_map_.destroy();
  }
  flags_ = false;
  expr_frame_info_ = nullptr;
  if (reuse_alloc_ != nullptr) {
    reuse_alloc_->reset();
    reuse_alloc_ = nullptr;
  }
}

void ObDASRef::reuse()
{
  das_factory_.cleanup();
  batched_tasks_.destroy();
  del_aggregated_tasks_.destroy();
  aggregated_tasks_.destroy();
  lookup_cnt_ = 0;
  task_cnt_ = 0;
  init_mem_used_ = 0;
  das_ref_count_ctx_.reuse();
  das_parallel_ctx_.reuse();
  if (task_map_.created()) {
    task_map_.destroy();
  }
  if (reuse_alloc_ != nullptr) {
    reuse_alloc_->reset_remain_one_page();
  } else {
    reuse_alloc_ = new(&reuse_alloc_buf_) common::ObArenaAllocator();
    reuse_alloc_->set_attr(das_alloc_.get_attr());
    das_alloc_.set_alloc(reuse_alloc_);
  }
}

void ObDasAggregatedTask::reset()
{
  server_.reset();
  tasks_.reset();
  failed_tasks_.reset();
  success_tasks_.reset();
}

void ObDasAggregatedTask::reuse()
{
  server_.reset();
  tasks_.reset();
  failed_tasks_.reset();
  success_tasks_.reset();
}

int ObDasAggregatedTask::push_back_task(ObIDASTaskOp *das_task)
{
  int ret = OB_SUCCESS;
  if (das_task->get_cur_agg_list()) {
    // if task already have linked list (in task retry), remove it first
    das_task->get_cur_agg_list()->remove(&das_task->get_node());
  }
  if (tasks_.get_size() == 0) {
    server_ = das_task->get_tablet_loc()->server_;
  }
  if (OB_UNLIKELY(!tasks_.add_last(&das_task->get_node()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to push back normal das task", K(ret));
  } else {
    das_task->set_cur_agg_list(&tasks_);
  }
  if (OB_SUCC(ret) && !das_task->get_agg_task()) {
    das_task->set_agg_task(this);
  }
  return ret;
}

int ObDasAggregatedTask::get_aggregated_tasks(common::ObIArray<ObIDASTaskOp *> &tasks) {
  int ret = OB_SUCCESS;
  ObIDASTaskOp *cur_task = nullptr;
  // 1. if have failed tasks, should explicitly get failed task via get_failed_tasks().
  if (OB_UNLIKELY(failed_tasks_.get_size() != 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("das aggregated failed task exist. couldn't get unstarted tasks.", K(ret));
  }

  // 3. if no unfinished high priority aggregated tasks exist, return all normal aggregated tasks.
  if (tasks.count() == 0 && OB_SUCC(ret)) {
    DLIST_FOREACH_X(curr, tasks_, OB_SUCC(ret)) {
      cur_task = curr->get_data();
      OB_ASSERT(cur_task != nullptr);
      OB_ASSERT(cur_task->get_cur_agg_list() == &tasks_);
      OB_ASSERT(ObDasTaskStatus::UNSTART == cur_task->get_task_status());
      if (OB_FAIL(tasks.push_back(cur_task))) {
        LOG_WARN("failed to push back high prio tasks", KR(ret), K(cur_task));
      }
    }
  }
  return ret;
}

int ObDasAggregatedTask::move_to_success_tasks(ObIDASTaskOp *das_task)
{
  int ret = OB_SUCCESS;
  das_task->get_cur_agg_list()->remove(&das_task->get_node());
  if (OB_UNLIKELY(!success_tasks_.add_last(&das_task->get_node()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to move task to success tasks", KR(ret));
  } else {
    das_task->set_cur_agg_list(&success_tasks_);
  }
  return ret;
}

int ObDasAggregatedTask::move_to_failed_tasks(ObIDASTaskOp *das_task)
{
  int ret = OB_SUCCESS;
  das_task->get_cur_agg_list()->remove(&das_task->get_node());
  if (OB_UNLIKELY(!failed_tasks_.add_last(&das_task->get_node()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to move task to success tasks", KR(ret));
  } else {
    das_task->set_cur_agg_list(&failed_tasks_);
  }
  return ret;
}

int ObDasAggregatedTask::get_failed_tasks(common::ObSEArray<ObIDASTaskOp *, 2> &tasks)
{
  int ret = OB_SUCCESS;
  ObIDASTaskOp *cur_task = nullptr;
  DLIST_FOREACH_X(curr, failed_tasks_, OB_SUCC(ret)) {
    cur_task = curr->get_data();
    OB_ASSERT(cur_task != nullptr);
    OB_ASSERT(ObDasTaskStatus::FAILED == cur_task->get_task_status());
    if (OB_FAIL(tasks.push_back(cur_task))) {
      LOG_WARN("failed to push back high prio tasks", KR(ret), K(cur_task));
    }
  }
  return ret;
}

bool ObDasAggregatedTask::has_unstart_tasks() const
{
  return tasks_.get_size() != 0 ;
}

int32_t ObDasAggregatedTask::get_unstart_task_size() const
{
  return tasks_.get_size();
}

int DASParallelContext::deep_copy_tx_desc(ObIAllocator &alloc, transaction::ObTxDesc *src_tx_desc)
{
  int ret = OB_SUCCESS;
  int64_t tx_desc_length = 0;
  int64_t ser_pos = 0;
  int64_t des_pos = 0;
  void *buf = nullptr;
  transaction::ObTxDesc *dst_tx_desc = nullptr;
  if (OB_ISNULL(src_tx_desc)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null ptr", K(ret));
  } else if (FALSE_IT(tx_desc_length = src_tx_desc->get_serialize_size())) {
    // do nothing
  } else if (OB_ISNULL(buf = alloc.alloc(tx_desc_length))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloca memory failed", K(ret));
  } else if (OB_FAIL(src_tx_desc->serialize(static_cast<char *>(buf), tx_desc_length, ser_pos))) {
    LOG_WARN("serialized tx_desc failed", K(ser_pos), K(tx_desc_length), K(ret));
  } else if (OB_FAIL(MTL(transaction::ObTransService*)->acquire_tx(static_cast<const char *>(buf), ser_pos, des_pos, dst_tx_desc))) {
    LOG_WARN("acquire tx_desc by deserialized failed", K(ser_pos), K(des_pos), K(ret));
  } else if (OB_ISNULL(dst_tx_desc)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(ser_pos), K(des_pos));
  } else if (ser_pos != des_pos) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("copy trans_desc failed", K(ret), K(ser_pos), K(des_pos));
  } else {
    tx_desc_bak_ = dst_tx_desc;
  }
  return ret;
}

int DASParallelContext::release_tx_desc()
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(tx_desc_bak_)) {
    transaction::ObTransService *txs = MTL(transaction::ObTransService*);
    txs->release_tx(*tx_desc_bak_);
    tx_desc_bak_ = NULL;
  }
  return ret;
}

int DASParallelContext::refresh_tx_desc_bak(ObIAllocator &alloc, transaction::ObTxDesc *src_tx_desc)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(tx_desc_bak_)) {
    if (OB_FAIL(deep_copy_tx_desc(alloc, src_tx_desc))) {
      LOG_WARN("fail to deep_copy tx_desc", K(ret));
    }
  } else if (tx_desc_bak_->get_op_sn() != src_tx_desc->get_op_sn()) {
    if (!has_refreshed_tx_desc_scn_) {
      if (OB_FAIL(release_tx_desc())) {
        LOG_WARN("fail to release tx_desc", K(ret));
      } else if (OB_FAIL(deep_copy_tx_desc(alloc, src_tx_desc))) {
        LOG_WARN("fail to deep copy tx_desc", K(ret), KPC(src_tx_desc));
      } else {
        has_refreshed_tx_desc_scn_ = true;
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected tx_desc op_scn", K(ret), K(tx_desc_bak_->get_op_sn()), K(src_tx_desc->get_op_sn()));
    }
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase

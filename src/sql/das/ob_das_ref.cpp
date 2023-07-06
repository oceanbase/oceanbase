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
#include "sql/das/ob_das_ref.h"
#include "sql/das/ob_das_extra_data.h"
#include "sql/das/ob_data_access_service.h"
#include "sql/das/ob_das_scan_op.h"
#include "sql/das/ob_das_insert_op.h"
#include "sql/das/ob_das_utils.h"
#include "storage/tx/ob_trans_service.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/das/ob_das_rpc_processor.h"
#include "sql/das/ob_das_retry_ctrl.h"
#include "observer/mysql/ob_query_retry_ctrl.h"

namespace oceanbase
{
using namespace common;
using namespace observer;
namespace sql
{
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
    frozen_op_node_(nullptr),
    expr_frame_info_(nullptr),
    wild_datum_info_(eval_ctx),
    aggregated_tasks_(das_alloc_),
    lookup_cnt_(0),
    task_cnt_(0),
    init_mem_used_(exec_ctx.get_allocator().used()),
    task_map_(),
    max_das_task_concurrency_(1),
    das_task_concurrency_limit_(1),
    cond_(),
    async_cb_list_(das_alloc_),
    flags_(0)
{
  int ret = OB_SUCCESS;
  max_das_task_concurrency_ = MTL(ObDataAccessService *)->get_das_concurrency_limit();
  OB_ASSERT(max_das_task_concurrency_ > 0);
  das_task_concurrency_limit_ = max_das_task_concurrency_;
  if (OB_FAIL(cond_.init(ObWaitEventIds::DAS_ASYNC_RPC_LOCK_WAIT))) {
    LOG_ERROR("Failed to init thread cond", K(ret), K(MTL_ID()));
  }
}

DASOpResultIter ObDASRef::begin_result_iter()
{
  return DASOpResultIter(batched_tasks_.begin(), wild_datum_info_);
}

ObIDASTaskOp* ObDASRef::find_das_task(const ObDASTabletLoc *tablet_loc, ObDASOpType op_type)
{
  int ret = OB_SUCCESS;
  ObIDASTaskOp *das_task = nullptr;
  if (nullptr == frozen_op_node_) {
    frozen_op_node_ = batched_tasks_.get_header_node();
  }
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
    DASTaskIter task_iter(frozen_op_node_->get_next(), batched_tasks_.get_header_node());
    for (; nullptr == das_task && !task_iter.is_end(); ++task_iter) {
      ObIDASTaskOp *tmp_task = *task_iter;
      if (tmp_task != nullptr &&
          tmp_task->get_tablet_loc() == tablet_loc &&
          tmp_task->get_type() == op_type) {
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
    DASTaskIter task_iter(frozen_op_node_->get_next(), batched_tasks_.get_header_node());
    for (; OB_SUCC(ret) && !task_iter.is_end(); ++task_iter) {
      ObIDASTaskOp *task = *task_iter;
      if (OB_ISNULL(task)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("task is null", KR(ret), KP(task));
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
  LOG_DEBUG("print all das_task before sort");
  print_all_das_task();
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

int ObDASRef::execute_all_task()
{
  int ret = OB_SUCCESS;
  const bool async = GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_4_1_0_0;
  // move local aggregated tasks to last for better concurrency.
  if (OB_FAIL(move_local_tasks_to_last())) {
    LOG_WARN("failed to move local tasks to last.", K(ret));
  } else {
    uint32_t finished_cnt = 0;
    uint32_t high_priority_task_execution_cnt = 0;
    bool has_unstart_high_priority_tasks = true;
    while (finished_cnt < aggregated_tasks_.get_size() && OB_SUCC(ret)) {
      finished_cnt = 0;
      // execute tasks follows aggregated task state machine.
      if (has_unstart_high_priority_tasks) {
        high_priority_task_execution_cnt = 0;
        DLIST_FOREACH_X(curr, aggregated_tasks_.get_obj_list(), OB_SUCC(ret)) {
          ObDasAggregatedTasks* aggregated_task = curr->get_obj();
          if (aggregated_task->has_unstart_high_priority_tasks()) {
            if (OB_FAIL(MTL(ObDataAccessService *)->execute_das_task(*this, *aggregated_task, async))) {
              LOG_WARN("failed to execute high priority aggregated das task", KR(ret), KPC(aggregated_task), K(async));
            } else {
              ++high_priority_task_execution_cnt;
              LOG_DEBUG("successfully executing aggregated task", "server", aggregated_task->server_);
            }
          }
        }
        if (high_priority_task_execution_cnt == 0) {
          has_unstart_high_priority_tasks = false;
        }
      }
      if (!has_unstart_high_priority_tasks) {
        DLIST_FOREACH_X(curr, aggregated_tasks_.get_obj_list(), OB_SUCC(ret)) {
          ObDasAggregatedTasks* aggregated_task = curr->get_obj();
          if (aggregated_task->has_unstart_tasks()) {
            if (OB_FAIL(MTL(ObDataAccessService *)->execute_das_task(*this, *aggregated_task, async))) {
              LOG_WARN("failed to execute aggregated das task", KR(ret), KPC(aggregated_task), K(async));
            } else {
              LOG_DEBUG("successfully executing aggregated task", "server", aggregated_task->server_);
            }
          }
        }
      }
      // wait all existing tasks to be finished
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(wait_executing_tasks())) {
        LOG_WARN("failed to process all async remote tasks", KR(ret));
      }
      ret = COVER_SUCC(tmp_ret);
      if (check_rcode_can_retry(ret)) {
        ret = OB_SUCCESS;
      }
      if (OB_SUCC(ret)) {
        // check das task status.
        DLIST_FOREACH_X(curr, aggregated_tasks_.get_obj_list(), OB_SUCC(ret)) {
          ObDasAggregatedTasks* aggregated_task = curr->get_obj();
          if (aggregated_task->has_unstart_tasks()) {
            if (aggregated_task->has_failed_tasks()) {
              // retry all failed tasks.
              common::ObSEArray<ObIDASTaskOp *, 2> failed_tasks;
              int tmp_ret = OB_SUCCESS;
              if (OB_TMP_FAIL(aggregated_task->get_failed_tasks(failed_tasks))) {
                LOG_WARN("failed to get failed tasks", K(ret));
              } else if (failed_tasks.count() == 0) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("failed to get failed tasks");
              } else {
                for (int i = 0; OB_SUCC(ret) && i < failed_tasks.count(); i++) {
                  ObIDASTaskOp *failed_task = failed_tasks.at(i);
                  if (!GCONF._enable_partition_level_retry || !failed_task->can_part_retry()) {
                    ret = failed_task->errcode_;
                  } else if (OB_FAIL(MTL(ObDataAccessService *)->retry_das_task(*this, *failed_tasks.at(i)))) {
                    LOG_WARN("Failed to retry das task", K(ret));
                  }
                }
              }
            } else {
              // proceed while loop for other unfinished tasks.
            }
          } else {
            ++finished_cnt;
#if !defined(NDEBUG)
            OB_ASSERT(aggregated_task->high_priority_tasks_.get_size() == 0);
            OB_ASSERT(aggregated_task->tasks_.get_size() == 0);
            OB_ASSERT(aggregated_task->failed_tasks_.get_size() == 0);
            OB_ASSERT(aggregated_task->success_tasks_.get_size() != 0);
            DLIST_FOREACH_X(curr_task, aggregated_task->success_tasks_, true) {
              ObIDASTaskOp *tmp_task = curr_task->get_data();
              OB_ASSERT(ObDasTaskStatus::FINISHED == tmp_task->get_task_status());
            }
#endif
          }
        }
        LOG_DEBUG("current das task status", K(finished_cnt), K(aggregated_tasks_.get_size()));
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

int ObDASRef::wait_executing_tasks()
{
  int ret = OB_SUCCESS;
  {
    ObThreadCondGuard guard(cond_);
    while (OB_SUCC(ret) && get_current_concurrency() < max_das_task_concurrency_) {
      // we cannot use ObCond here because it can not explicitly lock mutex, causing concurrency problem.
      if (OB_FAIL(cond_.wait())) {
        LOG_WARN("failed to wait all das tasks to be finished.", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(process_remote_task_resp())) {
      LOG_WARN("failed to process remote task resp", K(ret));
    }
  }
  return ret;
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
    ObDasAggregatedTasks* aggregated_task = curr->get_obj();
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

void ObDASRef::set_frozen_node()
{
  frozen_op_node_ = batched_tasks_.get_last_node();
  lookup_cnt_ = 0;
  task_cnt_ = 0;
  if (task_map_.created()) {
    task_map_.clear();
  }
}

int ObDASRef::close_all_task()
{
  int ret = OB_SUCCESS;
  int last_end_ret = OB_SUCCESS;
  if (has_task()) {
    FLTSpanGuard(close_das_task);
    ObSQLSessionInfo *session = nullptr;

    DASTaskIter task_iter = begin_task_iter();
    while (OB_SUCC(ret) && !task_iter.is_end()) {
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

    if (OB_SUCC(ret)) {
      if (OB_ISNULL(session = exec_ctx_.get_my_session())) {
        ret = OB_NOT_INIT;
        LOG_WARN("session is nullptr", K(ret));
      }
    }
    bool merge_trans_result_fail = (ret != OB_SUCCESS);
    // any fail during merge trans_result,
    // need set trans_result incomplete, in order to
    // indicate some transaction participants info unknown
    if (merge_trans_result_fail && OB_NOT_NULL(session)) {
      LOG_WARN("close all task fail, set trans_result to incomplete");
      session->get_trans_result().set_incomplete();
    }
    batched_tasks_.destroy();
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
  int64_t task_id;
  if (OB_FAIL(MTL(ObDataAccessService*)->get_das_task_id(task_id))) {
    LOG_WARN("get das task id failed", KR(ret));
  } else if (OB_FAIL(das_factory.create_das_task_op(op_type, task_op))) {
    LOG_WARN("create das task op failed", K(ret), KPC(task_op));
  } else {
    task_op->set_trans_desc(session->get_tx_desc());
    task_op->set_snapshot(&get_exec_ctx().get_das_ctx().get_snapshot());
    task_op->set_tenant_id(session->get_effective_tenant_id());
    task_op->set_task_id(task_id);
    task_op->in_stmt_retry_ = session->get_is_in_retry();
    task_op->set_tablet_id(tablet_loc->tablet_id_);
    task_op->set_ls_id(tablet_loc->ls_id_);
    task_op->set_tablet_loc(tablet_loc);
    if (OB_FAIL(add_aggregated_task(task_op))) {
      LOG_WARN("failed to add aggregated task", KR(ret));
    }
  }
  return ret;
}

int ObDASRef::add_aggregated_task(ObIDASTaskOp *das_task)
{
  int ret = OB_SUCCESS;
  bool aggregated = false;
  DLIST_FOREACH_X(curr, aggregated_tasks_.get_obj_list(), !aggregated && OB_SUCC(ret)) {
    ObDasAggregatedTasks* aggregated_task = curr->get_obj();
    if (aggregated_task->server_ == das_task->tablet_loc_->server_) {
      if (OB_FAIL(aggregated_task->push_back_task(das_task))) {
        LOG_WARN("failed to add aggregated tasks", KR(ret));
      } else {
        aggregated = true;
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (!aggregated) {
    void *buf = das_alloc_.alloc(sizeof(ObDasAggregatedTasks));
    ObDasAggregatedTasks *agg_tasks = nullptr;
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory for aggregated tasks", KR(ret));
    } else if (FALSE_IT(agg_tasks = new(buf) ObDasAggregatedTasks(das_alloc_))) {
    } else if (OB_FAIL(aggregated_tasks_.store_obj(agg_tasks))) {
      LOG_WARN("failed to add aggregated tasks", KR(ret));
    } else if (OB_FAIL(agg_tasks->push_back_task(das_task))) {
      LOG_WARN("failed to add task", K(ret));
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(add_batched_task(das_task))) {
    LOG_WARN("add batched task failed", KR(ret), KPC(das_task));
  }
  return ret;
}

void ObDASRef::reset()
{
  das_factory_.cleanup();
  batched_tasks_.destroy();
  aggregated_tasks_.destroy();
  lookup_cnt_ = 0;
  task_cnt_ = 0;
  init_mem_used_ = 0;
  if (task_map_.created()) {
    task_map_.destroy();
  }
  flags_ = false;
  frozen_op_node_ = nullptr;
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
  aggregated_tasks_.destroy();
  lookup_cnt_ = 0;
  task_cnt_ = 0;
  init_mem_used_ = 0;
  if (task_map_.created()) {
    task_map_.destroy();
  }
  frozen_op_node_ = nullptr;
  if (reuse_alloc_ != nullptr) {
    reuse_alloc_->reset_remain_one_page();
  } else {
    reuse_alloc_ = new(&reuse_alloc_buf_) common::ObArenaAllocator();
    reuse_alloc_->set_attr(das_alloc_.get_attr());
    das_alloc_.set_alloc(reuse_alloc_);
  }
}

int32_t ObDASRef::get_current_concurrency() const
{
  return ATOMIC_LOAD(&das_task_concurrency_limit_);
};

void ObDASRef::inc_concurrency_limit()
{
  ATOMIC_INC(&das_task_concurrency_limit_);
}

void ObDASRef::inc_concurrency_limit_with_signal()
{
  ObThreadCondGuard guard(cond_);
  if (__sync_add_and_fetch(&das_task_concurrency_limit_, 1) == max_das_task_concurrency_) {
    cond_.signal();
  }
}

int ObDASRef::dec_concurrency_limit()
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
int ObDASRef::acquire_task_execution_resource()
{
  int ret = OB_SUCCESS;
  OB_ASSERT(get_current_concurrency() >= 0);
  if (OB_FAIL(dec_concurrency_limit())) {
    LOG_WARN("failed to acquire das execution resource", K(ret), K(get_current_concurrency()));
  }
  if (OB_UNLIKELY(OB_SIZE_OVERFLOW == ret)) {
    ret = OB_SUCCESS;
    ObThreadCondGuard guard(cond_);
    if (OB_FAIL(cond_.wait(get_exec_ctx().get_my_session()->get_query_timeout_ts() -
        ObTimeUtility::current_time()))) {
      LOG_WARN("failed to acquire das task execution resource", K(ret), K(get_current_concurrency()));
    } else if (OB_FAIL(dec_concurrency_limit())) {
      LOG_WARN("failed to acquire das execution resource", K(ret), K(get_current_concurrency()));
    }
  }
  return ret;
}

void ObDasAggregatedTasks::reset()
{
  server_.reset();
  high_priority_tasks_.reset();
  tasks_.reset();
  failed_tasks_.reset();
  success_tasks_.reset();
}

void ObDasAggregatedTasks::reuse()
{
  server_.reset();
  high_priority_tasks_.reset();
  tasks_.reset();
  failed_tasks_.reset();
  success_tasks_.reset();
}

int ObDasAggregatedTasks::push_back_task(ObIDASTaskOp *das_task)
{
  int ret = OB_SUCCESS;
  if (das_task->get_cur_agg_list()) {
    // if task already have linked list (in task retry), remove it first
    das_task->get_cur_agg_list()->remove(&das_task->get_node());
  }
  if (high_priority_tasks_.get_size() == 0 && tasks_.get_size() == 0) {
    server_ = das_task->get_tablet_loc()->server_;
  }
  if (ObDASOpType::DAS_OP_TABLE_DELETE == das_task->get_type()) {
    // we move all DELETE das op to high priority tasks anyway.
    if (OB_UNLIKELY(!high_priority_tasks_.add_last(&das_task->get_node()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to push back high priority task", K(ret));
    } else {
      das_task->set_cur_agg_list(&high_priority_tasks_);
    }
  } else if (OB_UNLIKELY(!tasks_.add_last(&das_task->get_node()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to push back normal das task", K(ret));
  } else {
    das_task->set_cur_agg_list(&tasks_);
  }
  if (OB_SUCC(ret) && !das_task->get_agg_tasks()) {
    das_task->set_agg_tasks(this);
  }
  return ret;
}

int ObDasAggregatedTasks::get_aggregated_tasks(
    common::ObSEArray<ObIDASTaskOp *, 2> &tasks) {
  int ret = OB_SUCCESS;
  ObIDASTaskOp *cur_task = nullptr;
  // 1. if have failed tasks, should explicitly get failed task via get_failed_tasks().
  if (OB_UNLIKELY(failed_tasks_.get_size() != 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("das aggregated failed task exist. couldn't get unstarted tasks.", K(ret));
  }

  // 2. if no failed tasks exist and have unfinished high priority aggregated tasks, return all high priority tasks.
  if (tasks.count() == 0 && OB_SUCC(ret)) {
    DLIST_FOREACH_X(curr, high_priority_tasks_, OB_SUCC(ret)) {
      cur_task = curr->get_data();
      OB_ASSERT(cur_task != nullptr);
      OB_ASSERT(cur_task->get_cur_agg_list() == &high_priority_tasks_);
      OB_ASSERT(ObDASOpType::DAS_OP_TABLE_DELETE == cur_task->get_type());
      OB_ASSERT(ObDasTaskStatus::UNSTART == cur_task->get_task_status());
      if (OB_FAIL(tasks.push_back(cur_task))) {
        LOG_WARN("failed to push back high prio tasks", KR(ret), K(cur_task));
      }
    }
  }

  // 3. if no unfinished high priority aggregated tasks exist, return all normal aggregated tasks.
  if (tasks.count() == 0 && OB_SUCC(ret)) {
    DLIST_FOREACH_X(curr, tasks_, OB_SUCC(ret)) {
      cur_task = curr->get_data();
      OB_ASSERT(cur_task != nullptr);
      OB_ASSERT(cur_task->get_cur_agg_list() == &tasks_);
      OB_ASSERT(ObDASOpType::DAS_OP_TABLE_DELETE != cur_task->get_type());
      OB_ASSERT(ObDasTaskStatus::UNSTART == cur_task->get_task_status());
      if (OB_FAIL(tasks.push_back(cur_task))) {
        LOG_WARN("failed to push back high prio tasks", KR(ret), K(cur_task));
      }
    }
  }
  return ret;
}

int ObDasAggregatedTasks::get_aggregated_tasks(
    common::ObSEArray<common::ObSEArray<ObIDASTaskOp *, 2>, 2> &task_groups,
    int64_t count)
{
  int ret = OB_SUCCESS;
  ObIDASTaskOp *cur_task = nullptr;
  // 1. if have failed tasks, should explicitly get failed task via get_failed_tasks().
  if (OB_UNLIKELY(failed_tasks_.get_size() != 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("das aggregated failed task exist. couldn't get unstarted tasks.", K(ret));
  } else if (OB_LIKELY(count == 1)) {
    if (OB_FAIL(task_groups.push_back(common::ObSEArray<ObIDASTaskOp *, 2>()))) {
      LOG_WARN("failed to push back", K(ret));
    } else if (OB_FAIL(get_aggregated_tasks(task_groups.at(0)))) {
      LOG_WARN("Failed to get aggregated das task", KR(ret));
    }
  } else {
    // 2. if no failed tasks exist and have unfinished high priority aggregated tasks, return all high priority tasks.
    if (task_groups.count() == 0 && high_priority_tasks_.get_size() != 0 && OB_SUCC(ret)) {
      int idx = 0;
      if (high_priority_tasks_.get_size() < count) {
        count = high_priority_tasks_.get_size();
      }
      for (int i = 0; OB_SUCC(ret) && i < count; i++) {
        if (OB_FAIL(task_groups.push_back(common::ObSEArray<ObIDASTaskOp *, 2>()))) {
          LOG_WARN("failed to push back", K(ret));
        }
      }
      DLIST_FOREACH_X(curr, high_priority_tasks_, OB_SUCC(ret)) {
        cur_task = curr->get_data();
        OB_ASSERT(cur_task != nullptr);
        OB_ASSERT(ObDASOpType::DAS_OP_TABLE_DELETE == cur_task->get_type());
        OB_ASSERT(ObDasTaskStatus::UNSTART == cur_task->get_task_status());
        if (OB_FAIL(task_groups.at(idx++ % count).push_back(cur_task))) {
          LOG_WARN("failed to push back high prio tasks", KR(ret), K(cur_task));
        }
      }
    }

    // 3. if no unfinished high priority aggregated tasks exist, return all normal aggregated tasks.
    if (task_groups.count() == 0 && tasks_.get_size() != 0 && OB_SUCC(ret)) {
      int idx = 0;
      if (tasks_.get_size() < count) {
        count = tasks_.get_size();
      }
      for (int i = 0; OB_SUCC(ret) && i < count; i++) {
        if (OB_FAIL(task_groups.push_back(common::ObSEArray<ObIDASTaskOp *, 2>()))) {
          LOG_WARN("failed to push back", K(ret));
        }
      }
      DLIST_FOREACH_X(curr, tasks_, OB_SUCC(ret)) {
        cur_task = curr->get_data();
        OB_ASSERT(cur_task != nullptr);
        OB_ASSERT(ObDASOpType::DAS_OP_TABLE_DELETE != cur_task->get_type());
        OB_ASSERT(ObDasTaskStatus::UNSTART == cur_task->get_task_status());
        if (OB_FAIL(task_groups.at(idx++ % count).push_back(cur_task))) {
          LOG_WARN("failed to push back high prio tasks", KR(ret), K(cur_task));
        }
      }
    }
  }
  return ret;
}

int ObDasAggregatedTasks::move_to_success_tasks(ObIDASTaskOp *das_task)
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

int ObDasAggregatedTasks::move_to_failed_tasks(ObIDASTaskOp *das_task)
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

int ObDasAggregatedTasks::get_failed_tasks(common::ObSEArray<ObIDASTaskOp *, 2> &tasks)
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

bool ObDasAggregatedTasks::has_unstart_tasks() const
{
  return high_priority_tasks_.get_size() != 0 ||
         tasks_.get_size() != 0 ||
         failed_tasks_.get_size() != 0;
}

bool ObDasAggregatedTasks::has_unstart_high_priority_tasks() const
{
  return high_priority_tasks_.get_size() != 0;
}

int32_t ObDasAggregatedTasks::get_unstart_task_size() const
{
  return tasks_.get_size() + high_priority_tasks_.get_size();
}

}  // namespace sql
}  // namespace oceanbase

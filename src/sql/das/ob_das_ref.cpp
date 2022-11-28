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
namespace oceanbase
{
using namespace common;
namespace sql
{
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
    flags_(0)
{
}

DASOpResultIter ObDASRef::begin_result_iter()
{
  return DASOpResultIter(batched_tasks_.begin(), wild_datum_info_);
}

ObIDASTaskOp* ObDASRef::find_das_task(const ObDASTabletLoc *tablet_loc, ObDASOpType op_type)
{
  ObIDASTaskOp *das_task = nullptr;
  if (nullptr == frozen_op_node_) {
    frozen_op_node_ = batched_tasks_.get_header_node();
  }
  DASTaskIter task_iter(frozen_op_node_->get_next(), batched_tasks_.get_header_node());
  for (; nullptr == das_task && !task_iter.is_end(); ++task_iter) {
    ObIDASTaskOp *tmp_task = *task_iter;
    if (tmp_task != nullptr &&
        tmp_task->get_tablet_loc() == tablet_loc &&
        tmp_task->get_type() == op_type) {
      das_task = tmp_task;
    }
  }
  return das_task;
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
  bool DAS_TASK_AGGREGATION = false;
  if (DAS_TASK_AGGREGATION) {
    // TODO(roland.qk): DAS task aggregation.
  } else {
    DASTaskIter task_iter = begin_task_iter();
    while (OB_SUCC(ret) && !task_iter.is_end()) {
      if (OB_FAIL(MTL(ObDataAccessService*)->execute_das_task(*this, **task_iter))) {
        LOG_WARN("execute das task failed", K(ret));
      }
      ++task_iter;
    }
  }

  return ret;
}

void ObDASRef::set_frozen_node()
{
  frozen_op_node_ = batched_tasks_.get_last_node();
}

int ObDASRef::close_all_task()
{
  int ret = OB_SUCCESS;
  int last_end_ret = OB_SUCCESS;
  if (has_task()) {
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
  } else if (OB_FAIL(add_batched_task(task_op))) {
    LOG_WARN("add batched task failed", K(ret), KPC(task_op));
  } else {
    task_op->set_trans_desc(session->get_tx_desc());
    task_op->set_snapshot(&get_exec_ctx().get_das_ctx().get_snapshot());
    task_op->set_tenant_id(session->get_effective_tenant_id());
    task_op->set_task_id(task_id);
    task_op->in_stmt_retry_ = session->get_is_in_retry();
    task_op->set_tablet_id(tablet_loc->tablet_id_);
    task_op->set_ls_id(tablet_loc->ls_id_);
    task_op->set_tablet_loc(tablet_loc);
    if (OB_FAIL(task_op->init_task_info())) {
      LOG_WARN("init task info failed", K(ret));
    }
  }
  return ret;
}

void ObDASRef::reset()
{
  das_factory_.cleanup();
  batched_tasks_.destroy();
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
  frozen_op_node_ = nullptr;
  if (reuse_alloc_ != nullptr) {
    reuse_alloc_->reset_remain_one_page();
  } else {
    reuse_alloc_ = new(&reuse_alloc_buf_) common::ObArenaAllocator();
    reuse_alloc_->set_attr(das_alloc_.get_attr());
    das_alloc_.set_alloc(reuse_alloc_);
  }
}
}  // namespace sql
}  // namespace oceanbase

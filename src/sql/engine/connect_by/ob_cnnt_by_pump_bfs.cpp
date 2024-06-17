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

#define USING_LOG_PREFIX SQL_ENG

#include "sql/engine/connect_by/ob_nl_cnnt_by_with_index_op.h"
#include "sql/engine/connect_by/ob_cnnt_by_pump_bfs.h"
#include "sql/engine/ob_operator.h"

using namespace oceanbase::sql;
using namespace oceanbase::common;

int ObConnectByOpBFSPump::PathNode::init_path_array(const int64_t size)
{
  int ret = common::OB_SUCCESS;
  if (size < 0) {
    ret = common::OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(size));
  } else if (0 == size) {
  } else if (OB_FAIL(paths_.prepare_allocate(size))) {
    LOG_WARN("Failed to init array", K(ret), K(size));
  }
  return ret;
}

bool ObConnectByOpBFSPump::RowComparer::operator()(const PumpNode &pump_node1, const PumpNode &pump_node2)
{
  bool bret = false;
  const ObChunkDatumStore::StoredRow *l = pump_node1.output_row_;
  const ObChunkDatumStore::StoredRow *r = pump_node2.output_row_;
  if (OB_UNLIKELY(OB_SUCCESS != ret_)) {
    // do nothing if we already have an error,
  } else if (OB_ISNULL(l)
             || OB_ISNULL(r)
             || OB_UNLIKELY(l->cnt_ != r->cnt_)) {
    ret_ = OB_ERR_UNEXPECTED;
    LOG_WARN_RET(ret_, "invalid parameter", KPC(l), KPC(r), K(ret_));
  } else {
    const ObDatum *lcells = l->cells();
    const ObDatum *rcells = r->cells();
    int cmp = 0;
    for (int64_t i = 0; 0 == cmp && i < sort_cmp_funs_->count() && ret_ == OB_SUCCESS; i++) {
      const int64_t idx = sort_collations_->at(i).field_idx_;
      ret_ = sort_cmp_funs_->at(i).cmp_func_(lcells[idx], rcells[idx], cmp);
      if (OB_SUCCESS != ret_) {
        LOG_WARN_RET(ret_, "failed to compare", KPC(l), KPC(r), K(idx), K(ret_));
      } else if (cmp < 0) {
        bret = sort_collations_->at(i).is_ascending_;
      } else if (cmp > 0) {
        bret = !sort_collations_->at(i).is_ascending_;
      }
    }
  }
  return bret;
}

//在ObNLConnectByOp的rescan接口中会调用，需要保证其为open之后的状态
void ObConnectByOpBFSPump::reset()
{
  free_memory_for_rescan();
  pump_stack_.reset();
  path_stack_.reset();
  free_record_.reset();
  cur_level_ = 1;
}

int ObConnectByOpBFSPump::sort_sibling_rows()
{
  int ret = OB_SUCCESS;
  PumpNode pump_node;
  //sort siblings
  if (OB_ISNULL(sort_collations_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected sort columns", K(ret));
  } else if (OB_UNLIKELY(0 != sort_collations_->count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected sort in connect by", K(ret));
  }

  // Since there is an external sort operator, the sort function here is actually no longer used.
  // It is only used to reverse the order of the data in sort_stack and store it in pump_stack.

  //add siblings to pump stack
  while(OB_SUCC(ret) && false == sort_stack_.empty()) {
    pump_node.reset();
    if (OB_FAIL(sort_stack_.pop_back(pump_node))) {
      LOG_WARN("fail to pop back stack", K(ret));
    } else if (OB_FAIL(pump_stack_.push_back(pump_node))) {
      LOG_WARN("fail to pop back stack", K(ret));
      int tmp_ret = free_pump_node(pump_node);
      if (OB_SUCCESS != tmp_ret) {
        LOG_WARN("fail to free pump node", K(tmp_ret));
      }
    }
  }
  return ret;
}

int ObConnectByOpBFSPump::add_root_row(
  const ObIArray<ObExpr*> &root_row,
  const ObIArray<ObExpr*> &output_row)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(false == pump_stack_.empty())
      || OB_UNLIKELY(false == path_stack_.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pump stack must be empty", K(pump_stack_), K(path_stack_), K(ret));
  } else if (OB_FAIL(push_back_row_to_stack(root_row, output_row))) {
    LOG_WARN("fail to push back row", K(ret));
  } else {
    LOG_DEBUG("connect by add row", K(root_row), K(output_row), K(lbt()));
  }
  return ret;
}

void ObConnectByOpBFSPump::free_memory_for_rescan()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(free_path_stack())) {
    LOG_ERROR("fail to free path stack", K(ret));
  }

  if (OB_FAIL(free_pump_node_stack(pump_stack_))) {
    // overwrite ret
    LOG_ERROR("fail to free pump stack", K(ret));
  }

  if (OB_FAIL(free_pump_node_stack(sort_stack_))) {
    // overwrite ret
    LOG_ERROR("fail to free sort stack", K(ret));
  }

  if (OB_FAIL(free_record_rows())) {
    // overwrite ret
    LOG_ERROR("fail to free record rows", K(ret));
  }
}

void ObConnectByOpBFSPump::free_memory()
{
  free_memory_for_rescan();
}

int ObConnectByOpBFSPump::free_path_stack()
{
  int ret = OB_SUCCESS;
  PathNode pop_node;
  while(OB_SUCC(ret) && false == path_stack_.empty()) {
    if (OB_FAIL(path_stack_.pop_back(pop_node))) {
      LOG_WARN("fail to pop back", K(ret));
    } else if (OB_ISNULL(pop_node.prior_exprs_result_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid pop node", K(ret));
    } else {
      allocator_.free(const_cast<ObChunkDatumStore::StoredRow *>(pop_node.prior_exprs_result_));
      pop_node.prior_exprs_result_ = NULL;
      for (int64_t i = 0; OB_SUCC(ret) && i < connect_by_path_count_; ++i) {
        if (pop_node.paths_.at(i).ptr() != NULL) {
          allocator_.free(pop_node.paths_.at(i).ptr());
          pop_node.paths_.at(i).reset();
        }
      }
    }
  }
  return ret;
}

int ObConnectByOpBFSPump::free_pump_node(PumpNode &node)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(node.pump_row_)
      || OB_ISNULL(node.output_row_)
      || OB_ISNULL(node.path_node_.prior_exprs_result_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid pop node", K(node), K(ret));
  } else {
    allocator_.free(const_cast<ObChunkDatumStore::StoredRow *>(node.pump_row_));
    allocator_.free(const_cast<ObChunkDatumStore::StoredRow *>(node.output_row_));
    allocator_.free(
      const_cast<ObChunkDatumStore::StoredRow *>(node.path_node_.prior_exprs_result_));
    node.pump_row_ = NULL;
    node.output_row_ = NULL;
    node.path_node_.prior_exprs_result_ = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < connect_by_path_count_; ++i) {
      if (node.path_node_.paths_[i].ptr() != NULL) {
        LOG_ERROR("unexpected path value");
        allocator_.free(node.path_node_.paths_.at(i).ptr());
        node.path_node_.paths_.at(i).reset();
      }
    }
  }
  return ret;
}

int ObConnectByOpBFSPump::free_pump_node_stack(ObSegmentArray<PumpNode> &stack)
{
  int ret = OB_SUCCESS;
  PumpNode pop_node;
  while(OB_SUCC(ret) && false == stack.empty()) {
    if (OB_FAIL(stack.pop_back(pop_node))) {
      LOG_WARN("fail to pop back pump_stack", K(ret));
    } else if (OB_FAIL(free_pump_node(pop_node))) {
      LOG_WARN("fail to free node", K(ret));
    }
  }
  return ret;
}

int ObConnectByOpBFSPump::push_back_row_to_stack(
  const ObIArray<ObExpr*> &left_row,
  const ObIArray<ObExpr*> &output_row)
{
  int ret = OB_SUCCESS;
  PumpNode pump_node;
  const ObChunkDatumStore::StoredRow *new_left_row = nullptr;
  const ObChunkDatumStore::StoredRow *new_output_row = nullptr;
  LOG_DEBUG("push back row to stack",
    K(ObToStringExprRow(*eval_ctx_, output_row)),
    K(ObToStringExprRow(*eval_ctx_, left_row)),
    K(ObToStringExprRow(*eval_ctx_, *connect_by_prior_exprs_)),
    K(ObToStringExprRow(*eval_ctx_, *left_prior_exprs_)));
  // 这里特殊处理下：由于判断cycle的prior c1，c1是来自于left，但其实老逻辑用的是left_row，也即从right读出来的数据
  // 所以这里先 save left_prior_expr，然后calc_path_node，在restore恢复left_prior_expr
  if (OB_FAIL(deep_copy_row(left_row, new_left_row))) {
    LOG_WARN("fail to deep copy", K(ret));
  } else if (OB_FAIL(deep_copy_row(output_row, new_output_row))) {
    LOG_WARN("fail to deep copy row", K(ret));
  } else if (OB_FAIL(calc_path_node(pump_node))) {
    LOG_WARN("fail to calc path node", K(pump_node), K(ret));
  } else if (OB_FAIL(connect_by_->restore_prior_expr())) {
    LOG_WARN("failed to restore prior expr", K(ret));
  } else if (OB_ISNULL(pump_node.pump_row_ = new_left_row)
             || OB_ISNULL(pump_node.output_row_ = new_output_row)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("copy row is NULL", KPC(new_output_row), KPC(new_left_row), K(ret));
  } else if (OB_FAIL(sort_stack_.push_back(pump_node))) {
    LOG_WARN("fail to push back row", K(ret));
  } else if (pump_node.is_cycle_) {
    ret = OB_ERR_CBY_LOOP;
    LOG_WARN("there is a cycle", K(ret));
  } else {
    LOG_DEBUG("connect by pump node", K(pump_node), K(left_row), K(output_row),
      K(ObToStringExprRow(*eval_ctx_, *connect_by_prior_exprs_)));
  }

  if (OB_FAIL(ret) && OB_ERR_CBY_LOOP != ret) {//if fail free memory
    // 由于产生环，所以上面报错，导致没有restore，从而数据错了
    if (pump_node.path_node_.prior_exprs_result_ != NULL) {
      allocator_.free(const_cast<ObChunkDatumStore::StoredRow *>(pump_node.path_node_.prior_exprs_result_));
      pump_node.path_node_.prior_exprs_result_ = NULL;
    }
    if (new_left_row != NULL) {
      allocator_.free(const_cast<ObChunkDatumStore::StoredRow *>(new_left_row));
      new_left_row = NULL;
      pump_node.pump_row_ = NULL;
    }
    if (new_output_row != NULL) {
      allocator_.free(const_cast<ObChunkDatumStore::StoredRow *>(new_output_row));
      new_output_row = NULL;
      pump_node.output_row_ = NULL;
    }
  }
  return ret;
}

int ObConnectByOpBFSPump::append_row(
  const ObIArray<ObExpr*> &right_row,
  const ObIArray<ObExpr*> &output_row)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(push_back_row_to_stack(right_row, output_row))) {
    LOG_WARN("fail to push back row to stack", K(ret));
  }
  return ret;
}



int ObConnectByOpBFSPump::init(ObNLConnectByWithIndexSpec &connect_by,
  ObNLConnectByWithIndexOp &connect_by_op,
  ObEvalCtx &eval_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else {
    connect_by_ = &connect_by_op;
    connect_by_prior_exprs_ = &connect_by.connect_by_prior_exprs_;
    left_prior_exprs_ = &connect_by.left_prior_exprs_;
    sort_collations_ = &connect_by.sort_collations_;
    sort_cmp_funs_ = &connect_by.sort_cmp_funs_;
    cmp_funcs_ = &connect_by.cmp_funcs_;
    eval_ctx_ = &eval_ctx;
    never_meet_cycle_ = !connect_by.has_prior_;
    connect_by_path_count_ = connect_by.get_sys_connect_by_path_expression_count();
    is_nocycle_ = connect_by.is_nocycle_;
    is_inited_ = true;
  }
  return ret;
}

int ObConnectByOpBFSPump::free_record_rows()
{
  //free last pump row and output row
  int ret = OB_SUCCESS;
  int64_t free_record_count = free_record_.count();
  if (OB_UNLIKELY(free_record_count != 2)
      && OB_UNLIKELY(free_record_count != 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected free_record count", K(free_record_count));
  } else if (free_record_count != 0) {
    for (int i = 0; OB_SUCC(ret) && i < free_record_count; ++i) {
      const ObChunkDatumStore::StoredRow *free_row = NULL;
      if (OB_FAIL(free_record_.pop_back(free_row))) {
        LOG_WARN("fail to pop back value", K(i), K(ret));
      } else {
        allocator_.free(const_cast<ObChunkDatumStore::StoredRow *>(free_row));
        free_row = NULL;
      }
    }
  }
  return ret;
}

int ObConnectByOpBFSPump::add_path_stack(PathNode &path_node)
{
  int ret = OB_SUCCESS;
  bool has_added = false;
  PathNode pop_node;
  if (OB_FAIL(pop_node.init_path_array(connect_by_path_count_))) {
    LOG_WARN("Failed to init path array", K(ret));
  } else if (0 == path_stack_.count()) {
    if (OB_FAIL(path_stack_.push_back(path_node))) {
      LOG_WARN("fail to add path node", K(path_node), K(ret));
    } else {
      LOG_DEBUG("Push back path node", K(path_node));
      has_added = true;
    }
  } else {
    while(OB_SUCC(ret) && false == has_added && false == path_stack_.empty()) {
      PathNode &cur_node = path_stack_.at(path_stack_.count() - 1);
      if (cur_node.level_  == path_node.level_ - 1) {
        if (OB_FAIL(path_stack_.push_back(path_node))) {
          LOG_WARN("fail to add path node", K(path_node), K(ret));
        } else {
          has_added = true;
          LOG_DEBUG("Push back path node", K(path_node));
        }
      } else if (cur_node.level_ > path_node.level_ - 1) {
        if (OB_FAIL(path_stack_.pop_back(pop_node))) {
          LOG_WARN("fail to pop back", K(ret));
        } else if (OB_ISNULL(pop_node.prior_exprs_result_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid pop node", K(ret));
        } else {
          LOG_DEBUG("Pop path node", K(path_node));
          allocator_.free(const_cast<ObChunkDatumStore::StoredRow *>(pop_node.prior_exprs_result_));
          pop_node.prior_exprs_result_ = NULL;
          for (int64_t i = 0; OB_SUCC(ret) && i < connect_by_path_count_; ++i) {
            if (pop_node.paths_[i].ptr() != NULL) {
              allocator_.free(pop_node.paths_.at(i).ptr());
              pop_node.paths_.at(i).reset();
            }
          }
          if (0 == path_stack_.count()) {//current path_node is root node
            if (OB_FAIL(path_stack_.push_back(path_node))) {
              LOG_WARN("fail to push back path_node", K(path_node), K(ret));
            } else {
              LOG_DEBUG("Push back path node", K(path_node));
              has_added = true;
            }
          }
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid cur_node level", K(cur_node));
      }
    }
  }
  if (false == has_added) {//free memory
    allocator_.free(const_cast<ObChunkDatumStore::StoredRow *>(path_node.prior_exprs_result_));
    path_node.prior_exprs_result_ = NULL;
  }
  LOG_DEBUG("connect by get path node", K(path_node));
  return ret;
}

bool ObConnectByOpBFSPump::is_root_node()
{
  int is_root_node = false;
  if (OB_UNLIKELY(path_stack_.count() <= 0)) {
  } else {
    const PathNode &cur_node = path_stack_.at(path_stack_.count() - 1);
    is_root_node = (1 == cur_node.level_);
  }
  return is_root_node;
}

int ObConnectByOpBFSPump::get_next_row(
  const ObChunkDatumStore::StoredRow *&pump_row,
  const ObChunkDatumStore::StoredRow *&output_row)
{
  int ret = OB_SUCCESS;
  PumpNode pump_node;
  bool next_row_found = false;
  while (!next_row_found && OB_SUCC(ret)) {
    if (OB_FAIL(free_record_rows())) {
      LOG_WARN("fail to free record rows", K(ret));
    } else if (OB_FAIL(pump_stack_.pop_back(pump_node))) {
      if (ret != OB_ENTRY_NOT_EXIST) {
        LOG_WARN("fail to pop back value", K(ret));
      }
    } else if (OB_FAIL(free_record_.push_back(pump_node.pump_row_))) {
      LOG_WARN("fail to push back value", K(ret));
    } else if (OB_FAIL(free_record_.push_back(pump_node.output_row_))) {
      LOG_WARN("fail to push back value", K(ret));
    } else if (OB_UNLIKELY(pump_node.is_cycle_)) {
      if (!is_nocycle_) {
        ret = OB_ERR_CBY_LOOP;
        LOG_WARN("there is a cycle", K(ret));
      }
      if (pump_node.path_node_.prior_exprs_result_ != NULL) {
        allocator_.free(const_cast<ObChunkDatumStore::StoredRow *>(pump_node.path_node_.prior_exprs_result_));
        pump_node.path_node_.prior_exprs_result_ = NULL;
      }
    } else {
      next_row_found = true;
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(add_path_stack(pump_node.path_node_))) {
      LOG_WARN("fail to add path stack", K(pump_node), K(ret));
    } else {
      pump_row = pump_node.pump_row_;
      output_row = pump_node.output_row_;
      cur_level_ = pump_node.path_node_.level_ + 1;
      LOG_DEBUG("connect by get output pump node", K(pump_node));
    }
  } else if (OB_ENTRY_NOT_EXIST == ret) {
    ret = OB_ITER_END;
  }
  return ret;
}

int ObConnectByOpBFSPump::add_path_node(PumpNode &pump_node)
{
  int ret = OB_SUCCESS;
  PathNode node;
  if (OB_FAIL(node.init_path_array(connect_by_path_count_))) {
    LOG_WARN("Failed to init path array", K(ret));
  } else if (OB_FAIL(check_cycle_path())) {
    if (OB_ERR_CBY_LOOP == ret) {
      ret = OB_SUCCESS;
      pump_node.is_cycle_ = true;
    } else {
      LOG_WARN("fail to check cycle path", K(node), K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(deep_copy_row(*connect_by_prior_exprs_, node.prior_exprs_result_))) {
      LOG_WARN("fail to deep copy row", K(ret));
    } else {
      node.level_ = cur_level_;
      pump_node.path_node_ = node;
    }
  }
  return ret;
}

int ObConnectByOpBFSPump::calc_path_node(PumpNode &pump_node)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(connect_by_prior_exprs_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid data member", KPC(connect_by_prior_exprs_), K(ret));
  } else {
    LOG_DEBUG("connect by after prior exprs", K(cur_level_),
      K(ObToStringExprRow(*eval_ctx_, *connect_by_prior_exprs_)));
    //check cycle and add node
    if (OB_FAIL(add_path_node(pump_node))) {
      LOG_WARN("fail to add path node");
    }
  }
  return ret;
}

int ObConnectByOpBFSPump::check_cycle_path()
{
  int ret = OB_SUCCESS;
  bool always_false = never_meet_cycle_;
  /*
    * What is a pump row ?
    * We transform right row to a new left row, and we call this new
    * left row a pump row.
    * Why is the pump_row_desc_ empty ?
    * If the connect by join doesn't include any join condition,
    * say, select 1 from dual connect by level < 5, the pump_row_desc is empty.
    * We can say pump_row_desc_ is only for cycle detect.
    * A empty pump_row_desc_ means we never get a cycle in this connect by join.
    *
    */
  for(int64_t i = 0; OB_SUCC(ret) && i < path_stack_.count() && !always_false; ++i) {
    const PathNode &cur_node = path_stack_.at(i);
    if (connect_by_prior_exprs_->count() == 0) {
      //connect by后面都是prior常量表达式，如connect by prior 0 = 0,那么level=2时一定判断有环
      ret = OB_ERR_CBY_LOOP;
      if (!is_nocycle_) {
        LOG_WARN("CONNECT BY loop in user data", K(ret), K(cur_node));
      }
    } else if (connect_by_prior_exprs_->count() != cur_node.prior_exprs_result_->cnt_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected status: the column count is not match", K(ret),
        "expr cnt", connect_by_prior_exprs_->count(),
        "prior result expr cnt", cur_node.prior_exprs_result_->cnt_);
    } else {
      int cmp = 0;
      ObDatum *l_datum = nullptr;
      const ObDatum *r_datums = cur_node.prior_exprs_result_->cells();
      ObExpr *expr = nullptr;
      for (int64_t i = 0; i < connect_by_prior_exprs_->count() && 0 == cmp && !always_false && OB_SUCC(ret); ++i) {
        if (OB_FAIL(connect_by_prior_exprs_->at(i, expr))) {
          LOG_WARN("failed to get prior expr", K(ret), K(i));
        } else if (OB_FAIL(expr->eval(*eval_ctx_, l_datum))) {
          LOG_WARN("failed to eval expr", K(ret), K(i));
        } else if (OB_FAIL(cmp_funcs_->at(i).cmp_func_(*l_datum, r_datums[i], cmp))) {
          LOG_WARN("failed to compare", K(ret), K(i));
        }
#ifndef NDEBUG
        ObObj obj;
        r_datums->to_obj(obj, expr->obj_meta_);
        LOG_DEBUG("trace compare row", K(obj), K(i));
#endif
      }
      LOG_DEBUG("trace compare row", K(ObToStringExprRow(*eval_ctx_, *connect_by_prior_exprs_)));
      if (0 == cmp) {
        ret = OB_ERR_CBY_LOOP;
      }
    }
  }
  LOG_DEBUG("trace compare row", K(ObToStringExprRow(*eval_ctx_, *connect_by_prior_exprs_)),
    K(never_meet_cycle_));
  return ret;
}

int ObConnectByOpBFSPump::get_parent_path(int64_t idx, ObString &p_path) const
{
  int ret = OB_SUCCESS;
  int64_t node_cnt = path_stack_.count();
  if (OB_UNLIKELY(node_cnt <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid path stack", K(node_cnt), K(ret));
  } else if (1 == node_cnt) {
    p_path.reset();
  } else if (idx >= path_stack_.at(node_cnt - 1).paths_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid path stack", K(idx), K(path_stack_.at(node_cnt - 1).paths_.count()), K(ret));
  } else {
    p_path = path_stack_.at(node_cnt - 2).paths_.at(idx);
  }
  return ret;
}

int ObConnectByOpBFSPump::set_cur_node_path(int64_t idx, const ObString &path)
{
  int ret = OB_SUCCESS;
  int64_t node_cnt = path_stack_.count();
  if (OB_UNLIKELY(node_cnt <= 0) || idx < 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid path stack", K(node_cnt), K(ret));
  } else if (idx >= path_stack_.at(node_cnt - 1).paths_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid path stack", K(idx), K(path_stack_.at(node_cnt - 1).paths_.count()), K(ret));
  } else {
    PathNode &cur_node = path_stack_.at(node_cnt - 1);
    char *old_buf = cur_node.paths_.at(idx).ptr();
    if (OB_FAIL(ob_write_string(allocator_, path, cur_node.paths_.at(idx)))) {
      LOG_WARN("fail to copy string", K(path), K(ret));
    }
    if (NULL != old_buf) {
      allocator_.free(old_buf);
    }
  }
  return ret;
}


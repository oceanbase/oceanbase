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

#include "sql/engine/connect_by/ob_nl_cnnt_by_op.h"
#include "sql/engine/expr/ob_expr_null_safe_equal.h"
#include "ob_cnnt_by_pump.h"

using namespace oceanbase::sql;
using namespace oceanbase::common;

int ObConnectByOpPumpBase::deep_copy_row(const ObIArray<ObExpr*>& exprs, const ObChunkDatumStore::StoredRow*& dst_row)
{
  ObChunkDatumStore::StoredRow* sr = NULL;
  int ret = ObChunkDatumStore::StoredRow::build(sr, exprs, *eval_ctx_, allocator_);
  dst_row = sr;
  return ret;
}

uint64_t ObConnectByOpPump::ObHashColumn::inner_hash() const
{
  uint64_t result = 99194853094755497L;
  if (OB_ISNULL(exprs_) || OB_ISNULL(row_) || OB_UNLIKELY(exprs_->count() != row_->cnt_)) {
  } else {
    int64_t col_count = exprs_->count();
    ObExpr* expr = NULL;
    const ObDatum* datum = NULL;
    for (int64_t i = 0; i < col_count; i++) {
      if (OB_ISNULL(expr = exprs_->at(i)) || OB_ISNULL(expr->basic_funcs_)) {
      } else {
        datum = &row_->cells()[i];
        result = expr->basic_funcs_->murmur_hash_(*datum, result);
      }
    }
  }
  return result;
}

bool ObConnectByOpPump::ObHashColumn::operator==(const ObHashColumn& other) const
{
  bool result = true;
  if (OB_ISNULL(row_) || OB_ISNULL(exprs_) || OB_ISNULL(other.row_)) {
    result = false;
  } else {
    const ObDatum* lcell = row_->cells();
    const ObDatum* rcell = other.row_->cells();

    int64_t col_count = exprs_->count();
    ObExpr* expr = NULL;
    for (int64_t i = 0; result && i < col_count; i++) {
      if (OB_ISNULL(expr = exprs_->at(i)) || OB_ISNULL(expr->basic_funcs_)) {
      } else {
        result = (0 == expr->basic_funcs_->null_first_cmp_(lcell[i], rcell[i]));
      }
    }
  }
  return result;
}

// called in ObConnectBy rescan, reset to the state after open.
void ObConnectByOpPump::reset()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(free_pump_node_stack(pump_stack_))) {
    LOG_ERROR("fail to free pump stack", K(ret));
  }
  pump_stack_.reset();
  // datum_store_.reset();
  hash_filter_rows_.reuse();
  for (int64_t i = 0; i < sys_path_buffer_.count(); i++) {
    ObString& str = sys_path_buffer_.at(i);
    if (NULL != str.ptr()) {
      allocator_.free(const_cast<char*>(str.ptr()));
    }
  }
  sys_path_buffer_.reset();
  cur_level_ = 1;
  // row_store_constructed_ = false;
  sys_path_buffer_.reset();
}

int ObConnectByOpPump::add_root_row()
{
  int ret = OB_SUCCESS;
  PumpNode node;
  node.level_ = 1;
  for (int64_t i = 0; OB_SUCC(ret) && i < connect_by_path_count_; i++) {
    if (OB_FAIL(node.sys_path_length_.push_back(0))) {
      LOG_WARN("array push back failed", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(false == pump_stack_.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pump stack must be empty", K(pump_stack_), K(ret));
  } else if (OB_FAIL(deep_copy_row(*right_prior_exprs_, node.pump_row_))) {
    LOG_WARN("deep copy row failed", K(ret));
  } else if (OB_FAIL(deep_copy_row(*cur_output_exprs_, node.output_row_))) {
    LOG_WARN("deep copy row failed", K(ret));
  } else if (OB_FAIL(push_back_node_to_stack(node))) {
    LOG_WARN("fail to push back row", K(ret));
  } else if (OB_FAIL(alloc_iter(pump_stack_.at(pump_stack_.count() - 1)))) {
  }
  return ret;
}

void ObConnectByOpPump::free_memory()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(free_pump_node_stack(pump_stack_))) {
    LOG_ERROR("fail to free pump stack", K(ret));
  }
}

int ObConnectByOpPump::push_back_store_row()
{
  int ret = OB_SUCCESS;
  const ObChunkDatumStore::StoredRow* dst_row = NULL;
  if (OB_FAIL(deep_copy_row(*right_prior_exprs_, dst_row))) {
    LOG_WARN("deep copy row failed", K(ret));
  } else if (OB_FAIL(datum_store_.add_row(*dst_row))) {
    LOG_WARN("datum store add row failed", K(ret));
  }
  return ret;
}

int ObConnectByOpPump::alloc_iter(PumpNode& pop_node)
{
  int ret = OB_SUCCESS;
  void* buf = allocator_.alloc(sizeof(ObChunkDatumStore::Iterator));
  if (OB_ISNULL(buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory", K(ret));
  } else {
    pop_node.iter_ = new (buf) ObChunkDatumStore::Iterator();
  }
  return ret;
}

int ObConnectByOpPump::free_pump_node(PumpNode& pop_node)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(pop_node.pump_row_) || OB_ISNULL(pop_node.output_row_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid pop node", K(ret));
  } else {
    allocator_.free(const_cast<ObChunkDatumStore::StoredRow*>(pop_node.pump_row_));

    allocator_.free(const_cast<ObChunkDatumStore::StoredRow*>(pop_node.output_row_));
    if (OB_ISNULL(pop_node.prior_exprs_result_)) {
    } else {
      allocator_.free(const_cast<ObChunkDatumStore::StoredRow*>(pop_node.prior_exprs_result_));
    }
    if (OB_ISNULL(pop_node.first_child_)) {
    } else {
      allocator_.free(const_cast<ObChunkDatumStore::StoredRow*>(pop_node.first_child_));
    }

    if (OB_NOT_NULL(pop_node.iter_)) {
      allocator_.free(pop_node.iter_);
    }

    pop_node.pump_row_ = NULL;
    pop_node.output_row_ = NULL;
    pop_node.prior_exprs_result_ = NULL;
    pop_node.first_child_ = NULL;
    pop_node.iter_ = nullptr;
  }
  return ret;
}

int ObConnectByOpPump::free_pump_node_stack(ObIArray<PumpNode>& stack)
{
  int ret = OB_SUCCESS;
  PumpNode pop_node;
  while (OB_SUCC(ret) && false == stack.empty()) {
    if (OB_FAIL(stack.pop_back(pop_node))) {
      LOG_WARN("fail to pop back pump_stack", K(ret));
    } else if (OB_FAIL(free_pump_node(pop_node))) {
      LOG_WARN("free pump node failed", K(ret));
    }
  }
  return ret;
}

int ObConnectByOpPump::get_top_pump_node(PumpNode*& node)
{
  int ret = OB_SUCCESS;
  if (0 == pump_stack_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pump stack is empty", K(ret));
  } else {
    node = &pump_stack_.at(pump_stack_.count() - 1);
  }
  return ret;
}

int ObConnectByOpPump::push_back_node_to_stack(PumpNode& node)
{
  int ret = OB_SUCCESS;
  PumpNode* top_node = pump_stack_.count() > 0 ? &pump_stack_.at(pump_stack_.count() - 1) : NULL;
  if (OB_FAIL(calc_prior_and_check_cycle(node, true /* add node to hashset */, top_node))) {
    LOG_WARN("fail to calc path node", K(ret));
  } else if (false == node.is_cycle_ && OB_FAIL(pump_stack_.push_back(node))) {
    LOG_WARN("fail to push back row", K(ret));
  }

  if (OB_FAIL(ret)) {  // if fail free memory
    if (node.prior_exprs_result_ != NULL) {
      allocator_.free(const_cast<ObChunkDatumStore::StoredRow*>(node.prior_exprs_result_));
      node.prior_exprs_result_ = NULL;
    }
  }
  return ret;
}

int ObConnectByOpPump::init(const ObNLConnectBySpec& connect_by, ObNLConnectByOp& connect_by_op, ObEvalCtx& eval_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_ISNULL(eval_ctx.exec_ctx_.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is null", K(ret));
  } else if (OB_FAIL(hash_filter_rows_.create(CONNECT_BY_TREE_HEIGHT))) {
    LOG_WARN("create hash set failed", K(ret));
  } else {
    uint64_t tenant_id = eval_ctx.exec_ctx_.get_my_session()->get_effective_tenant_id();
    allocator_.set_tenant_id(tenant_id);
    connect_by_prior_exprs_ = &connect_by.connect_by_prior_exprs_;
    eval_ctx_ = &eval_ctx;
    connect_by_ = &connect_by_op;
    left_prior_exprs_ = &connect_by.left_prior_exprs_;
    right_prior_exprs_ = &connect_by.right_prior_exprs_;
    cur_output_exprs_ = &connect_by.cur_row_exprs_;
    connect_by_path_count_ = connect_by.get_sys_connect_by_path_expression_count();
    is_nocycle_ = connect_by.is_nocycle_;
    never_meet_cycle_ = !connect_by.has_prior_;
    ARRAY_FOREACH(connect_by.sys_connect_exprs_, i)
    {
      if (OB_FAIL(sys_path_buffer_.push_back(ObString()))) {
        LOG_WARN("init sys path buffer failed", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObConnectByOpPump::join_right_table(PumpNode& node, bool& matched)
{
  int ret = OB_SUCCESS;
  matched = false;
  const ObChunkDatumStore::StoredRow* new_output_row = nullptr;
  if (OB_FAIL(node.pump_row_->to_expr(*left_prior_exprs_, *eval_ctx_, left_prior_exprs_->count()))) {
    LOG_WARN("datum row to expr failed", K(ret));
    //  } else if (OB_NOT_NULL(node.first_child_)) {
    //    join_ctx->right_row_ = node.first_child_;
    //    node.first_child_ = NULL;
    //    matched = true;
  } else if (OB_FAIL(connect_by_->set_level_as_param(node.level_ + 1))) {
    LOG_WARN("set current level as param failed", K(ret));
  } else {
    while (OB_SUCC(ret) && false == matched) {
      connect_by_->clear_evaluated_flag();
      if (OB_FAIL(node.iter_->get_next_row(*right_prior_exprs_, *eval_ctx_))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("get row from row store failed", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(connect_by_->calc_other_conds(matched))) {
        LOG_WARN("calc other conds failed", K(ret));
      } else if (matched) {
        PumpNode next_node;
        next_node.level_ = node.level_ + 1;
        if (next_node.level_ >= CONNECT_BY_MAX_NODE_NUM) {
          ret = OB_ERR_CBY_NO_MEMORY;
          uint64_t current_level = node.level_;
          int64_t max_node_num = CONNECT_BY_MAX_NODE_NUM;
          LOG_WARN("connect by reach memory limit", K(ret), K(current_level), K(max_node_num));
        }
        for (int64_t i = 0; OB_SUCC(ret) && i < connect_by_path_count_; i++) {
          if (OB_FAIL(next_node.sys_path_length_.push_back(0))) {
            LOG_WARN("array push back failed", K(ret));
          }
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(deep_copy_row(*cur_output_exprs_, next_node.output_row_))) {
          LOG_WARN("deep copy row failed", K(ret));
        } else if (OB_FAIL(deep_copy_row(*right_prior_exprs_, next_node.pump_row_))) {
          LOG_WARN("deep copy row failed", K(ret));
        } else if (OB_FAIL(push_back_node_to_stack(next_node))) {
          LOG_WARN("push back node to stack failed", K(ret));
        } else if (next_node.is_cycle_) {
          // if node is same as an ancestor and there is no_cycle, ignore this node and continue to search.
          matched = false;
          if (OB_FAIL(free_pump_node(next_node))) {
            LOG_WARN("free pump node failed", K(ret));
          }
        } else if (OB_FAIL(alloc_iter(pump_stack_.at(pump_stack_.count() - 1)))) {
        }
      }
    }
  }

  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObConnectByOpPump::get_next_row()
{
  int ret = OB_SUCCESS;
  bool matched = false;
  PumpNode pop_node;
  while (OB_SUCC(ret) && false == matched && pump_stack_.count() > 0) {
    PumpNode& node = pump_stack_.at(pump_stack_.count() - 1);
    cur_level_ = node.level_ + 1;
    if (OB_FAIL(join_right_table(node, matched))) {
      LOG_WARN("join right table failed", K(ret));
    } else if (false == matched) {
      if (OB_FAIL(pump_stack_.pop_back(pop_node))) {
        LOG_WARN("pump stack pop back failed", K(ret));
      } else {
        // when pop a node from pump_stack, need to reduce length of strings of
        // all sys_connect_by_path expressions in the sys_path_buffer_.
        if (pump_stack_.empty()) {
          for (int64_t i = 0; i < sys_path_buffer_.count(); i++) {
            ObString& str = sys_path_buffer_.at(i);
            str.set_length(0);
          }
        } else {
          PumpNode& parent = pump_stack_.at(pump_stack_.count() - 1);
          if (parent.sys_path_length_.count() != sys_path_buffer_.count()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("different sys_connect_by_path size", K(ret));
          } else {
            for (int64_t i = 0; i < sys_path_buffer_.count(); i++) {
              ObString& str = sys_path_buffer_.at(i);
              str.set_length(parent.sys_path_length_.at(i));
            }
          }
        }
      }
      if (OB_SUCC(ret) && NULL != pop_node.prior_exprs_result_) {
        ObHashColumn hash_col(pop_node.prior_exprs_result_, connect_by_prior_exprs_);
        if (OB_FAIL(hash_filter_rows_.erase_refactored(hash_col))) {
          LOG_WARN("fail to erase prior_exprs_result from hashset", K(ret), KPC(pop_node.prior_exprs_result_));
        }
      }
      if (OB_SUCC(ret) && OB_FAIL(free_pump_node(pop_node))) {
        LOG_WARN("free pump node failed", K(ret));
      }
    }
  }
  if (OB_SUCC(ret) && pump_stack_.empty()) {
    ret = OB_ITER_END;
  }

  return ret;
}

// If not never_meet_cycle_, check whether there is a loop.
// Calculate result of prior_exprs_result_ and a loop is found if it exists in the hash_set.
// If a loop is found and there is no is_nocycle_, report loop error.
int ObConnectByOpPump::calc_prior_and_check_cycle(PumpNode& node, bool set_refactored, PumpNode* left_node)
{
  UNUSED(left_node);
  int ret = OB_SUCCESS;
  if (never_meet_cycle_) {
  } else if (OB_ISNULL(connect_by_prior_exprs_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid data member", KPC(connect_by_prior_exprs_), K(ret));
  } else if (OB_UNLIKELY(connect_by_prior_exprs_->empty())) {
    // all prior exprs in connect_by conds are prior const, there should be a loop when level>1.
    if (node.level_ > 1) {
      node.is_cycle_ = true;
      if (false == is_nocycle_) {
        ret = OB_ERR_CBY_LOOP;
      }
    }
  } else if (OB_ISNULL(node.pump_row_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pump row is null", K(ret));
  } else {
    connect_by_->clear_evaluated_flag();
    if (OB_FAIL(deep_copy_row(*connect_by_prior_exprs_, node.prior_exprs_result_))) {
      LOG_WARN("deep copy row failed", K(ret));
    } else if (OB_FAIL(check_cycle(node.prior_exprs_result_, set_refactored))) {
      if (OB_ERR_CBY_LOOP == ret) {
        node.is_cycle_ = true;
        if (is_nocycle_) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("CONNECT BY loop in user data", K(ret));
        }
      } else {
        LOG_WARN("fail to check cycle", K(ret), K(node));
      }
    } else {
      node.is_cycle_ = false;
    }
  }
  return ret;
}

int ObConnectByOpPump::check_cycle(const ObChunkDatumStore::StoredRow* row, bool set_refactored)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(row)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("row is null", K(ret));
  } else {
    ObHashColumn hash_col(row, connect_by_prior_exprs_);
    if (OB_FAIL(hash_filter_rows_.exist_refactored(hash_col))) {
      if (OB_HASH_NOT_EXIST == ret) {
        if (set_refactored && OB_FAIL(hash_filter_rows_.set_refactored(hash_col))) {
          LOG_WARN("Failed to insert into hashset", K(ret), K(row));
        } else {
          ret = OB_SUCCESS;
        }
      } else if (OB_HASH_EXIST == ret) {
        ret = OB_ERR_CBY_LOOP;
      } else {
        LOG_WARN("Failed to find in hashset", K(ret));
      }
    }
  }

  return ret;
}

int ObConnectByOpPump::check_child_cycle(PumpNode& node, PumpNode* left_node)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(calc_prior_and_check_cycle(node, false, left_node))) {
    LOG_WARN("calc prior expr and check cycle failed", K(ret));
  }
  return ret;
}

int ObConnectByOpPump::get_sys_path(uint64_t sys_connect_by_path_id, ObString& parent_path)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(sys_path_buffer_.count() <= sys_connect_by_path_id)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid sys connect by path id", K(ret), K(sys_connect_by_path_id));
  } else {
    ObString& str = sys_path_buffer_.at(sys_connect_by_path_id);
    parent_path.assign_ptr(str.ptr(), str.length());
  }
  return ret;
}

int ObConnectByOpPump::concat_sys_path(uint64_t sys_connect_by_path_id, const ObString& cur_path)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(pump_stack_.empty()) || OB_UNLIKELY(sys_path_buffer_.count() <= sys_connect_by_path_id)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid sys connect by path id", K(ret), K(sys_connect_by_path_id));
  } else {
    ObString& str = sys_path_buffer_.at(sys_connect_by_path_id);
    if (0 == str.size()) {
      char* buf = NULL;
      int64_t buf_size = SYS_PATH_BUFFER_INIT_SIZE;
      while (buf_size < cur_path.length()) {
        buf_size *= 2;
      }
      if (OB_ISNULL(buf = static_cast<char*>(allocator_.alloc(buf_size)))) {
        LOG_WARN("allocate buf failed", K(ret), K(buf_size));
      } else {
        str.assign_buffer(buf, buf_size);
      }
    } else if (str.length() + cur_path.length() > str.size()) {
      int64_t buf_size = str.size();
      while (buf_size < str.length() + cur_path.length()) {
        buf_size *= 2;
      }
      char* buf = NULL;
      ObString new_str;
      if (OB_ISNULL(buf = static_cast<char*>(allocator_.alloc(buf_size)))) {
        LOG_WARN("allocate buf failed", K(ret), K(buf_size));
      } else if (FALSE_IT(new_str.assign_buffer(buf, buf_size))) {
      } else if (str.length() != new_str.write(str.ptr(), str.length())) {
        LOG_WARN("copy origin string failed", K(ret));
      } else {
        allocator_.free(const_cast<char*>(str.ptr()));
        str.assign_buffer(new_str.ptr(), new_str.size());
      }
    }
    if (OB_SUCC(ret) && cur_path.length() != str.write(cur_path.ptr(), cur_path.length())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("write cur_path to sys_path_buffer failed", K(ret));
    }
    if (OB_SUCC(ret)) {
      PumpNode& node = pump_stack_.at(pump_stack_.count() - 1);
      if (sys_connect_by_path_id >= node.sys_path_length_.count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid pump node", K(ret), K(sys_connect_by_path_id), K(node.sys_path_length_.count()));
      } else {
        uint64_t& length = node.sys_path_length_.at(sys_connect_by_path_id);
        length = str.length();
      }
    }
  }
  return ret;
}

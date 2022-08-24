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

#include "sql/engine/connect_by/ob_nested_loop_connect_by.h"
#include "sql/engine/connect_by/ob_connect_by_utility.h"
#include "sql/engine/expr/ob_expr_null_safe_equal.h"

using namespace oceanbase::sql;
using namespace oceanbase::common;

int ObConnectByPumpBase::deep_copy_row(const ObNewRow& src_row, const ObNewRow*& dst_row)
{
  int ret = OB_SUCCESS;
  char* buf = NULL;
  int64_t pos = sizeof(ObNewRow);
  ObNewRow* new_row = NULL;
  int64_t buf_len = src_row.get_deep_copy_size() + sizeof(ObNewRow);
  if (OB_ISNULL(buf = static_cast<char*>(allocator_.alloc(buf_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc new row failed", K(ret), K(buf_len));
  } else if (OB_ISNULL(new_row = new (buf) ObNewRow())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("new_row is null", K(ret), K(buf_len));
  } else if (OB_FAIL(new_row->deep_copy(src_row, buf, buf_len, pos))) {
    LOG_WARN("deep copy row failed", K(ret), K(buf_len), K(pos));
  } else {
    dst_row = new_row;
  }
  return ret;
}

int ObConnectByPumpBase::construct_pump_row(const ObNewRow* row)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(pump_row_desc_) || OB_ISNULL(row) || OB_UNLIKELY(row->is_invalid()) ||
      OB_UNLIKELY(shallow_row_.is_invalid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid data member", K(shallow_row_), KPC(pump_row_desc_), KPC(row), K(is_inited_), K(ret));
  } else if (pump_row_desc_->empty()) {
    // do nothing
    LOG_DEBUG("pump row empty", K(never_meet_cycle_), K(ret));
  } else if (shallow_row_.count_ != pump_row_desc_->count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected pump_row_desc", K(pump_row_desc_->count()), K(shallow_row_.count_), K(row->count_), K(ret));
  } else {
    int64_t cell_count = shallow_row_.count_;
    ObObj val;
    val.set_int(1);
    ObObj invalid_val(ObMaxType);
    for (int64_t i = 0; OB_SUCC(ret) && i < cell_count; ++i) {
      if (pump_row_desc_->at(i) == ObJoin::DUMMY_OUPUT) {
        shallow_row_.cells_[i] = val;
      } else if (pump_row_desc_->at(i) == ObJoin::UNUSED_POS) {
        shallow_row_.cells_[i] = invalid_val;
      } else if (OB_UNLIKELY(pump_row_desc_->at(i) + 1 > row->count_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected pump_row_desc", K(i), K(pump_row_desc_->at(i)), K(row->count_));
      } else {
        shallow_row_.cells_[i] = row->cells_[pump_row_desc_->at(i)];
      }
    }
    LOG_DEBUG("construct pump row", K(*pump_row_desc_), K(shallow_row_), K(*row));
  }
  return ret;
}

int ObConnectByPumpBase::alloc_shallow_row_cells(const ObPhyOperator& left_op)
{
  int ret = OB_SUCCESS;
  void* cell_ptr = NULL;
  int64_t column_count = left_op.get_column_count();
  if (column_count <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(column_count));
  } else if (OB_ISNULL(cell_ptr = allocator_.alloc(column_count * sizeof(ObObj)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("alloc memory for row failed", "size", column_count * sizeof(ObObj));
  } else {
    shallow_row_.cells_ = new (cell_ptr) common::ObObj[column_count];
    shallow_row_.count_ = column_count;
  }
  return ret;
}

int ObConnectByPumpBase::alloc_shallow_row_projector(const ObPhyOperator& left_op)
{
  int ret = OB_SUCCESS;
  int64_t projector_size = left_op.get_projector_size();
  const int32_t* projector = left_op.get_projector();
  int64_t size = projector_size * sizeof(int32_t);
  void* project_ptr = NULL;
  if (OB_ISNULL(project_ptr = allocator_.alloc(size))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("alloc memory for row failed", K(size));
  } else {
    shallow_row_.projector_ = static_cast<int32_t*>(project_ptr);
    shallow_row_.projector_size_ = projector_size;
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < projector_size; ++i) {
    shallow_row_.projector_[i] = projector[i];
  }
  return ret;
}

int ObConnectByPumpBase::check_output_pseudo_columns()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(pseudo_column_row_desc_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid data member", K(ret));
  } else if (pseudo_column_row_desc_->count() != CONNECT_BY_PSEUDO_COLUMN_CNT) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid row_desc", K(pseudo_column_row_desc_->count()), K(ret));
  } else {
    is_output_level_ = (ObJoin::UNUSED_POS != pseudo_column_row_desc_->at(LEVEL));
    is_output_cycle_ = (ObJoin::UNUSED_POS != pseudo_column_row_desc_->at(CONNECT_BY_ISCYCLE));
    is_output_leaf_ = (ObJoin::UNUSED_POS != pseudo_column_row_desc_->at(CONNECT_BY_ISLEAF));
  }
  return ret;
}

// loop only exists when there is prior in connect_by conditions.
int ObConnectByPumpBase::check_pump_row_desc()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(pump_row_desc_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid data member", K(ret));
  } else {
    never_meet_cycle_ = pump_row_desc_->empty();
    LOG_DEBUG("check pump row desc", K(ret));
  }
  return ret;
}

uint64_t ObConnectByPump::ObHashColumn::inner_hash() const
{
  uint64_t result = 99194853094755497L;
  if (row_ != NULL && row_->is_valid()) {
    const ObObj* cells = row_->cells_;
    for (int64_t i = 0; i < row_->count_; ++i) {
      const ObObj& cell = cells[i];
      result = cell.hash_wy(result);
    }
  }
  return result;
}

bool ObConnectByPump::ObHashColumn::operator==(const ObHashColumn& other) const
{
  bool result = true;
  ObObj cmp_result;
  if (NULL == expr_ctx_) {
    result = false;
  } else {
    int tmp_ret = ObExprNullSafeEqual::compare_row2(cmp_result, row_, other.row_, *expr_ctx_);
    result = cmp_result.is_true() && OB_SUCCESS == tmp_ret;
  }
  return result;
}

void ObConnectByPump::close(ObIAllocator* allocator)
{
  reset();
  row_store_.reset();
  row_store_constructed_ = false;
  hash_table_.free(allocator);
}

// called in ObConnectBy rescan, reset to the state after open.
void ObConnectByPump::reset()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(free_pump_node_stack(pump_stack_))) {
    LOG_ERROR("fail to free pump stack", K(ret));
  }
  pump_stack_.reset();
  hash_filter_rows_.reuse();
  for (int64_t i = 0; i < sys_path_buffer_.count(); i++) {
    ObString& str = sys_path_buffer_.at(i);
    if (NULL != str.ptr()) {
      allocator_.free(const_cast<char*>(str.ptr()));
    }
  }
  sys_path_buffer_.reset();
  cur_level_ = 1;
  if (NULL != connect_by_root_row_) {
    allocator_.free(const_cast<ObNewRow*>(connect_by_root_row_));
    connect_by_root_row_ = NULL;
  }
  // TODO:shanting not need to rebuild row_store_ if there is no push_down parameter
  if (true) {
    row_store_.reuse();
    row_store_constructed_ = false;
    hash_table_.reset();
  }
}

int ObConnectByPump::add_root_row(const ObNewRow* root_row, const ObNewRow& mock_right_row, const ObNewRow* output_row)
{
  UNUSED(mock_right_row);
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
  } else if (OB_ISNULL(root_row) || OB_ISNULL(output_row)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid parameter", K(root_row), K(output_row), K(ret));
  } else if (OB_FAIL(deep_copy_row(*root_row, node.pump_row_))) {
    LOG_WARN("deep copy row failed", K(ret));
  } else if (OB_FAIL(deep_copy_row(*output_row, node.output_row_))) {
    LOG_WARN("deep copy row failed", K(ret));
    //  } else if (is_output_cycle_ && OB_FAIL(deep_copy_row(mock_right_row, node.right_row_))) {
    //    LOG_WARN("deep copy row failed", K(ret));
  } else if (OB_FAIL(alloc_iter(node))) {
    LOG_WARN("alloc iterator failed", K(ret));
  } else if (OB_FAIL(push_back_node_to_stack(node))) {
    LOG_WARN("fail to push back row", K(ret));
  } else {
  }
  UNUSED(mock_right_row);
  return ret;
}

void ObConnectByPump::free_memory()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(free_pump_node_stack(pump_stack_))) {
    LOG_ERROR("fail to free pump stack", K(ret));
  }

  if (shallow_row_.cells_ != NULL) {
    allocator_.free(shallow_row_.cells_);
    shallow_row_.cells_ = NULL;
  }

  if (shallow_row_.projector_ != NULL) {
    allocator_.free(shallow_row_.projector_);
    shallow_row_.projector_ = NULL;
  }

  if (prior_exprs_result_row_.cells_ != NULL) {
    allocator_.free(prior_exprs_result_row_.cells_);
    prior_exprs_result_row_.cells_ = NULL;
  }

  if (connect_by_root_row_ != NULL) {
    allocator_.free(const_cast<ObNewRow*>(connect_by_root_row_));
    connect_by_root_row_ = NULL;
  }
}

int ObConnectByPump::free_pump_node(PumpNode& pop_node)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(pop_node.pump_row_) || OB_ISNULL(pop_node.output_row_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid pop node", K(ret));
  } else {
    allocator_.free(const_cast<ObNewRow*>(pop_node.pump_row_));
    if (NULL != pop_node.right_row_) {
      allocator_.free(const_cast<ObNewRow*>(pop_node.right_row_));
    }
    allocator_.free(const_cast<ObNewRow*>(pop_node.output_row_));
    if (OB_ISNULL(pop_node.prior_exprs_result_)) {
    } else {
      allocator_.free(const_cast<ObNewRow*>(pop_node.prior_exprs_result_));
    }
    if (OB_ISNULL(pop_node.first_child_)) {
    } else {
      allocator_.free(const_cast<ObNewRow*>(pop_node.first_child_));
    }
    if (OB_NOT_NULL(pop_node.row_fetcher_.iterator_)) {
      allocator_.free(pop_node.row_fetcher_.iterator_);
    }
    pop_node.pump_row_ = NULL;
    pop_node.right_row_ = NULL;
    pop_node.output_row_ = NULL;
    pop_node.prior_exprs_result_ = NULL;
    pop_node.first_child_ = NULL;
    pop_node.row_fetcher_.iterator_ = NULL;
  }
  return ret;
}

int ObConnectByPump::free_pump_node_stack(ObIArray<PumpNode>& stack)
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

int ObConnectByPump::get_top_pump_node(PumpNode*& node)
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

int ObConnectByPump::push_back_node_to_stack(PumpNode& node)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(calc_prior_and_check_cycle(node, true /* add node to hashset */))) {
    LOG_WARN("fail to calc path node", K(ret));
  } else if (false == node.is_cycle_ && OB_FAIL(pump_stack_.push_back(node))) {
    LOG_WARN("fail to push back row", K(ret));
  }

  if (OB_FAIL(ret)) {  // if fail free memory
    if (node.prior_exprs_result_ != NULL) {
      allocator_.free(const_cast<ObNewRow*>(node.prior_exprs_result_));
      node.prior_exprs_result_ = NULL;
    }
  }
  return ret;
}

int ObConnectByPump::alloc_prior_row_cells(uint64_t row_count)
{
  int ret = OB_SUCCESS;
  void* cell_ptr = NULL;
  if (row_count <= 0) {
  } else if (OB_ISNULL(cell_ptr = allocator_.alloc(row_count * sizeof(ObObj)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("alloc memory for row failed", "size", row_count * sizeof(ObObj));
  } else {
    prior_exprs_result_row_.cells_ = new (cell_ptr) common::ObObj[row_count];
    prior_exprs_result_row_.count_ = row_count;
  }
  return ret;
}

int ObConnectByPump::alloc_iter(PumpNode& pop_node)
{
  int ret = OB_SUCCESS;
  void* buf = allocator_.alloc(sizeof(ObChunkRowStore::Iterator));
  if (OB_ISNULL(buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory", K(ret));
  } else {
    pop_node.row_fetcher_.iterator_ = new (buf) ObChunkRowStore::Iterator();
  }
  return ret;
}

int ObConnectByPump::init(const ObConnectBy& connect_by, common::ObExprCtx* expr_ctx)
{
  int ret = OB_SUCCESS;
  const ObPhyOperator* left_op = NULL;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_ISNULL(expr_ctx) || OB_ISNULL(expr_ctx->my_session_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr_ctx or session is null", K(ret));
  } else if (OB_UNLIKELY(2 != connect_by.get_child_num())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid child num", K(ret));
  } else if (OB_ISNULL(left_op = connect_by.get_child(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("left operator is NULL", K(ret));
  } else if (OB_FAIL(alloc_shallow_row_cells(*left_op))) {
    LOG_WARN("fail to alloc shallow row cells", K(ret));
  } else if (OB_FAIL(alloc_shallow_row_projector(*left_op))) {
    LOG_WARN("fail to alloc shallow row projector", K(ret));
  } else if (OB_FAIL(alloc_prior_row_cells(connect_by.get_connect_by_prior_exprs()->get_size()))) {
    LOG_WARN("fail to alloc prior exprs result row", K(ret));
  } else if (OB_FAIL(hash_filter_rows_.create(CONNECT_BY_TREE_HEIGHT))) {
    LOG_WARN("create hash set failed", K(ret));
  } else {
    uint64_t tenant_id = expr_ctx->my_session_->get_effective_tenant_id();
    allocator_.set_tenant_id(tenant_id);
    pump_row_desc_ = connect_by.get_pump_row_desc();
    pseudo_column_row_desc_ = connect_by.get_pseudo_column_row_desc();
    connect_by_prior_exprs_ = connect_by.get_connect_by_prior_exprs();
    expr_ctx_ = expr_ctx;
    connect_by_ = &connect_by;
    connect_by_path_count_ = connect_by.get_sys_connect_by_path_expression_count();
    is_nocycle_ = connect_by.get_nocycle();
    DLIST_FOREACH(calc_node, connect_by.sys_connect_exprs_)
    {
      if (OB_FAIL(sys_path_buffer_.push_back(ObString()))) {
        LOG_WARN("init sys path buffer failed", K(ret));
      }
    }
    if (OB_FAIL(check_output_pseudo_columns())) {
      LOG_WARN("fail to check output pseudo columns", K(ret));
    } else if (OB_FAIL(check_pump_row_desc())) {
      LOG_WARN("fail to check pump row desc", K(ret));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObConnectByPump::join_right_table(PumpNode& node, bool& matched, ObPhyOperator::ObPhyOperatorCtx* phy_ctx)
{
  int ret = OB_SUCCESS;
  const ObNewRow* output_row = NULL;
  matched = false;
  ObConnectBy::ObConnectByCtx* join_ctx = static_cast<ObConnectBy::ObConnectByCtx*>(phy_ctx);
  if (OB_ISNULL(join_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("join ctx is null", K(ret));
  } else if (FALSE_IT(join_ctx->left_row_ = node.pump_row_)) {
  } else if (OB_NOT_NULL(node.first_child_)) {
    join_ctx->right_row_ = node.first_child_;
    node.first_child_ = NULL;
    matched = true;
  } else if (OB_FAIL(connect_by_->set_level_as_param(*join_ctx, node.level_ + 1))) {
    LOG_WARN("set current level as param failed", K(ret));
  } else {
    while (OB_SUCC(ret) && false == matched) {
      if (OB_FAIL(node.row_fetcher_.get_next_row(const_cast<ObNewRow*&>(join_ctx->right_row_)))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("get row from row store failed", K(ret));
        }
      } else if (OB_FAIL(connect_by_->calc_other_conds(*join_ctx, matched))) {
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
        } else if (OB_FAIL(connect_by_->join_rows(*join_ctx, output_row))) {
          LOG_WARN("join rows failed", K(ret));
        } else if (OB_FAIL(construct_pump_row(join_ctx->right_row_))) {
          LOG_WARN("construct pump node failed", K(ret));
        } else if (OB_FAIL(deep_copy_row(shallow_row_, next_node.pump_row_))) {
          LOG_WARN("deep copy row failed", K(ret));
        } else if (OB_FAIL(deep_copy_row(*output_row, next_node.output_row_))) {
          LOG_WARN("deep copy row failed", K(ret));
        } else if (OB_FAIL(deep_copy_row(*join_ctx->right_row_, next_node.right_row_))) {
          LOG_WARN("deep copy row failed", K(ret));
        } else if (OB_FAIL(alloc_iter(next_node))) {
          LOG_WARN("alloc iterator failed", K(ret));
        } else if (OB_FAIL(push_back_node_to_stack(next_node))) {
          LOG_WARN("push back node to stack failed", K(ret));
        } else if (next_node.is_cycle_) {
          // nocycle mode, if the matched child node is the same as an ancestor node, then abandon
          // this child node and continue searching
          matched = false;
          if (OB_FAIL(free_pump_node(next_node))) {
            LOG_WARN("free pump node failed", K(ret));
          }
        }
      }
    }
  }

  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObConnectByPump::get_next_row(ObPhyOperator::ObPhyOperatorCtx* phy_ctx)
{
  int ret = OB_SUCCESS;
  bool matched = false;
  PumpNode pop_node;
  while (OB_SUCC(ret) && false == matched && pump_stack_.count() > 0) {
    PumpNode& node = pump_stack_.at(pump_stack_.count() - 1);
    cur_level_ = node.level_ + 1;
    if (OB_FAIL(join_right_table(node, matched, phy_ctx))) {
      LOG_WARN("join right table failed", K(ret));
    } else if (false == matched) {
      if (OB_FAIL(pump_stack_.pop_back(pop_node))) {
        LOG_WARN("pump stack pop back failed", K(ret));
      } else {
        // To pop a node in /pump_stack, you need to add the ObString of all sys_connect_by_path expressions in
        // sys_path_buffer_ All subtract the part corresponding to pop_node at the end.
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
        ObHashColumn hash_col;
        hash_col.init(pop_node.prior_exprs_result_, expr_ctx_);
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
    if (NULL != connect_by_root_row_) {
      allocator_.free(const_cast<ObNewRow*>(connect_by_root_row_));
      connect_by_root_row_ = NULL;
    }
  }

  return ret;
}

int ObConnectByPump::calc_prior_and_check_cycle(PumpNode& node, bool set_refactored)
{
  int ret = OB_SUCCESS;
  if (never_meet_cycle_) {
  } else {
    if (OB_ISNULL(connect_by_prior_exprs_) || OB_ISNULL(expr_ctx_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid data member", KPC(connect_by_prior_exprs_), K(ret));
    } else if (OB_UNLIKELY(connect_by_prior_exprs_->get_size() == 0)) {
      if (node.level_ > 1) {
        node.is_cycle_ = true;
        if (false == is_nocycle_) {
          ret = OB_ERR_CBY_LOOP;
        }
      }
    } else if (connect_by_prior_exprs_->get_size() > 0) {
      // allocate prior_expr_result_row
      int result_cnt = connect_by_prior_exprs_->get_size();
      if (OB_ISNULL(prior_exprs_result_row_.cells_) || OB_UNLIKELY(result_cnt != prior_exprs_result_row_.count_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("not init", K(ret));
      } else {
        // calc prior expr result
        int idx = 0;
        DLIST_FOREACH(expr, *connect_by_prior_exprs_)
        {
          if (OB_FAIL(expr->calc(*expr_ctx_, *node.pump_row_, prior_exprs_result_row_.cells_[idx]))) {
            LOG_WARN("fail to calc prior expr", KPC(expr));
          } else {
            ++idx;
          }
        }
      }

      LOG_DEBUG("connect by after prior exprs", K(prior_exprs_result_row_), K(cur_level_));
      // check cycle and add node
      if (OB_SUCC(ret)) {
        const ObNewRow* check_row = NULL;
        if (OB_FAIL(set_refactored && deep_copy_row(prior_exprs_result_row_, node.prior_exprs_result_))) {
          LOG_WARN("deep copy row failed", K(ret));
        } else if (FALSE_IT(check_row = set_refactored ? node.prior_exprs_result_ : &prior_exprs_result_row_)) {
        } else if (OB_FAIL(check_cycle(*check_row, set_refactored))) {
          if (OB_ERR_CBY_LOOP == ret) {
            node.is_cycle_ = true;
            if (is_nocycle_) {
              ret = OB_SUCCESS;
            }
          } else {
            LOG_WARN("fail to check cycle", K(ret), K(node), K(prior_exprs_result_row_));
          }
        } else {
          node.is_cycle_ = false;
        }
      }
    }
  }
  return ret;
}

int ObConnectByPump::check_cycle(const ObNewRow& row, bool set_refactored)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(expr_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid data member", K(ret));
  } else {
    ObHashColumn hash_col;
    hash_col.init(&row, expr_ctx_);
    if (OB_FAIL(hash_filter_rows_.exist_refactored(hash_col))) {
      if (OB_HASH_NOT_EXIST == ret) {
        if (set_refactored && OB_FAIL(hash_filter_rows_.set_refactored(hash_col))) {
          LOG_WARN("Failed to insert into hashset", K(ret), K(row));
        } else {
          ret = OB_SUCCESS;
        }
      } else if (OB_HASH_EXIST == ret) {
        ret = OB_ERR_CBY_LOOP;
        LOG_WARN("CONNECT BY loop in user data", K(ret), K(row));

      } else {
        LOG_WARN("Failed to find in hashset", K(ret));
      }
    }
  }

  return ret;
}

int ObConnectByPump::check_child_cycle(const ObNewRow& right_row, PumpNode& node)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(construct_pump_row(&right_row))) {
    LOG_WARN("construct pump node failed", K(ret));
  } else if (FALSE_IT(node.pump_row_ = &shallow_row_)) {
  } else if (OB_FAIL(calc_prior_and_check_cycle(node, false))) {
    LOG_WARN("calc prior expr and check cycle failed", K(ret));
  }
  return ret;
}

int ObConnectByPump::get_sys_path(uint64_t sys_connect_by_path_id, ObString& parent_path)
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

int ObConnectByPump::concat_sys_path(uint64_t sys_connect_by_path_id, const ObString& cur_path)
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

int ObConnectByPump::ConnectByHashTable::init(ObIAllocator& alloc)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    void* alloc_buf = alloc.alloc(sizeof(ModulePageAllocator));
    void* bucket_buf = alloc.alloc(sizeof(BucketArray));
    void* cell_buf = alloc.alloc(sizeof(AllCellArray));
    if (OB_ISNULL(bucket_buf) || OB_ISNULL(cell_buf) || OB_ISNULL(alloc_buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      if (OB_NOT_NULL(bucket_buf)) {
        alloc.free(bucket_buf);
      }
      if (OB_NOT_NULL(cell_buf)) {
        alloc.free(cell_buf);
      }
      if (OB_NOT_NULL(alloc_buf)) {
        alloc.free(alloc_buf);
      }
      LOG_WARN("failed to alloc memory", K(ret));
    } else {
      ht_alloc_ = new (alloc_buf) ModulePageAllocator(alloc);
      ht_alloc_->set_label("HtAlloc");
      buckets_ = new (bucket_buf) BucketArray(*ht_alloc_);
      all_cells_ = new (cell_buf) AllCellArray(*ht_alloc_);
      inited_ = true;
    }
  }
  return ret;
}

void ObConnectByPump::ConnectByHashTable::reverse_bucket_cells()
{
  HashTableCell* head_tuple = NULL;
  HashTableCell* cur_tuple = NULL;
  HashTableCell* next_tuple = NULL;
  for (int64_t bucket_idx = 0; bucket_idx < nbuckets_; bucket_idx++) {
    if (NULL != (head_tuple = buckets_->at(bucket_idx))) {
      cur_tuple = head_tuple->next_tuple_;
      head_tuple->next_tuple_ = NULL;
      while (NULL != cur_tuple) {
        next_tuple = cur_tuple->next_tuple_;
        cur_tuple->next_tuple_ = head_tuple;
        head_tuple = cur_tuple;
        cur_tuple = next_tuple;
      }
      buckets_->at(bucket_idx) = head_tuple;
    }
  }
}

int ObConnectByPump::calc_hash_value(
    const ObArray<ObSqlExpression*>& hash_exprs, const ObNewRow& row, ObExprCtx& expr_ctx, uint64_t& hash_value) const
{
  int ret = OB_SUCCESS;
  ObObj obj_value;
  hash_value = 0;
  if (OB_ISNULL(connect_by_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("connect by is null", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < hash_exprs.count(); i++) {
    ObSqlExpression* hash_expr = hash_exprs.at(i);
    ObConnectBy::EqualConditionInfo equal_cond_info = connect_by_->equal_cond_infos_.at(i);
    if (OB_ISNULL(hash_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("hash probe expr is null", K(ret));
    } else if (OB_FAIL(hash_expr->calc(expr_ctx, row, obj_value))) {
      LOG_WARN("calc left expr value failed", K(ret));
    } else if (obj_value.get_type() != equal_cond_info.cmp_type_ ||
               (ObStringTC == obj_value.get_type_class() && obj_value.get_collation_type() != equal_cond_info.ctype_)) {
      ObObj buf_obj;
      const ObObj* res_obj = NULL;
      EXPR_DEFINE_CAST_CTX(expr_ctx, CM_NONE);
      cast_ctx.dest_collation_ = equal_cond_info.ctype_;
      if (OB_FAIL(ObObjCaster::to_type(equal_cond_info.cmp_type_, cast_ctx, obj_value, buf_obj, res_obj))) {
        LOG_WARN("failed to cast obj", K(ret), K(equal_cond_info.cmp_type_), K(obj_value));
      } else if (OB_ISNULL(res_obj)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("res_obj is null", K(ret));
      } else {
        hash_value = res_obj->hash_murmur(hash_value);
      }
    } else {
      hash_value = obj_value.hash_murmur(hash_value);
    }
  }
  return ret;
}

int ObConnectByPump::build_hash_table(ObIAllocator& alloc)
{
  int ret = OB_SUCCESS;
  int64_t row_store_cnt = row_store_.get_row_cnt();
  int64_t bucket_num = 32;
  // The order of the rows in ChunkRowStore is the same as the order of reading in the right table,
  // so traverse the ChunkRowStore and add each row to the head of the linked list of the corresponding bucket
  // The order of the rows in the linked list of each bucket is opposite to the order read in the right table.
  // Although it does not affect the correctness, in order to be the same as the result before the modification, try to
  // be compatible with oracle, Processed here.
  // 1. If you can apply for the space to record the pointer of the tuple at the end of each bucket,
  //    then use this array to ensure that the order is ideal when creating the hash table
  // 2. If you can't apply for space, then after the hash table is built, reverse each bucket list. At this time,
  // need_reverse_bucket is true.
  bool need_reverse_bucket = true;
  while (bucket_num < row_store_cnt * 2) {
    bucket_num *= 2;
  }
  if (OB_FAIL(hash_table_.init(alloc))) {
    LOG_WARN("init hash table failed", K(ret));
  } else {
    hash_table_.nbuckets_ = bucket_num;
    hash_table_.row_count_ = row_store_.get_row_cnt();
    hash_table_.buckets_->reuse();
    hash_table_.all_cells_->reuse();
  }

  HashTableCell** bucket_end_cell = NULL;
  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(0 == connect_by_->hash_key_exprs_.count()) || OB_ISNULL(expr_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("hash key expr or expr_ctx_ is null", K(ret), K(connect_by_->hash_key_exprs_));
  } else if (OB_FAIL(hash_table_.buckets_->init(hash_table_.nbuckets_))) {
    LOG_WARN("alloc bucket array failed", K(ret), K(hash_table_.nbuckets_));
  } else if (0 < hash_table_.row_count_ && OB_FAIL(hash_table_.all_cells_->init(hash_table_.row_count_))) {
    LOG_WARN("alloc hash cell failed", K(ret), K(hash_table_.row_count_));
  } else if (OB_ISNULL(
                 bucket_end_cell = static_cast<HashTableCell**>(alloc.alloc(sizeof(HashTableCell*) * bucket_num)))) {
    need_reverse_bucket = true;
    LOG_TRACE("alloc bucket end cell failed, need reverse bucket", K(bucket_num));
  } else {
    for (int64_t i = 0; i < bucket_num; i++) {
      bucket_end_cell[i] = NULL;
    }
    need_reverse_bucket = false;
  }
  if (OB_FAIL(ret)) {
  } else {
    ObChunkRowStore::Iterator iterator;
    const ObChunkRowStore::StoredRow* store_row = NULL;
    ObNewRow* new_row = NULL;
    HashTableCell* tuple = NULL;
    int64_t cell_index = 0;
    int64_t bucket_id = 0;
    uint64_t hash_value = 0;
    if (OB_FAIL(row_store_.begin(iterator))) {
      LOG_WARN("begin row store failed", K(ret));
    }
    while (OB_SUCC(ret)) {
      if (OB_FAIL(iterator.get_next_row(store_row))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("get next row failed", K(ret));
        }
      } else if (OB_ISNULL(store_row)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("row is null", K(ret));
      } else if (OB_FAIL(iterator.convert_to_row(store_row, new_row))) {
        LOG_WARN("convert to row failed", K(ret));
      } else if (OB_FAIL(calc_hash_value(connect_by_->hash_key_exprs_, *new_row, *expr_ctx_, hash_value))) {
        LOG_WARN("calc hash value", K(ret));
      } else {
        bucket_id = hash_value & (hash_table_.nbuckets_ - 1);
        tuple = &(hash_table_.all_cells_->at(cell_index));
        tuple->stored_row_ = store_row;
        if (need_reverse_bucket) {
          tuple->next_tuple_ = hash_table_.buckets_->at(bucket_id);
          hash_table_.buckets_->at(bucket_id) = tuple;
        } else {
          tuple->next_tuple_ = NULL;
          if (NULL == bucket_end_cell[bucket_id]) {
            hash_table_.buckets_->at(bucket_id) = tuple;
          } else {
            bucket_end_cell[bucket_id]->next_tuple_ = tuple;
          }
          bucket_end_cell[bucket_id] = tuple;
        }
        ++cell_index;
      }
    }
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
      if (need_reverse_bucket) {
        hash_table_.reverse_bucket_cells();
      }
      use_hash_ = true;
      LOG_TRACE("build hash table succeed", K(connect_by_->hash_key_exprs_), K(row_store_cnt), K(bucket_num));
    }
    if (!OB_ISNULL(bucket_end_cell)) {
      alloc.free(bucket_end_cell);
    }
  }
  return ret;
}

int ObConnectByPump::RowFetcher::init(
    ObConnectByPump& connect_by_pump, const ObArray<ObSqlExpression*>& hash_probe_exprs, const ObNewRow* left_row)
{
  int ret = OB_SUCCESS;
  use_hash_ = false;
  if (OB_ISNULL(iterator_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("row fetcher iterator is null", K(ret));
  } else if (OB_FAIL(connect_by_pump.row_store_.begin(*iterator_))) {
    LOG_WARN("init chunk row store iterator failed", K(ret));
  } else if (!connect_by_pump.use_hash_) {
  } else {
    uint64_t hash_value = 0;
    if (!connect_by_pump.hash_table_.inited_) {
      ret = OB_NOT_INIT;
      LOG_WARN("hash table not init", K(ret));
    } else if (OB_ISNULL(left_row) || OB_UNLIKELY(0 == hash_probe_exprs.count()) ||
               OB_ISNULL(connect_by_pump.expr_ctx_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("left row or hash probe expr is null", K(left_row), K(hash_probe_exprs), K(connect_by_pump.expr_ctx_));
    } else if (OB_FAIL(connect_by_pump.calc_hash_value(
                   hash_probe_exprs, *left_row, *connect_by_pump.expr_ctx_, hash_value))) {
      LOG_WARN("calc hash value", K(ret));
    } else {
      use_hash_ = true;
      int64_t bucket_id = hash_value & (connect_by_pump.hash_table_.nbuckets_ - 1);
      tuple_ = connect_by_pump.hash_table_.buckets_->at(bucket_id);
    }
  }
  return ret;
}

int ObConnectByPump::RowFetcher::get_next_row(ObNewRow*& row)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(iterator_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("row fetcher iterator is null", K(ret));
  } else if (!use_hash_) {
    if (OB_FAIL(iterator_->get_next_row(row))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("get next row failed", K(ret));
      }
    }
  } else {
    if (NULL == tuple_) {
      ret = OB_ITER_END;
    } else if (OB_ISNULL(tuple_->stored_row_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("store row is null", K(ret));
    } else if (OB_FAIL(iterator_->convert_to_row(tuple_->stored_row_, row))) {
      LOG_WARN("convert to row failed", K(ret));
    } else {
      tuple_ = tuple_->next_tuple_;
    }
  }
  return ret;
}

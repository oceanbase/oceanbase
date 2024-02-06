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

int ObConnectByOpPumpBase::deep_copy_row(
  const ObIArray<ObExpr*> &exprs,
  const ObChunkDatumStore::StoredRow *&dst_row)
{
  ObChunkDatumStore::StoredRow *sr = NULL;
  int ret = ObChunkDatumStore::StoredRow::build(sr, exprs, *eval_ctx_, allocator_);
  dst_row = sr;
  return ret;
}

int ObConnectByOpPump::ObHashColumn::inner_hash(uint64_t &result) const
{
  int ret = OB_SUCCESS;
  result = 99194853094755497L;
  if (OB_ISNULL(exprs_) || OB_ISNULL(row_) || OB_UNLIKELY(exprs_->count() != row_->cnt_)) {
  } else {
    int64_t col_count = exprs_->count();
    ObExpr *expr = NULL;
    const ObDatum *datum = NULL;
    for (int64_t i = 0; i < col_count && OB_SUCC(ret); i++) {
      if (OB_ISNULL(expr = exprs_->at(i))
          || OB_ISNULL(expr->basic_funcs_)) {
      } else {
        datum = &row_->cells()[i];
        if (OB_FAIL(expr->basic_funcs_->murmur_hash_v2_(*datum, result, result))) {
          LOG_WARN("do hash failed", K(ret));
        }
      }
    }
  }
  return ret;
}

bool ObConnectByOpPump::ObHashColumn::operator ==(const ObHashColumn &other) const
{
  bool result = true;
	if (OB_ISNULL(row_) || OB_ISNULL(exprs_) || OB_ISNULL(other.row_)) {
    result = false;
  } else {
    const ObDatum *lcell = row_->cells();
    const ObDatum *rcell = other.row_->cells();

    int64_t col_count = exprs_->count();
    ObExpr *expr = NULL;
    int cmp_ret = 0;
    for (int64_t i = 0; result && i < col_count; i++) {
      if (OB_ISNULL(expr = exprs_->at(i))
          || OB_ISNULL(expr->basic_funcs_)) {
      } else {
        (void)expr->basic_funcs_->null_first_cmp_(lcell[i], rcell[i], cmp_ret);
        result = (0 == cmp_ret);
      }
    }
  }
  return result;
}

//在ObConnectBy的rescan接口中会调用，需要保证其为open之后的状态
void ObConnectByOpPump::reset()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(free_pump_node_stack(pump_stack_))) {
    LOG_ERROR("fail to free pump stack", K(ret));
  }
  pump_stack_.reset();
  hash_filter_rows_.reuse();
  for (int64_t i = 0; i < sys_path_buffer_.count(); i++) {
    ObString &str = sys_path_buffer_.at(i);
    if (NULL != str.ptr()) {
      allocator_.free(const_cast<char *>(str.ptr()));
    }
    str.reset();
  }
  cur_level_ = 1;
  // TODO:shanting 计划生成时记录右表是否有含参数的条件下推，如果没有可以不做datum_store_的rebuild
  if (true) {
    datum_store_.reset();
    datum_store_constructed_ = false;
    hash_table_.reset();
  }
}


int ObConnectByOpPump::add_root_row()
{
  int ret = OB_SUCCESS;
  PumpNode node;
  node.level_ = 1;
  bool is_push = false;
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
  } else if (OB_FAIL(push_back_node_to_stack(node, is_push))) {
    LOG_WARN("fail to push back row", K(ret));
  } else if (OB_FAIL(alloc_iter(pump_stack_.at(pump_stack_.count() - 1)))) {
    LOG_WARN("alloc iterator failed", K(ret));
  }
  if (!is_push) {
    free_pump_node(node);
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
  const ObChunkDatumStore::StoredRow *dst_row = NULL;
  if (OB_FAIL(deep_copy_row(*right_prior_exprs_, dst_row))) {
    LOG_WARN("deep copy row failed", K(ret));
  } else if (OB_FAIL(datum_store_.add_row(*dst_row))) {
    LOG_WARN("datum store add row failed", K(ret));
  }
  if (OB_NOT_NULL(dst_row)) {
    allocator_.free(const_cast<ObChunkDatumStore::StoredRow *>(dst_row));
    dst_row = NULL;
  }
  return ret;
}

int ObConnectByOpPump::alloc_iter(PumpNode &pop_node)
{
  int ret = OB_SUCCESS;
  void *buf = allocator_.alloc(sizeof(ObChunkDatumStore::Iterator));
  if (OB_ISNULL(buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory", K(ret));
  } else {
    pop_node.row_fetcher_.iterator_ = new (buf) ObChunkDatumStore::Iterator();
  }
  return ret;
}

void ObConnectByOpPump::free_pump_node(PumpNode &pop_node)
{
  if (OB_NOT_NULL(pop_node.pump_row_)) {
    allocator_.free(const_cast<ObChunkDatumStore::StoredRow *>(pop_node.pump_row_));
  }
  if (OB_NOT_NULL(pop_node.output_row_)) {
    allocator_.free(const_cast<ObChunkDatumStore::StoredRow *>(pop_node.output_row_));
  }
  if (OB_NOT_NULL(pop_node.prior_exprs_result_)) {
    allocator_.free(const_cast<ObChunkDatumStore::StoredRow *>(pop_node.prior_exprs_result_));
  }
  if (OB_NOT_NULL(pop_node.first_child_)) {
    allocator_.free(const_cast<ObChunkDatumStore::StoredRow *>(pop_node.first_child_));
  }
  if (OB_NOT_NULL(pop_node.row_fetcher_.iterator_)) {
    pop_node.row_fetcher_.iterator_->~Iterator();
    allocator_.free(pop_node.row_fetcher_.iterator_);
  }
  pop_node.pump_row_ = NULL;
  pop_node.output_row_ = NULL;
  pop_node.prior_exprs_result_ = NULL;
  pop_node.first_child_ = NULL;
  pop_node.row_fetcher_.iterator_ = NULL;
}

int ObConnectByOpPump::free_pump_node_stack(ObIArray<PumpNode> &stack)
{
  int ret = OB_SUCCESS;
  PumpNode pop_node;
  while(OB_SUCC(ret) && false == stack.empty()) {
    if (OB_FAIL(stack.pop_back(pop_node))) {
      LOG_WARN("fail to pop back pump_stack", K(ret));
    } else {
      free_pump_node(pop_node);
    }
  }
  return ret;
}

int ObConnectByOpPump::get_top_pump_node(PumpNode *&node)
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

int ObConnectByOpPump::push_back_node_to_stack(PumpNode &node, bool &is_push)
{
  int ret = OB_SUCCESS;
  is_push = false;
  PumpNode *top_node = pump_stack_.count() > 0 ? &pump_stack_.at(pump_stack_.count() - 1) : NULL;
  if (OB_FAIL(calc_prior_and_check_cycle(node, true/* add node to hashset */, top_node))) {
    LOG_WARN("fail to calc path node", K(ret));
  } else if (false == node.is_cycle_) {
    if (OB_FAIL(pump_stack_.push_back(node))) {
      LOG_WARN("fail to push back row", K(ret));
    } else {
      is_push = true;
    }
  }

  if (OB_FAIL(ret)) {//if fail free memory
    if (node.prior_exprs_result_ != NULL) {
      allocator_.free(const_cast<ObChunkDatumStore::StoredRow *>(node.prior_exprs_result_));
      node.prior_exprs_result_ = NULL;
    }
  }
  return ret;
}

int ObConnectByOpPump::init(const ObNLConnectBySpec &connect_by, ObNLConnectByOp &connect_by_op,
    ObEvalCtx &eval_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_FAIL(hash_filter_rows_.create(CONNECT_BY_TREE_HEIGHT))) {
    LOG_WARN("create hash set failed", K(ret));
  } else {
    connect_by_prior_exprs_ = &connect_by.connect_by_prior_exprs_;
    eval_ctx_ = &eval_ctx;
    connect_by_ = &connect_by_op;
    left_prior_exprs_ = &connect_by.left_prior_exprs_;
    right_prior_exprs_ = &connect_by.right_prior_exprs_;
    cur_output_exprs_ = &connect_by.cur_row_exprs_;
    connect_by_path_count_ = connect_by.get_sys_connect_by_path_expression_count();
    is_nocycle_ = connect_by.is_nocycle_;
    never_meet_cycle_ = !connect_by.has_prior_;
    ARRAY_FOREACH(connect_by.sys_connect_exprs_, i) {
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

int ObConnectByOpPump::join_right_table(PumpNode &node, bool &matched)
{
  int ret = OB_SUCCESS;
  matched = false;
  LOG_DEBUG("join right table", KPC(node.pump_row_), K(&node.row_fetcher_));
  if (OB_FAIL(node.pump_row_->to_expr(*left_prior_exprs_, *eval_ctx_,
                                      left_prior_exprs_->count()))) {
    LOG_WARN("datum row to expr failed", K(ret));
//  } else if (OB_NOT_NULL(node.first_child_)) {
//    join_ctx->right_row_ = node.first_child_;
//    node.first_child_ = NULL;
//    matched = true;
  } else if (OB_FAIL(connect_by_->set_level_as_param(node.level_ + 1))) {
    LOG_WARN("set current level as param failed", K(ret));
  } else {
    while(OB_SUCC(ret) && false == matched) {
      connect_by_->clear_evaluated_flag();
      if (OB_FAIL(node.row_fetcher_.get_next_row(*right_prior_exprs_, *eval_ctx_))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("get row from row store failed", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(connect_by_->calc_other_conds(matched))) {
        LOG_WARN("calc other conds failed", K(ret));
      } else if (matched) {
        PumpNode next_node;
        bool is_push = false;
        next_node.level_ = node.level_ + 1;
        if (next_node.level_ >= CONNECT_BY_MAX_NODE_NUM) {
          ret = OB_ERR_CBY_NO_MEMORY;
          uint64_t current_level = node.level_;
          int64_t max_node_num = CONNECT_BY_MAX_NODE_NUM; 
          LOG_WARN("connect by reach memory limit", K(ret), K(current_level),
                    K(max_node_num));
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
        } else if (OB_FAIL(push_back_node_to_stack(next_node, is_push))) {
          LOG_WARN("push back node to stack failed", K(ret));
        } else if (next_node.is_cycle_) {
          //nocycle模式如果匹配到的子节点与某个祖先节点相同，那么放弃这个子节点继续搜索
          matched = false;
        } else if (OB_FAIL(alloc_iter(pump_stack_.at(pump_stack_.count() - 1)))) {
          LOG_WARN("alloc iterator failed", K(ret));
        }
        if (!is_push) {
          free_pump_node(next_node);
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
    PumpNode &node = pump_stack_.at(pump_stack_.count() - 1);
    cur_level_ = node.level_ + 1;
    if (OB_FAIL(join_right_table(node, matched))) {
      LOG_WARN("join right table failed", K(ret));
    } else if (false == matched) {
      if (OB_FAIL(pump_stack_.pop_back(pop_node))) {
        LOG_WARN("pump stack pop back failed", K(ret));
      } else {
        //pump_stack中pop出一个节点，需要将sys_path_buffer_中所有sys_connect_by_path表达式的ObString
        //都减去末尾对应pop_node的那部分。
        if (pump_stack_.empty()) {
          for (int64_t i = 0; i < sys_path_buffer_.count(); i++) {
            ObString &str = sys_path_buffer_.at(i);
            str.set_length(0);
          }
        } else {
          PumpNode &parent = pump_stack_.at(pump_stack_.count() - 1);
          if (parent.sys_path_length_.count() != sys_path_buffer_.count()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("different sys_connect_by_path size", K(ret));
          } else {
            for (int64_t i = 0; i < sys_path_buffer_.count(); i++) {
              ObString &str = sys_path_buffer_.at(i);
              str.set_length(parent.sys_path_length_.at(i));
            }
          }
        }
      }
      if (OB_SUCC(ret) && NULL != pop_node.prior_exprs_result_) {
        ObHashColumn hash_col(pop_node.prior_exprs_result_, connect_by_prior_exprs_);
        if (OB_FAIL(hash_filter_rows_.erase_refactored(hash_col))) {
          LOG_WARN("fail to erase prior_exprs_result from hashset", K(ret),
                    KPC(pop_node.prior_exprs_result_));
        }
      }
      if (OB_SUCC(ret)) {
        free_pump_node(pop_node);
      }
    }
  }
  if (OB_SUCC(ret) && pump_stack_.empty()) {
    ret = OB_ITER_END;
  }

  return ret;
}

//如果没有never_meet_cycle_，进行环检查.检测到环并且没有is_nocycle_则报错
//计算节点的prior_exprs_result_,加入hash_set中，existed则报有环错误
int ObConnectByOpPump::calc_prior_and_check_cycle(PumpNode &node, bool set_refactored,
                                                  PumpNode *left_node)
{
  UNUSED(left_node);
  int ret = OB_SUCCESS;
  if (never_meet_cycle_) {
  } else if (OB_ISNULL(connect_by_prior_exprs_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid data member", KPC(connect_by_prior_exprs_), K(ret));
  } else  if (OB_UNLIKELY(connect_by_prior_exprs_->empty())) {
    //只有connect by prior const会走到这里，所以level>1时一定有环。
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

int ObConnectByOpPump::check_cycle(const ObChunkDatumStore::StoredRow *row, bool set_refactored)
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

int ObConnectByOpPump::check_child_cycle(PumpNode &node, PumpNode *left_node)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(calc_prior_and_check_cycle(node, false, left_node))) {
    LOG_WARN("calc prior expr and check cycle failed", K(ret));
  }
  return ret;
}

int ObConnectByOpPump::get_sys_path(uint64_t sys_connect_by_path_id, ObString &parent_path)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(sys_path_buffer_.count() <= sys_connect_by_path_id)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid sys connect by path id", K(ret), K(sys_connect_by_path_id));
  } else {
    ObString &str = sys_path_buffer_.at(sys_connect_by_path_id);
    parent_path.assign_ptr(str.ptr(), str.length());
  }
  return ret;
}

int ObConnectByOpPump::concat_sys_path(uint64_t sys_connect_by_path_id, const ObString &cur_path)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(pump_stack_.empty())
      || OB_UNLIKELY(sys_path_buffer_.count() <= sys_connect_by_path_id)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid sys connect by path id", K(ret), K(sys_connect_by_path_id));
  } else {
    ObString &str = sys_path_buffer_.at(sys_connect_by_path_id);
    if (0 == str.size()) {
      char *buf = NULL;
      int64_t buf_size = SYS_PATH_BUFFER_INIT_SIZE;
      while (buf_size < cur_path.length()) {
        buf_size *= 2;
      }
      if (OB_ISNULL(buf = static_cast<char *>(allocator_.alloc(buf_size)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate buf failed", K(ret), K(buf_size));
      } else {
        str.assign_buffer(buf, buf_size);
      }
    } else if (str.length() + cur_path.length() > str.size()) {
      int64_t buf_size = str.size();
      while (buf_size < str.length() + cur_path.length()) {
        buf_size *= 2;
      }
      char *buf = NULL;
      ObString new_str;
      if (OB_ISNULL(buf = static_cast<char *>(allocator_.alloc(buf_size)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate buf failed", K(ret), K(buf_size));
      } else if (FALSE_IT(new_str.assign_buffer(buf, buf_size))) {
      } else if (str.length() != new_str.write(str.ptr(), str.length())) {
        LOG_WARN("copy origin string failed", K(ret));
      } else {
        allocator_.free(const_cast<char *>(str.ptr()));
        str.assign_buffer(new_str.ptr(), new_str.size());
        str.set_length(new_str.length());
      }
    }
    if (OB_SUCC(ret) && cur_path.length() != str.write(cur_path.ptr(), cur_path.length())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("write cur_path to sys_path_buffer failed", K(ret));
    }
    if (OB_SUCC(ret)) {
      PumpNode &node = pump_stack_.at(pump_stack_.count() - 1);
      if (sys_connect_by_path_id >= node.sys_path_length_.count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid pump node", K(ret), K(sys_connect_by_path_id),
                  K(node.sys_path_length_.count()));
      } else {
        uint64_t &length = node.sys_path_length_.at(sys_connect_by_path_id);
        length = str.length();
      }
    }
  }
  return ret;
}

void ObConnectByOpPump::close(ObIAllocator *allocator)
{
  reset();
  datum_store_.reset();
  datum_store_constructed_ = false;
  hash_table_.free(allocator);
}

int ObConnectByOpPump::ConnectByHashTable::init(ObIAllocator &alloc)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    void *alloc_buf = alloc.alloc(sizeof(ModulePageAllocator));
    void *bucket_buf = alloc.alloc(sizeof(BucketArray));
    void *cell_buf = alloc.alloc(sizeof(AllCellArray));
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

void ObConnectByOpPump::ConnectByHashTable::reverse_bucket_cells()
{
  HashTableCell *head_tuple = NULL;
  HashTableCell *cur_tuple = NULL;
  HashTableCell *next_tuple = NULL;
  for (int64_t bucket_idx = 0; bucket_idx < nbuckets_; bucket_idx++) {
    if (NULL != (head_tuple = buckets_->at(bucket_idx))) {
      cur_tuple = head_tuple->next_tuple_;
      head_tuple->next_tuple_ = NULL;
      while(NULL != cur_tuple) {
        next_tuple = cur_tuple->next_tuple_;
        cur_tuple->next_tuple_ = head_tuple;
        head_tuple = cur_tuple;
        cur_tuple = next_tuple;
      }
      buckets_->at(bucket_idx) = head_tuple;
    }
  }
}

int ObConnectByOpPump::calc_hash_value(const ExprFixedArray &exprs, uint64_t &hash_value)
{
  int ret = OB_SUCCESS;
  ObDatum *datum = NULL;
  hash_value = 0;
  const ObIArray<ObExpr*> *hash_exprs = &exprs;
  if (OB_ISNULL(hash_exprs) || OB_ISNULL(eval_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("eval ctx is null", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < hash_exprs->count(); i++) {
    ObExpr *hash_expr = hash_exprs->at(i);
    if (OB_ISNULL(hash_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("hash probe expr is null", K(ret));
    } else if (OB_FAIL(hash_expr->eval(*eval_ctx_, datum))) {
      LOG_WARN("calc left expr value failed", K(ret));
    } else if (OB_FAIL(hash_expr->basic_funcs_->murmur_hash_v2_(*datum, hash_value, hash_value))) {
      LOG_WARN("calc hash value failed", K(ret));
    } else {
      LOG_DEBUG("calc hash value", KPC(datum), K(hash_value), K(hash_expr), KPC(hash_expr));
    }
  }
  return ret;
}

int ObConnectByOpPump::build_hash_table(ObIAllocator &alloc)
{
  int ret = OB_SUCCESS;
  int64_t row_store_cnt = datum_store_.get_row_cnt();
  int64_t bucket_num = 32;
  // ChunkRowStore中行的顺序与右表读入的顺序相同，因此遍历ChunkRowStore，将每一行加入对应bucket的链表头部后
  // 每个bucket的链表的行的顺序与右表读入的顺序相反。虽然不影响正确性，但是为了与修改前的结果相同，尽量与oracle兼容，
  // 这里进行了处理。
  // 1.如果能够申请到空间记录每个bucket末尾的tuple的指针，那么使用这个数组可以在建立hash表时确保顺序是理想的
  // 2.如果申请不到空间，那么在建完hash表后，reverse每个bucket链表。 此时need_reverse_bucket为true。
  bool need_reverse_bucket = true;
  while (bucket_num < row_store_cnt * 2) {
    bucket_num *= 2;
  }
  if (OB_FAIL(hash_table_.init(alloc))) {
    LOG_WARN("init hash table failed", K(ret));
  } else {
    hash_table_.nbuckets_ = bucket_num;
    hash_table_.row_count_ = datum_store_.get_row_cnt();
    hash_table_.buckets_->reuse();
    hash_table_.all_cells_->reuse();
  }
  HashTableCell **bucket_end_cell = NULL;
  const ExprFixedArray &hash_key_exprs =
    (static_cast<const ObNLConnectBySpec &>(connect_by_->get_spec())).hash_key_exprs_;
  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(0 == hash_key_exprs.count()) || OB_ISNULL(eval_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("hash key expr or eval_ctx_ is null", K(ret), K(hash_key_exprs));
  } else if (OB_FAIL(hash_table_.buckets_->init(hash_table_.nbuckets_))) {
    LOG_WARN("alloc bucket array failed", K(ret), K(hash_table_.nbuckets_));
  } else if (0 < hash_table_.row_count_ &&
             OB_FAIL(hash_table_.all_cells_->init(hash_table_.row_count_))) {
    LOG_WARN("alloc hash cell failed", K(ret), K(hash_table_.row_count_));
  } else if (OB_ISNULL(bucket_end_cell = static_cast<HashTableCell **>(
                                  alloc.alloc(sizeof(HashTableCell *) * bucket_num)))) {
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
    ObChunkDatumStore::Iterator iterator;
    const ObChunkDatumStore::StoredRow *store_row = NULL;
    HashTableCell *tuple = NULL;
    int64_t cell_index = 0;
    int64_t bucket_id = 0;
    uint64_t hash_value = 0;
    if (OB_FAIL(datum_store_.begin(iterator))) {
      LOG_WARN("begin row store failed", K(ret));
    }
    LOG_DEBUG("build hash table", KPC(right_prior_exprs_), K(hash_key_exprs));
    while(OB_SUCC(ret)) {
      connect_by_->clear_evaluated_flag();
      if (OB_FAIL(iterator.get_next_row(store_row))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("get next row failed", K(ret));
        }
      } else if (OB_ISNULL(store_row)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("row is null", K(ret));
      } else if (store_row->to_expr(*right_prior_exprs_, *eval_ctx_)) {
      } else if (OB_FAIL(calc_hash_value(hash_key_exprs, hash_value))) {
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
        LOG_DEBUG("build hash table add row", K(hash_value), K(bucket_id), K(tuple), K(tuple->stored_row_), KPC(store_row));
        ++cell_index;
      }
    }
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
      if (need_reverse_bucket) {
        hash_table_.reverse_bucket_cells();
      }
      use_hash_ = true;
      LOG_TRACE("build hash table succeed", K(hash_key_exprs), K(row_store_cnt), K(bucket_num));
    }
    if (!OB_ISNULL(bucket_end_cell)) {
      alloc.free(bucket_end_cell);
    }
  }
  return ret;
}

int ObConnectByOpPump::RowFetcher::init(ObConnectByOpPump &connect_by_pump,
                                        const ExprFixedArray &hash_probe_exprs)
{
  int ret = OB_SUCCESS;
  use_hash_ = false;
  if (OB_FAIL(connect_by_pump.datum_store_.begin(*iterator_))) {
    LOG_WARN("init chunk row store iterator failed", K(ret));
  } else if (!connect_by_pump.use_hash_) {
  } else {
    uint64_t hash_value = 0;
    if (!connect_by_pump.hash_table_.inited_) {
      ret = OB_NOT_INIT;
      LOG_WARN("hash table not init", K(ret));
    } else if (OB_UNLIKELY(0 == hash_probe_exprs.count()) || OB_ISNULL(connect_by_pump.eval_ctx_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("left row or hash probe expr is null", K(ret), K(hash_probe_exprs));
    } else if (OB_FAIL(connect_by_pump.calc_hash_value(hash_probe_exprs, hash_value))) {
      LOG_WARN("calc hash value", K(ret));
    } else {
      use_hash_ = true;
      int64_t bucket_id = hash_value & (connect_by_pump.hash_table_.nbuckets_ - 1);
      tuple_ = connect_by_pump.hash_table_.buckets_->at(bucket_id);
      LOG_DEBUG("row fetcher init", K(this), K(hash_value), K(bucket_id), K(tuple_));
    }
  }
  return ret;
}

int ObConnectByOpPump::RowFetcher::get_next_row(const ObChunkDatumStore::StoredRow *&store_row)
{
  int ret = OB_SUCCESS;
  LOG_DEBUG("fetcher get next row start", K(this), K(use_hash_), K(tuple_));
  if (!use_hash_) {
    if (OB_FAIL(iterator_->get_next_row(store_row))) {
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
    } else {
      store_row = tuple_->stored_row_;
      tuple_ = tuple_->next_tuple_;
      LOG_DEBUG("fetcher get next row end", K(this), K(store_row), K(tuple_));
    }
  }
  return ret;
}

int ObConnectByOpPump::RowFetcher::get_next_row(const ObIArray<ObExpr *> &exprs, ObEvalCtx &eval_ctx)
{
  int ret = OB_SUCCESS;
  const ObChunkDatumStore::StoredRow *store_row = NULL;
  if (OB_FAIL(get_next_row(store_row))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("fail to get next row", K(ret));
    }
  } else if (OB_ISNULL(store_row)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("store row is null", K(ret));
  } else if (OB_FAIL(store_row->to_expr(exprs, eval_ctx))) {
    LOG_WARN("to exprs failed", K(ret));
  }
  return ret;
}

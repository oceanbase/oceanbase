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
#include "ob_hash_set_operator.h"
#include "lib/allocator/ob_mod_define.h"
#include "lib/utility/utility.h"
#include "share/ob_cluster_version.h"
#include "sql/engine/px/ob_px_util.h"

using namespace oceanbase::sql;
using namespace oceanbase::common;

int ObHashSetOperator::HashCols::init(const ObNewRow* row, const ObIArray<ObCollationType>* col_collation)
{
  row_ = row;
  col_collation_ = col_collation;
  return OB_SUCCESS;
}

uint64_t ObHashSetOperator::HashCols::hash() const
{
  uint64_t result = 0;
  if (!OB_ISNULL(col_collation_) && !OB_ISNULL(row_) && row_->is_valid()) {
    int64_t N = col_collation_->count();
    const ObObj* cells = row_->cells_;
    const int32_t* projector = row_->projector_;
    for (int64_t i = 0; i < N; ++i) {
      int64_t real_index = row_->projector_size_ > 0 ? projector[i] : i;
      const ObObj& cell = cells[real_index];
      result = cell.is_string_type() ? cell.varchar_hash(col_collation_->at(i), result) : cell.hash(result);
    }
  }
  return result;
}

bool ObHashSetOperator::HashCols::operator==(const ObHashSetOperator::HashCols& other) const
{
  bool result = true;
  const ObObj* lcell = NULL;
  const ObObj* rcell = NULL;
  int64_t real_idx = -1;
  if (OB_ISNULL(col_collation_)) {
    result = false;
  } else {
    int64_t N = col_collation_->count();
    for (int32_t i = 0; i < N && result; ++i) {
      if (NULL != row_ && row_->is_valid()) {
        real_idx = row_->projector_size_ > 0 ? row_->projector_[i] : i;
        lcell = &row_->cells_[real_idx];
      }
      if (NULL != row_ && row_->is_valid()) {
        real_idx = other.row_->projector_size_ > 0 ? other.row_->projector_[i] : i;
        rcell = &other.row_->cells_[real_idx];
      }
      if (NULL == lcell || NULL == rcell) {
        result = false;
      } else {
        result = lcell->is_equal(*rcell, col_collation_->at(i));
      }
    }
  }
  return result;
}

int ObHashSetOperator::ObHashSetOperatorCtx::is_left_has_row(ObExecContext& ctx, bool& left_has_row)
{
  int ret = OB_SUCCESS;
  left_has_row = true;
  if (OB_ISNULL(left_op_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("left_op is null", K(ret), K(left_op_));
  } else if (OB_FAIL(left_op_->get_next_row(ctx, first_left_row_))) {
    if (OB_ITER_END == ret) {
      left_has_row = false;
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to get next row from left op", K(ret));
    }
  }
  return ret;
}

void ObHashSetOperator::ObHashSetOperatorCtx::reset()
{
  first_get_left_ = true;
  has_got_part_ = false;
  iter_end_ = false;
  first_left_row_ = NULL;
  hp_infras_.reset();
}

int ObHashSetOperator::ObHashSetOperatorCtx::get_left_row(ObExecContext& ctx, const ObNewRow*& cur_row)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(left_op_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("left_op is null", K(ret), K(left_op_));
  } else if (first_get_left_) {
    if (OB_ISNULL(first_left_row_)) {
      ret = OB_ITER_END;
    } else {
      cur_row = first_left_row_;
      first_left_row_ = NULL;
    }
    first_get_left_ = false;
  } else if (OB_FAIL(left_op_->get_next_row(ctx, cur_row))) {
    if (ret != OB_ITER_END) {
      LOG_WARN("child operator get next row failed", K(ret));
    }
  }
  return ret;
}

ObHashSetOperator::ObHashSetOperator(common::ObIAllocator& alloc) : ObSetOperator(alloc)
{}

ObHashSetOperator::~ObHashSetOperator()
{}

int ObHashSetOperator::rescan(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObHashSetOperatorCtx* hash_ctx = NULL;
  if (OB_FAIL(ObSetOperator::rescan(ctx))) {
    LOG_WARN("rescan child operator failed", K(ret));
  } else if (OB_ISNULL(hash_ctx = GET_PHY_OPERATOR_CTX(ObHashSetOperatorCtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get except ctx", K(ret), K(hash_ctx));
  } else {
    hash_ctx->reset();
    hash_ctx->left_op_ = get_child(FIRST_CHILD);
  }
  return ret;
}

int64_t ObHashSetOperator::to_string_kv(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_KV(N_DISTINCT, distinct_, "collation_types", cs_types_);
  return pos;
}

int ObHashSetOperator::init_hash_partition_infras(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObHashSetOperatorCtx* hash_ctx = NULL;
  int64_t est_rows = get_rows();
  if (OB_ISNULL(hash_ctx = GET_PHY_OPERATOR_CTX(ObHashSetOperatorCtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get physical operator context failed", K(ret), K(ctx), K_(id));
  } else if (OB_FAIL(ObPxEstimateSizeUtil::get_px_size(&ctx, px_est_size_factor_, get_rows(), est_rows))) {
    LOG_WARN("failed to get px size", K(ret));
  } else if (OB_FAIL(hash_ctx->sql_mem_processor_.init(&hash_ctx->exec_ctx_.get_allocator(),
                 hash_ctx->exec_ctx_.get_my_session()->get_effective_tenant_id(),
                 est_rows * get_width(),
                 get_type(),
                 get_id(),
                 &hash_ctx->exec_ctx_))) {
    LOG_WARN("failed to init sql mem processor", K(ret));
  } else if (OB_FAIL(hash_ctx->hp_infras_.init(ctx.get_my_session()->get_effective_tenant_id(),
                 GCONF.is_sql_operator_dump_enabled() && !(GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_2250),
                 true,
                 true,
                 2,
                 &hash_ctx->sql_mem_processor_))) {
    LOG_WARN("failed to init hash partition infrastructure", K(ret));
  } else {
    for (int64_t i = 0; i < cs_types_.count() && OB_SUCC(ret); ++i) {
      ObColumnInfo column_info;
      column_info.index_ = i;
      column_info.cs_type_ = cs_types_.at(i);
      if (OB_FAIL(hash_ctx->hp_infras_.add_part_col_idx(column_info))) {
        LOG_WARN("failed to add part column index", K(ret));
      }
    }
    int64_t est_bucket_num = hash_ctx->hp_infras_.est_bucket_count(est_rows, get_width());
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(hash_ctx->hp_infras_.start_round())) {
      LOG_WARN("failed to start round", K(ret));
    } else if (OB_FAIL(hash_ctx->hp_infras_.init_hash_table(est_bucket_num))) {
      LOG_WARN("failed to init hash table", K(ret));
    }
  }
  return ret;
}

int ObHashSetOperator::build_hash_table(ObExecContext& ctx, bool from_child) const
{
  int ret = OB_SUCCESS;
  ObHashSetOperatorCtx* hash_ctx = NULL;
  const ObNewRow* cur_row = NULL;
  const ObChunkRowStore::StoredRow* store_row = NULL;
  bool inserted = false;
  if (OB_ISNULL(hash_ctx = GET_PHY_OPERATOR_CTX(ObHashSetOperatorCtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get physical operator context failed", K(ret));
  } else {
    if (!from_child) {
      if (OB_FAIL(hash_ctx->hp_infras_.open_cur_part(InputSide::RIGHT))) {
        LOG_WARN("failed to open cur part", K(ret));
      } else if (OB_FAIL(hash_ctx->hp_infras_.resize(hash_ctx->hp_infras_.get_cur_part_row_cnt(InputSide::RIGHT)))) {
        LOG_WARN("failed to init hash table", K(ret));
      } else if (OB_FAIL(hash_ctx->sql_mem_processor_.init(&hash_ctx->exec_ctx_.get_allocator(),
                     hash_ctx->exec_ctx_.get_my_session()->get_effective_tenant_id(),
                     hash_ctx->hp_infras_.get_cur_part_file_size(InputSide::RIGHT),
                     get_type(),
                     get_id(),
                     &hash_ctx->exec_ctx_))) {
        LOG_WARN("failed to init sql mem processor", K(ret));
      }
    }
    // switch to right, dump right partition
    hash_ctx->hp_infras_.switch_right();
    bool has_exists = false;
    while (OB_SUCC(ret)) {
      if (from_child) {
        ret = get_child(SECOND_CHILD)->get_next_row(ctx, cur_row);
      } else {
        ret = hash_ctx->hp_infras_.get_right_next_row(store_row, cur_row);
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(try_check_status(ctx))) {
        LOG_WARN("check status exit", K(ret));
      } else if (OB_ISNULL(cur_row)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("row is null", K(ret), K(cur_row));
      } else if (OB_FAIL(hash_ctx->hp_infras_.insert_row(cur_row, has_exists, inserted))) {
        LOG_WARN("failed to insert row", K(ret));
      }
    }  // end of while
    if (OB_ITER_END == ret) {
      if (OB_FAIL(hash_ctx->hp_infras_.finish_insert_row())) {
        LOG_WARN("failed to finish insert row", K(ret));
      } else if (!from_child && OB_FAIL(hash_ctx->hp_infras_.close_cur_part(InputSide::RIGHT))) {
        LOG_WARN("failed to close cur part", K(ret));
      }
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

int ObHashSetOperator::inner_open(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObHashSetOperatorCtx* hash_ctx = NULL;
  if (OB_FAIL(init_op_ctx(ctx))) {
    LOG_WARN("failed to init op ctx", K(ret));
  } else if (OB_ISNULL(hash_ctx = GET_PHY_OPERATOR_CTX(ObHashSetOperatorCtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get physical operator context failed", K(ret));
  } else {
    hash_ctx->left_op_ = get_child(FIRST_CHILD);
    ;
  }
  return ret;
}

int ObHashSetOperator::inner_close(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObHashSetOperatorCtx* hash_ctx = NULL;
  if (OB_ISNULL(hash_ctx = GET_PHY_OPERATOR_CTX(ObHashSetOperatorCtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get physical operator context failed", K(ret));
  } else {
    hash_ctx->reset();
    hash_ctx->sql_mem_processor_.unregister_profile();
  }
  return OB_SUCCESS;
}

OB_DEF_SERIALIZE(ObHashSetOperator)
{
  int ret = OB_SUCCESS;
  OB_UNIS_ENCODE(distinct_);
  OB_UNIS_ENCODE(cs_types_);
  if (OB_SUCC(ret)) {
    ret = ObPhyOperator::serialize(buf, buf_len, pos);
  }
  OB_UNIS_ENCODE(child_num_);
  return ret;
}

OB_DEF_DESERIALIZE(ObHashSetOperator)
{
  int ret = OB_SUCCESS;
  bool is_distinct = false;
  child_num_ = 2;  // for compatibility, set a default two child op value
  OB_UNIS_DECODE(is_distinct);
  OB_UNIS_DECODE(cs_types_);
  if (OB_SUCC(ret)) {
    ret = ObPhyOperator::deserialize(buf, data_len, pos);
  }
  OB_UNIS_DECODE(child_num_);
  if (OB_SUCC(ret)) {
    set_distinct(is_distinct);
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObHashSetOperator)
{
  int64_t len = 0;
  OB_UNIS_ADD_LEN(distinct_);
  OB_UNIS_ADD_LEN(cs_types_);
  len += ObPhyOperator::get_serialize_size();
  OB_UNIS_ADD_LEN(child_num_);
  return len;
}

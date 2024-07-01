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
#include "sql/das/iter/ob_das_lookup_iter.h"
#include "sql/das/iter/ob_das_merge_iter.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

int ObDASLookupIter::inner_init(ObDASIterParam &param)
{
  int ret = OB_SUCCESS;
  if (!IS_LOOKUP_ITER(param.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("inner init das iter with bad param type", K(param), K(ret));
  } else {
    ObDASLookupIterParam &lookup_param = static_cast<ObDASLookupIterParam&>(param);
    state_ = LookupState::INDEX_SCAN;
    index_end_ = false;
    default_batch_row_count_ = lookup_param.default_batch_row_count_;
    lookup_rowkey_cnt_ = 0;
    lookup_row_cnt_ = 0;
    index_table_iter_ = lookup_param.index_table_iter_;
    data_table_iter_ = lookup_param.data_table_iter_;
    index_ctdef_ = lookup_param.index_ctdef_;
    index_rtdef_ = lookup_param.index_rtdef_;
    lookup_ctdef_ = lookup_param.lookup_ctdef_;
    lookup_rtdef_ = lookup_param.lookup_rtdef_;
    lib::ContextParam param;
    param.set_mem_attr(MTL_ID(), ObModIds::OB_SQL_TABLE_LOOKUP, ObCtxIds::DEFAULT_CTX_ID)
         .set_properties(lib::USE_TL_PAGE_OPTIONAL);
    if (OB_FAIL(CURRENT_CONTEXT->CREATE_CONTEXT(lookup_memctx_, param))) {
      LOG_WARN("failed to create lookup memctx", K(ret));
    }
  }
  return ret;
}

int ObDASLookupIter::inner_reuse()
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(lookup_memctx_)) {
    lookup_memctx_->reset_remain_one_page();
  }
  lookup_row_cnt_ = 0;
  lookup_rowkey_cnt_ = 0;
  index_end_ = false;
  state_ = LookupState::INDEX_SCAN;
  return ret;
}

int ObDASLookupIter::inner_release()
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(lookup_memctx_)) {
    DESTROY_CONTEXT(lookup_memctx_);
    lookup_memctx_ = nullptr;
  }
  index_table_iter_ = nullptr;
  data_table_iter_ = nullptr;
  rowkey_exprs_.reset();
  return ret;
}

void ObDASLookupIter::reset_lookup_state()
{
  lookup_row_cnt_ = 0;
  lookup_rowkey_cnt_ = 0;
  index_end_ = false;
  state_ = LookupState::INDEX_SCAN;
  if (OB_NOT_NULL(data_table_iter_)) {
    data_table_iter_->reuse();
  }
  if (OB_NOT_NULL(lookup_memctx_)) {
    lookup_memctx_->reset_remain_one_page();
  }
}

int ObDASLookupIter::inner_get_next_row()
{
  int ret = OB_SUCCESS;
  bool got_next_row = false;
  int64_t simulate_batch_row_cnt = - EVENT_CALL(EventTable::EN_TABLE_LOOKUP_BATCH_ROW_COUNT);
  const bool use_simulate_batch_row_cnt = simulate_batch_row_cnt > 0 && simulate_batch_row_cnt < default_batch_row_count_;
  int64_t default_row_batch_cnt  = use_simulate_batch_row_cnt ? simulate_batch_row_cnt : default_batch_row_count_;
  LOG_DEBUG("simulate lookup row batch count", K(simulate_batch_row_cnt), K(default_row_batch_cnt));
  do {
    switch (state_) {
      case INDEX_SCAN: {
        reset_lookup_state();
        while (OB_SUCC(ret) && !index_end_ && lookup_rowkey_cnt_ < default_row_batch_cnt) {
          index_table_iter_->clear_evaluated_flag();
          if (OB_FAIL(index_table_iter_->get_next_row())) {
            if(OB_UNLIKELY(OB_ITER_END != ret)) {
              LOG_WARN("failed to get next row from index table", K(ret));
            } else {
              index_end_ = true;
              ret = OB_SUCCESS;
            }
          } else if (OB_FAIL(add_rowkey())) {
            LOG_WARN("failed to add row key", K(ret));
          } else {
            ++lookup_rowkey_cnt_;
          }
        }

        if (OB_SUCC(ret)) {
          if (OB_LIKELY(lookup_rowkey_cnt_ > 0)) {
            state_ = DO_LOOKUP;
          } else {
            state_ = FINISHED;
          }
        }
        break;
      }

      case DO_LOOKUP: {
        if (OB_FAIL(do_index_lookup())) {
          LOG_WARN("failed to do index lookup", K(ret));
        } else {
          state_ = OUTPUT_ROWS;
        }
        break;
      }

      case OUTPUT_ROWS: {
        data_table_iter_->clear_evaluated_flag();
        if (OB_FAIL(data_table_iter_->get_next_row())) {
          if (OB_LIKELY(OB_ITER_END == ret)) {
            ret = OB_SUCCESS;
            if (OB_FAIL(check_index_lookup())) {
              LOG_WARN("failed to check table lookup", K(ret));
            } else {
              state_ = INDEX_SCAN;
            }
          } else {
            LOG_WARN("failed to get next row from data table", K(ret));
          }
        } else {
          got_next_row = true;
          ++lookup_row_cnt_;
        }
        break;
      }

      case FINISHED: {
        ret = OB_ITER_END;
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected lookup state", K_(state));
      }
    }
  } while (!got_next_row && OB_SUCC(ret));

  return ret;
}

int ObDASLookupIter::inner_get_next_rows(int64_t &count, int64_t capacity)
{
  int ret = OB_SUCCESS;
  bool get_next_rows = false;
  int64_t simulate_batch_row_cnt = - EVENT_CALL(EventTable::EN_TABLE_LOOKUP_BATCH_ROW_COUNT);
  const bool use_simulate_batch_row_cnt = simulate_batch_row_cnt > 0 && simulate_batch_row_cnt < default_batch_row_count_;
  int64_t default_row_batch_cnt  = use_simulate_batch_row_cnt ? simulate_batch_row_cnt : default_batch_row_count_;
  LOG_DEBUG("simulate lookup row batch count", K(simulate_batch_row_cnt), K(default_row_batch_cnt));
  do {
    switch (state_) {
      case INDEX_SCAN: {
        reset_lookup_state();
        int64_t storage_count = 0;
        int64_t index_capacity = 0;
        while (OB_SUCC(ret) && !index_end_ && lookup_rowkey_cnt_ < default_row_batch_cnt) {
          storage_count = 0;
          index_capacity = std::min(max_size_, default_row_batch_cnt - lookup_rowkey_cnt_);
          index_table_iter_->clear_evaluated_flag();
          if (OB_FAIL(index_table_iter_->get_next_rows(storage_count, index_capacity))) {
            if (OB_UNLIKELY(OB_ITER_END != ret)) {
              LOG_WARN("failed to get next rows from index table", K(ret));
            } else {
              if (storage_count == 0) {
                index_end_ = true;
              }
              ret = OB_SUCCESS;
            }
          }
          if (OB_SUCC(ret) && storage_count > 0) {
            if (OB_FAIL(add_rowkeys(storage_count))) {
              LOG_WARN("failed to add row keys", K(ret));
            } else {
              lookup_rowkey_cnt_ += storage_count;
            }
          }
        }

        if (OB_SUCC(ret)) {
          if (OB_LIKELY(lookup_rowkey_cnt_ > 0)) {
            state_ = DO_LOOKUP;
          } else {
            state_ = FINISHED;
          }
        }
        break;
      }

      case DO_LOOKUP: {
        if (OB_FAIL(do_index_lookup())) {
          LOG_WARN("failed to do index lookup", K(ret));
        } else {
          state_ = OUTPUT_ROWS;
        }
        break;
      }

      case OUTPUT_ROWS: {
        count = 0;
        data_table_iter_->clear_evaluated_flag();
        if (OB_FAIL(data_table_iter_->get_next_rows(count, capacity))) {
          if (OB_LIKELY(OB_ITER_END == ret)) {
            ret = OB_SUCCESS;
            if (count > 0) {
              lookup_row_cnt_ += count;
              get_next_rows = true;
            } else {
              if (OB_FAIL(check_index_lookup())) {
                LOG_WARN("failed to check table lookup", K(ret));
              } else {
                state_ = INDEX_SCAN;
              }
            }
          } else {
            LOG_WARN("failed to get next rows from data table", K(ret));
          }
        } else {
          lookup_row_cnt_ += count;
          get_next_rows = true;
        }
        break;
      }

      case FINISHED: {
        ret = OB_ITER_END;
        break;
      }
    }
  } while (!get_next_rows && OB_SUCC(ret));

  return ret;
}

int ObDASLookupIter::build_lookup_range(ObNewRange &range)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(eval_ctx_) || OB_UNLIKELY(rowkey_exprs_.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid eval ctx or rowkey exprs", K_(eval_ctx), K_(rowkey_exprs), K(ret));
  } else {
    ObObj *obj_ptr = nullptr;
    void *buf = nullptr;
    int64_t rowkey_cnt = rowkey_exprs_.count();
    common::ObArenaAllocator& lookup_alloc = lookup_memctx_->get_arena_allocator();
    if (OB_ISNULL(buf = lookup_alloc.alloc(sizeof(ObObj) * rowkey_cnt))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate enough memory", K(rowkey_cnt), K(ret));
    } else {
      obj_ptr = new (buf) ObObj[rowkey_cnt];
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_cnt; i++) {
      ObObj tmp_obj;
      const ObExpr *expr = rowkey_exprs_.at(i);
      ObDatum &col_datum = expr->locate_expr_datum(*eval_ctx_);
      if (OB_UNLIKELY(T_PSEUDO_GROUP_ID == expr->type_ || T_PSEUDO_ROW_TRANS_INFO_COLUMN == expr->type_)) {
        // skip.
      } else if (OB_FAIL(col_datum.to_obj(tmp_obj, expr->obj_meta_, expr->obj_datum_map_))) {
        LOG_WARN("failed to convert datum to obj", K(ret));
      } else if (OB_FAIL(ob_write_obj(lookup_alloc, tmp_obj, obj_ptr[i]))) {
        LOG_WARN("failed to deep copy rowkey", K(ret), K(tmp_obj));
      }
    }

    if (OB_SUCC(ret)) {
      ObRowkey row_key(obj_ptr, rowkey_cnt);
      if (OB_FAIL(range.build_range(lookup_ctdef_->ref_table_id_, row_key))) {
        LOG_WARN("failed to build lookup range", K(ret), K(lookup_ctdef_->ref_table_id_), K(row_key));
      }
    }
  }

  return ret;
}

int ObDASLookupIter::build_trans_info_datum(const ObExpr *trans_info_expr, ObDatum *&datum_ptr)
{
  int ret = OB_SUCCESS;
  datum_ptr = nullptr;
  if (OB_ISNULL(trans_info_expr) || OB_ISNULL(eval_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(ret), K(trans_info_expr), K(eval_ctx_));
  } else {
    void *buf = nullptr;
    ObDatum &col_datum = trans_info_expr->locate_expr_datum(*eval_ctx_);
    int64_t pos = sizeof(ObDatum);
    int64_t len = sizeof(ObDatum) + col_datum.len_;
    if (OB_ISNULL(buf = lookup_memctx_->get_arena_allocator().alloc(len))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate enough memory", K(ret));
    } else if (FALSE_IT(datum_ptr = new (buf) ObDatum)) {
    } else if (OB_FAIL(datum_ptr->deep_copy(col_datum, static_cast<char*>(buf), len, pos))) {
      LOG_WARN("failed to deep copy datum", K(ret), K(pos), K(len));
    }
  }

  return ret;
}

}  // namespace sql
}  // namespace oceanbase

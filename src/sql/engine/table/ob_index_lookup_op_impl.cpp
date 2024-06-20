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

#include "sql/engine/table/ob_index_lookup_op_impl.h"
#include "lib/utility/ob_tracepoint.h"
using namespace oceanbase::common;

namespace oceanbase
{

namespace sql
{
ObIndexLookupOpImpl::ObIndexLookupOpImpl(LookupType lookup_type, const int64_t default_batch_row_count)
  : lookup_type_(lookup_type),
    default_batch_row_count_(default_batch_row_count),
    state_(INDEX_SCAN),
    index_end_(false),
    lookup_rowkey_cnt_(0),
    lookup_row_cnt_(0)
{}

int ObIndexLookupOpImpl::get_next_row()
{
  int ret = OB_SUCCESS;
  bool got_next_row = false;
  int64_t simulate_batch_row_cnt = - EVENT_CALL(EventTable::EN_TABLE_LOOKUP_BATCH_ROW_COUNT);
  int64_t default_row_batch_cnt  = simulate_batch_row_cnt > 0 ? simulate_batch_row_cnt : default_batch_row_count_;
  LOG_DEBUG("simulate lookup row batch count", K(simulate_batch_row_cnt), K(default_row_batch_cnt));
  do {
    switch (state_) {
      case INDEX_SCAN: {
        lookup_rowkey_cnt_ = 0;
        lookup_row_cnt_ = 0;
        reset_lookup_state();
        while (OB_SUCC(ret) && !index_end_ && lookup_rowkey_cnt_ < default_row_batch_cnt) {
          do_clear_evaluated_flag();
          if (OB_FAIL(get_next_row_from_index_table())) {
            if (OB_ITER_END != ret) {
              LOG_WARN("get next row from index table failed", K(ret));
            } else {
              index_end_ = true;
              ret = OB_SUCCESS;
            }
          } else if (OB_FAIL(process_data_table_rowkey())) {
            LOG_WARN("process data table rowkey with das failed", K(ret));
          } else {
            ++lookup_rowkey_cnt_;
          }
        }
        if (OB_SUCC(ret)) {
          if (lookup_rowkey_cnt_ > 0) {
            state_ = DO_LOOKUP;
          } else {
            state_ = FINISHED;
          }
        }
        break;
      }
      case DO_LOOKUP: {
        if (OB_FAIL(do_index_lookup())) {
          LOG_WARN("do index lookup failed", K(ret));
        } else {
          state_ = OUTPUT_ROWS;
        }
        break;
      }
      case OUTPUT_ROWS: {
        if (OB_FAIL(get_next_row_from_data_table())) {
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
            if (OB_FAIL(check_lookup_row_cnt())) {
              LOG_WARN("failed to check table lookup", K(ret));
            } else {
              state_ = INDEX_SCAN;
            }
          } else {
            LOG_WARN("look up get next row failed", K(ret));
          }
        } else {
          got_next_row = true;
          ++lookup_row_cnt_;
          LOG_DEBUG("local index lookup get next row",  K(ret), K(lookup_row_cnt_), K(lookup_rowkey_cnt_),
                    "main table output", ROWEXPR2STR(get_eval_ctx(), get_output_expr()));
        }
        break;
      }
      case FINISHED: {
        ret = OB_ITER_END;
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected state", K(state_));
      }
    }
  } while (!got_next_row && OB_SUCC(ret));

  return ret;
}

int ObIndexLookupOpImpl::get_next_rows(int64_t &count, int64_t capacity)
{
  int ret = OB_SUCCESS;
  bool got_next_rows = false;
  int64_t simulate_batch_row_cnt = - EVENT_CALL(EventTable::EN_TABLE_LOOKUP_BATCH_ROW_COUNT);
  int64_t default_row_batch_cnt  = simulate_batch_row_cnt > 0 ? simulate_batch_row_cnt : default_batch_row_count_;
  LOG_DEBUG("simulate lookup row batch count", K(simulate_batch_row_cnt), K(default_row_batch_cnt));
  do {
    switch (state_) {
      case INDEX_SCAN: {
        int64_t rowkey_count = 0;
        lookup_rowkey_cnt_ = 0;
        lookup_row_cnt_ = 0;
        reset_lookup_state();
        while (OB_SUCC(ret) && !index_end_ && lookup_rowkey_cnt_ < default_row_batch_cnt) {
          do_clear_evaluated_flag();
          if (OB_FAIL(get_next_rows_from_index_table(rowkey_count, default_row_batch_cnt - lookup_rowkey_cnt_))) {
            if (OB_ITER_END != ret) {
              LOG_WARN("get next rows from index table failed", K(ret));
            } else {
              if (rowkey_count == 0) {
                index_end_ = true;
              }
              ret = OB_SUCCESS;
            }
          }
          if (OB_SUCC(ret) && rowkey_count > 0) {
            if (OB_FAIL(process_data_table_rowkeys(rowkey_count, nullptr))) {
              LOG_WARN("process data table rowkeys with das failed", K(ret));
            } else {
              lookup_rowkey_cnt_ += rowkey_count;
            }
          }
        }

        if (OB_SUCC(ret)) {
          if (lookup_rowkey_cnt_ > 0) {
            state_ = DO_LOOKUP;
          } else {
            state_ = FINISHED;
          }
        }
        break;
      }
      case DO_LOOKUP: {
        if (OB_FAIL(do_index_lookup())) {
          LOG_WARN("do index lookup failed", K(ret));
        } else {
          state_ = OUTPUT_ROWS;
        }
        break;
      }
      case OUTPUT_ROWS: {
        count = 0;
        if (OB_FAIL(get_next_rows_from_data_table(count, capacity))) {
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
            if (count > 0) {
              lookup_row_cnt_ += count;
              got_next_rows = true;
            } else {
              if (OB_FAIL(check_lookup_row_cnt())) {
                LOG_WARN("failed to check table lookup", K(ret));
              } else {
                state_ = INDEX_SCAN;
              }
            }
          } else {
            LOG_WARN("look up get next rows failed", K(ret));
          }
        } else {
          got_next_rows = true;
          lookup_row_cnt_ += count;
          const ObBitVector *skip = NULL;
          PRINT_VECTORIZED_ROWS(SQL, DEBUG, get_eval_ctx(), get_output_expr(), count, skip,
                                K(ret), K(lookup_row_cnt_), K(lookup_rowkey_cnt_));
        }
        break;
      }
      case FINISHED: {
        ret = OB_ITER_END;
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected state", K(state_));
      }
    }
  } while (!got_next_rows && OB_SUCC(ret));

  return ret;
}

int ObIndexLookupOpImpl::build_trans_datum(ObExpr *expr,
                                           ObEvalCtx *eval_ctx,
                                           ObIAllocator &alloc,
                                           ObDatum *&datum_ptr)
{
  int ret = OB_SUCCESS;
  datum_ptr = nullptr;
  if (OB_ISNULL(expr) || OB_ISNULL(eval_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(ret), K(expr), K(eval_ctx));
  }
  if (OB_SUCC(ret)) {
    void *buf = nullptr;
    ObDatum &col_datum = expr->locate_expr_datum(*eval_ctx);
    int64_t pos = sizeof(ObDatum);
    int64_t len = sizeof(ObDatum) + col_datum.len_;
    if (OB_ISNULL(buf = alloc.alloc(len))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate buffer failed", K(ret));
    } else if (FALSE_IT(datum_ptr = new (buf) ObDatum)) {
      // do nothing
    } else if (OB_FAIL(datum_ptr->deep_copy(col_datum, static_cast<char *>(buf), sizeof(ObDatum) + col_datum.len_, pos))) {
      LOG_WARN("failed to deep copy datum", K(ret), K(pos), K(len));
    }
  }

  return ret;
}

} // end namespace sql
} // end namespace oceanbase

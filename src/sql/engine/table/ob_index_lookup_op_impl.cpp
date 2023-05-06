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
    index_group_cnt_(1),
    lookup_group_cnt_(1),
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
        if (OB_FAIL(switch_index_table_and_rowkey_group_id())) {
          LOG_WARN("failed to switch index table and rowkey group id", K(ret));
        }
        int64_t start_group_idx = get_index_group_cnt() - 1;
        while (OB_SUCC(ret) && lookup_rowkey_cnt_ < default_row_batch_cnt) {
          do_clear_evaluated_flag();
          if (OB_FAIL(get_next_row_from_index_table())) {
            if (OB_ITER_END != ret) {
              LOG_WARN("get next row from index table failed", K(ret));
            } else {
              LOG_DEBUG("get next row from index table",K(ret), K(index_group_cnt_), K(lookup_rowkey_cnt_));
            }
          } else if (OB_FAIL(process_data_table_rowkey())) {
            LOG_WARN("process data table rowkey with das failed", K(ret));
          } else {
            ++lookup_rowkey_cnt_;
          }
        }
        if (OB_SUCC(ret) || OB_ITER_END == ret) {
          state_ = DO_LOOKUP;
          index_end_ = (OB_ITER_END == ret);
          ret = OB_SUCCESS;
          if (is_group_scan()) {
            if (OB_FAIL((init_group_range(start_group_idx, get_index_group_cnt())))) {
              LOG_WARN("failed to init group range",K(ret), K(start_group_idx), K(get_index_group_cnt()));
            }
          }
        }
        break;
      }
      case DO_LOOKUP: {
        lookup_row_cnt_ = 0;
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
            if (OB_FAIL(process_next_index_batch_for_row())) {
              LOG_WARN("failed to process next index batch for row", K(ret));
            }
          } else {
            LOG_WARN("look up get next row failed", K(ret));
          }
        } else {
          got_next_row = true;
          ++lookup_row_cnt_;
          LOG_DEBUG("got next row from table lookup",  K(ret), K(lookup_row_cnt_), K(lookup_rowkey_cnt_), K(lookup_group_cnt_), K(index_group_cnt_), "main table output", ROWEXPR2STR(get_eval_ctx(), get_output_expr()) );
        }
        break;
      }
      case FINISHED: {
        if (OB_SUCC(ret) || OB_ITER_END == ret) {
          ret = OB_ITER_END;
        }
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
  bool got_next_row = false;
  int64_t simulate_batch_row_cnt = - EVENT_CALL(EventTable::EN_TABLE_LOOKUP_BATCH_ROW_COUNT);
  int64_t default_row_batch_cnt  = simulate_batch_row_cnt > 0 ? simulate_batch_row_cnt : default_batch_row_count_;
  LOG_DEBUG("simulate lookup row batch count", K(simulate_batch_row_cnt), K(default_row_batch_cnt));
  do {
    switch (state_) {
      case INDEX_SCAN: {
        lookup_rowkey_cnt_ = 0;
        if (OB_FAIL(switch_index_table_and_rowkey_group_id())) {
          LOG_WARN("failed to switch index table and rowkey group id", K(ret));
        }
        int64_t start_group_idx = get_index_group_cnt() - 1;
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(do_index_table_scan_for_rows(capacity ,start_group_idx, default_row_batch_cnt))) {
          LOG_WARN("failed to do index table scan",K(ret));
        }
        break;
      }
      case DO_LOOKUP: {
        lookup_row_cnt_ = 0;
        if (OB_FAIL(do_index_lookup())) {
          LOG_WARN("do index lookup failed", K(ret));
        } else {
          LOG_DEBUG("do index lookup end", K(get_index_group_cnt()), K(ret));
          state_ = OUTPUT_ROWS;
        }
        break;
      }
      case OUTPUT_ROWS: {
        if (OB_FAIL(get_next_rows_from_data_table(count, capacity))) {
          if (OB_ITER_END == ret) {
            if (OB_FAIL(process_next_index_batch_for_rows(count))) {
              LOG_WARN("failed to process next index batch for rows", K(ret));
            }
          } else {
            LOG_WARN("look up get next row failed", K(ret));
          }
        } else {
          got_next_row = true;
          update_state_in_output_rows_state(count);
          PRINT_VECTORIZED_ROWS(SQL, DEBUG, get_eval_ctx(), get_output_expr(), count,
                                K(ret), K(lookup_row_cnt_), K(lookup_rowkey_cnt_),
                                K(lookup_group_cnt_), K(index_group_cnt_));
        }
        break;
      }
      case FINISHED: {
        update_states_in_finish_state();
        if (OB_SUCC(ret) || OB_ITER_END == ret) {
          ret = OB_ITER_END;
        }
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected state", K(state_));
      }
    }
  } while (!got_next_row && OB_SUCC(ret));
  if (lookup_type_ == GLOBAL_INDEX && OB_ITER_END == ret) {
    ret = OB_SUCCESS;
    update_states_after_finish_state();
  }
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

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
#include "sql/das/iter/ob_das_cache_lookup_iter.h"

namespace oceanbase
{
using namespace common;
namespace sql
{
int ObDASCacheLookupIter::inner_get_next_rows(int64_t &count, int64_t capacity)
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
        // TODO: @zyx439997 support the outputs of index scan as the project columns by the deep copy {
        bool need_accumulation = true;
        // }
        while (OB_SUCC(ret) && need_accumulation && !index_end_ && lookup_rowkey_cnt_ < default_row_batch_cnt) {
          storage_count = 0;
          index_capacity = std::min(capacity, std::min(max_size_, default_row_batch_cnt - lookup_rowkey_cnt_));
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
          } else {
            need_accumulation = false;
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
        if (OB_SUCC(ret) && OB_UNLIKELY(lookup_row_cnt_ != lookup_rowkey_cnt_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected lookup row count", K_(lookup_row_cnt), K_(lookup_rowkey_cnt), K(ret));
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

}  // namespace sql
}  // namespace oceanbase
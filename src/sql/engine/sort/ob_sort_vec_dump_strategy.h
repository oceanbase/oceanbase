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

#ifndef OCEANBASE_SQL_ENGINE_SORT_OB_SORT_VEC_DUMP_STRATEGY_H_
#define OCEANBASE_SQL_ENGINE_SORT_OB_SORT_VEC_DUMP_STRATEGY_H_

#include <utility>
#include "sql/engine/sort/ob_partition_topn_sort_vec_op.h"
#include "sql/engine/basic/ob_temp_row_store.h"
#include "sql/engine/basic/ob_compact_row.h"

namespace oceanbase
{
namespace sql
{

template <typename Compare, typename Store_Row, bool has_addon>
class PartitionTopnDumpStrategy {
public:
  PartitionTopnDumpStrategy(int64_t &cur_node_idx,
                            int64_t &row_idx,
                            ObPartitionTopNSortStrategy<Compare, Store_Row, has_addon> *part_topn_sort_strategy)
    : cur_node_idx_(cur_node_idx),
      row_idx_(row_idx),
      part_topn_sort_strategy_(part_topn_sort_strategy) {}

  int operator()(const Store_Row *&sk_row, const Store_Row *&addon_row) {
    int ret = OB_SUCCESS;
    if (OB_FAIL(part_topn_sort_strategy_->part_topn_node_next(cur_node_idx_, row_idx_, sk_row, addon_row))) {
      if (OB_ITER_END != ret) {
        SQL_ENG_LOG(WARN, "get row from part topn node failed", K(ret));
      }
    }
    return ret;
  }

private:
  int64_t &cur_node_idx_;
  int64_t &row_idx_;
  ObPartitionTopNSortStrategy<Compare, Store_Row, has_addon> *part_topn_sort_strategy_;
};

template <typename Compare, typename Store_Row, bool has_addon, typename IMMSHeapNextFunc>
class IMMSDumpStrategy {
public:
  IMMSDumpStrategy(IMMSHeapNextFunc &&imms_heap_next_func,
                   const RowMeta *sk_row_meta,
                   common::ObArray<Store_Row *> *sorted_dumped_rows_ptrs,
                   bool is_topn_sort,
                   bool is_topn_filter_enabled)
    : imms_heap_next_func_(std::forward<IMMSHeapNextFunc>(imms_heap_next_func)),
      sk_row_meta_(sk_row_meta),
      sorted_dumped_rows_ptrs_(sorted_dumped_rows_ptrs),
      is_topn_sort_(is_topn_sort),
      is_topn_filter_enabled_(is_topn_filter_enabled) {}

  int operator()(const Store_Row *&sk_row, const Store_Row *&addon_row) {
    int ret = OB_SUCCESS;
    if (OB_FAIL(imms_heap_next_func_(sk_row))) {
      if (OB_ITER_END != ret) {
        SQL_ENG_LOG(WARN, "get row from memory heap failed", K(ret));
      }
    } else if (has_addon) {
      addon_row = sk_row->get_addon_ptr(*sk_row_meta_);
    }
    if (OB_SUCC(ret) && is_topn_sort_ && is_topn_filter_enabled_) {
      sorted_dumped_rows_ptrs_->push_back(const_cast<Store_Row *>(sk_row));
    }
    return ret;
  }

private:
  IMMSHeapNextFunc imms_heap_next_func_;
  const RowMeta *sk_row_meta_;
  common::ObArray<Store_Row *> *sorted_dumped_rows_ptrs_;
  bool is_topn_sort_;
  bool is_topn_filter_enabled_;
};

template <typename Compare, typename Store_Row, bool has_addon>
class NormalDumpStrategy {
public:
  NormalDumpStrategy(common::ObIArray<Store_Row *> *rows,
                     common::ObIArray<Store_Row *> *ties_array,
                     const RowMeta *sk_row_meta)
    : row_pos_(0),
      ties_array_pos_(0),
      rows_(rows),
      ties_array_(ties_array),
      sk_row_meta_(sk_row_meta) {}

  int operator()(const Store_Row *&sk_row, const Store_Row *&addon_row) {
    int ret = OB_SUCCESS;
    if (row_pos_ >= rows_->count() && (nullptr == ties_array_ || ties_array_pos_ >= ties_array_->count())) {
      ret = OB_ITER_END;
    } else if (row_pos_ < rows_->count()) {
      sk_row = rows_->at(row_pos_);
      if (has_addon) {
        addon_row = sk_row->get_addon_ptr(*sk_row_meta_);
      }
      row_pos_ += 1;
    } else {
      sk_row = ties_array_->at(ties_array_pos_);
      if (has_addon) {
        addon_row = sk_row->get_addon_ptr(*sk_row_meta_);
      }
      ties_array_pos_ += 1;
    }
    return ret;
  }

private:
  int64_t row_pos_;
  int64_t ties_array_pos_;
  common::ObIArray<Store_Row *> *rows_;
  common::ObIArray<Store_Row *> *ties_array_;
  const RowMeta *sk_row_meta_;
};

} // namespace sql
} // namespace oceanbase

#endif /* OCEANBASE_SQL_ENGINE_SORT_OB_SORT_VEC_DUMP_STRATEGY_H_ */

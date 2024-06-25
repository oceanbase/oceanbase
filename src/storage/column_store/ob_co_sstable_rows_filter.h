/**
 * Copyright (c) 2022 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */
#ifndef OB_STORAGE_COLUMN_STORE_OB_CO_SSTABLE_ROWS_FILTER_H_
#define OB_STORAGE_COLUMN_STORE_OB_CO_SSTABLE_ROWS_FILTER_H_
#include "lib/container/ob_se_array.h"
#include "sql/engine/basic/ob_pushdown_filter.h"
#include "storage/access/ob_sample_filter.h"
#include "storage/access/ob_table_access_param.h"
#include "ob_i_cg_iterator.h"
#include "ob_cg_iter_param_pool.h"
#include "ob_cg_bitmap.h"

namespace oceanbase
{
namespace storage
{
struct ObTableIterParam;
struct ObTableAccessContext;
class ObCOSSTableV2;

class ObCOSSTableRowsFilter
{
public:
  static constexpr uint32_t MAX_NUM_OF_CG_ITER_TO_LOCATE_IN_ADVANCE = 2;
  static constexpr uint8_t filter_tree_merge_status[sql::ObCommonFilterTreeStatus::MAX_STATUS][sql::ObCommonFilterTreeStatus::MAX_STATUS] =
  { {0, 0, 0, 0}, \
    {0, 1, 0, 0}, \
    {0, 0, 2, 3}, \
    {0, 0, 3, 3}
  };

public:
  ObCOSSTableRowsFilter();
  ~ObCOSSTableRowsFilter();
  int init(
      const ObTableIterParam &param,
      ObTableAccessContext &context,
      ObITable *table);
  int switch_context(
      const ObTableIterParam &param,
      ObTableAccessContext &context,
      ObITable *table,
      const bool col_cnt_changed);
  void reset();
  void reuse();
  int apply(const ObCSRange &range);
  int prepare_apply(const ObCSRange &range);
  const ObCGBitmap* get_result_bitmap();
  static int switch_context_for_cg_iter(
      const bool is_projector,
      const bool project_single_row,
      const bool without_filter,
      ObCOSSTableV2 *co_sstable,
      ObTableAccessContext &context,
      common::ObIArray<ObTableIterParam*> &cg_params,
      const bool col_cnt_changed,
      ObICGIterator *&cg_iter);
  inline bool can_continuous_filter() const { return can_continuous_filter_; }
  TO_STRING_KV(K_(is_inited), K_(subtree_filter_iter_to_locate), K_(batch_size),
      KPC_(iter_param), KP_(access_ctx), KP_(co_sstable), K_(filter), K_(filter_iters),
      K_(iter_filter_node), K_(bitmap_buffer), K_(pd_filter_info));

private:
  int apply_filter(
      sql::ObPushdownFilterExecutor *parent,
      sql::ObPushdownFilterExecutor *filter,
      const ObCSRange &range,
      const uint32_t depth);
  int post_apply_filter(
      sql::ObPushdownFilterExecutor &filter,
      ObCGBitmap &result,
      const ObCGBitmap &child_result,
      bool &is_skip);
  int push_cg_iter(sql::ObPushdownFilterExecutor *filter);
  int construct_cg_iter_params(
      const sql::ObPushdownFilterExecutor *filter,
      common::ObIArray<ObTableIterParam*> &iter_params);
  int rewrite_filter(uint32 &depth);
  int judge_whether_use_common_cg_iter(
      sql::ObPushdownFilterExecutor *filter);
  int transform_filter_tree(
      sql::ObPushdownFilterExecutor &filter);
  int find_common_sub_filter_tree(
      sql::ObPushdownFilterExecutor &filter,
      ObIArray<uint32_t> &filter_indexes,
      common::ObIArray<uint32_t> &common_cg_ids,
      common::ObIArray<sql::ObExpr *> &common_cg_exprs,
      const int64_t base_filter_idx);
  int rewrite_filter_tree(
      sql::ObPushdownFilterExecutor *filter,
      uint32_t &depth);
  int init_co_sstable(ObITable *table);
  int try_locating_cg_iter(
      const int64_t iter_idx_to_filter_next,
      const ObCSRange &range);
  int prepare_bitmap_buffer(
      const ObCSRange &range,
      const uint32_t buffer_idx,
      const sql::ObPushdownFilterExecutor *parent,
      sql::ObPushdownFilterExecutor &filter,
      ObCGBitmap *&result);
  int init_bitmap_buffer(uint32_t bitmap_buffer_count);
  void adjust_batch_size();
  OB_INLINE ObCGBitmap* get_child_bitmap(uint32_t depth);
  static int assign_common_col_groups(
      const sql::ObPushdownFilterExecutor *filter,
      const sql::ObCommonFilterTreeStatus prev_status,
      common::ObIArray<uint32_t> &common_cg_ids,
      common::ObIArray<sql::ObExpr *> &common_cg_exprs,
      bool &is_common);
  static sql::ObCommonFilterTreeStatus merge_common_filter_tree_status(
      const sql::ObCommonFilterTreeStatus status_one,
      const sql::ObCommonFilterTreeStatus status_two);
  static bool is_common_filter_tree_status(
      const sql::ObCommonFilterTreeStatus status_one,
      const sql::ObCommonFilterTreeStatus status_two);
  static void set_status_of_filter_tree(sql::ObPushdownFilterExecutor *filter);
  static void clear_filter_state(sql::ObPushdownFilterExecutor *filter);
  int filter_tree_can_continuous_filter(sql::ObPushdownFilterExecutor *filter,
                                        bool &can_continuous_filter) const;
private:
  bool is_inited_;
  bool prepared_;
  uint32_t subtree_filter_iter_to_locate_;
  uint32_t subtree_filter_iter_to_filter_;
  uint32_t batch_size_;
  const ObTableIterParam *iter_param_;  // row store iter param
  ObTableAccessContext* access_ctx_;
  ObCOSSTableV2* co_sstable_;
  common::ObIAllocator *allocator_;
  sql::ObPushdownFilterExecutor *filter_;
  ObSEArray<ObICGIterator*, 4> filter_iters_;
  ObSEArray<sql::ObPushdownFilterExecutor*, 4> iter_filter_node_;
  ObSEArray<ObCGBitmap*, 4> bitmap_buffer_;
  sql::PushdownFilterInfo pd_filter_info_;
  bool can_continuous_filter_;
};
}
}

#endif

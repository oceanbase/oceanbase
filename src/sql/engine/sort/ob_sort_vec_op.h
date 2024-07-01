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

#ifndef OCEANBASE_SQL_ENGINE_SORT_SORT_VEC_OP_H_
#define OCEANBASE_SQL_ENGINE_SORT_SORT_VEC_OP_H_

#include "common/object/ob_object.h"
#include "share/datum/ob_datum_funcs.h"
#include "sql/engine/basic/ob_temp_row_store.h"
#include "sql/engine/ob_operator.h"
#include "sql/engine/sort/ob_sort_basic_info.h"
#include "sql/engine/sort/ob_sort_vec_op_provider.h"
#include "sql/engine/px/p2p_datahub/ob_pushdown_topn_filter_msg.h"

namespace oceanbase
{
namespace sql
{

class ObSortVecSpec : public ObOpSpec
{
  OB_UNIS_VERSION_V(1);

public:
  ObSortVecSpec(common::ObIAllocator &alloc, const ObPhyOperatorType type);

  inline bool enable_pd_topn_filter() const { return pd_topn_filter_info_.enabled_; }
  INHERIT_TO_STRING_KV("op_spec", ObOpSpec, K_(topn_expr), K_(topk_limit_expr),
                       K_(topk_offset_expr), K_(prefix_pos), K_(minimum_row_count),
                       K_(topk_precision), K_(prefix_pos), K_(is_local_merge_sort),
                       K_(prescan_enabled), K_(enable_encode_sortkey_opt), K_(has_addon),
                       K_(part_cnt), K_(compress_type), K_(pd_topn_filter_info));

public:
  ObExpr *topn_expr_;
  ObExpr *topk_limit_expr_;
  ObExpr *topk_offset_expr_;
  // sort exprs + output_exprs
  ExprFixedArray sk_exprs_;
  ExprFixedArray addon_exprs_;
  ObSortCollations sk_collations_;
  ObSortCollations addon_collations_;
  int64_t minimum_row_count_;
  int64_t topk_precision_;
  // for prefix_sort
  int64_t prefix_pos_;
  bool is_local_merge_sort_;
  bool is_fetch_with_ties_;
  bool prescan_enabled_;
  bool enable_encode_sortkey_opt_;
  bool has_addon_;
  // if use, all_exprs_ is : hash(part_by) + part_by + order_by.
  int64_t part_cnt_;
  // pushdown topn filter: pushdown the heap top data to table scan for filtering out data early.
  ObPushDownTopNFilterInfo pd_topn_filter_info_;
};

class ObSortVecOp : public ObOperator
{
public:
  static const int64_t SORTKEY_STORE_SEPARATELY_THRESHOLD = 8;
  ObSortVecOp(ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input);
  virtual int inner_rescan() override;
  virtual int inner_get_next_row() override
  {
    return common::OB_NOT_IMPLEMENT;
  }
  virtual int inner_get_next_batch(const int64_t max_row_cnt) override;
  virtual void destroy() override;
  virtual int inner_close() override;

  int64_t get_sort_row_count() const
  {
    return sort_row_count_;
  }

private:
  void reset();
  int sort_component_next_batch(const int64_t max_cnt);
  int get_int_value(const ObExpr *in_val, int64_t &out_val);
  int get_topn_count(int64_t &topn_cnt);
  int process_sort_batch();
  int scan_all_then_sort_batch();
  int init_prescan_row_store();
  int add_batch_prescan_store(const ObBatchRows &input_brs);
  int finish_add_prescan_store();
  int get_next_batch_prescan_store(const int64_t max_rows, int64_t &read_rows,
                                   const ObCompactRow **sk_stored_rows,
                                   const ObCompactRow **addon_stored_rows = nullptr);
  int init_sort(int64_t tenant_id, int64_t row_count, int64_t topn_cnt = INT64_MAX);
  int init_temp_row_store(const common::ObIArray<ObExpr *> &exprs, const int64_t batch_size,
                          const ObMemAttr &mem_attr, const bool is_sort_key, ObCompressorType compress_type,
                          ObTempRowStore &row_store);
  void reset_pd_topn_filter_expr_ctx();

private:
  ObSortVecOpProvider sort_op_provider_;
  int64_t sort_row_count_;
  bool is_first_;
  int64_t ret_row_count_;
  ObTempRowStore sk_row_store_;
  ObTempRowStore addon_row_store_;
  ObTempRowStore::Iterator sk_row_iter_;
  ObTempRowStore::Iterator addon_row_iter_;
  ObTempRowStore::BlockHolder blk_holder_;
};

} // end namespace sql
} // end namespace oceanbase

#endif /* OCEANBASE_SQL_ENGINE_SORT_SORT_VEC_OP_H_ */

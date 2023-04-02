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

#ifndef OCEANBASE_SQL_ENGINE_SORT_SORT_OP_H_
#define OCEANBASE_SQL_ENGINE_SORT_SORT_OP_H_

#include "sql/engine/ob_operator.h"
#include "sql/engine/sort/ob_sort_op_impl.h"
#include "common/object/ob_object.h"
#include "share/datum/ob_datum_funcs.h"
#include "sql/engine/sort/ob_sort_basic_info.h"

namespace oceanbase
{
namespace sql
{

class ObSortSpec : public ObOpSpec
{
OB_UNIS_VERSION_V(1);
public:
  ObSortSpec(common::ObIAllocator &alloc, const ObPhyOperatorType type);

  INHERIT_TO_STRING_KV("op_spec", ObOpSpec,
    K_(topn_expr), K_(topk_limit_expr), K_(topk_offset_expr), K_(prefix_pos),
    K_(minimum_row_count), K_(topk_precision), K_(prefix_pos), K_(is_local_merge_sort),
    K_(prescan_enabled), K_(enable_encode_sortkey_opt), K_(part_cnt));
public:
  ObExpr *topn_expr_;
  ObExpr *topk_limit_expr_;
  ObExpr *topk_offset_expr_;
  // sort exprs + output_exprs
  // 理论上这里应该是包含上层Operator需要的所有行
  // 因为如果不包含，则上层就无法得到对应列的正确值
  // 其实是所有算子都应该这样，只要有自产生数据逻辑，本质上必须包含上层operator所有Expr
  // 否则拿到的可能就跨Operator了，则数据可能就完全乱掉了
  ExprFixedArray all_exprs_;
  ObSortCollations sort_collations_;
  ObSortFuncs sort_cmp_funs_;
  int64_t minimum_row_count_;
  int64_t topk_precision_;
  // for prefix_sort
  int64_t prefix_pos_;
  bool is_local_merge_sort_;
  bool is_fetch_with_ties_;
  bool prescan_enabled_;
  bool enable_encode_sortkey_opt_;
  // if use, all_exprs_ is : hash(part_by) + part_by + order_by.
  int64_t part_cnt_;
};

class ObSortOp : public ObOperator
{
public:
  ObSortOp(ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input);

  virtual int inner_open() override;
  virtual int inner_rescan() override;
  virtual int inner_get_next_row() override;
  virtual int inner_get_next_batch(const int64_t max_row_cnt) override;
  virtual void destroy() override;
  virtual int inner_close() override;

  int64_t get_sort_row_count() const { return sort_row_count_; }
private:
  void reset();
  template <typename T>
  int sort_component_next(T &component)
  {
    return component.get_next_row(MY_SPEC.all_exprs_);
  }

  int sort_impl_next()
  {
    return sort_component_next(sort_impl_);
  }

  int prefix_sort_impl_next()
  {
    return sort_component_next(prefix_sort_impl_);
  }

  template <typename T>
  int sort_component_next_batch(T &component, const int64_t max_cnt)
  {
    int64_t read_rows = 0;
    int ret = component.get_next_batch(MY_SPEC.all_exprs_, max_cnt, read_rows);
    brs_.size_ = read_rows;
    if (common::OB_ITER_END == ret) {
      ret = OB_SUCCESS;
      brs_.size_ = 0;
      brs_.end_ = true;
    }
    return ret;
  }

  int sort_impl_next_batch(const int64_t max_cnt)
  {
    return sort_component_next_batch(sort_impl_, max_cnt);
  }

  int prefix_sort_impl_next_batch(const int64_t max_cnt)
  {
    return sort_component_next_batch(prefix_sort_impl_, max_cnt);
  }

  int get_int_value(const ObExpr *in_val, int64_t &out_val);
  int get_topn_count(int64_t &topn_cnt);
  int process_sort();
  int process_sort_batch();
  int scan_all_then_sort();
  int scan_all_then_sort_batch();
  int init_prefix_sort(int64_t tenant_id,
                       int64_t row_count,
                       bool is_batch,
                       int64_t topn_cnt = INT64_MAX);
  int init_sort(int64_t tenant_id,
                int64_t row_count,
                bool is_batch,
                int64_t topn_cnt = INT64_MAX);
private:
  ObSortOpImpl sort_impl_;
  ObPrefixSortImpl prefix_sort_impl_;
  int (ObSortOp::*read_func_)();
  int (ObSortOp::*read_batch_func_)(const int64_t max_cnt);
  int64_t sort_row_count_;
  bool is_first_;
  int64_t ret_row_count_;
  bool iter_end_;
};

} // end namespace sql
} // end namespace oceanbase

#endif /* OCEANBASE_SQL_ENGINE_SORT_SORT_OP_H_ */



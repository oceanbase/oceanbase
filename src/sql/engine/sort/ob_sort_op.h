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

namespace oceanbase {
namespace sql {

class ObSortSpec : public ObOpSpec {
  OB_UNIS_VERSION_V(1);

public:
  ObSortSpec(common::ObIAllocator& alloc, const ObPhyOperatorType type);

  INHERIT_TO_STRING_KV("op_spec", ObOpSpec, K_(topn_expr), K_(topk_limit_expr), K_(topk_offset_expr), K_(prefix_pos),
      K_(minimum_row_count), K_(topk_precision), K_(prefix_pos), K_(is_local_merge_sort));

public:
  ObExpr* topn_expr_;
  ObExpr* topk_limit_expr_;
  ObExpr* topk_offset_expr_;
  // sort exprs + output_exprs
  // In theory, this should contain all the rows needed by the upper operator.
  // This mainly because if it is not included, the upper layer will not
  // be able to get the correct value of the corresponding column. In fact,
  // all operators should be like this. As long as there is self-generated
  // data logic, it must essentially include all Expr of the upper operator
  // Otherwise, the obtained may cross the Operator, and the data may be completely messed up.
  ExprFixedArray all_exprs_;
  ObSortCollations sort_collations_;
  ObSortFuncs sort_cmp_funs_;
  int64_t minimum_row_count_;
  int64_t topk_precision_;
  // for prefix_sort
  int64_t prefix_pos_;
  bool is_local_merge_sort_;
  bool is_fetch_with_ties_;
};

class ObSortOp : public ObOperator {
public:
  ObSortOp(ObExecContext& exec_ctx, const ObOpSpec& spec, ObOpInput* input);

  virtual int inner_open() override;
  virtual int rescan() override;
  virtual int inner_get_next_row() override;
  virtual void destroy() override;
  virtual int inner_close() override;

  int64_t get_sort_row_count() const
  {
    return sort_row_count_;
  }

private:
  void reset();
  int topn_sort_next();
  template <typename T>
  int sort_component_next(T& component)
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
  int get_int_value(const ObExpr* in_val, int64_t& out_val);
  int get_topn_count(int64_t& topn_cnt);
  int process_sort();

private:
  ObSortOpImpl sort_impl_;
  ObPrefixSortImpl prefix_sort_impl_;
  ObInMemoryTopnSortImpl topn_sort_;
  int (ObSortOp::*read_func_)();
  int64_t sort_row_count_;
  bool is_first_;
  int64_t ret_row_count_;
  bool iter_end_;
};

}  // end namespace sql
}  // end namespace oceanbase

#endif /* OCEANBASE_SQL_ENGINE_SORT_SORT_OP_H_ */

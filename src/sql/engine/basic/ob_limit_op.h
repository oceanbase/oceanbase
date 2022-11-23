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

#ifndef OCEANBASE_BASIC_OB_LIMIT_OP_H_
#define OCEANBASE_BASIC_OB_LIMIT_OP_H_

#include "sql/engine/ob_operator.h"
#include "sql/engine/basic/ob_chunk_datum_store.h"

namespace oceanbase
{
namespace sql
{

class ObLimitSpec : public ObOpSpec
{
  OB_UNIS_VERSION_V(1);
public:
  ObLimitSpec(common::ObIAllocator &alloc, const ObPhyOperatorType type);

  INHERIT_TO_STRING_KV("op_spec", ObOpSpec,
                       K_(limit_expr), K_(offset_expr), K_(percent_expr),
                       K_(calc_found_rows), K_(is_top_limit), K_(is_fetch_with_ties),
                       K_(sort_columns));

  ObExpr *limit_expr_;
  ObExpr *offset_expr_;
  ObExpr *percent_expr_;
  // mysql SQL_CALC_FOUND_ROWS query modifier, count found rows after reach limit.
  bool calc_found_rows_;
  bool is_top_limit_;
  bool is_fetch_with_ties_;

  ExprFixedArray sort_columns_;
};

class ObLimitOp : public ObOperator
{
public:
  ObLimitOp(ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input);

  virtual int inner_open() override;
  virtual int inner_rescan() override;

  virtual int inner_get_next_row() override;
  virtual int inner_get_next_batch(const int64_t max_row_cnt) override;

  virtual void destroy() override { ObOperator::destroy(); }

  static int get_int_val(ObExpr *expr, ObEvalCtx &eval_ctx, int64_t &val, bool &is_null_value);
  static int get_double_val(ObExpr *expr, ObEvalCtx &eval_ctx, double &val);

private:
  int convert_limit_percent();
  int is_row_order_by_item_value_equal(bool &is_equal);
  // batch version for routine is_row_order_by_item_value_equal()
  int compare_value_in_batch(bool &is_iterator_end, const ObBitVector &skip,
                             const int64_t batch_size,
                             uint32_t &row_count_match);
  OB_INLINE int64_t find_last_available_row_cnt(const ObBitVector &skip, const int64_t batch_size)
  {
    int64_t row_num = 0;
    for (int64_t i = (batch_size - 1); i >= 0; i--) {
      if (!skip.at(i)) {
        row_num = i;
        break;
      }
    }
    return row_num;
  }

private:
  int64_t limit_;
  int64_t offset_;
  int64_t input_cnt_;
  int64_t output_cnt_;
  int64_t total_cnt_;
  bool is_percent_first_;

  ObChunkDatumStore::LastStoredRow pre_sort_columns_;
};

} // end namespace sql
} // end namespace oceanbase

#endif // OCEANBASE_BASIC_OB_LIMIT_OP_H_

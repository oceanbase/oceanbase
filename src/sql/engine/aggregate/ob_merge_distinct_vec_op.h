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
#ifndef OCEANBASE_SRC_SQL_ENGINE_AGGREGATE_OB_MERGE_DISTINCT_VEC_OP_H_
#define OCEANBASE_SRC_SQL_ENGINE_AGGREGATE_OB_MERGE_DISTINCT_VEC_OP_H_

#include "sql/engine/ob_operator.h"
#include "sql/engine/aggregate/ob_distinct_op.h"
#include "sql/engine/basic/ob_chunk_datum_store.h"


namespace oceanbase
{
namespace sql
{
class ObCompactRow;
class RowMeta;

class ObMergeDistinctVecSpec : public ObDistinctSpec
{
  OB_UNIS_VERSION_V(1);
public:
  ObMergeDistinctVecSpec(common::ObIAllocator &alloc, const ObPhyOperatorType type);
};

class ObMergeDistinctVecOp : public ObOperator
{
public:
  ObMergeDistinctVecOp(ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input);

  virtual int inner_open() override;
  virtual int inner_close() override;
  virtual int inner_rescan() override;
  virtual int inner_get_next_row() override { return common::OB_NOT_IMPLEMENT; }
  virtual int inner_get_next_batch(const int64_t max_row_cnt);
  virtual void destroy() override;

  template<typename InputVec, bool ALL_ROWS_ACTIVE, bool FIRST_COL, bool HAS_NULL>
  int compare_in_column(InputVec * vec, int64_t first_no_skip_idx,
                      const ObBatchRows *child_brs, int64_t col_idx, int64_t &last_idx);

  template<typename InputVec>
  int compare_in_column_with_format(InputVec *vec, const ObBatchRows *child_brs, int64_t first_no_skip_idx,
                                    int64_t col_idx, int64_t &last_idx);

  typedef ObFixedLengthVector<int64_t, VectorBasicOp<VEC_TC_INTEGER>> FixedLengthVectorBigInt;
  typedef ObDiscreteVector<VectorBasicOp<VEC_TC_STRING>> DiscreteVectorString;

  class Compare
  {
  public:
    Compare() : eval_ctx_(nullptr), cmp_funcs_(nullptr), ret_code_(common::OB_SUCCESS)
    {}

    int init(ObEvalCtx *eval_ctx, const common::ObIArray<ObCmpFunc> *cmp_funcs);
    int equal_in_row(const common::ObIArray<ObExpr*> *set_exprs,
                        const sql::LastCompactRow *r,
                        const int64_t curr_idx,
                        bool &equal);
    ObEvalCtx *eval_ctx_;
    const common::ObIArray<ObCmpFunc> *cmp_funcs_;
    int ret_code_;
  };
  int deduplicate_for_batch(bool has_last, const ObBatchRows *child_brs);
  bool first_got_row_; // 是否第一次拿数据
  common::ObArenaAllocator alloc_;
  LastCompactRow last_row_;
  ObBitVector *out_;
  Compare cmp_;
};

} // end sql
} // end oceanabse
#endif // OCEANBASE_SRC_SQL_ENGINE_AGGREGATE_OB_MERGE_DISTINCT_VEC_OP_H_
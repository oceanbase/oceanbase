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

#ifndef OCEANBASE_BASIC_OB_SET_OB_MERGE_SET_VEC_OP_H_
#define OCEANBASE_BASIC_OB_SET_OB_MERGE_SET_VEC_OP_H_

#include "sql/engine/set/ob_set_op.h"
#include "share/datum/ob_datum_funcs.h"
#include "sql/engine/basic/ob_chunk_datum_store.h"

namespace oceanbase
{
namespace sql
{

class ObMergeSetVecSpec : public ObSetSpec
{
OB_UNIS_VERSION_V(1);
public:
  ObMergeSetVecSpec(common::ObIAllocator &alloc, const ObPhyOperatorType type);
};

class ObMergeSetVecOp : public ObOperator
{
public:
  ObMergeSetVecOp(ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input);

  virtual int inner_open() override;
  virtual int inner_close() override;
  virtual int inner_rescan() override;
  virtual void destroy() override;
  class Compare
  {
  public:
    Compare() : sort_collations_(nullptr), cmp_funcs_(nullptr), ret_code_(common::OB_SUCCESS)
    {}
    int init(const common::ObIArray<ObSortFieldCollation> *sort_collations,
      const common::ObIArray<common::ObCmpFunc> *cmp_funcs);
    int operator() (const common::ObIArray<ObExpr*> &l,
                    const common::ObIArray<ObExpr*> &r,
                    const int64_t l_idx,
                    const int64_t r_idx,
                    ObEvalCtx &eval_ctx,
                    int &cmp);
    int operator() (const ObCompactRow &l_store_rows,
                    const RowMeta &meta,
                    const common::ObIArray<ObExpr*> &r,
                    const int64_t r_idx,
                    ObEvalCtx &eval_ctx,
                    int &cmp);
    const common::ObIArray<ObSortFieldCollation> *sort_collations_;
    const common::ObIArray<common::ObCmpFunc> *cmp_funcs_;
    int ret_code_;
  };
protected:
  int convert_batch(const common::ObIArray<ObExpr*> &src_exprs,
                    const common::ObIArray<ObExpr*> &dst_exprs,
                    ObBatchRows &brs,
                    bool is_union_all = false /* other cases can filter rows*/);
  bool get_need_skip_init_row() const { return need_skip_init_row_; }
  void set_need_skip_init_row(bool need_skip_init_row)
  { need_skip_init_row_ = need_skip_init_row; }
  //locate next valid left rows, do strict disitnct in it
  int locate_next_left_inside(ObOperator &child_op, const int64_t last_idx,
                              const ObBatchRows &row_brs, int64_t &curr_idx);
  //locate next valid right rows, simply move to next, if a batch is end, get next batch
  int locate_next_right(ObOperator &child_op, const int64_t batch_size,
                        const ObBatchRows *&child_brs, int64_t &curr_idx);

  int distinct_for_batch(ObOperator &child_op, const ObBatchRows &row_brs, bool &is_first,
                         const common::ObIArray<ObExpr*> &compare_expr,
                         const int64_t compare_idx,
                         ObBatchRows &result_brs);
  
  template<typename InputVec, bool ALL_ROWS_ACTIVE, bool FIRST_COL, bool HAS_NULL>
  int compare_in_column(InputVec * vec, int64_t first_no_skip_idx, const ObBatchRows *child_brs,
                        int64_t &last_idx, const sql::ObExpr &col_expr, ObBatchRows &result_brs);

  template<typename InputVec> 
  int compare_in_column_with_format(InputVec *vec, const ObBatchRows *child_brs, int64_t first_no_skip_idx, 
                                    int64_t col_idx, int64_t &last_idx, const sql::ObExpr &col_expr, 
                                    ObBatchRows &result_brs);

  typedef ObFixedLengthVector<int64_t, VectorBasicOp<VEC_TC_INTEGER>> FixedLengthVectorBigInt;
  typedef ObDiscreteVector<VectorBasicOp<VEC_TC_STRING>> DiscreteVectorString;

protected:
  common::ObArenaAllocator alloc_;
  Compare cmp_;
  bool need_skip_init_row_; //是否需要跳过和最初的 last_output_row_ 比较; false: 不需要; true: 需要;
                            //目前仅针对 merge except 和 merge intersect 置为TRUE, 因为无法区分 last_output_row_
                            //是来自初始化时的全NULL or 左侧child的全NULL, see bug
  int64_t last_row_idx_;
  bool use_last_row_;
  // for vec2.0
  LastCompactRow last_row_;
};

} // end namespace sql
} // end namespace oceanbase

#endif // OCEANBASE_BASIC_OB_SET_OB_MERGE_SET_VEC_OP_H_

/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OB_EXPR_LOCAL_DYNAMIC_FILTER_H_
#define OB_EXPR_LOCAL_DYNAMIC_FILTER_H_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{

class ObDynamicFilterExecutor;

class ObExprLocalDynamicFilterContext final: public ObExprOperatorCtx
{
public:
  ObExprLocalDynamicFilterContext()
      : ObExprOperatorCtx(), is_inited_(false), do_local_dynamic_filter_(false), hash_set_()
  {}
  virtual ~ObExprLocalDynamicFilterContext() { hash_set_.destroy(); }

public:
  int init_params(const common::ObIArray<ObDatum> &params);
  int is_filtered(const ObDatum &datum, bool &is_filtered);
  inline bool need_reset_in_rescan() override final { return true; }
  inline bool dynamic_disable() override final { return false; }
  inline void set_do_local_dynamic_filter(bool do_local_dynamic_filter)
  { do_local_dynamic_filter_ = do_local_dynamic_filter; }
  inline bool do_local_dynamic_filter() const { return do_local_dynamic_filter_; }

  // local dynamic filter will not be updated for now
  inline bool is_data_version_updated(int64_t old_version) override { return false; }

private:
  bool is_inited_;
  bool do_local_dynamic_filter_;
  hash::ObHashSet<uint64_t, hash::NoPthreadDefendMode> hash_set_;
};

// unlike previous dynamic filters such as join filter, local dynamic filter must be executed locally.
class ObExprLocalDynamicFilter : public ObExprOperator
{
public:
  explicit ObExprLocalDynamicFilter(common::ObIAllocator &alloc)
      : ObExprOperator(alloc, T_OP_LOCAL_DYNAMIC_FILTER, "LOCAL_DYNAMIC_FILTER", MORE_THAN_ZERO,
                       VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
  {}
  ~ObExprLocalDynamicFilter()
  {}
  int calc_result_typeN(ObExprResType &type, ObExprResType *types, int64_t param_num,
                        common::ObExprTypeCtx &type_ctx) const override;
  int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr, ObExpr &rt_expr) const override;

  inline bool need_rt_ctx() const override { return true; }

public:
  typedef common::ObSEArray<common::ObDatum, 4> LocalDynamicFilterParams;
  static int eval_local_dynamic_filter(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  static int eval_local_dynamic_filter_batch(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip,
                                            const int64_t batch_size);

  static int prepare_storage_white_filter_data(const ObExpr &expr,
                                              ObDynamicFilterExecutor &dynamic_filter,
                                              ObEvalCtx &eval_ctx, LocalDynamicFilterParams &params,
                                              bool &is_data_prepared);

  static int update_storage_white_filter_data(const ObExpr &expr,
                                              ObDynamicFilterExecutor &dynamic_filter,
                                              ObEvalCtx &eval_ctx, LocalDynamicFilterParams &params,
                                              bool &is_update);
  inline static int pk_increment_hash_func(const common::ObDatum &datum, const uint64_t seed, uint64_t &res);
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprLocalDynamicFilter);
};

} // end namespace sql
} // end namespace oceanbase

#endif /* OB_EXPR_LOCAL_DYNAMIC_FILTER_H_ */

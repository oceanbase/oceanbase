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

#ifndef OCEANBASE_SQL_ENGINE_BASIC_OB_VALUES_TABLE_ACCESS_OP_
#define OCEANBASE_SQL_ENGINE_BASIC_OB_VALUES_TABLE_ACCESS_OP_

#include "sql/engine/ob_operator.h"
#include "sql/engine/expr/ob_datum_cast.h"

namespace oceanbase
{
namespace sql
{
class ObPhyOpSeriCtx;

class ObValuesTableAccessSpec : public ObOpSpec
{
  OB_UNIS_VERSION_V(1);
public:
  ObValuesTableAccessSpec(common::ObIAllocator &alloc, const ObPhyOperatorType type)
    : ObOpSpec(alloc, type), access_type_(ObValuesTableDef::ACCESS_EXPR), column_exprs_(alloc),
      value_exprs_(alloc), start_param_idx_(-1), end_param_idx_(-1), obj_params_(alloc) {}
  INHERIT_TO_STRING_KV("op_spec", ObOpSpec, K_(access_type));
  ObValuesTableDef::TableAccessType access_type_;
  common::ObFixedArray<ObExpr *, common::ObIAllocator> column_exprs_;
  common::ObFixedArray<ObExpr *, common::ObIAllocator> value_exprs_;
  int64_t start_param_idx_;
  int64_t end_param_idx_;
  common::ObFixedArray<ObObj, common::ObIAllocator> obj_params_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObValuesTableAccessSpec);
};

class ObValuesTableAccessOp : public ObOperator
{
public:
  ObValuesTableAccessOp(ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input);
  virtual int inner_open() override;
  virtual int inner_rescan() override;
  virtual int switch_iterator() override;
  virtual int inner_get_next_row() override;
  virtual int inner_close() override;
  virtual void destroy() override { ObOperator::destroy(); }
private:
  int calc_next_row();
  int calc_datum_from_expr(const int64_t col_idx,
                           const int64_t row_idx,
                           ObExpr *src_expr,
                           ObExpr *dst_expr);
  int calc_datum_from_param(const ObObj &src_obj, ObExpr *dst_expr);
  void update_src_meta(const ObObjMeta &src_obj_meta,
                       const ObAccuracy &src_obj_acc,
                       ObDatumMeta &src_meta);
  int get_real_src_obj_type(const int64_t row_idx,
                            ObExpr &src_expr,
                            ObDatumMeta &src_meta,
                            ObObjMeta &src_obj_meta);
private:
  DISALLOW_COPY_AND_ASSIGN(ObValuesTableAccessOp);

private:
  ObDatumCaster datum_caster_;
  common::ObCastMode cm_;
  int64_t row_idx_;
  int64_t row_cnt_;
};

} // end namespace sql
} // end namespace oceanbase
#endif

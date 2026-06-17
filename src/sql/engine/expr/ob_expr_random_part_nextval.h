/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef _OB_EXPR_RANDOM_EXPR_NEXTVAL_H
#define _OB_EXPR_RANDOM_EXPR_NEXTVAL_H

#include "sql/engine/expr/ob_expr_operator.h"
#include "sql/engine/expr/ob_expr_autoinc_nextval.h"
#include "share/ob_autoincrement_service.h"

namespace oceanbase
{
namespace share
{
namespace schema
{
class ObTableSchema;
class ObSchemaGetterGuard;
}
}
namespace observer
{
struct ObTableLoadParam;
}
namespace sql
{
class ObPhysicalPlanCtx;

enum class ObRandomPartSeqUsedOutAction
{
  TRY_ADD_PART_AND_RETRY_IF_NO_ACTIVE = 0,
  ERROR_IF_NO_ACTIVE = 1,
  ADD_PART = 2,
};

class ObExprRandomPartNextval : public ObExprAutoincNextval
{
  OB_UNIS_VERSION_V(1);
public:
  explicit ObExprRandomPartNextval(common::ObIAllocator &alloc);
  ObExprRandomPartNextval(
      common::ObIAllocator &alloc,
      ObExprOperatorType type,
      const char *name,
      int32_t param_num,
      ObValidForGeneratedColFlag valid_for_generated_col,
      int32_t dimension,
      bool is_internal_for_mysql = false,
      bool is_internal_for_oracle = false);
  virtual ~ObExprRandomPartNextval();
  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  static int eval_nextval(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
  static int eval_nextval(
      const uint64_t sql_mode,
      const bool is_ddl_idempotent_autoinc,
      const ObRandomPartSeqUsedOutAction seq_used_out_action,
      share::schema::ObSchemaGetterGuard &schema_guard,
      AutoincParam &autoinc_param,
      const ObObjTypeClass *input_tc,
      ObDatum *input_value,
      const ObObjTypeClass &output_tc,
      ObDatum &expr_datum,
      bool &is_to_generate,
      uint64_t &new_val);
  static ObRandomPartSeqUsedOutAction get_table_load_action(const observer::ObTableLoadParam &param);
private:
  static int generate_autoinc_value(const ObRandomPartSeqUsedOutAction seq_used_out_action,
                                    share::schema::ObSchemaGetterGuard &schema_guard,
                                    share::AutoincParam &autoinc_param,
                                    uint64_t &new_val);
  static int send_add_random_partition_rpc(const uint64_t tenant_id, const uint64_t table_id, const ObIArray<ObTabletID> &inactive_tablet_ids, const uint64_t specified_value, ObIArray<ObTabletID> &active_tablet_ids);

  // get the input value && check need generate
  static int get_input_value(const uint64_t sql_mode,
                             const ObObjTypeClass *input_tc,
                             ObDatum *input_value,
                             share::AutoincParam &autoinc_param,
                             bool &is_to_generate,
                             uint64_t &casted_value);

  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObExprRandomPartNextval);
};

struct RandomPartSyncTabletCtxCmp final
{
public:
  RandomPartSyncTabletCtxCmp() {}
  ~RandomPartSyncTabletCtxCmp() = default;
  bool operator()(const RandomPartSyncTabletCtx &lhs, const RandomPartSyncTabletCtx &rhs);
private:
};

}//end namespace sql
}//end namespace oceanbase
#endif

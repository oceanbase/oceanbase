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

#ifndef OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_CALC_PARTITION_ID_
#define OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_CALC_PARTITION_ID_
#include "sql/engine/expr/ob_expr_operator.h"
#include "sql/resolver/ob_stmt_type.h"
#include "sql/engine/expr/ob_i_expr_extra_info.h"
#include "share/schema/ob_schema_struct.h"

namespace oceanbase {
namespace share {
namespace schema {
class ObTableSchema;
}
}  // namespace share
namespace sql {

struct CalcPartitionIdInfo : public ObIExprExtraInfo {
  OB_UNIS_VERSION(1);

public:
  CalcPartitionIdInfo(common::ObIAllocator& alloc, ObExprOperatorType type)
      : ObIExprExtraInfo(alloc, type),
        ref_table_id_(common::OB_INVALID_ID),
        part_level_(share::schema::PARTITION_LEVEL_ZERO),
        part_type_(share::schema::PARTITION_FUNC_TYPE_MAX),
        subpart_type_(share::schema::PARTITION_FUNC_TYPE_MAX),
        part_num_(common::OB_INVALID_COUNT),
        subpart_num_(common::OB_INVALID_COUNT)
  {}

  virtual int deep_copy(
      common::ObIAllocator& allocator, const ObExprOperatorType type, ObIExprExtraInfo*& copied_info) const override;

  int64_t ref_table_id_;
  share::schema::ObPartitionLevel part_level_;
  share::schema::ObPartitionFuncType part_type_;
  share::schema::ObPartitionFuncType subpart_type_;
  int64_t part_num_;
  int64_t subpart_num_;

  TO_STRING_KV(K_(ref_table_id), K_(part_level), K_(part_type), K_(subpart_type), K_(part_num), K_(subpart_num));
};

// return NONE_PARTITION_ID(-1) if part id not found
class ObExprCalcPartitionId : public ObFuncExprOperator {
public:
  static const int64_t NONE_PARTITION_ID = -1;
  enum OptRouteType { OPT_ROUTE_NONE, OPT_ROUTE_HASH_ONE };
  explicit ObExprCalcPartitionId(common::ObIAllocator& alloc);
  virtual ~ObExprCalcPartitionId();
  virtual int calc_result_typeN(
      ObExprResType& type, ObExprResType* types_array, int64_t param_num, common::ObExprTypeCtx& type_ctx) const;
  virtual int cg_expr(ObExprCGCtx& expr_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const override;
  static int calc_no_partition_location(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& res_datum);
  static int calc_partition_level_one(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& res_datum);
  static int calc_partition_level_two(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& res_datum);
  static int calc_opt_route_hash_one(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& res_datum);

private:
  static int init_calc_part_info(
      ObExprCGCtx& expr_cg_ctx, const share::schema::ObTableSchema& table_schema, CalcPartitionIdInfo*& calc_part_info);
  static int build_row(ObEvalCtx& ctx, common::ObIAllocator& allocator, const ObExpr& expr, common::ObNewRow& row);
  static int calc_partition_id(const ObExpr& expr, ObEvalCtx& ctx, const CalcPartitionIdInfo& tl_expr_info,
      int64_t first_part_id, ObDatum& res_datum);
  static int get_opt_route(
      const share::schema::ObTableSchema& table_schema, const ObRawExpr& raw_expr, OptRouteType& opt_route_type);
  static int enable_opt_route_hash_one(
      const share::schema::ObTableSchema& table_schema, const ObRawExpr& raw_expr, bool& enable_opt_route_hash_one);

private:
  DISALLOW_COPY_AND_ASSIGN(ObExprCalcPartitionId);
};

}  // end namespace sql
}  // end namespace oceanbase
#endif

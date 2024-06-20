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
#include "sql/ob_sql_define.h"
#include "share/schema/ob_schema_struct.h"

namespace oceanbase
{
namespace share
{
namespace schema
{
  class ObTableSchema;
}
}
namespace sql
{
class ObExprResType;
typedef common::ObFixedArray<common::ObTableID, common::ObIAllocator> TableIDFixedArray;
//calc partition base info
enum CalcPartIdType {
  CALC_TABLET_ID,
  CALC_PARTITION_ID,
  CALC_PARTITION_TABLET_ID,
};

struct CalcPartitionBaseInfo : public ObIExprExtraInfo
{
  OB_UNIS_VERSION(1);
public:
  CalcPartitionBaseInfo(common::ObIAllocator &alloc, const ObExprOperatorType type)
    : ObIExprExtraInfo(alloc, type),
      ref_table_id_(common::OB_INVALID_ID),
      related_table_ids_(alloc),
      part_level_(share::schema::PARTITION_LEVEL_ZERO),
      part_type_(share::schema::PARTITION_FUNC_TYPE_MAX),
      subpart_type_(share::schema::PARTITION_FUNC_TYPE_MAX),
      part_num_(common::OB_INVALID_COUNT),
      subpart_num_(common::OB_INVALID_COUNT),
      partition_id_calc_type_(CALC_INVALID),
      may_add_interval_part_(MayAddIntervalPart::NO),
      calc_id_type_(CALC_TABLET_ID),
      first_part_id_(OB_INVALID_ID)
  {}
  virtual ~CalcPartitionBaseInfo() { }

  virtual int deep_copy(common::ObIAllocator &allocator,
                        const ObExprOperatorType type,
                        ObIExprExtraInfo *&copied_info) const;

  int64_t ref_table_id_;
  TableIDFixedArray related_table_ids_;
  share::schema::ObPartitionLevel part_level_;
  share::schema::ObPartitionFuncType part_type_;
  share::schema::ObPartitionFuncType subpart_type_;
  int64_t part_num_;
  int64_t subpart_num_;
  PartitionIdCalcType partition_id_calc_type_; //used to mark expr set partition id calc type.
  MayAddIntervalPart may_add_interval_part_; // a further action if cann't found interval partition
  CalcPartIdType calc_id_type_; // mark calc tablet_id or partition_id
  int64_t first_part_id_; // for pkey enchance, no need serialize
  TO_STRING_KV(K_(ref_table_id), K_(related_table_ids),
               K_(part_level), K_(part_type),
               K_(subpart_type), K_(part_num), K_(subpart_num),
               K_(partition_id_calc_type),
               K_(may_add_interval_part), K_(calc_id_type));
};
//calc partition base
// 计算某一行数据对应的分区id, 如果没有找到对应的分区id,
// 则结果返回NONE_PARTITION_ID(-1)
class ObExprCalcPartitionBase : public ObFuncExprOperator
{
public:
  static const ObObjectID NONE_PARTITION_ID = OB_INVALID_ID;
  enum OptRouteType {
    OPT_ROUTE_NONE,
    OPT_ROUTE_HASH_ONE
  };
  explicit ObExprCalcPartitionBase(common::ObIAllocator &alloc, ObExprOperatorType type,
                                   const char *name, int32_t param_num, int32_t dimension)
    : ObFuncExprOperator(alloc, type, name, param_num, NOT_VALID_FOR_GENERATED_COL, dimension)
  {};
  virtual ~ObExprCalcPartitionBase() {}
  virtual int calc_result_typeN(ObExprResType &type,
                                ObExprResType *types_array,
                                int64_t param_num,
                                common::ObExprTypeCtx &type_ctx) const;
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;

  virtual CalcPartIdType get_calc_id_type() const = 0;

  static int set_may_add_interval_part(ObExpr *expr,
                                       const MayAddIntervalPart info);

  static int calc_no_partition_location(const ObExpr &expr,
                                        ObEvalCtx &ctx,
                                        ObDatum &res_datum);
  static int calc_partition_level_one(const ObExpr &expr,
                                      ObEvalCtx &ctx,
                                      ObDatum &res_datum);
  static int calc_partition_level_one_vector(const ObExpr &expr, ObEvalCtx &ctx,
                                             const ObBitVector &skip, const EvalBound &bound);
  static int calc_partition_level_two(const ObExpr &expr,
                                      ObEvalCtx &ctx,
                                      ObDatum &res_datum);
  static int extract_part_and_tablet_id(const ObDatum &part_datum,
                                        common::ObObjectID &part_id,
                                        common::ObTabletID &tablet_id);
  static int calc_part_and_tablet_id(const ObExpr *calc_part_id,
                                     ObEvalCtx &eval_ctx,
                                     common::ObObjectID &partition_id,
                                     common::ObTabletID &tablet_id);
private:
 int init_calc_part_info(common::ObIAllocator *allocator,
                         const share::schema::ObTableSchema &table_schema,
                         PartitionIdCalcType calc_type,
                         MayAddIntervalPart add_part,
                         CalcPartitionBaseInfo *&calc_part_info) const;
  static int concat_part_and_tablet_id(const ObExpr &expr,
                                       ObEvalCtx &ctx,
                                       ObDatum &res_datum,
                                       uint64_t part_id,
                                       uint64_t tablet_id);
  static int build_row(ObEvalCtx &ctx,
                       common::ObIAllocator &allocator,
                       const ObExpr &expr,
                       common::ObNewRow &row);
  static int calc_partition_id(const ObExpr &expr,
                               ObEvalCtx &ctx,
                               const CalcPartitionBaseInfo &tl_expr_info,
                               common::ObObjectID first_part_id,
                               common::ObTabletID &tablet_id,
                               common::ObObjectID &partition_id);
  static int add_interval_part(ObExecContext &exec_ctx,
                const CalcPartitionBaseInfo &calc_part_info,
                ObIAllocator &allocator, ObNewRow &row);
};

//calc partition id
class ObExprCalcPartitionId : public ObExprCalcPartitionBase
{
public:
  static const ObObjectID NONE_PARTITION_ID = OB_INVALID_ID;
  explicit ObExprCalcPartitionId(common::ObIAllocator &alloc);
  virtual ~ObExprCalcPartitionId();
  virtual CalcPartIdType get_calc_id_type() const {
    return CALC_PARTITION_ID;
  };

private:
  DISALLOW_COPY_AND_ASSIGN(ObExprCalcPartitionId);
};

//calc tablet id
class ObExprCalcTabletId : public ObExprCalcPartitionBase
{
public:
  explicit ObExprCalcTabletId(common::ObIAllocator &alloc);
  virtual ~ObExprCalcTabletId();
  virtual CalcPartIdType get_calc_id_type() const {
    return CALC_TABLET_ID;
  };

private:
  DISALLOW_COPY_AND_ASSIGN(ObExprCalcTabletId);
};

//calc tablet id
class ObExprCalcPartitionTabletId : public ObExprCalcPartitionBase
{
public:
  explicit ObExprCalcPartitionTabletId(common::ObIAllocator &alloc);
  virtual ~ObExprCalcPartitionTabletId();
  virtual CalcPartIdType get_calc_id_type() const {
    return CALC_PARTITION_TABLET_ID;
  };

private:
  DISALLOW_COPY_AND_ASSIGN(ObExprCalcPartitionTabletId);
};

} // end namespace sql
} // end namespace oceanbase
#endif

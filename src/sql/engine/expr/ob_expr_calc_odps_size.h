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

#ifndef OCEANBASE_SQL_ENGINE_EXPR_CALC_ODPS_SIZE_
#define OCEANBASE_SQL_ENGINE_EXPR_CALC_ODPS_SIZE_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
class ObExprCalcOdpsSize : public ObFuncExprOperator
{
public:
  explicit  ObExprCalcOdpsSize(common::ObIAllocator &alloc);
  virtual ~ObExprCalcOdpsSize() {};

  virtual int calc_result_type2(ObExprResType &type,
                                ObExprResType &type1,
                                ObExprResType &type2,
                                common::ObExprTypeCtx &type_ctx) const;
  virtual int calc_result_type3(ObExprResType &type,
                                ObExprResType &type1,
                                ObExprResType &type2,
                                ObExprResType &type3,
                                common::ObExprTypeCtx &type_ctx) const;
  virtual int calc_result_typeN(ObExprResType &type,
                                ObExprResType *types_array,
                                int64_t param_num,
                                common::ObExprTypeCtx &type_ctx) const;
  static int calc_odps_size(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res_datum);
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                       ObExpr &rt_expr) const override;

  template <typename T, typename U>
  static int fetch_size_or_row_count_wrapper(
      const ObString &part_spec, const ObString &properties,
      ObOdpsJniConnector::OdpsFetchType fetch_type, int64_t &row_count,
      ObIAllocator &allocator, ObString &session_id) {
    int ret = OB_SUCCESS;
    T odps_driver;
    if (OB_FAIL(U::init_odps_driver(fetch_type, THIS_WORKER.get_session(),
                                    properties, odps_driver))) {
      LOG_WARN("failed to init odps driver", K(ret));
    } else if (fetch_type ==
               ObOdpsJniConnector::OdpsFetchType::GET_ODPS_TABLE_SIZE) {
      if (OB_FAIL(U::fetch_odps_partition_size(part_spec, odps_driver,
                                               row_count))) {
        LOG_WARN("failed to fetch size", K(ret));
      }
    } else {
      if (OB_FAIL(U::fetch_odps_partition_row_count_and_session_id(
              odps_driver, part_spec, row_count, allocator, session_id))) {
        LOG_WARN("failed to fetch row count", K(ret));
      }
    }
    return ret;
  }

private:
  DISALLOW_COPY_AND_ASSIGN(ObExprCalcOdpsSize) const;
};
} // namespace sql
} // namespace oceanbase

#endif // OCEANBASE_SQL_ENGINE_EXPR_CALC_ODPS_SIZE_

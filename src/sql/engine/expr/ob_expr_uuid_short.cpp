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

#define USING_LOG_PREFIX SQL_RESV
#include "sql/engine/expr/ob_expr_uuid_short.h"
#include "observer/ob_server_struct.h"
#include "lib/time/ob_time_utility.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace sql
{

ObExprUuidShort::ObExprUuidShort(ObIAllocator &alloc)
  : ObFuncExprOperator(alloc, T_FUN_SYS_UUID_SHORT, N_UUID_SHORT, 0, NOT_VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprUuidShort::~ObExprUuidShort()
{
}

/**
 * Note:
 * The total number of serverids over(>=) 256 will not guarantee uniqueness, 
 * but we will not report an error, because this is a undefined behavior in mysql. 
 * In short, users should need to know this.
 */
uint64_t ObExprUuidShort::generate_uuid_short()
{
  //                        uuid_short
  // |      <8>       |        <32>       |       <24>
  //     server_id      server_start_time   incremented_variable
  static volatile uint64_t server_id_and_server_startup_time = ((GCTX.server_id_ & 255) << 56) |
                                                               ((static_cast<uint64_t>(common::ObTimeUtility::current_time() / 1000000) << 24) &
                                                               ((static_cast<uint64_t>(1) << 56) - 1));
  uint64_t uuid_short = ATOMIC_AAF(&server_id_and_server_startup_time, 1);
  LOG_DEBUG("uuid_short generated.", K(uuid_short));
  return uuid_short;
}

int ObExprUuidShort::cg_expr(ObExprCGCtx &expr_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const
{
  UNUSED(raw_expr);
  UNUSED(expr_cg_ctx);
  rt_expr.eval_func_ = ObExprUuidShort::eval_uuid_short;
  return OB_SUCCESS;
}

int ObExprUuidShort::eval_uuid_short(const ObExpr &expr,
                      ObEvalCtx &ctx,
                      ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  UNUSED(expr);
  UNUSED(ctx);
  expr_datum.set_uint(generate_uuid_short());
  return ret;
}

} // namespace sql
} // namespace oceanbase

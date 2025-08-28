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

#ifndef OCEANBASE_SQL_ENGINE_DECODE_TRACE_ID_
#define OCEANBASE_SQL_ENGINE_DECODE_TRACE_ID_
#include "sql/engine/expr/ob_expr_operator.h"
#define MAX_DECODE_TRACE_ID_RES_LEN 128 // max string buffer length
namespace oceanbase {
namespace sql {
class ObExprDecodeTraceId : public ObFuncExprOperator {
public:
  explicit ObExprDecodeTraceId(common::ObIAllocator &alloc);
  virtual ~ObExprDecodeTraceId();
  virtual int calc_result_type1(ObExprResType &type, ObExprResType &trace_id,
                                common::ObExprTypeCtx &type_ctx) const;
  static int calc_decode_trace_id_expr(const ObExpr &expr, ObEvalCtx &ctx,
                                       ObDatum &res_datum);
  static int calc_decode_trace_id_expr_batch(const ObExpr &expr, ObEvalCtx &ctx,
                                             const ObBitVector &skip,
                                             const int64_t batch_size);
                                             
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  template <typename T>
  static int calc_one_row(const ObExpr &expr, ObEvalCtx &ctx, const T &param, T &res)
  {
    int ret = OB_SUCCESS;
    ObCurTraceId::TraceId trace_id;
    int32_t len = 0;
    char *buf = expr.get_str_res_mem(ctx, MAX_DECODE_TRACE_ID_RES_LEN);
    ObString trace_id_str = param.get_string();
    if (OB_ISNULL(buf)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("allocate string res buf failed", K(ret));
    } else if (trace_id_str.empty()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid trace id string", K(ret));
    } else if (OB_FAIL(trace_id.parse_from_buf(trace_id_str.ptr()))) {
      LOG_WARN("parse trace_id failed", K(ret));
    } else if (OB_FAIL(trace_id.get_addr().addr_to_buffer(buf, MAX_DECODE_TRACE_ID_RES_LEN, len))) {
      SQL_ENG_LOG(WARN, "fail to databuff_printf", K(ret));
    } else {
      res.set_string(buf, len);
    }
    return ret;
  }
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprDecodeTraceId);
};
} // namespace sql
} // namespace oceanbase
#endif /* OCEANBASE_SQL_ENGINE_DECODE_TRACE_ID_ */

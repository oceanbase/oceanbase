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
#include "sql/engine/expr/vector_cast/vector_cast.h"
#include "sql/engine/expr/ob_datum_cast.h"
#include "share/object/ob_obj_cast_util.h"

namespace oceanbase
{
namespace sql
{

// Cast func processing logic is a reference to ob_datum_cast.cpp::CAST_FUNC_NAME(IN_TYPE, OUT_TYPE)
template<typename ArgVec, typename ResVec>
struct ToTimeCastImpl
{
  static int string_time(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const EvalBound &bound)
  {
    EVAL_COMMON_ARG()
    {
      ObBasicSessionInfo *session = ctx.exec_ctx_.get_my_session();
      const common::ObTimeZoneInfo *tz_info_local = NULL;
      ObSolidifiedVarsGetter helper(expr, ctx, session);
      if (OB_ISNULL(session)) {
        ret = OB_ERR_UNEXPECTED;
        SQL_LOG(WARN, "session is NULL", K(ret));
      } else if (OB_FAIL(helper.get_time_zone_info(tz_info_local))) {
        SQL_LOG(WARN, "get time zone info failed", K(ret));
      } else {
        class StringToTimeFn : public CastFnBase {
        public:
          StringToTimeFn(CAST_ARG_LIST_DECL, ArgVec* arg_vec, ResVec* res_vec,
                          ObDateSqlMode date_sql_mode, ObTimeConvertCtx cvrt_ctx)
              : CastFnBase(CAST_ARG_DECL), arg_vec_(arg_vec), res_vec_(res_vec),
                date_sql_mode_(date_sql_mode), cvrt_ctx_(cvrt_ctx) {}

          OB_INLINE int operator() (const ObExpr &expr, int idx)
          {
            int ret = OB_SUCCESS;
            int warning = OB_SUCCESS;
            int64_t out_val = 0;
            ObScale res_scale = -1;
            ObString in_str(arg_vec_->get_string(idx));
            if (CAST_FAIL(ObTimeConverter::str_to_time(in_str, out_val, &res_scale, cvrt_ctx_.need_truncate_))) {
            }
            if (OB_SUCC(ret)) {
              SET_RES_TIME(idx, out_val);
            }
            return ret;
          }
        private:
          ArgVec *arg_vec_;
          ResVec *res_vec_;
          ObDateSqlMode date_sql_mode_;
          ObTimeConvertCtx cvrt_ctx_;
        };

        ObDateSqlMode date_sql_mode;
        const ObCastMode cast_mode = expr.extra_;
        date_sql_mode.allow_invalid_dates_ = CM_IS_ALLOW_INVALID_DATES(cast_mode);
        date_sql_mode.no_zero_date_ = CM_IS_NO_ZERO_DATE(cast_mode);
        date_sql_mode.no_zero_in_date_ = CM_IS_NO_ZERO_IN_DATE(cast_mode);
        date_sql_mode.implicit_first_century_year_ = CM_IS_IMPLICIT_FIRST_CENTURY_YEAR(expr.extra_);
        bool need_truncate = CM_IS_COLUMN_CONVERT(cast_mode)
                            ? CM_IS_TIME_TRUNCATE_FRACTIONAL(cast_mode) : false;
        ObTimeConvertCtx cvrt_ctx(tz_info_local, ObTimestampType == out_type, need_truncate);
        if (lib::is_oracle_mode()) {
          if (OB_FAIL(common_get_nls_format(session, ctx, &expr, out_type,
                                            CM_IS_FORCE_USE_STANDARD_NLS_FORMAT(cast_mode),
                                            cvrt_ctx.oracle_nls_format_))) {
            SQL_LOG(WARN, "common_get_nls_format failed", K(ret));
          }
        } else {
          date_sql_mode.allow_invalid_dates_ = CM_IS_ALLOW_INVALID_DATES(cast_mode);
          date_sql_mode.no_zero_date_ = CM_IS_NO_ZERO_DATE(cast_mode);
          date_sql_mode.implicit_first_century_year_ = CM_IS_IMPLICIT_FIRST_CENTURY_YEAR(cast_mode);
        }
        if (OB_SUCC(ret)) {
          StringToTimeFn cast_fn(CAST_ARG_DECL, arg_vec, res_vec, date_sql_mode, cvrt_ctx);
          if (OB_FAIL(CastHelperImpl::batch_cast(cast_fn, expr, arg_vec, res_vec, eval_flags,
                                            skip, bound, is_diagnosis, diagnosis_manager))) {
            SQL_LOG(WARN, "cast failed", K(ret), K(in_type), K(out_type));
          }
        }
      }
    }
    return ret;
  }
};

DEF_VECTOR_IMPLICIT_CAST_FUNC(VEC_TC_STRING, VEC_TC_TIME)
{                                                                                  \
  return ToTimeCastImpl<IN_VECTOR, OUT_VECTOR>::\
            string_time(expr, ctx, skip, bound);\
}
} // end sql
} // namespace oceanbase

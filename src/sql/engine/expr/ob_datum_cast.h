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

#ifndef _OB_EXPR_DATUM_CAST_
#define _OB_EXPR_DATUM_CAST_

#include "common/object/ob_object.h"
#include "common/ob_zerofill_info.h"
#include "lib/timezone/ob_timezone_info.h"
#include "lib/timezone/ob_time_convert.h"
#include "lib/charset/ob_charset.h"
#include "share/ob_errno.h"
#include "share/datum/ob_datum.h"
#include "share/object/ob_obj_cast.h"
#include "sql/engine/expr/ob_expr.h"

namespace oceanbase {
namespace sql {
class ObPhysicalPlanCtx;
// extract accuracy info from %expr and call datum_accuracy_check() below.
int datum_accuracy_check(const ObExpr& expr, const uint64_t cast_mode, ObEvalCtx& ctx, const common::ObDatum& in_datum,
    ObDatum& res_datum, int& warning);

// check if accuracy in %in_datum is ok. if ok, call res_datum.set_datum(in_datum).
// if not, will trunc data in %in_datum and put it in res_datum.
// this func makes sure data in %in_datum and %in_datum itself will not be changed.
int datum_accuracy_check(const ObExpr& expr, const uint64_t cast_mode, ObEvalCtx& ctx, const common::ObAccuracy& acc,
    const common::ObDatum& in_datum, ObDatum& res_datum, int& warning);
// Check whether str is legal with check_cs_type as the character set
// In strict_mode, if the above check fails, an error code is returned
// Otherwise, return the maximum legal string with check_cs_type as the character set
int string_collation_check(const bool is_strict_mode, const common::ObCollationType check_cs_type,
    const common::ObObjType str_type, common::ObString& str);

int ob_datum_to_ob_time_with_date(const common::ObDatum& datum, const common::ObObjType type,
    const common::ObTimeZoneInfo* tz_info, common::ObTime& ob_time, const int64_t cur_ts_value,
    bool is_dayofmonth = false);
int ob_datum_to_ob_time_without_date(const common::ObDatum& datum, const common::ObObjType type,
    const common::ObTimeZoneInfo* tz_info, common::ObTime& ob_time);
int common_datetime_string(const common::ObObjType in_type, const common::ObObjType out_type,
    const common::ObScale in_scale, bool force_use_std_nls_format, const int64_t in_val, ObEvalCtx& ctx, char* buf,
    int64_t buf_len, int64_t& out_len);
int padding_char_for_cast(int64_t padding_cnt, const common::ObCollationType& padding_cs_type,
    common::ObIAllocator& alloc, common::ObString& padding_res);

class ObDatumHexUtils {
public:
  static int hextoraw_string(const ObExpr& expr, const common::ObString& in_str, ObEvalCtx& ctx, ObDatum& res_datum);
  static int hextoraw(const ObExpr& expr, const common::ObDatum& in, const common::ObObjType& in_type,
      const common::ObCollationType& in_cs_type, ObEvalCtx& ctx, ObDatum& res);
  static int get_uint(const common::ObObjType& in_type, const common::ObDatum& in, common::ObIAllocator& alloc,
      common::number::ObNumber& out);
  static int uint_to_raw(
      const common::number::ObNumber& uint_num, const ObExpr& expr, ObEvalCtx& ctx, ObDatum& res_datum);
  static int unhex(const ObExpr& expr, const common::ObString& in_str, ObEvalCtx& ctx, ObDatum& res_datum);
  static int rawtohex(const ObExpr& expr, const common::ObString& in_str, ObEvalCtx& ctx, ObDatum& res_datum);
  static int hex(const ObExpr& expr, const common::ObString& in_str, ObEvalCtx& ctx, common::ObIAllocator& calc_alloc,
      ObDatum& res_datum);
};

class ObDatumCast {
public:
  static int get_implicit_cast_function(const common::ObObjType in_type, const common::ObCollationType in_cs_type,
      const common::ObObjType out_type, const common::ObCollationType out_cs_type, const int64_t cast_mode,
      ObExpr::EvalFunc& eval_func);
  static int choose_cast_function(const common::ObObjType in_type, const common::ObCollationType in_cs_type,
      const common::ObObjType out_type, const common::ObCollationType out_cs_type, const int64_t cast_mode,
      common::ObIAllocator& allocator, ObExpr& rt_expr);
  static int get_enumset_cast_function(
      const common::ObObjTypeClass in_tc, const common::ObObjType out_type, ObExpr::EvalEnumSetFunc& eval_func);
  static int check_can_cast(const common::ObObjType in_type, const common::ObCollationType in_cs_type,
      const common::ObObjType out_type, const common::ObCollationType out_cs_type);
  static int is_trivial_cast(const common::ObObjType in_type, const common::ObCollationType in_cs_type,
      const common::ObObjType out_type, const common::ObCollationType out_cs_type, const common::ObCastMode& cast_mode,
      bool& just_eval_arg);

  // The same as:
  //    EXPR_DEFINE_CAST_CTX(expr_ctx, cast_mode)
  //    EXPR_CAST_OBJ_V2(obj_type, obj, res_obj)
  static int cast_obj(ObEvalCtx& ctx, common::ObIAllocator& alloc, const common::ObObjType& dst_type,
      const common::ObCollationType& dst_cs_type, const common::ObObj& src_obj, common::ObObj& dst_obj);

  static bool is_implicit_cast(const ObExpr& expr)
  {
    bool bret = false;
    if (T_FUN_SYS_CAST == expr.type_ && CM_IS_IMPLICIT_CAST(expr.extra_)) {
      bret = true;
    }
    return bret;
  }

  static bool is_explicit_cast(const ObExpr& expr)
  {
    bool bret = false;
    if (T_FUN_SYS_CAST == expr.type_ && CM_IS_EXPLICIT_CAST(expr.extra_)) {
      bret = true;
    }
    return bret;
  }
};

class ObDatumCaster {
public:
  ObDatumCaster() : inited_(false), eval_ctx_(NULL), cast_expr_(NULL), extra_cast_expr_(NULL)
  {}
  ~ObDatumCaster()
  {}

  // init eval_ctx_/cast_expr_/extra_cast_expr_/frame. all mem comes from ObExecContext.
  // frame layout:
  // ObDatum | ObDatum | ObDynReserveBuf | res_buf | ObDynReserveBuf | res_buf
  // res_buf_len is 128
  int init(ObExecContext& ctx);

  // same with ObObjCaster::to_type().
  // input is ObExpr, and output is ObDatum, it's better if input is also ObDatum.
  // we will do this later if necessary.
  int to_type(const ObDatumMeta& dst_type, const ObExpr& src_expr, const common::ObCastMode& cm, common::ObDatum*& res);
  // for xxx -> enumset.
  int to_type(const ObDatumMeta& dst_type, const common::ObIArray<common::ObString>& str_values, const ObExpr& src_expr,
      const common::ObCastMode& cm, common::ObDatum*& res);

  int destroy();

private:
  DISALLOW_COPY_AND_ASSIGN(ObDatumCaster);

  // setup following data member of ObExpr:
  // datum_meta_, obj_meta_, obj_datum_map_, eval_func_,
  // args_, arg_cnt_, parents_, parent_cnt_, basic_funcs_.
  int setup_cast_expr(
      const ObDatumMeta& dst_type, const ObExpr& src_expr, const common::ObCastMode cm, ObExpr& cast_expr);
  bool inited_;
  ObEvalCtx* eval_ctx_;
  ObExpr* cast_expr_;
  ObExpr* extra_cast_expr_;
};

}  // namespace sql
}  // namespace oceanbase

#endif  // _OB_EXPR_DATUM_CAST_

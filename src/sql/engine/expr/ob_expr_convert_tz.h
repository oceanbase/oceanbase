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

#ifndef OCEANBASE_SQL_OB_EXPR_CONVERT_TZ_H_
#define OCEANBASE_SQL_OB_EXPR_CONVERT_TZ_H_
#include "sql/engine/expr/ob_expr_operator.h"
namespace oceanbase
{
namespace sql
{
class ObExprConvertTZ : public ObFuncExprOperator{
    class ConvertTZInfoWrap {
    public:
      enum ObTZInfoClass
      {
        NONE = 0,
        POSITION = 1,
        OFFSET = 2
      };
      explicit ConvertTZInfoWrap(): tz_info_pos_() {
        tz_offset_ = 0;
        class_ = NONE;
      }
      ~ConvertTZInfoWrap() {
        tz_offset_ = 0;
        class_ = NONE;
      }
      ObTimeZoneInfoPos& get_tz_info_pos() const { return const_cast<ObTimeZoneInfoPos&>(tz_info_pos_); }
      int32_t get_tz_offset() const { return tz_offset_; }
      bool is_offset_class() const { return OFFSET == class_; }
      bool is_position_class() const { return POSITION == class_; }
      void set_position_class() { class_ = POSITION; }
      void set_tz_offset(int32_t tz_offset) {
        tz_offset_ = tz_offset;
        class_ = OFFSET;
      }
      void set_info_class(ObTZInfoClass cls) { class_ = cls; }
      ObTZInfoClass get_info_class() const { return class_; }
    private:
      ObTimeZoneInfoPos tz_info_pos_;
      int32_t tz_offset_;
      ObTZInfoClass class_;
    };
    class ObExprConvertTZCtx: public ObExprOperatorCtx
    {
    public:
      explicit ObExprConvertTZCtx() : ObExprOperatorCtx(), find_tz_ret_(OB_SUCCESS) {}
      virtual ~ObExprConvertTZCtx() {}
      ConvertTZInfoWrap tz_info_wrap_src_;
      ConvertTZInfoWrap tz_info_wrap_dst_;
      int find_tz_ret_;
    };
public:
  explicit ObExprConvertTZ(common::ObIAllocator &alloc);
  virtual ~ObExprConvertTZ(){}
  virtual int calc_result_type3(ObExprResType &type,
                                ObExprResType &input1,
                                ObExprResType &input2,
                                ObExprResType &input3,
                                common::ObExprTypeCtx &type_ctx)const;
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                      ObExpr &expr) const override;
  virtual bool need_rt_ctx() const override { return true; }
  static int eval_convert_tz(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  static int find_time_zone_pos(const ObString &tz_name,
                                        const ObTimeZoneInfo &tz_info,
                                        ObTimeZoneInfoPos &tz_info_pos);
  static int find_time_zone_pos(const ObString &tz_name,
                                        const ObTimeZoneInfo &tz_info,
                                        ObTimeZoneInfoPos *&tz_info_pos);
  static int calc_convert_tz(int64_t timestamp_data, const ObString &tz_str_s,//source time zone (input2)
                        const ObString &tz_str_d,//destination time zone (input3)
                        ObSQLSessionInfo *session,
                        ObDatum &result);
  static int calc_convert_tz_const(const ObExpr &expr, ObEvalCtx &ctx, int64_t &timestamp_data, const ObString &tz_str_s,//source time zone (input2)
                        const ObString &tz_str_d,//destination time zone (input3)
                        ObSQLSessionInfo *session,
                        ObDatum &result);
  static int calc(int64_t &timestamp_data, const ObTimeZoneInfoPos &tz_info_pos,
                 const bool input_utc_time);
  static int parse_string(int64_t &timestamp_data, const ObString &tz_str,
                          ObSQLSessionInfo *session, const bool input_utc_time);
  static int calc_convert_tz_vector(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const EvalBound &bound);

private:
  // disallow copy
  static int get_cvrt_tz_info(const ObString &tz_str, ObSQLSessionInfo *session, ConvertTZInfoWrap &tz_info_wrap);
  static int handle_timezone_offset(int64_t &timestamp_data, const ConvertTZInfoWrap &tz_info_wrap, bool is_destination);
  static int get_offset_by_couple_tz(int64_t timestamp_data, int32_t &off, const ObString &tz_str_s, const ObString &tz_str_d, ObSQLSessionInfo *session);

  static int calc_convert_tz_timestamp(const ObExpr &expr, ObEvalCtx &ctx, int64_t &timestamp_data, const ObString &tz_str_s,//source time zone (input2)
                      const ObString &tz_str_d,//destination time zone (input3)
                      ObSQLSessionInfo *session);
  template <typename ArgVec, typename ResVec, typename IN_TYPE>
  static int convert_tz_vector(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const EvalBound &bound);
  template <typename ArgVec, typename ResVec, typename IN_TYPE>
  static int convert_tz_vector_const(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const EvalBound &bound);
  DISALLOW_COPY_AND_ASSIGN(ObExprConvertTZ);

};


} //sql
} //oceanbasel
#endif //OCEANBASE_SQL_OB_EXPR_CONVERT_TZ_H_

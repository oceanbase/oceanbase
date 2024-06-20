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
 * This file is for func json_value.
 */

#ifndef OCEANBASE_SQL_OB_EXPR_JSON_VALUE_H_
#define OCEANBASE_SQL_OB_EXPR_JSON_VALUE_H_

#include "sql/engine/expr/ob_expr_operator.h"
#include "lib/json_type/ob_json_tree.h"
#include "lib/json_type/ob_json_base.h"
#include "ob_json_param_type.h"
#include "ob_expr_json_utils.h"
#include "ob_expr_json_func_helper.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace sql
{

/* process ascii */
const static uint8_t OB_JSON_ON_ASCII_IMPLICIT    = 0;
const static uint8_t OB_JSON_ON_ASCII_USE         = 1;

class 
ObExprJsonValue : public ObFuncExprOperator
{
public:
  explicit ObExprJsonValue(common::ObIAllocator &alloc);
  virtual ~ObExprJsonValue();
  virtual int calc_result_typeN(ObExprResType& type,
                                ObExprResType* types,
                                int64_t param_num, 
                                common::ObExprTypeCtx& type_ctx)
                                const override;
  static int eval_json_value(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  static int eval_ora_json_value(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);

  static int calc_input_type(ObExprResType& types_stack, bool &is_json_input);

  static int deal_item_method_in_seek(ObIJsonBase*& in,
                                      bool &is_null_result,
                                      ObJsonPath *j_path,
                                      ObIAllocator *allocator,
                                      uint8_t &is_type_mismatch);

  /* code for cast accuracy check */
  template<typename Obj>
  static int check_default_val_accuracy(const ObAccuracy &accuracy,
                                        const ObObjType &type,
                                        const Obj *obj);

  DECLARE_SET_LOCAL_SESSION_VARS;

private:
  /* cast wrapper to dst type with accuracy check*/
  static int get_cast_ret(int ret);
  static int cast_to_int(ObIJsonBase *j_base, ObObjType dst_type, int64_t &val);
  static int cast_to_uint(ObIJsonBase *j_base, ObObjType dst_type, uint64_t &val);
  static int cast_to_datetime(ObIJsonBase *j_base,
                              common::ObIAllocator *allocator,
                              const ObBasicSessionInfo *session,
                              ObEvalCtx &ctx,
                              const ObExpr *expr,
                              common::ObAccuracy &accuracy,
                              int64_t &val,
                              uint8_t &is_type_cast);
  static bool type_cast_to_string(ObString &json_string,
                                  common::ObIAllocator *allocator,
                                  ObIJsonBase *j_base,
                                  ObAccuracy &accuracy);
  static int cast_to_otimstamp(ObIJsonBase *j_base,
                               const ObBasicSessionInfo *session,
                               ObEvalCtx &ctx,
                               const ObExpr *expr,
                               common::ObAccuracy &accuracy,
                               ObObjType dst_type,
                               ObOTimestampData &out_val,
                               uint8_t &is_type_cast);
  static int cast_to_date(ObIJsonBase *j_base, int32_t &val, uint8_t &is_type_cast);
  static int cast_to_time(ObIJsonBase *j_base,
                          common::ObAccuracy &accuracy,
                          int64_t &val);
  static int cast_to_year(ObIJsonBase *j_base, uint8_t &val);
  static int cast_to_float(ObIJsonBase *j_base, ObObjType dst_type, float &val);
  static int cast_to_double(ObIJsonBase *j_base, ObObjType dst_type, double &val);
  static int cast_to_number(common::ObIAllocator *allocator,
                            ObIJsonBase *j_base,
                            common::ObAccuracy &accuracy,
                            ObObjType dst_type,
                            number::ObNumber &val,
                            uint8_t &is_type_cast);
  static int cast_to_string(common::ObIAllocator *allocator,
                            ObIJsonBase *j_base,
                            ObCollationType in_cs_type,
                            ObCollationType dst_cs_type,
                            common::ObAccuracy &accuracy,
                            ObObjType dst_type,
                            ObString &val,
                            uint8_t &is_type_cast,
                            uint8_t is_truncate);
  static int cast_to_bit(ObIJsonBase *j_base, uint64_t &val);
  static int cast_to_json(common::ObIAllocator *allocator, ObIJsonBase *j_base,
                          ObString &val, uint8_t &is_type_cast);

  static int get_empty_or_error_type(const ObExpr &expr,
                                      ObEvalCtx &ctx,
                                      uint8_t index,
                                      bool &is_cover_by_error,
                                      const ObAccuracy &accuracy,
                                      uint8_t &type,
                                      ObObjType dst_type);
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  virtual bool need_rt_ctx() const override { return true; }
  virtual common::ObCastMode get_cast_mode() const { return CM_ERROR_ON_SCALE_OVER;}
  static int calc_empty_error_type(ObExprResType* types_stack, uint8_t pos, ObExprResType &dst_type, ObExprTypeCtx& type_ctx);

private:
  static bool try_set_error_val(const ObExpr &expr,
                                ObEvalCtx &ctx,
                                ObDatum &res, int &ret,
                                ObJsonExprParam* json_param,
                                uint8_t &is_type_mismatch);
  static int doc_do_seek(ObJsonSeekResult &hits, bool &is_null_result, ObJsonExprParam* json_param,
                         ObIJsonBase *j_base, const ObExpr &expr, ObEvalCtx &ctx, bool &is_cover_by_error,
                         ObDatum *&return_val,
                         uint8_t &is_type_mismatch);

  // new sql engine
  static inline void set_val(ObDatum &res, ObDatum *val)
  { res.set_datum(*val); }

  // old sql engine
  static inline void set_val(ObObj &res, ObObj *val)
  { res = *val; }
  static int get_default_value(ObExpr *expr,
                                  ObEvalCtx &ctx,
                                  const ObAccuracy &accuracy,
                                  ObDatum **default_value);
  static int get_default_empty_error_value(const ObExpr &expr,
                                            ObJsonExprParam* json_param,
                                            ObEvalCtx &ctx);

  static int get_on_mismatch(const ObExpr &expr,
                             ObEvalCtx &ctx,
                             uint8_t index,
                             bool &is_cover_by_error,
                             const ObAccuracy &accuracy,
                             ObIArray<int8_t> &val,
                             ObIArray<int8_t> &type);
  /* code from ob_expr_cast for cal_result_type */
  int get_cast_type(const ObExprResType param_type2, ObExprResType &dst_type) const;
  int set_dest_type(ObExprResType &type1, ObExprResType &type, ObExprResType &dst_type, ObExprTypeCtx &type_ctx) const;
  int get_cast_string_len(ObExprResType &type1,
                          ObExprResType &type2,
                          common::ObExprTypeCtx &type_ctx,
                          int32_t &res_len,
                          int16_t &length_semantics,
                          common::ObCollationType conn) const;
  int get_cast_inttc_len(ObExprResType &type1,
                         ObExprResType &type2,
                         common::ObExprTypeCtx &type_ctx,
                         int32_t &res_len,
                         int16_t &length_semantics,
                         common::ObCollationType conn) const;
  static int check_default_value(ObExprResType* types_stack, int8_t pos, ObExprResType &dst_type);
  static int get_clause_param_value(const ObExpr &expr, ObEvalCtx &ctx,
                            ObJsonExprParam* json_param,
                            bool &is_cover_by_error);
public:
  static int extract_plan_cache_param(const ObExprJsonQueryParamInfo *info, ObJsonExprParam& json_param);
  static int check_param_valid(const ObExpr &expr, ObJsonExprParam* json_param,
                                ObJsonPath *j_path, bool &is_cover_by_error);
  static int init_ctx_var(const ObExpr &expr, ObJsonParamCacheCtx* param_ctx);
  static void get_mismatch_option(ObIArray<int8_t> &mismatch_val,
                                ObIArray<int8_t> &mismatch_type,
                                bool &is_null_res,
                                bool &set_default_val);

  static void get_error_option(int8_t error_type,
                    bool &is_null, bool &has_default_val);
  static int get_empty_option(ObDatum *&empty_res,
                              bool &is_cover_by_error,
                              int8_t empty_type,
                              ObDatum *empty_datum,
                              bool &is_null_result);
  static int set_result(const ObExpr &expr,
                        ObJsonExprParam* json_param,
                        ObEvalCtx &ctx,
                        bool &is_null_result,
                        bool &is_cover_by_error,
                        uint8_t &is_type_mismatch,
                        ObDatum &res,
                        ObDatum *return_val,
                        ObIAllocator *allocator,
                        ObJsonSeekResult &hits);

  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObExprJsonValue);
};

} // sql
} // oceanbase
#endif // OCEANBASE_SQL_OB_EXPR_JSON_VALUE_H_

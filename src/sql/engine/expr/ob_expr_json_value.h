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

using namespace oceanbase::common;

namespace oceanbase
{
namespace sql
{

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
  static int cast_to_res(common::ObIAllocator *allocator,
                         const ObExpr &expr,
                         ObEvalCtx &ctx,
                         ObIJsonBase *j_base,
                         uint8_t error_type,
                         ObDatum *error_val,
                         common::ObAccuracy &accuracy,
                         ObObjType dst_type,
                         common::ObCollationType in_coll_type,
                         common::ObCollationType dst_coll_type,
                         ObDatum &res,
                         ObVector<uint8_t> &mismatch_val,
                         ObVector<uint8_t> &mismatch_type,
                         uint8_t &is_type_cast,
                         uint8_t ascii_type,
                         uint8_t is_truncate);
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  virtual common::ObCastMode get_cast_mode() const { return CM_ERROR_ON_SCALE_OVER;}
private:
  /* code for cast accuracy check */
  template<typename Obj>
  static int check_default_val_accuracy(const ObAccuracy &accuracy,
                                        const ObObjType &type,
                                        const Obj *obj);
  static int get_accuracy_internal(common::ObAccuracy &accuracy,
                                  ObObjType &dest_type,
                                  const int64_t value,
                                  const ObLengthSemantics &length_semantics);
  static int get_accuracy(const ObExpr &expr,
                          ObEvalCtx& ctx,
                          common::ObAccuracy &accuracy,
                          ObObjType &dest_type,
                          bool &is_cover_by_error);
  static int number_range_check(const common::ObAccuracy &accuracy,
                                ObIAllocator *allocator,
                                number::ObNumber &val,
                                bool strict = false);
  static int datetime_scale_check(const common::ObAccuracy &accuracy,
                                  int64_t &value,
                                  bool strict = false);
  static int time_scale_check(const common::ObAccuracy &accuracy, int64_t &value,
                              bool strict = false);
  /* cast wrapper to dst type with accuracy check*/
  static int get_cast_ret(int ret);
  static int cast_to_int(ObIJsonBase *j_base, ObObjType dst_type, int64_t &val);
  static int cast_to_uint(ObIJsonBase *j_base, ObObjType dst_type, uint64_t &val);
  static int cast_to_datetime(ObIJsonBase *j_base,
                              common::ObIAllocator *allocator,
                              const ObBasicSessionInfo *session,
                              common::ObAccuracy &accuracy,
                              int64_t &val,
                              uint8_t &is_type_cast);
  static bool type_cast_to_string(ObString &json_string,
                                  common::ObIAllocator *allocator,
                                  ObIJsonBase *j_base,
                                  ObAccuracy &accuracy);
  static int cast_to_otimstamp(ObIJsonBase *j_base,
                               const ObBasicSessionInfo *session,
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
  template<typename Obj>
  static bool try_set_error_val(const ObExpr &expr,
                                ObEvalCtx &ctx,
                                Obj &res, int &ret, uint8_t &error_type,
                                Obj *&error_val, ObVector<uint8_t> &mismatch_val,
                                ObVector<uint8_t> &mismatch_type,
                                uint8_t &is_type_cast,
                                const ObAccuracy &accuracy, ObObjType dst_type);
  static int error_convert(int ret_old);
  static int doc_do_seek(ObJsonBaseVector &hits, bool &is_null_result,
                         ObDatum *json_datum, ObJsonPath *j_path,
                         ObIJsonBase *j_base, const ObExpr &expr,
                         ObEvalCtx &ctx, bool &is_cover_by_error,
                         const ObAccuracy &accuracy, ObObjType dst_type,
                         ObDatum *&return_val, ObDatum *error_datum,
                         uint8_t error_type, ObDatum *empty_datum,
                         uint8_t &empty_type, ObObjType &default_val_type, uint8_t &is_type_cast);
  // new sql engine
  static inline void set_val(ObDatum &res, ObDatum *val)
  { res.set_datum(*val); }

  // old sql engine
  static inline void set_val(ObObj &res, ObObj *val)
  { res = *val; }

  /* process ascii */
  const static uint8_t OB_JSON_ON_ASCII_IMPLICIT    = 0;
  const static uint8_t OB_JSON_ON_ASCII_USE         = 1;

  /* process empty or error */
  const static uint8_t OB_JSON_ON_RESPONSE_ERROR    = 0;
  const static uint8_t OB_JSON_ON_RESPONSE_NULL     = 1;
  const static uint8_t OB_JSON_ON_RESPONSE_DEFAULT  = 2;
  const static uint8_t OB_JSON_ON_RESPONSE_IMPLICIT = 3;

  /* process on mismatch { error : 0, null : 1, ignore : 2 }*/
  const static uint8_t OB_JSON_ON_MISMATCH_ERROR    = 0;
  const static uint8_t OB_JSON_ON_MISMATCH_NULL     = 1;
  const static uint8_t OB_JSON_ON_MISMATCH_IGNORE   = 2;
  const static uint8_t OB_JSON_ON_MISMATCH_IMPLICIT = 3;


  /* process mismatch type { MISSING : 4 (1), EXTRA : 5 (2), TYPE : 6 (4), EMPTY : 7 (0)} make diff with mismatch type  */
  const static uint8_t OB_JSON_TYPE_MISSING_DATA    = 4;
  const static uint8_t OB_JSON_TYPE_EXTRA_DATA      = 5;
  const static uint8_t OB_JSON_TYPE_TYPE_ERROR      = 6;
  const static uint8_t OB_JSON_TYPE_IMPLICIT        = 7;

  const static uint8_t json_doc_id      = 0;
  const static uint8_t json_path_id     = 1;
  const static uint8_t ret_type_id      = 2;
  const static uint8_t opt_truncate_id  = 3;
  const static uint8_t opt_ascii_id     = 4;
  const static uint8_t empty_type_id    = 5;
  const static uint8_t empty_val_id     = 6;
  const static uint8_t empty_val_pre_id = 7;
  const static uint8_t error_type_id    = 8;
  const static uint8_t error_val_id     = 9;
  const static uint8_t error_val_pre_id = 10;
  const static uint8_t opt_mismatch_id  = 11;

  static int get_on_empty_or_error(const ObExpr &expr,
                                   ObEvalCtx &ctx,
                                   uint8_t index,
                                   bool &is_cover_by_error,
                                   const ObAccuracy &accuracy,
                                   uint8 &type,
                                   ObDatum **default_value,
                                   ObObjType dst_type,
                                   ObObjType &default_val_type);
  static int get_on_ascii(const ObExpr &expr,
                          ObEvalCtx &ctx,
                          uint8_t index,
                          bool &is_cover_by_error,
                          uint8 &type);

  static int get_on_mismatch(const ObExpr &expr,
                             ObEvalCtx &ctx,
                             uint8_t index,
                             bool &is_cover_by_error,
                             const ObAccuracy &accuracy,
                             ObVector<uint8_t> &val,
                             ObVector<uint8_t> &type);
  /* code from ob_expr_cast for cal_result_type */
  const static int32_t OB_LITERAL_MAX_INT_LEN = 21;
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
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObExprJsonValue);
};

} // sql
} // oceanbase
#endif // OCEANBASE_SQL_OB_EXPR_JSON_VALUE_H_
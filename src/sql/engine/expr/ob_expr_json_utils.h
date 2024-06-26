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
 * This file is for implement of func json expr helper
 */

#ifndef OCEANBASE_SQL_OB_EXPR_JSON_UTILS_H_
#define OCEANBASE_SQL_OB_EXPR_JSON_UTILS_H_

#include "sql/engine/expr/ob_expr_util.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"
#include "share/object/ob_obj_cast.h"
#include "objit/common/ob_item_type.h"
#include "sql/session/ob_sql_session_info.h"
#include "lib/json_type/ob_json_tree.h"
#include "lib/json_type/ob_json_base.h"
#include "lib/json_type/ob_json_bin.h"
#include "lib/json_type/ob_json_path.h"
#include "lib/json_type/ob_json_parse.h"
#include "sql/engine/expr/ob_expr_result_type_util.h"
#include "sql/engine/expr/ob_datum_cast.h"
#include "ob_expr_json_func_helper.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace sql
{

const size_t ObMaxItemMethod = ObJsonPathNodeType::JPN_END_FUNC_FLAG - ObJsonPathNodeType::JPN_BEGIN_FUNC_FLAG - 1;
const size_t ObMaxJsonType = (size_t)ObJsonNodeType::J_MAX_TYPE;
// from sql_parser_base.h
#define DEFAULT_STR_LENGTH -1

/* process on mismatch { error : 0, null : 1, ignore : 2 }*/
typedef enum JsnValueMisMatch {
  OB_JSON_ON_MISMATCH_ERROR,          // 0
  OB_JSON_ON_MISMATCH_NULL,           // 1
  OB_JSON_ON_MISMATCH_IGNORE,         // 2
  OB_JSON_ON_MISMATCH_IMPLICIT,       // 3
  OB_JSON_TYPE_MISSING_DATA,          // 4
  OB_JSON_TYPE_EXTRA_DATA,            // 5
  OB_JSON_TYPE_TYPE_ERROR,            // 6
  OB_JSON_TYPE_IMPLICIT,              // 7
  OB_JSON_TYPE_DOT,                   // 8
} JsnValueMisMatch;

struct ObJsonCastParam {
  ObJsonCastParam(ObObjType dst_type, ObCollationType in_coll_type, ObCollationType dst_coll_type, int8_t ascii_type) :
    in_coll_type_(in_coll_type),
    dst_coll_type_(dst_coll_type),
    dst_type_(dst_type),
    ascii_type_(ascii_type),
    is_quote_(false),
    is_const_(false),
    is_trunc_(false),
    is_pretty_(false),
    is_only_check_(false),
    relaxed_time_convert_(false),
    rt_expr_(nullptr)
  {}
  ~ObJsonCastParam() {}
  ObCollationType in_coll_type_;
  ObCollationType dst_coll_type_;
  ObObjType dst_type_;
  int8_t ascii_type_;
  bool is_quote_;
  bool is_const_;
  bool is_trunc_;
  bool is_pretty_;
  bool is_only_check_; // only check cast, not set result
  bool relaxed_time_convert_; // relaxed_time_convert_ for json_table and multivalue index.
  const ObExpr *rt_expr_; // get nls format expr
};

struct ObExprJsonQueryParamInfo : public ObIExprExtraInfo
{
  OB_UNIS_VERSION(1);
public:
  ObExprJsonQueryParamInfo(common::ObIAllocator &alloc, ObExprOperatorType type)
      : ObIExprExtraInfo(alloc, type), allocator_(alloc), truncate_(0),
        format_json_(0), wrapper_(0), empty_type_(0), error_type_(0),
        pretty_type_(0), ascii_type_(0), scalars_type_(0),
        path_str_(), on_mismatch_(alloc),
        on_mismatch_type_(alloc), j_path_(nullptr)
  {
  }
  virtual int deep_copy(common::ObIAllocator &allocator,
                        const ObExprOperatorType type,
                        ObIExprExtraInfo *&copied_info) const override;
  int init_jsn_val_expr_param(ObIAllocator &alloc, ObExprCGCtx &op_cg_ctx, const ObRawExpr* raw_expr);
  int init_jsn_query_expr_param(ObIAllocator &alloc, ObExprCGCtx &op_cg_ctx, const ObRawExpr* raw_expr);
  int get_int_val_from_raw(ObIAllocator &alloc, ObExecContext *exec_ctx, const ObRawExpr* raw_expr, ObObj &const_data);
  int init_mismatch_array(const ObRawExpr* raw_expr,
                          ObExecContext *exec_ctx);
  int parse_json_path(ObString path_str, ObJsonPath*& j_path_);

  common::ObIAllocator &allocator_;
  int8_t truncate_;
  int8_t format_json_;
  int8_t wrapper_;
  int8_t empty_type_;
  int8_t error_type_;
  int8_t pretty_type_;
  int8_t ascii_type_;
  int8_t scalars_type_;
  ObString path_str_;
  common::ObFixedArray<int8_t, common::ObIAllocator> on_mismatch_;
  common::ObFixedArray<int8_t, common::ObIAllocator> on_mismatch_type_;
  ObJsonPath *j_path_;
};

class ObJsonUtil final
{
public:
  static int get_query_item_method_null_option(ObJsonPath* j_path,
                                            ObIJsonBase* j_base);
  static int cast_to_res(common::ObIAllocator *allocator,
                         ObEvalCtx &ctx,
                         ObIJsonBase *j_base,
                         common::ObAccuracy &accuracy,
                         ObJsonCastParam &cast_param,
                         ObDatum &res,
                         uint8_t &is_type_mismatch);
  static int set_lob_datum(common::ObIAllocator *allocator,
                            const ObExpr &expr,
                            ObEvalCtx &ctx,
                            ObObjType dst_type,
                            uint8_t ascii_type,
                            ObDatum &res);
  static int time_scale_check(const common::ObAccuracy &accuracy, int64_t &value,
                              bool strict = false);
  static bool type_cast_to_string(ObString &json_string,
                                  common::ObIAllocator *allocator,
                                  ObEvalCtx &ctx,
                                  ObIJsonBase *j_base,
                                  common::ObAccuracy &accuracy);
  static int cast_to_number_type(common::ObIAllocator *allocator,
                                ObIJsonBase *j_base,
                                common::ObAccuracy &accuracy,
                                ObJsonCastParam &cast_param,
                                uint8_t &is_type_mismatch,
                                number::ObNumber &val);

  static int cast_json_scalar_to_sql_obj(common::ObIAllocator *allocator,
                                         ObExecContext* exec_ctx,
                                         ObIJsonBase *j_base,
                                         ObExprResType col_res_type,
                                         ObObj &res_obj);

  static int cast_json_scalar_to_sql_obj(common::ObIAllocator *allocator,
                                         ObEvalCtx& eval_ctx,
                                         ObIJsonBase *j_base,
                                         ObCollationType collation,
                                         ObAccuracy &accuracy,
                                         ObObjType obj_type,
                                         ObScale scale,
                                         ObObj &res_obj);

  typedef int (*ObItemMethodValid)(ObIJsonBase*& in,
                                  bool &is_null_result,
                                  common::ObIAllocator *allocator,
                                  uint8_t &is_type_mismatch);
  typedef int (*ObJsonCastSqlDatum)(common::ObIAllocator *allocator,
                                    ObEvalCtx &ctx,
                                    ObIJsonBase *j_base,
                                    common::ObAccuracy &accuracy,
                                    ObJsonCastParam &cast_param,
                                    ObDatum &res,
                                    uint8_t &is_type_mismatch,
                                    const ObExpr *rt_expr);
  typedef int (*ObJsonCastSqlObj)(common::ObIAllocator *allocator,
                                  ObEvalCtx &ctx,
                                  ObIJsonBase *j_base,
                                  common::ObAccuracy &accuracy,
                                  ObJsonCastParam &cast_param,
                                  ObObj &res,
                                  uint8_t &is_type_mismatch,
                                  const ObExpr *rt_expr);
  typedef int (*ObJsonCastSqlScalar)(common::ObIAllocator *allocator,
                                    ObEvalCtx &ctx,
                                    ObIJsonBase *j_base,
                                    common::ObAccuracy &accuracy,
                                    ObJsonCastParam &cast_param,
                                    ObDatum &res,
                                    uint8_t &is_type_mismatch);
  static int get_accuracy_internal(common::ObAccuracy &accuracy,
                                  ObEvalCtx& ctx,
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
  static ObJsonUtil::ObItemMethodValid get_item_method_cast_res_func(ObJsonPath* j_path,
                                          ObIJsonBase* j_base);
  static ObJsonUtil::ObJsonCastSqlObj get_json_obj_cast_func(ObObjType dst_type);
  static ObJsonUtil::ObJsonCastSqlDatum get_json_datum_cast_func(ObObjType dst_type);
  static ObJsonUtil::ObJsonCastSqlScalar get_json_cast_func(ObObjType dst_type);
  static int get_json_path(ObExpr* expr,
                          ObEvalCtx &ctx,
                          bool &is_null_result,
                          ObJsonParamCacheCtx *&param_ctx,
                          common::ObIAllocator &temp_allocator,
                          bool &is_cover_by_error);
  static int get_json_doc(ObExpr *expr,
                          ObEvalCtx &ctx,
                          common::ObIAllocator &allocator,
                          ObIJsonBase*& j_base,
                          bool &is_null, bool & is_cover_by_error,
                          bool relax = false);
  static bool is_number_item_method(ObJsonPath* j_path);
  static int bit_length_check(const ObAccuracy &accuracy,
                              uint64_t &value);
  static int padding_char_for_cast(int64_t padding_cnt,
                                   const ObCollationType &padding_cs_type,
                                   ObIAllocator &alloc,
                                   ObString &padding_res);
  static int set_mismatch_val(ObIArray<int8_t>& val, ObIArray<int8_t>& type, int64_t& opt_val, uint32_t& pos);
  template<typename T>
  static void wrapper_set_uint(ObObjType type, uint64_t val, T& obj);
  template<typename T>
  static void wrapper_set_string(ObObjType type, ObString& val, T& obj);
  template<typename T>
  static void wrapper_set_timestamp_tz(ObObjType type, ObOTimestampData val, T& obj);
  template<typename T>
  static void wrapper_set_decimal_int(const ObDecimalInt *decint, ObScale scale, int32_t int_bytes, T& obj);

  static int init_json_path(ObIAllocator &alloc, ObExprCGCtx &op_cg_ctx,
                            const ObRawExpr* path,
                            ObExprJsonQueryParamInfo& res);
private:
  DISALLOW_COPY_AND_ASSIGN(ObJsonUtil);
};

} // sql
} // oceanbase
#endif // OCEANBASE_SQL_OB_EXPR_JSON_UTILS_H_

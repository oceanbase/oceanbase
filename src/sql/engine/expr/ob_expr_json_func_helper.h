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
 * This file is for define of func json expr helper
 */

#ifndef OCEANBASE_SQL_OB_EXPR_JSON_FUNC_HELPER_H_
#define OCEANBASE_SQL_OB_EXPR_JSON_FUNC_HELPER_H_

#include "sql/engine/expr/ob_expr_util.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"
#include "share/object/ob_obj_cast.h"
#include "objit/common/ob_item_type.h"
#include "sql/session/ob_sql_session_info.h"
#include "lib/json_type/ob_json_tree.h"
#include "lib/json_type/ob_json_base.h"
#include "lib/json_type/ob_json_bin.h"
#include "lib/json_type/ob_json_parse.h"
#include "sql/engine/expr/ob_expr_result_type_util.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace sql
{
class ObJsonExprHelper final
{
  class ObJsonPathCacheCtx : public ObExprOperatorCtx
  {
  public:
    ObJsonPathCacheCtx(common::ObIAllocator *allocator)
      : ObExprOperatorCtx(),
        path_cache_(allocator)
    {}
    virtual ~ObJsonPathCacheCtx() {}
    ObJsonPathCache *get_path_cache() { return &path_cache_; }
  private:
    ObJsonPathCache path_cache_;
  };
public:
  static int get_json_or_str_data(ObExpr *expr, ObEvalCtx &ctx,
                                  common::ObArenaAllocator &allocator,
                                  ObString& str, bool& is_null);
  /*
  get json doc to JsonBase in static_typing_engine
  @param[in]  expr       the input arguments
  @param[in]  ctx        the eval context
  @param[in]  allocator  the Allocator in context
  @param[in]  index      the input arguments index
  @param[out] j_base     the pointer to JsonBase
  @param[out] is_null    the flag for null situation
  @return Returns OB_SUCCESS on success, error code otherwise.
  */
  static int get_json_doc(const ObExpr &expr, ObEvalCtx &ctx,
                          common::ObArenaAllocator &allocator,
                          uint16_t index, ObIJsonBase*& j_base,
                          bool &is_null, bool need_to_tree=true, bool relax = true);

  static int cast_to_json_tree(ObString &text, common::ObIAllocator *allocator, uint32_t parse_flag = 0);
  /*
  get json value to JsonBase in static_typing_engine
  @param[in]  expr       the input arguments
  @param[in]  ctx        the eval context
  @param[in]  allocator  the Allocator in context
  @param[in]  index      the input arguments index
  @param[out] j_base     the pointer to JsonBase
  @param[in]  to_bin     expect value is binary or not
  @return Returns OB_SUCCESS on success, error code otherwise.
  */
  static int get_json_val(const ObExpr &expr, ObEvalCtx &ctx,
                          common::ObIAllocator *allocator, uint16_t index,
                          ObIJsonBase*& j_base, bool to_bin = false, bool format_json = false,
                          uint32_t parse_flag = ObJsonParser::JSN_RELAXED_FLAG);

  /*
  get json value to JsonBase in old_typing_engine
  @param[in]  data       the input argument
  @param[in]  is_bool    whether input data is JsonBoolean
  @param[in]  ctx        the eval context
  @param[in]  allocator  the Allocator in context
  @param[in]  to_bin     expect value is binary or not
  @param[out] j_base     the pointer to JsonBase
  @return Returns OB_SUCCESS on success, error code otherwise.
  */
  static int get_json_val(const common::ObObj &data, ObExprCtx &ctx,
                          bool is_bool, common::ObIAllocator *allocator,
                          ObIJsonBase*& j_base, bool to_bin = false);
  static int oracle_datum2_json_val(const ObDatum *json_datum,  ObObjMeta& data_meta, common::ObIAllocator *allocator,
                                    ObBasicSessionInfo *session, ObIJsonBase*& j_base, bool is_bool_data_type,
                                    bool format_json = false, bool is_strict = false, bool is_bin = false);

  static int eval_oracle_json_val(ObExpr *expr, ObEvalCtx &ctx, common::ObIAllocator *allocator,
                                ObIJsonBase*& j_base, bool format_json = false, bool is_strict = false, bool is_bin = false, bool is_absent_null = false);
 
  /*
  replace json_old with json_new in json_doc
  @param[in]  json_old   the old json node to be replaced
  @param[in]  json_new   the new json node to replace
  @param[in]  json_doc   the root json node of old json node
  @return Returns OB_SUCCESS on success, error code otherwise.
  */
  static int json_base_replace(ObIJsonBase *json_old, ObIJsonBase *json_new,
                               ObIJsonBase *&json_doc);

  static int find_and_add_cache(ObJsonPathCache* path_cache, ObJsonPath*& res_path,
                                ObString& path_str, int arg_idx, bool enable_wildcard);

  static ObJsonPathCache* get_path_cache_ctx(const uint64_t& id, ObExecContext *exec_ctx);

  static int is_json_zero(const ObString& data, int& result);
  
  /*
  try to transfrom scalar data to jsonBase
  @param[in]  datum          the input datum
  @param[in]  type           the type of input argument
  @param[in]  allocator      the Allocator in context
  @param[in]  scale          the scale for json decimal
  @param[in]  ObTimeZoneInfo the timeZone info
  @param[out] j_base        the pointer to ObIJsonBase
  @param[in]  to_bin         whether convert to binary
  @return Returns OB_SUCCESS on success, error code otherwise.
  */
  template <typename T>
  static int transform_scalar_2jsonBase(const T &datum,
                                        ObObjType type,
                                        common::ObIAllocator *allocator,
                                        ObScale scale,
                                        const ObTimeZoneInfo *tz_info,
                                        ObBasicSessionInfo *session,
                                        ObIJsonBase*& j_base,
                                        bool to_bin,
                                        bool is_bool = false);
  /*
  try to transfrom from type which is_convertible_to_json to jsonBase
  @param[in]  datum          the input datum
  @param[in]  type           the type of input argument
  @param[in]  allocator      the Allocator in context
  @param[in]  cs_type        the collation type
  @param[out] j_base         the pointer to jsonBase
  @param[in]  to_bin         whether convert to binary
  @param[in]  deep_copy      whether deep copy input string from datum
  @return Returns OB_SUCCESS on success, error code otherwise.
  */
  template <typename T>
  static int transform_convertible_2jsonBase(const T &datum,
                                             ObObjType type,
                                             common::ObIAllocator *allocator,
                                             ObCollationType cs_type,
                                             ObIJsonBase*& j_base,
                                             bool to_bin,
                                             bool has_lob_header,
                                             bool deep_copy = false,
                                             bool relax_type = true,
                                             bool format_json = false);

  static bool is_cs_type_bin(ObCollationType &cs_type);
  static int get_timestamp_str_in_oracle_mode(ObEvalCtx &ctx,
                                              const ObDatum &datum,
                                              ObObjType type,
                                              ObScale scale,
                                              const ObTimeZoneInfo *tz_info,
                                              ObJsonBuffer &j_buf);

  static bool is_convertible_to_json(ObObjType &type);
  static int is_valid_for_json(ObExprResType* types_stack, uint32_t index, const char* func_name);
  static int is_valid_for_json(ObExprResType& type, uint32_t index, const char* func_name);
  static int is_valid_for_path(ObExprResType* types_stack, uint32_t index);
  static void set_type_for_value(ObExprResType* types_stack, uint32_t index);
  static int ensure_collation(ObObjType type, ObCollationType cs_type);
  static ObJsonInType get_json_internal_type(ObObjType type);
  static int convert_string_collation_type(ObCollationType in,
                                           ObCollationType dst,
                                           ObIAllocator* allocator,
                                           ObString& in_str,
                                           ObString &out_str);
  template <typename T>
  static int pack_json_str_res(const ObExpr &expr,
                               ObEvalCtx &ctx,
                               ObDatum &res,
                               T &str,
                               common::ObIAllocator *allocator = nullptr)
  {
    int ret = OB_SUCCESS;
    ObTextStringDatumResult text_result(expr.datum_meta_.type_, &expr, &ctx, &res);
    if (OB_FAIL(text_result.init(str.length(), allocator))) {
      LOG_WARN("init lob result failed");
    } else if (OB_FAIL(text_result.append(str.ptr(), str.length()))) {
      LOG_WARN("failed to append realdata", K(ret), K(str), K(text_result));
    } else {
      text_result.set_result();
    }
    return ret;
  }

  /**
   * the following 3 functions is used for json_query and json_mergepatch
   * as the returning type is the same
   *
   *  get_cast_type
   *  set_dest_type
   *  get_cast_string_len
   *
  */
  static int get_cast_type(const ObExprResType param_type2, ObExprResType &dst_type);
  // check item function and returning type
  static int check_item_func_with_return(ObJsonPathNodeType path_type, ObObjType dst_type, common::ObCollationType dst_coll_type, int8_t JSON_EXPR_FLAG);
  static int set_dest_type(ObExprResType &type1, ObExprResType &type,
                           ObExprResType &dst_type, ObExprTypeCtx &type_ctx);
  static int get_expr_option_value(const ObExprResType param_type2, int64_t &dst_type);
  static int calc_asciistr_in_expr(const ObString &src,
                                  const ObCollationType src_cs_type,
                                  const ObCollationType dst_cs_type,
                                  char* buf, const int64_t buf_len, int32_t &pos);
  static int get_dest_type(const ObExpr &expr, int64_t pos,
                          ObEvalCtx& ctx,
                          ObObjType &dest_type, int32_t &dst_len);
  static int get_cast_string_len(ObExprResType &type1,
                                 ObExprResType &type2,
                                 common::ObExprTypeCtx &type_ctx,
                                 int32_t &res_len,
                                 int16_t &length_semantics,
                                 common::ObCollationType conn);
  static int eval_and_check_res_type(int64_t value, ObObjType& type, int32_t& dst_len);
  static int parse_res_type(ObExprResType& type1,
                            ObExprResType& json_res_type,
                            ObExprResType& result_type,
                            ObExprTypeCtx& type_ctx);
  static int parse_asc_option(ObExprResType& asc,
                              ObExprResType& type1,
                              ObExprResType& res_type,
                              ObExprTypeCtx& type_ctx);
  static int pre_default_value_check(ObObjType dst_type, ObString time_str, ObObjType val_type);

  static int character2_ascii_string(common::ObIAllocator *allocator,
                                     const ObExpr &expr,
                                     ObEvalCtx &ctx,
                                     ObString& result,
                                     int32_t reserve_len = 0);
private:
  const static uint32_t RESERVE_MIN_BUFF_SIZE = 32;
  DISALLOW_COPY_AND_ASSIGN(ObJsonExprHelper);
};

} // sql
} // oceanbase
#endif // OCEANBASE_SQL_OB_EXPR_JSON_FUNC_HELPER_H_
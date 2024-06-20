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
#include "lib/lob/ob_lob_base.h"
#include "lib/json_type/ob_json_tree.h"
#include "lib/json_type/ob_json_base.h"
#include "lib/json_type/ob_json_bin.h"
#include "lib/json_type/ob_json_parse.h"
#include "lib/json_type/ob_json_schema.h"
#include "lib/json_type/ob_json_diff.h"
#include "sql/engine/expr/ob_expr_result_type_util.h"
#include "storage/lob/ob_lob_util.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace storage
{
class ObLobCursor;
}

namespace sql
{
const static int32_t OB_LITERAL_MAX_INT_LEN = 21;

struct ObJsonExprParam {
public:
  ObJsonExprParam()
  : truncate_(0),
    format_json_(0),
    wrapper_(0),
    empty_type_(0),
    error_type_(0),
    is_empty_default_const_(false),
    is_error_default_const_(false),
    is_alias_(false),
    is_multivalue_(false),
    empty_val_(),
    error_val_(),
    on_mismatch_(),
    on_mismatch_type_(),
    accuracy_(),
    dst_type_(),
    pretty_type_(0),
    ascii_type_(0),
    scalars_type_(0),
    path_str_(),
    json_path_(nullptr),
    is_init_from_cache_(false)
  {}
  virtual ~ObJsonExprParam() {}
public:
  int8_t truncate_;
  int8_t format_json_;
  int8_t wrapper_;
  int8_t empty_type_;
  int8_t error_type_;
  bool is_empty_default_const_;
  bool is_error_default_const_;
  bool is_alias_;
  bool is_multivalue_;
  ObDatum *empty_val_;
  ObDatum *error_val_;
  common::ObSEArray<int8_t, 1, common::ModulePageAllocator, true> on_mismatch_;
  common::ObSEArray<int8_t, 1, common::ModulePageAllocator, true> on_mismatch_type_;
  ObAccuracy accuracy_;
  ObObjType dst_type_;
  int8_t pretty_type_;
  int8_t ascii_type_;
  int8_t scalars_type_;
  ObString path_str_;
  ObJsonPath* json_path_;
  bool is_init_from_cache_;
};

class ObJsonParamCacheCtx : public ObExprOperatorCtx
{
  public:
  ObJsonParamCacheCtx(common::ObIAllocator *allocator)
    : ObExprOperatorCtx(),
      path_cache_(allocator),
      is_first_exec_(true),
      is_json_path_const_(false),
      json_param_()
  {}
  virtual ~ObJsonParamCacheCtx() {}
  ObJsonPathCache *get_path_cache() { return &path_cache_; }

private:
  ObJsonPathCache path_cache_;
public:
  bool is_first_exec_;
  bool is_json_path_const_;
  ObJsonExprParam json_param_;
};

struct ObConv2JsonParam {
  ObConv2JsonParam(bool to_bin, bool has_lob_header) :
  to_bin_(to_bin),
  has_lob_header_(has_lob_header),
  deep_copy_(false),
  relax_type_(true),
  format_json_(false),
  is_schema_(false)
  {}
  ObConv2JsonParam(bool to_bin, bool has_lob_header, bool deep_copy) :
  to_bin_(to_bin),
  has_lob_header_(has_lob_header),
  deep_copy_(deep_copy),
  relax_type_(true),
  format_json_(false),
  is_schema_(false)
  {}
  ObConv2JsonParam(bool to_bin, bool has_lob_header, bool deep_copy, bool relax_type) :
  to_bin_(to_bin),
  has_lob_header_(has_lob_header),
  deep_copy_(deep_copy),
  relax_type_(relax_type),
  format_json_(false),
  is_schema_(false)
  {}
  ObConv2JsonParam(bool to_bin, bool has_lob_header, bool deep_copy, bool relax_type, bool format_json) :
  to_bin_(to_bin),
  has_lob_header_(has_lob_header),
  deep_copy_(deep_copy),
  relax_type_(relax_type),
  format_json_(format_json),
  is_schema_(false)
  {}
  ObConv2JsonParam(bool to_bin, bool has_lob_header, bool deep_copy, bool relax_type, bool format_json, bool is_schema) :
  to_bin_(to_bin),
  has_lob_header_(has_lob_header),
  deep_copy_(deep_copy),
  relax_type_(relax_type),
  format_json_(format_json),
  is_schema_(is_schema)
  {}

  ObConv2JsonParam(bool to_bin, bool has_lob_header, bool deep_copy, bool relax_type, bool format_json, bool is_schema, bool is_wrap_fail) :
    to_bin_(to_bin),
    has_lob_header_(has_lob_header),
    deep_copy_(deep_copy),
    relax_type_(relax_type),
    format_json_(format_json),
    is_schema_(is_schema),
    wrap_on_fail_(is_wrap_fail) {}
  bool to_bin_;
  bool has_lob_header_;
  bool deep_copy_ = false;
  bool relax_type_ = true;
  bool format_json_ = false;
  bool is_schema_ = false;
  bool wrap_on_fail_ = false;
};

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
  class ObJsonSchemaCacheCtx : public ObExprOperatorCtx
  {
  public:
    ObJsonSchemaCacheCtx(common::ObIAllocator *allocator)
      : ObExprOperatorCtx(),
        schema_cache_(allocator)
    {}
    virtual ~ObJsonSchemaCacheCtx() {}
    ObJsonSchemaCache *get_schema_cache() { return &schema_cache_; }
  private:
    ObJsonSchemaCache schema_cache_;
  };
public:
  static ObJsonParamCacheCtx* get_param_cache_ctx(const uint64_t& id, ObExecContext *exec_ctx);
  static int get_json_or_str_data(ObExpr *expr, ObEvalCtx &ctx,
                                  common::ObIAllocator &allocator,
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
                          bool &is_null, bool need_to_tree=true,
                          bool relax = true, bool preserve_dup = false);
  static int get_json_schema(const ObExpr &expr, ObEvalCtx &ctx,
                            common::ObArenaAllocator &allocator,
                            uint16_t index, ObIJsonBase*& j_base,
                            bool &is_null);


  static int get_json_for_partial_update(
      const ObExpr &expr,
      const ObExpr &json_expr,
      ObEvalCtx &ctx,
      ObIAllocator &allocator,
      ObDatum &json_datum,
      ObIJsonBase *&j_base);

  static int get_partial_json_bin(
      ObIAllocator &allocator,
      ObILobCursor *cursor,
      ObJsonBinUpdateCtx *update_ctx,
      ObIJsonBase *&j_base);
  static int get_json_for_partial_update_with_curosr(
      const ObExpr &expr,
      const ObExpr &json_expr,
      ObEvalCtx &ctx,
      ObIAllocator &allocator,
      ObDatum &json_datum,
      ObIJsonBase *&j_base);


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
  static int get_const_json_schema(const common::ObObj &data, const char* func_name,
                                   common::ObIAllocator *allocator, ObIJsonBase*& j_schema);

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
  static int get_json_val(const common::ObObj &data, ObExecContext *ctx,
                          bool is_bool, common::ObIAllocator *allocator,
                          ObIJsonBase*& j_base, bool to_bin= false);
  static int refine_range_json_value_const(
                          const common::ObObj &data, ObExecContext *ctx,
                          bool is_bool, common::ObIAllocator *allocator,
                          ObIJsonBase*& j_base, bool to_bin = false);
  static int get_json_val(const common::ObDatum &data,
                          ObExecContext &ctx,
                          ObExpr* expr,
                          common::ObIAllocator *allocator,
                          ObObjType val_type,
                          ObCollationType &cs_type,
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
                                ObString& path_str, int arg_idx, bool enable_wildcard,
                                bool is_const = false);
  static int find_and_add_schema_cache(ObJsonSchemaCache* schema_cache, ObIJsonBase*& j_schema,
                                      ObString& schema_str, int arg_idx, const ObJsonInType& in_type);

  static ObJsonPathCache* get_path_cache_ctx(const uint64_t& id, ObExecContext *exec_ctx);
  static ObJsonSchemaCache* get_schema_cache_ctx(const uint64_t& id, ObExecContext *exec_ctx);

  static int is_json_zero(const ObString& data, int& result);

  static int is_json_true(const ObString& data, int& result);

  static int get_sql_scalar_type(ObEvalCtx& ctx,
                                 int64_t origin,
                                 ObObjType& scalar_type,
                                 int32_t& scalar_len,
                                 int32_t& precision,
                                 int32_t& scale,
                                 ObAccuracy& accuracy,
                                 ObLengthSemantics& length_semantics);
  
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
                                             ObConv2JsonParam flags);

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

  static int pack_json_res(
      const ObExpr &expr,
      ObEvalCtx &ctx,
      ObIAllocator &temp_allocator,
      ObIJsonBase *json_doc,
      ObDatum &res);

  /**
   * the following 3 functions is used for json_query and json_mergepatch
   * as the returning type is the same
   *
   *  get_cast_type
   *  set_dest_type
   *  get_cast_string_len
   *
  */
  static int get_cast_type(const ObExprResType param_type2, ObExprResType &dst_type, ObExprTypeCtx &type_ctx);
  // check item function and returning type
  static int check_item_func_with_return(ObJsonPathNodeType path_type, ObObjType dst_type, common::ObCollationType dst_coll_type, int8_t JSON_EXPR_FLAG);
  static int set_dest_type(ObExprResType &type1, ObExprResType &type,
                           ObExprResType &dst_type, ObExprTypeCtx &type_ctx);
  static int get_expr_option_value(const ObExprResType param_type2, int8_t &dst_type);
  static int calc_asciistr_in_expr(const ObString &src,
                                  const ObCollationType src_cs_type,
                                  const ObCollationType dst_cs_type,
                                  char* buf, const int64_t buf_len, int32_t &pos);
  static int get_dest_type(const ObExpr &expr, int64_t pos,
                          ObEvalCtx& ctx,
                          ObObjType &dest_type, int64_t &dst_len);

  static int get_cast_inttc_len(ObExprResType &type1,
                                ObExprResType &type2,
                                common::ObExprTypeCtx &type_ctx,
                                int32_t &res_len,
                                int16_t &length_semantics,
                                common::ObCollationType conn);
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
  static int pre_default_value_check(ObObjType dst_type, ObString val_str, ObObjType val_type, size_t length);

  static int character2_ascii_string(common::ObIAllocator *allocator,
                                     const ObExpr &expr,
                                     ObEvalCtx &ctx,
                                     ObString& result,
                                     int32_t reserve_len = 0);
  static int cast_to_res(ObIAllocator &allocator,
                          ObDatum &src_datum,
                          const ObExpr &expr,
                          const ObExpr &default_expr,
                          ObEvalCtx &ctx,
                          ObDatum &res,
                          bool xt_need_acc_check);
  static void get_accuracy_from_expr(const ObExpr &expr, ObAccuracy &accuracy);
  static int get_clause_opt(ObExpr *expr,
                            ObEvalCtx &ctx,
                            int8_t &type);

  static int is_allow_partial_update(const ObExpr &expr, ObEvalCtx &ctx, const ObString &locator_str, bool &allow_partial_update);
  static bool is_json_partial_update_mode(const ObExpr &expr);
  static bool is_json_partial_update_mode(const uint64_t flag) { return (flag & OB_JSON_PARTIAL_UPDATE_ALLOW) != 0; }
  static bool is_json_partial_update_last_expr(const uint64_t flag) { return (flag & OB_JSON_PARTIAL_UPDATE_LAST_EXPR) != 0; }
  static bool is_json_partial_update_first_expr(const uint64_t flag) { return (flag & OB_JSON_PARTIAL_UPDATE_FIRST_EXPR) != 0; }

  static int pack_json_diff_res(
    const ObExpr &expr,
    ObEvalCtx &ctx,
    ObIAllocator &temp_allocator,
    ObIJsonBase *json_doc,
    ObDatum &res);

  static int refresh_root_when_bin_rebuild_all(ObIJsonBase *j_base);

  static int init_json_expr_extra_info(
      ObIAllocator *allocator,
      const ObRawExpr &raw_expr,
      const ObExprOperatorType type,
      ObExpr &rt_expr);

  static int get_session_query_timeout_ts(ObEvalCtx &ctx, int64_t &timeout_ts);

private:
  const static uint32_t RESERVE_MIN_BUFF_SIZE = 32;
  DISALLOW_COPY_AND_ASSIGN(ObJsonExprHelper);
};

class ObJsonDeltaLob : public ObDeltaLob {
public:
  ObJsonDeltaLob():
    allocator_(nullptr),
    update_ctx_(nullptr),
    j_base_(nullptr),
    cursor_(nullptr),
    partial_data_(nullptr),
    query_timeout_ts_(0)
  {}

  int init(ObJsonBin *j_bin);
  int init(ObIAllocator *allocator, ObLobLocatorV2 locator, int64_t query_timeout_ts);
  void reset();

  int64_t get_partial_data_serialize_size() const;
  int64_t get_lob_diff_serialize_size() const;
  uint32_t get_lob_diff_cnt() const;

  int serialize_partial_data(char* buf, const int64_t buf_len, int64_t& pos) const;
  int deserialize_partial_data(storage::ObLobDiffHeader *diff_header);
  int serialize_lob_diffs(char* buf, const int64_t buf_len, storage::ObLobDiffHeader *diff_header) const;
  int deserialize_lob_diffs(char* buf, const int64_t buf_len, storage::ObLobDiffHeader *diff_header);

  int check_binary_diff() const;
  ObIJsonBase* get_json_bin() { return j_base_; }
  storage::ObLobDiff::DiffType get_diff_type() const { return  storage::ObLobDiff::DiffType::WRITE_DIFF; }

protected:
  ObIAllocator *allocator_;
  ObJsonBinUpdateCtx *update_ctx_;

  ObIJsonBase *j_base_;
  storage::ObLobCursor *cursor_;
  storage::ObLobPartialData *partial_data_;
  int64_t query_timeout_ts_;
};

struct ObJsonZeroVal
{
  static const int32_t OB_JSON_ZERO_VAL_LENGTH = sizeof(ObLobCommon) + 2;
  ObJsonZeroVal() : header_(), json_bin_() {
    json_bin_[0] = '\0';
    json_bin_[1] = '\0';
  }
  ObLobCommon header_;
  char json_bin_[4];
};

} // sql
} // oceanbase
#endif // OCEANBASE_SQL_OB_EXPR_JSON_FUNC_HELPER_H_
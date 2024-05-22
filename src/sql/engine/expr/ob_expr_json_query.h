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
 * This file is for func json_query.
 */

#ifndef OCEANBASE_SRC_SQL_ENGINE_EXPR_OB_EXPR_JSON_QUERY_H
#define OCEANBASE_SRC_SQL_ENGINE_EXPR_OB_EXPR_JSON_QUERY_H

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
class
ObExprJsonQuery : public ObFuncExprOperator
{
public:
    explicit ObExprJsonQuery(common::ObIAllocator &alloc);
    virtual ~ObExprJsonQuery();
    virtual int calc_result_typeN(ObExprResType& type,
                              ObExprResType* types,
                              int64_t param_num,
                              common::ObExprTypeCtx& type_ctx)
                              const override;
    static int eval_json_query(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
    virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                    ObExpr &rt_expr) const override;
    virtual common::ObCastMode get_cast_mode() const { return CM_ERROR_ON_SCALE_OVER;}
  virtual bool need_rt_ctx() const override { return true; }
private:
  static int calc_returning_type(ObExprResType& type,
                                  ObExprResType* types_stack,
                                  ObExprTypeCtx& type_ctx,
                                  ObExprResType& dst_type,
                                  common::ObIAllocator *allocator,
                                  bool is_json_input);
  static bool try_set_error_val(common::ObIAllocator *allocator, ObEvalCtx &ctx,
                                ObJsonExprParam* json_param,
                                const ObExpr &expr, ObDatum &res, int &ret);
  static int set_result(ObJsonExprParam* json_param,
                        ObIJsonBase *jb_res,
                        common::ObIAllocator *allocator,
                        ObEvalCtx &ctx,
                        const ObExpr &expr,
                        ObDatum &res);
  static int get_dest_type(const ObExpr &expr,
                          int32_t &dst_len,
                          ObEvalCtx& ctx,
                          ObObjType &dest_type,
                          bool &is_cover_by_error);
  static int set_multivalue_result(ObEvalCtx& ctx,
                                   ObIAllocator& allocator,
                                   ObIJsonBase* json_base,
                                   const ObExpr &expr,
                                   uint8_t opt_error,
                                   ObCollationType in_coll_type,
                                   ObCollationType dst_coll_type,
                                   ObDatum *on_error,
                                   ObAccuracy &accuracy,
                                   ObJsonCastParam &cast_param,
                                   ObDatum &res);
  static int get_clause_opt(const ObExpr &expr,
                            ObEvalCtx &ctx,
                            uint8_t index,
                            bool &is_cover_by_error,
                            uint8_t &type,
                            uint8_t size_para);
  /*
  oracle mode get json path to JsonBase in static_typing_engine
  @param[in]  expr       the input arguments
  @param[in]  ctx        the eval context
  @param[in]  allocator  the Allocator in context
  @param[in]  index      the input arguments index
  @param[out] j_path     the pointer to JsonPath
  @param[out] is_null    the flag for null situation
  @param[out] is_cover_by_error    the flag for whether need cover by error clause
  @return Returns OB_SUCCESS on success, error code otherwise.
  */
  static int get_ora_json_path(const ObExpr &expr, ObEvalCtx &ctx,
                          common::ObArenaAllocator &allocator, ObJsonPath*& j_path,
                          uint16_t index, bool &is_null, bool &is_cover_by_error,
                          ObDatum*& json_datum);

    /*
  oracle mode get json doc to JsonBase in static_typing_engine
  @param[in]  expr       the input arguments
  @param[in]  ctx        the eval context
  @param[in]  allocator  the Allocator in context
  @param[in]  index      the input arguments index
  @param[out] j_base     the pointer to JsonBase
  @param[out] j_in_type     the pointer to input type
  @param[out] is_null    the flag for null situation
  @param[out] is_cover_by_error    the flag for whether need cover by error clause
  @return Returns OB_SUCCESS on success, error code otherwise.
  */
  static int get_ora_json_doc(const ObExpr &expr, ObEvalCtx &ctx,
                          common::ObArenaAllocator &allocator,
                          uint16_t index, ObIJsonBase*& j_base,
                           ObObjType dst_type,
                          bool &is_null, bool &is_cover_by_error);

  static int get_clause_pre_asc_sca_opt(const ObExpr &expr, ObEvalCtx &ctx,
                                        bool &is_cover_by_error, uint8_t &pretty_type,
                                        uint8_t &ascii_type, uint8_t &scalars_type);
  static int check_enable_cast_index_array(ObIJsonBase* json_base, bool disable_container, ObObjType dest_type);

public:
  static int get_empty_option(bool &is_cover_by_error,
                            int8_t empty_type, bool &is_null_result,
                            bool &is_json_arr, bool &is_json_obj);
  static int get_single_obj_wrapper(int8_t wrapper_type, int8_t &use_wrapper, ObJsonNodeType in_type, int8_t scalars_type);
  static int get_multi_scalars_wrapper_type(int8_t wrapper_type, int8_t &use_wrapper);
  static int get_clause_param_value(const ObExpr &expr, ObEvalCtx &ctx,
                            ObJsonExprParam* json_param, int64_t &dst_len,
                            bool &is_cover_by_error);
  static int check_params_valid(const ObExpr &expr, ObJsonExprParam* json_param,
                                bool &is_cover_by_error);
  static int check_item_method_valid_with_wrapper(ObJsonPath *j_path, int8_t wrapper_type);
  static int append_node_into_res(ObIJsonBase*& jb_res, ObJsonPath* j_path,
                          ObJsonSeekResult &hits, common::ObIAllocator *allocator);
  static int append_binary_node_into_res(ObIJsonBase*& jb_res,
                                         ObJsonPath* j_path,
                                         ObJsonSeekResult &hits,
                                         common::ObIAllocator *allocator);
  static int doc_do_seek(ObIJsonBase* j_base, ObJsonExprParam *json_param,
                          ObJsonSeekResult &hits, int8_t &use_wrapper,
                          bool &is_cover_by_error,
                          bool &is_null_result,
                          bool& is_json_arr,
                          bool& is_json_obj);
  static int deal_item_method_special_case(ObJsonPath* j_path,
                                            ObJsonSeekResult &hits,
                                            bool &is_null_result,
                                            size_t pos,
                                            bool use_wrapper);
  static int get_error_option(int8_t &error_type, ObIJsonBase *&error_val, ObIJsonBase *jb_arr, ObIJsonBase *jb_obj, bool &is_null);
  static int get_mismatch_option(int8_t &mismatch_type, int &ret);
  static int init_ctx_var(ObJsonParamCacheCtx*& param_ctx, ObEvalCtx &ctx, const ObExpr &expr);
  static int extract_plan_cache_param(const ObExprJsonQueryParamInfo *info, ObJsonExprParam& json_param);
  static int check_data_type_allowed(const ObExprResType* types_stack, const ObExprResType& data_type);
  /* code from ob_expr_cast for cal_result_type */
  const static int32_t OB_LITERAL_MAX_INT_LEN = 21;

  DISALLOW_COPY_AND_ASSIGN(ObExprJsonQuery);

};

struct ObJsonObjectCompare {
  int operator()(const ObObj &left, const ObObj &right)
  {
    int result = 0;
    left.compare(right, result);
    return result > 0 ? 1 : 0;
  }
};

} // sql
} // oceanbase

#endif  //OCEANBASE_SRC_SQL_ENGINE_EXPR_OB_EXPR_JSON_QUERY_H
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
#include "ob_expr_json_utils.h"
#include "ob_expr_json_func_helper.h"
#include "ob_json_param_type.h"

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
  static int init_ctx_var(ObJsonParamCacheCtx*& param_ctx, const ObExpr &expr);

  static int extract_plan_cache_param(const ObExprJsonQueryParamInfo *info, ObJsonExprParam& json_param);
/* code from ob_expr_cast for cal_result_type */
  const static int32_t OB_LITERAL_MAX_INT_LEN = 21;

  DISALLOW_COPY_AND_ASSIGN(ObExprJsonQuery);

};

} // sql
} // oceanbase

#endif  //OCEANBASE_SRC_SQL_ENGINE_EXPR_OB_EXPR_JSON_QUERY_H
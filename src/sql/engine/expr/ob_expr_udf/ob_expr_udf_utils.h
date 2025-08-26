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

#ifndef OCEANBASE_SQL_OB_EXPR_UDF_UTIL_H_
#define OCEANBASE_SQL_OB_EXPR_UDF_UTIL_H_

#include "common/object/ob_object.h"
#include "lib/container/ob_2d_array.h"
#include "sql/engine/expr/ob_expr_operator.h"
#include "sql/engine/expr/ob_i_expr_extra_info.h"
#include "sql/resolver/expr/ob_raw_expr.h"
#include "sql/engine/expr/ob_expr_result_type_util.h"
#include "ob_udf_result_cache_mgr.h"
#include "pl/ob_pl.h"

namespace oceanbase
{
namespace share
{
namespace schema
{
class ObSchemaGetterGuard;
}
}
namespace sql
{
class ObExprUDFCtx;
class ObExprUDFEnvGuard;
class ObExprUDFUtils
{
public:
  ObExprUDFUtils() {}
  ~ObExprUDFUtils() {}

  template <typename ARG_VEC>
  static int inner_vec_to_obj(ObIVector *in_vec, ObObj &obj, const ObObjMeta &meta, const int64_t batch_idx);
  template<VecValueTypeClass vec_tc>
  static int dispatch_transfer_vec_to_obj(ObObj& obj, ObIVector *arg_vec, ObObjMeta meta, int64_t idx);
  template <typename RES_VEC>
  static int inner_obj_to_vec(ObIVector *in_vec,
                              ObObj& res,
                              const int64_t batch_idx,
                              const ObExpr &expr,
                              ObEvalCtx &eval_ctx);
  template<VecValueTypeClass vec_tc>
  static int dispatch_transfer_obj_to_vec(ObObj& result,
                                          ObIVector *res_vec,
                                          int64_t idx,
                                          const ObExpr &expr,
                                          ObEvalCtx &eval_ctx);
  static int transfer_vec_to_obj(ObObj *objs, ObIVector **arg_vec, const ObExpr &expr, int64_t idx);
  static int transfer_obj_to_vec(ObObj& result,
                                  ObIVector *res_vec,
                                  int64_t idx,
                                  const ObExpr &expr,
                                  ObEvalCtx &eval_ctx);

  static int extract_allocator_and_restore_obj(
    const ObObj &obj, ObObj &new_obj, ObIAllocator *&composite_allocator);
  static int is_child_of(const ObObj &parent, const ObObj &child, bool &is_child);
  static
  int need_deep_copy_in_parameter(const ObObj *objs_stack,
                                                int64_t param_num,
                                                const ObIArray<ObUDFParamDesc> &params_desc,
                                                const ObIArray<ObExprResType> &params_type,
                                                const ObObj &element,
                                                bool &need_deep_copy);
  static
  int process_in_params(ObExprUDFCtx &udf_ctx, ObIArray<ObObj> &deep_in_objs);
  static
  int process_in_params(const ObObj *objs_stack,
                                      int64_t param_num,
                                      const ObIArray<ObExprResType> &params_type,
                                      ParamStore& iparams);
  static
  int process_return_value(ObObj &result,
                                         ObObj &tmp_result,
                                         ObEvalCtx &eval_ctx,
                                         ObExprUDFCtx &udf_ctx,
                                         ObExprUDFEnvGuard &guard);
  static
  int process_in_params(const ObObj *objs_stack,
                                      int64_t param_num,
                                      const ObIArray<ObUDFParamDesc> &params_desc,
                                      const ObIArray<ObExprResType> &params_type,
                                      ParamStore& iparams,
                                      ObIAllocator &allocator,
                                      ObIArray<ObObj> *deep_in_objs = nullptr);
  static
  int process_out_params(ObExprUDFCtx &udf_ctx, ObEvalCtx &eval_ctx);
  static
  int process_out_params(const ObObj *objs_stack,
                                       int64_t param_num,
                                       ParamStore& iparams,
                                       ObIAllocator &alloc,
                                       ObExecContext &exec_ctx,
                                       const ObIArray<int64_t> &nocopy_params,
                                       const ObIArray<ObUDFParamDesc> &params_desc,
                                       const ObIArray<ObExprResType> &params_type);
  static
  int process_singal_out_param(int64_t i,
                                             ObIArray<bool> &dones,
                                             const ObObj *objs_stack,
                                             int64_t param_num,
                                             ParamStore& iparams,
                                             ObIAllocator &alloc,
                                             ObExecContext &exec_ctx,
                                             const ObIArray<int64_t> &nocopy_params,
                                             const ObIArray<ObUDFParamDesc> &params_desc,
                                             const ObIArray<ObExprResType> &params_type);
  static
  int process_package_out_param(int64_t idx,
                                              ObIArray<bool> &dones,
                                              const ObObj *objs_stack,
                                              int64_t param_num,
                                              ParamStore& iparams,
                                              ObIAllocator &alloc,
                                              ObExecContext &exec_ctx,
                                              const ObIArray<int64_t> &nocopy_params,
                                              const ObIArray<ObUDFParamDesc> &params_desc,
                                              const ObIArray<ObExprResType> &params_type);

  static
  int ob_adjust_lob_obj(const ObObj &origin_obj,
                                              const common::ObObjMeta &obj_meta,
                                              ObIAllocator &allocator,
                                              ObObj *out_obj);
  static
  int adjust_return_value(ObObj &result,
                                        const common::ObObjMeta &obj_meta,
                                        ObIAllocator &allocator,
                                        ObExprUDFCtx &udf_ctx);
  static
  int transfer_datum_to_objs(const ObExpr &expr, ObEvalCtx &eval_ctx, ObObj *objs);


private:
  DISALLOW_COPY_AND_ASSIGN(ObExprUDFUtils);
};

}
}

#endif
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

#ifndef OCEANBASE_SQL_OB_EXPR_UDF_H_
#define OCEANBASE_SQL_OB_EXPR_UDF_H_

#include "common/object/ob_object.h"
#include "lib/container/ob_2d_array.h"
#include "sql/engine/expr/ob_expr_operator.h"
#include "sql/engine/expr/ob_i_expr_extra_info.h"
#include "sql/resolver/expr/ob_raw_expr.h"
#include "sql/engine/expr/ob_expr_result_type_util.h"
#include "ob_udf_result_cache_mgr.h"
#include "pl/ob_pl.h"
#include "ob_expr_udf_utils.h"
#include "ob_expr_udf_ctx.h"

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

typedef common::ParamStore ParamStore;

class ObSqlCtx;
class ObUDFParamDesc;

struct ObExprUDFInfo : public ObIExprExtraInfo
{
  OB_UNIS_VERSION(1);
public:
  ObExprUDFInfo(common::ObIAllocator &alloc, ObExprOperatorType type)
    : ObIExprExtraInfo(alloc, type),
      subprogram_path_(alloc),
      params_type_(alloc),
      params_desc_(alloc),
      nocopy_params_(alloc),
      dblink_id_(OB_INVALID_ID),
      is_result_cache_(false),
      is_deterministic_(false),
      has_out_param_(false),
      external_routine_type_(ObExternalRoutineType::INTERNAL_ROUTINE),
      external_routine_entry_(),
      external_routine_url_(),
      external_routine_resource_()
  {
  }

  virtual int deep_copy(common::ObIAllocator &allocator,
                        const ObExprOperatorType type,
                        ObIExprExtraInfo *&copied_info) const override;

  template <typename RE> int from_raw_expr(RE &expr);

  int64_t udf_id_;
  int64_t udf_package_id_;
  common::ObFixedArray<int64_t, common::ObIAllocator> subprogram_path_;
  ObExprResType result_type_;
  common::ObFixedArray<ObExprResType, common::ObIAllocator> params_type_;
  common::ObFixedArray<ObUDFParamDesc, common::ObIAllocator> params_desc_;
  common::ObFixedArray<int64_t, common::ObIAllocator> nocopy_params_;
  bool is_udt_udf_;
  uint64_t loc_;
  bool is_udt_cons_;
  bool is_called_in_sql_;
  uint64_t dblink_id_;
  bool is_result_cache_;
  bool is_deterministic_;
  bool has_out_param_;
  ObExternalRoutineType external_routine_type_;
  common::ObString external_routine_entry_;
  common::ObString external_routine_url_;
  common::ObString external_routine_resource_;
};

class ObExprUDFEnvGuard
{
public:
  ObExprUDFEnvGuard(ObEvalCtx &ctx, ObExprUDFCtx &udf_ctx, int &ret, ObObj &tmp_result);
  ~ObExprUDFEnvGuard();

  inline int64_t get_cur_obj_count() { return cur_obj_count_; }
  inline ObIArray<ObObj> &get_deep_in_objs() { return deep_in_objs_; }
  inline pl::ExecCtxBak &get_exec_ctx_bak() { return exec_ctx_bak_; }

  void restore_exec_ctx();

private:
  bool need_end_stmt_;
  int64_t cur_obj_count_;
  ObSEArray<ObObj,1> deep_in_objs_;
  bool need_free_udt_;
  ObEvalCtx &ctx_;
  ObExprUDFCtx &udf_ctx_;
  int ret_;
  pl::ExecCtxBak exec_ctx_bak_;
  int64_t start_time_;
  ObObj &tmp_result_;
};

class ObExprUDF : public ObFuncExprOperator, public ObExprUDFUtils
{
  OB_UNIS_VERSION(1);

public:
  explicit ObExprUDF(common::ObIAllocator &alloc);
  virtual ~ObExprUDF() {}

  virtual inline void reset();
  virtual int assign(const ObExprOperator &other);
  virtual bool need_rt_ctx() const override { return true; }

  virtual int calc_result_typeN(ObExprResType &type,
                                ObExprResType *types_stack,
                                int64_t param_num,
                                common::ObExprTypeCtx &type_ctx) const;
  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;

  static int eval_udf(const ObExpr &expr,
                      ObEvalCtx &ctx,
                      ObDatum &res);
  static int eval_udf_batch(const ObExpr &expr,
                            ObEvalCtx &ctx,
                            const ObBitVector &skip,
                            const int64_t batch_size);
  static int eval_udf_vector(const ObExpr &expr,
                            ObEvalCtx &ctx,
                            const ObBitVector &skip,
                            const EvalBound &bound);

  static int eval_external_udf(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);

  static int eval_external_udf_vector(const ObExpr &expr,
                                      ObEvalCtx &ctx,
                                      const ObBitVector &skip,
                                      const EvalBound &bound);

  inline void set_udf_id(int64_t udf_id) { udf_id_ = udf_id; }
  inline int64_t get_udf_id() const { return udf_id_;}
  inline void set_udf_package_id(int64_t udf_package_id) { udf_package_id_ = udf_package_id; }
  inline int64_t get_udf_package_id() const { return udf_package_id_;}
  inline void set_result_type(const ObExprResType &result_type) { result_type_ = result_type; }
  inline const ObExprResType &get_result_type() const { return result_type_;}
  inline void set_is_udt_udf(bool is_udt_udf) { is_udt_udf_ = is_udt_udf; }
  inline bool get_is_udt_udf() const { return is_udt_udf_; }
  inline void set_loc(uint64_t loc) { loc_ = loc; }
  inline uint64_t get_loc() const { return loc_; }
  inline void set_is_udt_cons(bool flag) { is_udt_cons_ = flag; }
  inline bool get_is_udt_cons() const { return is_udt_cons_; }
  inline int set_subprogram_path(const common::ObIArray<int64_t> &path) { return subprogram_path_.assign(path);}
  inline const common::ObIArray<int64_t> &get_subprogram_path() const { return subprogram_path_;}
  inline int set_params_type(common::ObIArray<ObRawExprResType> &params_type)
  {
    return ObExprResultTypeUtil::assign_type_array(params_type, params_type_);
  }
  inline const common::ObIArray<ObExprResType> &get_params_type() const { return params_type_;}
  inline int set_params_desc(common::ObIArray<ObUDFParamDesc> &params_desc)
  {
    return params_desc_.assign(params_desc);
  }
  inline const common::ObIArray<ObUDFParamDesc> &get_params_desc() const { return params_desc_; }
  inline int set_nocopy_params(common::ObIArray<int64_t> &nocopy_params)
  {
    return nocopy_params_.assign(nocopy_params);
  }
  inline const common::ObIArray<int64_t> &get_nocopy_params() const { return nocopy_params_;}

private:
  static int build_udf_ctx(const ObExpr &expr,
                           ObExecContext &exec_ctx,
                           ObExprUDFCtx *&udf_ctx);
  static bool enable_eval_vector(ObExpr &expr);
  static int eval_udf_single(const ObExpr &expr, ObEvalCtx &ctx, ObExprUDFCtx &udf_ctx, ObObj& out_result);

private:
  int64_t udf_id_;
  int64_t udf_package_id_;
  common::ObSEArray<int64_t, 8> subprogram_path_;
  ObExprResType result_type_;
  common::ObSEArray<ObExprResType, 5> params_type_;
  common::ObSEArray<ObUDFParamDesc, 5> params_desc_;
  common::ObSEArray<int64_t, 8> nocopy_params_;
  bool is_udt_udf_;
  bool call_in_sql_;
  uint64_t loc_;
  bool is_udt_cons_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObExprUDF);
};

} //sql
} //oceanbase
#endif //OCEANBASE_SQL_OB_EXPR_USER_DEFINED_FUNC_H_

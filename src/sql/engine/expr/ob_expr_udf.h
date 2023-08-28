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

// struct ObSqlCtx;
struct ObExprUDFInfo : public ObIExprExtraInfo
{
  OB_UNIS_VERSION(1);
public:
  ObExprUDFInfo(common::ObIAllocator &alloc, ObExprOperatorType type)
      : ObIExprExtraInfo(alloc, type),
      subprogram_path_(alloc), params_type_(alloc), params_desc_(alloc), nocopy_params_(alloc)
  {
  }

  virtual int deep_copy(common::ObIAllocator &allocator,
                        const ObExprOperatorType type,
                        ObIExprExtraInfo *&copied_info) const override;

  template <typename RE>
  int from_raw_expr(RE &expr);

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
};
class ObSqlCtx;
class ObUDFParamDesc;

class ObExprUDF : public ObFuncExprOperator
{
  class ObExprUDFCtx : public ObExprOperatorCtx
  {
    public:
    ObExprUDFCtx() :
    ObExprOperatorCtx(),
    param_store_buf_(nullptr),
    params_(nullptr) {}

    ~ObExprUDFCtx() {}

    int init_param_store(ObIAllocator &allocator,
                         int param_num);
    void reuse()
    {
      if (OB_NOT_NULL(params_)) {
        params_->reuse();
      }
    }

    ParamStore* get_param_store() { return params_; }
    int64_t get_param_count() { return OB_ISNULL(params_) ? 0 : params_->count(); }

    private:
    void* param_store_buf_;
    ParamStore* params_;
  };

  OB_UNIS_VERSION(1);
public:
  explicit ObExprUDF(common::ObIAllocator &alloc);
  virtual ~ObExprUDF();

  virtual int calc_result_typeN(ObExprResType &type,
                                ObExprResType *types_stack,
                                int64_t param_num,
                                common::ObExprTypeCtx &type_ctx) const;
  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  static int eval_udf(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);

  static int build_udf_ctx(int64_t udf_ctx_id,
                           int64_t param_num,
                           ObExecContext &exec_ctx,
                           ObExprUDFCtx *&udf_ctx);

  virtual inline void reset();
  virtual int assign(const ObExprOperator &other);
  int check_types(common::ObObj &result, const common::ObObj *objs_stack, int64_t param_num) const;
  inline void set_udf_id(int64_t udf_id) { udf_id_ = udf_id; }
  inline void set_udf_package_id(int64_t udf_package_id) { udf_package_id_ = udf_package_id; }
  inline int set_subprogram_path(const common::ObIArray<int64_t> &path)
  {
    return subprogram_path_.assign(path);
  }
  inline void set_result_type(const ObExprResType &result_type) { result_type_ = result_type; }
  inline int set_params_type(common::ObIArray<ObExprResType> &params_type)
  {
    return params_type_.assign(params_type);
  }
  inline int set_params_desc(common::ObIArray<ObUDFParamDesc> &params_desc)
  {
    return params_desc_.assign(params_desc);
  }
  inline int set_nocopy_params(common::ObIArray<int64_t> &nocopy_params)
  {
    return nocopy_params_.assign(nocopy_params);
  }

  inline void set_is_udt_udf(bool is_udt_udf) { is_udt_udf_ = is_udt_udf; }
  inline bool get_is_udt_udf() const { return is_udt_udf_; }
  inline void set_loc(uint64_t loc) { loc_ = loc; }
  inline uint64_t get_loc() const { return loc_; }
  inline void set_is_udt_cons(bool flag) { is_udt_cons_ = flag; }
  inline bool get_is_udt_cons() const { return is_udt_cons_; }

  static int process_in_params(const common::ObObj *objs_stack,
                               int64_t param_num,
                               const common::ObIArray<ObUDFParamDesc> &params_desc,
                               const common::ObIArray<ObExprResType> &params_type,
                               common::ParamStore& iparams,
                               common::ObIAllocator &allocator,
                               ObIArray<ObObj> *deep_in_objs = NULL);
  static int process_out_params(const common::ObObj *objs_stack,
                                int64_t param_num,
                                common::ParamStore& iparams,
                                common::ObIAllocator &alloc,
                                ObExecContext &exec_ctx,
                                const common::ObIArray<int64_t> &nocopy_params,
                                const common::ObIArray<ObUDFParamDesc> &params_desc,
                                const common::ObIArray<ObExprResType> &params_type);
  static bool need_deep_copy_in_parameter(
                        const common::ObObj *objs_stack,
                        int64_t param_num,
                        const common::ObIArray<ObUDFParamDesc> &params_desc,
                        const common::ObIArray<ObExprResType> &params_type,
                        const common::ObObj &element);
  int64_t get_udf_id() const { return udf_id_;}
  int64_t get_udf_package_id() const { return udf_package_id_;}
  const common::ObIArray<int64_t> &get_subprogram_path() const { return subprogram_path_;}
  const ObExprResType &get_result_type() const { return result_type_;}
  const common::ObIArray<ObExprResType> &get_params_type() const { return params_type_;}
  const common::ObIArray<ObUDFParamDesc> &get_params_desc() const { return params_desc_; }
  const common::ObIArray<int64_t> &get_nocopy_params() const { return nocopy_params_;}

  virtual bool need_rt_ctx() const override { return true; }

private:
  static int fill_obj_stack(const ObExpr &expr, ObEvalCtx &ctx, common::ObObj *objs);
  static int check_types(const ObExpr &expr, const ObExprUDFInfo &info);
  int64_t udf_id_;
  int64_t udf_package_id_;
  common::ObSEArray<int64_t, 8> subprogram_path_;
  ObExprResType result_type_;
  common::ObSEArray<ObExprResType, 5> params_type_;
  common::ObSEArray<ObUDFParamDesc, 5> params_desc_;
  common::ObSEArray<int64_t, 8> nocopy_params_;
  bool is_udt_udf_;
  bool call_in_sql_; // 已经被弃用了，有兼容性问题不能删，现在改用基类里的 is_called_in_sql()。
  uint64_t loc_; // 这个是col 和line number的组合，
  bool is_udt_cons_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObExprUDF);
};

} //sql
} //oceanbase
#endif //OCEANBASE_SQL_OB_EXPR_USER_DEFINED_FUNC_H_

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

#ifndef DEV_SRC_SQL_ENGINE_EXPR_OB_EXPR_OBJ_ACCESS_H_
#define DEV_SRC_SQL_ENGINE_EXPR_OB_EXPR_OBJ_ACCESS_H_
#include "sql/engine/expr/ob_expr_operator.h"
#include "lib/container/ob_fast_array.h"
#include "lib/container/ob_2d_array.h"
#include "pl/ob_pl_user_type.h"
#include "sql/engine/expr/ob_i_expr_extra_info.h"

namespace oceanbase
{
namespace sql
{
class ObObjAccessRawExpr;
class ObExprObjAccess : public ObExprOperator
{
  OB_UNIS_VERSION(1);
public:
  class ExtraInfo;
  explicit ObExprObjAccess(common::ObIAllocator &alloc);
  virtual ~ObExprObjAccess();

  virtual void reset();
  int assign(const ObExprOperator &other);
  inline int init_param_idxs(int64_t param_idx_cnt) { return info_.param_idxs_.init(param_idx_cnt); }

  ExtraInfo &get_info() { return info_; }

  int calc_result(common::ObObj &result, common::ObIAllocator &alloc,
                  const common::ObObj *objs_stack, int64_t param_num,
                  const ParamStore &param_store, ObEvalCtx *ctx = nullptr) const;

  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr, ObExpr &rt_expr) const override;
  static int eval_obj_access(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);

  VIRTUAL_TO_STRING_KV(N_EXPR_TYPE, get_type_name(type_),
                       N_EXPR_NAME, name_,
                       N_PARAM_NUM, param_num_,
                       N_DIM, row_dimension_,
                       N_REAL_PARAM_NUM, real_param_num_,
                       K_(info));

private:
  DISALLOW_COPY_AND_ASSIGN(ObExprObjAccess);
public:
  struct ExtraInfo : public ObIExprExtraInfo
  {
    OB_UNIS_VERSION(1);
  public:
    typedef common::ObFastArray<int64_t, 4> ParamArray;
    ExtraInfo(common::ObIAllocator &alloc, ObExprOperatorType type);
    int from_raw_expr(const ObObjAccessRawExpr &raw_access);
    void reset();
    virtual int deep_copy(common::ObIAllocator &allocator,
                          const ObExprOperatorType type,
                          ObIExprExtraInfo *&copied_info) const override;
    int assign(const ExtraInfo &other);

    int init_param_array(const ParamStore &param_store,
                                const common::ObObj *objs_stack,
                                int64_t param_num,
                                ParamArray &param_array) const;

    int update_coll_first_last(
        const ParamStore &param_store, const ObObj *objs_stack, int64_t param_num) const;

    int get_attr_func(int64_t param_cnt,
                      int64_t* params,
                      int64_t *element_val,
                      ObEvalCtx &ctx) const;
    int get_record_attr(const pl::ObObjAccessIdx &current_access,
                        uint64_t udt_id,
                        bool for_write,
                        void *&current_value,
                        ObEvalCtx &ctx) const;
    int get_collection_attr(int64_t* params,
                            const pl::ObObjAccessIdx &current_access,
                            bool for_write,
                            void *&current_value) const;


    int calc(ObObj &result,
             ObIAllocator &alloc,
             const ObObjMeta &res_type,
             const int32_t extend_size,
             const ParamStore &param_store,
             const common::ObObj *params,
             int64_t param_num,
             ObEvalCtx *ctx) const;

    TO_STRING_KV(K_(get_attr_func),
                 K_(param_idxs),
                 K_(access_idx_cnt),
                 K_(for_write),
                 K_(property_type),
                 K_(coll_idx),
                 K_(extend_size));

    uint64_t get_attr_func_;
    common::ObFixedArray<int64_t, common::ObIAllocator> param_idxs_;
    int64_t access_idx_cnt_;
    bool for_write_;
    pl::ObCollectionType::PropertyType property_type_;
    int64_t coll_idx_; // index of Collection in ParamArray
    // extend size only used in static engine, the old expr get extend size from ObExprResType
    int32_t extend_size_;
    common::ObFixedArray<pl::ObObjAccessIdx, common::ObIAllocator> access_idxs_;
  };
private:
  ExtraInfo info_;
};

}  // namespace sql
}  // namespace oceanbase
#endif /* DEV_SRC_SQL_ENGINE_EXPR_OB_EXPR_OBJ_ACCESS_H_ */

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

namespace oceanbase {
using namespace common;
namespace sql {
typedef Ob2DArray<ObObjParam, OB_MALLOC_BIG_BLOCK_SIZE, ObWrapperAllocator, false> ParamStore;
class ObExprObjAccess : public ObExprOperator {
  OB_UNIS_VERSION(1);

public:
  explicit ObExprObjAccess(common::ObIAllocator& alloc);
  virtual ~ObExprObjAccess();

  virtual void reset();
  int assign(const ObExprOperator& other);
  inline int init_param_idxs(int64_t param_idx_cnt)
  {
    return param_idxs_.init(param_idx_cnt);
  }
  virtual int calc_resultN(
      common::ObObj& result, const common::ObObj* objs_stack, int64_t param_num, common::ObExprCtx& expr_ctx) const;
  inline void set_get_attr_func(uint64_t attr_func)
  {
    get_attr_func_ = attr_func;
  }
  inline int add_param_idx(int64_t param_idx)
  {
    return param_idxs_.push_back(param_idx);
  }
  inline const common::ObFixedArray<int64_t, common::ObIAllocator>& get_param_idxs() const
  {
    return param_idxs_;
  }
  inline void set_write(bool for_write)
  {
    for_write_ = for_write;
  }
  inline bool get_write() const
  {
    return for_write_;
  }
  inline void set_access_idx_cnt(int64_t cnt)
  {
    access_idx_cnt_ = cnt;
  }
  inline int64_t get_access_idx_cnt() const
  {
    return access_idx_cnt_;
  }
  inline void set_coll_idx(int64_t idx)
  {
    coll_idx_ = idx;
  }
  inline int64_t get_coll_idx()
  {
    return coll_idx_;
  }
  int calc_result(
      common::ObObj& result, const common::ObObj* objs_stack, int64_t param_num, const ParamStore& param_store) const;

  VIRTUAL_TO_STRING_KV(N_EXPR_TYPE, get_type_name(type_), N_EXPR_NAME, name_, N_PARAM_NUM, param_num_, N_DIM,
      row_dimension_, N_REAL_PARAM_NUM, real_param_num_, K_(get_attr_func), K_(param_idxs), K_(access_idx_cnt),
      K_(for_write), K_(coll_idx));

private:
  typedef common::ObFastArray<int64_t, 4> ParamArray;
  int init_param_array(
      const ParamStore& param_store, const common::ObObj* objs_stack, int64_t param_num, ParamArray& param_array) const;

  DISALLOW_COPY_AND_ASSIGN(ObExprObjAccess);

private:
  uint64_t get_attr_func_;
  common::ObFixedArray<int64_t, common::ObIAllocator> param_idxs_;
  int64_t access_idx_cnt_;
  bool for_write_;
  int64_t coll_idx_;
};

}  // namespace sql
}  // namespace oceanbase
#endif /* DEV_SRC_SQL_ENGINE_EXPR_OB_EXPR_OBJ_ACCESS_H_ */

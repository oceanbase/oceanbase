/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_EXPR_LOCK_FUNC_H
#define OCEANBASE_EXPR_LOCK_FUNC_H

#include "sql/engine/expr/ob_expr_operator.h"
#include "sql/engine/expr/ob_expr_to_temporal_base.h"

namespace oceanbase
{
namespace sql
{

class ObExprLockFunc : public ObFuncExprOperator
{
public:
  explicit ObExprLockFunc(common::ObIAllocator &alloc,
                          ObExprOperatorType type,
                          const char *name,
                          int32_t param_num);
  virtual ~ObExprLockFunc() {}
  virtual int calc_result_type0(ObExprResType &type,
                                common::ObExprTypeCtx &type_ctx) const override;
  virtual int calc_result_type1(ObExprResType &type,
                                ObExprResType &type1,
                                common::ObExprTypeCtx &type_ctx) const override;
  virtual int calc_result_type2(ObExprResType &type,
                                ObExprResType &type1,
                                ObExprResType &type2,
                                common::ObExprTypeCtx &type_ctx) const override;
  virtual common::ObCastMode get_cast_mode() const { return CM_NULL_ON_WARN;}
  static bool proxy_is_support(const ObExecContext &exec_ctx);
  int support_check_() const;

  class ObTimeOutCheckGuard
  {
  public:
    ObTimeOutCheckGuard(int &ret,
                        const int64_t lock_timeout_us,
                        const int64_t abs_query_expire_us);
    ~ObTimeOutCheckGuard();
    int get_timeout_us(int64_t &timeout_us);
  private:
    int &ret_;
    int64_t start_time_;
    int64_t abs_lock_expire_us_;
    int64_t abs_query_expire_us_;
  };
private :
  //disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObExprLockFunc);
};

class ObExprGetLock : public ObExprLockFunc
{
public:
  explicit ObExprGetLock(common::ObIAllocator &alloc);
  virtual ~ObExprGetLock() {}
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  static int get_lock(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &result);
private :
  //disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObExprGetLock);
};

class ObExprIsFreeLock : public ObExprLockFunc
{
public:
  explicit ObExprIsFreeLock(common::ObIAllocator &alloc);
  virtual ~ObExprIsFreeLock() {}
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  static int is_free_lock(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &result);
private :
  //disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObExprIsFreeLock);
};

class ObExprIsUsedLock : public ObExprLockFunc
{
public:
  explicit ObExprIsUsedLock(common::ObIAllocator &alloc);
  virtual ~ObExprIsUsedLock() {}
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  static int is_used_lock(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &result);
private :
  //disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObExprIsUsedLock);
};

class ObExprReleaseLock : public ObExprLockFunc
{
public:
  explicit ObExprReleaseLock(common::ObIAllocator &alloc);
  virtual ~ObExprReleaseLock() {}
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  static int release_lock(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &result);
private :
  //disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObExprReleaseLock);
};

class ObExprReleaseAllLocks : public ObExprLockFunc
{
public:
  explicit ObExprReleaseAllLocks(common::ObIAllocator &alloc);
  virtual ~ObExprReleaseAllLocks() {}
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  static int release_all_locks(const ObExpr &expr,
                               ObEvalCtx &ctx,
                               ObDatum &result);
private :
  //disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObExprReleaseAllLocks);
};


}
}

#endif

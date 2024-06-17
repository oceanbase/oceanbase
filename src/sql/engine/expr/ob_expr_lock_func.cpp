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

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/expr/ob_expr_lock_func.h"

#include "lib/ob_name_def.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/timezone/ob_time_convert.h"
#include "sql/engine/ob_physical_plan_ctx.h"
#include "sql/engine/ob_exec_context.h"
#include "share/system_variable/ob_system_variable.h"
#include "storage/tablelock/ob_lock_func_executor.h"

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace transaction::tablelock;
namespace sql
{

ObExprLockFunc::ObExprLockFunc(ObIAllocator &alloc,
                               ObExprOperatorType type,
                               const char *name,
                               int32_t param_num)
  : ObFuncExprOperator(alloc, type, name, param_num,
                       NOT_VALID_FOR_GENERATED_COL,
                       NOT_ROW_DIMENSION)
{
}

// define the return value type
int ObExprLockFunc::calc_result_type0(ObExprResType &type,
                                      ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  type.set_type(ObIntType);
  type_ctx.set_cast_mode(type_ctx.get_cast_mode() | CM_NULL_ON_WARN);
  return ret;
}

int ObExprLockFunc::calc_result_type1(ObExprResType &type,
                                      ObExprResType &type1,
                                      ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  type.set_type(ObIntType);
  type_ctx.set_cast_mode(type_ctx.get_cast_mode() | CM_NULL_ON_WARN);

  // lock name
  type1.set_calc_type(ObVarcharType);
  type1.set_calc_collation_type(ObCharset::get_system_collation());
  return ret;
}

int ObExprLockFunc::calc_result_type2(ObExprResType &type,
                                      ObExprResType &type1,
                                      ObExprResType &type2,
                                      ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  type.set_type(ObIntType);
  type_ctx.set_cast_mode(type_ctx.get_cast_mode() | CM_NULL_ON_WARN);

  // lock name
  type1.set_calc_type(ObVarcharType);
  type1.set_calc_collation_type(ObCharset::get_system_collation());

  // lock timeout
  type2.set_calc_type(ObIntType);
  return ret;
}

int ObExprLockFunc::support_check_() const
{
  int ret = OB_SUCCESS;
  uint64_t data_version = 0;
  if (OB_FAIL(GET_MIN_DATA_VERSION(MTL_ID(), data_version))) {
    LOG_WARN("fail to get min data version", KR(ret));
  } else if (data_version < DATA_VERSION_4_3_1_0) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("only support after data version greater than", K(DATA_VERSION_4_3_1_0));
  } else if (!lib::is_mysql_mode()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("this is only support in MySQL", K(ret));
  }
  return ret;
}

bool ObExprLockFunc::proxy_is_support(const ObExecContext &exec_ctx)
{
  ObSQLSessionInfo *session = exec_ctx.get_my_session();
  bool is_support = false;
  if (OB_ISNULL(session)) {
    LOG_ERROR_RET(OB_INVALID_ARGUMENT, "session is null!");
  } else {
    is_support = ((session->is_feedback_proxy_info_support() && session->is_client_sessid_support())
                  || !session->is_obproxy_mode())
                 && session->get_client_sessid() != INVALID_SESSID;
    if (!is_support) {
      LOG_WARN_RET(OB_NOT_SUPPORTED,
                   "proxy is not support this feature",
                   K(session->get_sessid()),
                   K(session->is_feedback_proxy_info_support()),
                   K(session->is_client_sessid_support()));
    }
  }
  return is_support;
}

ObExprLockFunc::ObTimeOutCheckGuard::ObTimeOutCheckGuard(int &ret,
                                                         const int64_t lock_timeout_us,
                                                         const int64_t abs_query_expire_us)
  : ret_(ret), abs_query_expire_us_(abs_query_expire_us)
{
  start_time_ = ObTimeUtility::current_time();
  abs_lock_expire_us_ = start_time_ + lock_timeout_us;
}

ObExprLockFunc::ObTimeOutCheckGuard::~ObTimeOutCheckGuard()
{
  int ret = OB_SUCCESS;
  int64_t end_time = ObTimeUtility::current_time();
  if (abs_lock_expire_us_ == 0 || abs_query_expire_us_ == 0) {
    ret_ = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid timeout ts", K_(ret), K(abs_lock_expire_us_), K(abs_query_expire_us_));
  } else if (OB_ERR_EXCLUSIVE_LOCK_CONFLICT == ret_) {
    if (end_time > abs_lock_expire_us_) {
      // stay OB_ERR_EXCLUSIVE_LOCK_CONFLICT
    } else if (end_time > abs_query_expire_us_) {
      ret_ = OB_ERR_QUERY_INTERRUPTED;
      LOG_USER_ERROR(OB_ERR_QUERY_INTERRUPTED, "maximum statement execution time exceeded");
    } else {
      // stay OB_ERR_EXCLUSIVE_LOCK_CONFLICT
    }
  }
}

int ObExprLockFunc::ObTimeOutCheckGuard::get_timeout_us(int64_t &timeout_us)
{
  int ret = OB_SUCCESS;
  int64_t min_abs_expire_us = OB_MIN(abs_lock_expire_us_, abs_query_expire_us_);
  if (min_abs_expire_us < start_time_) {
    ret = OB_TIMEOUT;
    LOG_WARN("timeout before lock", K(ret), K(abs_lock_expire_us_), K(abs_query_expire_us_));
  } else {
    timeout_us = (min_abs_expire_us - start_time_);
  }
  return ret;
}

ObExprGetLock::ObExprGetLock(ObIAllocator &alloc)
  : ObExprLockFunc(alloc, T_FUN_SYS_GET_LOCK, N_GET_LOCK, 2)
{
}

int ObExprGetLock::cg_expr(ObExprCGCtx &expr_cg_ctx,
                           const ObRawExpr &raw_expr,
                           ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);

  OZ(support_check_());
  CK(2 == rt_expr.arg_cnt_);
  const ObExpr *lock_name = rt_expr.args_[0];
  const ObExpr *timeout = rt_expr.args_[1];
  CK(NULL != lock_name);
  CK(NULL != timeout);

  if (OB_SUCC(ret)) {
    rt_expr.eval_func_ = ObExprGetLock::get_lock;
  }

  return ret;
}

int ObExprGetLock::get_lock(const ObExpr &expr,
                            ObEvalCtx &ctx,
                            ObDatum &result)
{
  int ret = OB_SUCCESS;

  // NOTICE: use this to make it same like never timeout.
  static const int64_t MAX_LOCK_TIME = 365L * 24 * 3600 * 1000 * 1000;
  ObDatum *lock_name = NULL;
  ObDatum *lock_timeout = NULL;

  if (!proxy_is_support(ctx.exec_ctx_)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("obproxy is not support mysql lock function", K(ret));
  } else if (OB_FAIL(expr.eval_param_value(ctx, lock_name, lock_timeout))) {
    LOG_WARN("calc param failed", K(ret));
  } else if (lock_name->is_null()) {
    // TODO: yichang.yyf use the error code of mysql ER_USER_LOCK_WRONG_NAME or ER_USER_LOCK_OVERLONG_NAME;
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument, lock name should not be null", K(ret));
  } else {
    ObString lock_name_str = lock_name->get_string();
    int64_t timeout_us = lock_timeout->is_null() ? 0 : lock_timeout->get_int() * 1000 * 1000;
    if (lock_name_str.empty()) {
      // TODO: yichang.yyf use the error code of mysql ER_USER_LOCK_WRONG_NAME or ER_USER_LOCK_OVERLONG_NAME;
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument, lock name should not be null", K(ret));
    }
    if (timeout_us < 0) {
      timeout_us = MAX_LOCK_TIME;
    }

    ObTimeOutCheckGuard guard(ret, timeout_us,
                              ctx.exec_ctx_.get_my_session()->get_query_timeout_ts());
    ObGetLockExecutor executor;
    if (FAILEDx(guard.get_timeout_us(timeout_us))) {
      LOG_WARN("get timeout us failed", K(ret));
    } else if (OB_FAIL(executor.execute(ctx.exec_ctx_,
                                        lock_name_str,
                                        timeout_us))) {
      LOG_WARN("get lock execute failed", K(ret), K(lock_name_str), K(timeout_us));
    }
  }

  if (OB_SUCC(ret)) {
    result.set_int(1);
  } else if (OB_ERR_EXCLUSIVE_LOCK_CONFLICT == ret) {
    // lock conflict until timeout
    result.set_int(0);
    ret = OB_SUCCESS;
  } else if (OB_ERR_QUERY_INTERRUPTED == ret) {
    result.set_int(0);
  } else {
    // TODO: yanyuan.cxf shall we rewrite the return code?
    result.set_null();
  }
  return ret;
}

ObExprIsFreeLock::ObExprIsFreeLock(ObIAllocator &alloc)
  : ObExprLockFunc(alloc, T_FUN_SYS_IS_FREE_LOCK, N_IS_FREE_LOCK, 1)
{
}

int ObExprIsFreeLock::cg_expr(ObExprCGCtx &expr_cg_ctx,
                              const ObRawExpr &raw_expr,
                              ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);

  OZ(support_check_());
  CK(1 == rt_expr.arg_cnt_);
  const ObExpr *lock_name = rt_expr.args_[0];
  CK(NULL != lock_name);

  if (OB_SUCC(ret)) {
    rt_expr.eval_func_ = ObExprIsFreeLock::is_free_lock;
  }

  return ret;
}

int ObExprIsFreeLock::is_free_lock(const ObExpr &expr,
                                   ObEvalCtx &ctx,
                                   ObDatum &result)
{
  int ret = OB_SUCCESS;

  ObDatum *lock_name = NULL;
  if (OB_FAIL(expr.eval_param_value(ctx, lock_name))) {
    LOG_WARN("calc param failed", K(ret));
  } else if (lock_name->is_null()) {
    // TODO: yichang.yyf use the error code of mysql ER_USER_LOCK_WRONG_NAME or ER_USER_LOCK_OVERLONG_NAME;
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument, lock name should not be null", K(ret));
  } else {
    ObString lock_name_str = lock_name->get_string();
    ObISFreeLockExecutor executor;
    if (lock_name_str.empty()) {
      // TODO: yichang.yyf use the error code of mysql ER_USER_LOCK_WRONG_NAME or ER_USER_LOCK_OVERLONG_NAME;
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument, lock name should not be null", K(ret));
    } else {
      ret = executor.execute(ctx.exec_ctx_, lock_name_str);
    }
  }
  // 1 the lock name may has been locked.
  if (OB_SUCC(ret)) {
    result.set_int(0);
  } else if (OB_EMPTY_RESULT == ret) {
    // 2 there is no lock now
    ret = OB_SUCCESS;
    result.set_int(1);
  } else {
    // 3. internal error
    // TODO: yanyuan.cxf shall we rewrite the return code?
    result.set_null();
  }

  return ret;
}

ObExprIsUsedLock::ObExprIsUsedLock(ObIAllocator &alloc)
  : ObExprLockFunc(alloc, T_FUN_SYS_IS_USED_LOCK, N_IS_USED_LOCK, 1)
{
}

int ObExprIsUsedLock::cg_expr(ObExprCGCtx &expr_cg_ctx,
                              const ObRawExpr &raw_expr,
                              ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);

  OZ(support_check_());
  CK(1 == rt_expr.arg_cnt_);
  const ObExpr *lock_name = rt_expr.args_[0];
  CK(NULL != lock_name);

  if (OB_SUCC(ret)) {
    rt_expr.eval_func_ = ObExprIsUsedLock::is_used_lock;
  }

  return ret;
}

int ObExprIsUsedLock::is_used_lock(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &result)
{
  int ret = OB_SUCCESS;
  // we can get session info use this
  const ObSQLSessionInfo *session = ctx.exec_ctx_.get_my_session();
  uint32_t sess_id = 0;
  ObDatum *lock_name = NULL;

  if (OB_FAIL(expr.eval_param_value(ctx, lock_name))) {
    LOG_WARN("calc param failed", K(ret));
  } else if (lock_name->is_null()) {
    // TODO: yichang.yyf use the error code of mysql ER_USER_LOCK_WRONG_NAME or ER_USER_LOCK_OVERLONG_NAME;
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument, lock name should not be null", K(ret));
  } else {
    ObString lock_name_str = lock_name->get_string();
    ObISUsedLockExecutor executor;
    if (lock_name_str.empty()) {
      // TODO: yichang.yyf use the error code of mysql ER_USER_LOCK_WRONG_NAME or ER_USER_LOCK_OVERLONG_NAME;
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument, lock name should not be null", K(ret));
    } else {
      ret = executor.execute(ctx.exec_ctx_,
                             lock_name_str,
                             sess_id);
    }
  }

  // 1. get the lock session id
  if (OB_SUCC(ret)) {
    result.set_uint32(sess_id);
  } else if (OB_EMPTY_RESULT == ret) {
    // 2. there is nobody hold the lock
    ret = OB_SUCCESS;
    result.set_null();
  } else {
    // 3. internal error.
    // TODO: yanyuan.cxf shall we rewrite the return code?
    result.set_null();
  }
  return ret;
}

ObExprReleaseLock::ObExprReleaseLock(ObIAllocator &alloc)
  : ObExprLockFunc(alloc, T_FUN_SYS_RELEASE_LOCK, N_RELEASE_LOCK, 1)
{
}

int ObExprReleaseLock::cg_expr(ObExprCGCtx &expr_cg_ctx,
                               const ObRawExpr &raw_expr,
                               ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);

  OZ(support_check_());
  CK(1 == rt_expr.arg_cnt_);
  const ObExpr *lock_name = rt_expr.args_[0];
  CK(NULL != lock_name);

  if (OB_SUCC(ret)) {
    rt_expr.eval_func_ = ObExprReleaseLock::release_lock;
  }

  return ret;
}

int ObExprReleaseLock::release_lock(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &result)
{
  int ret = OB_SUCCESS;

  ObDatum *lock_name = NULL;
  int64_t release_cnt = ObLockFuncExecutor::INVALID_RELEASE_CNT;

  if (!proxy_is_support(ctx.exec_ctx_)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("obproxy is not support mysql lock function", K(ret));
  } else if (OB_FAIL(expr.eval_param_value(ctx, lock_name))) {
    LOG_WARN("calc param failed", K(ret));
  } else if (lock_name->is_null()) {
    // TODO: yichang.yyf use the error code of mysql ER_USER_LOCK_WRONG_NAME or ER_USER_LOCK_OVERLONG_NAME;
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument, lock name should not be null", K(ret));
  } else {
    ObString lock_name_str = lock_name->get_string();
    ObReleaseLockExecutor executor;
    if (lock_name_str.empty()) {
      // TODO: yichang.yyf use the error code of mysql ER_USER_LOCK_WRONG_NAME or ER_USER_LOCK_OVERLONG_NAME;
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument, lock name should not be null", K(ret));
    } else if (OB_FAIL(executor.execute(ctx.exec_ctx_,
                                        lock_name_str,
                                        release_cnt))) {
      LOG_WARN("release lock failed", K(ret), K(lock_name_str));
    }
  }

  if (OB_SUCC(ret)) {
    if (release_cnt < 0) {
      result.set_null();
    } else {
      result.set_int(release_cnt);
    }
  } else {
    result.set_null();
  }

  return ret;
}

ObExprReleaseAllLocks::ObExprReleaseAllLocks(ObIAllocator &alloc)
  : ObExprLockFunc(alloc, T_FUN_SYS_RELEASE_ALL_LOCKS, N_RELEASE_ALL_LOCKS, 0)
{
}

int ObExprReleaseAllLocks::cg_expr(ObExprCGCtx &expr_cg_ctx,
                                   const ObRawExpr &raw_expr,
                                   ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);

  OZ(support_check_());
  if (OB_SUCC(ret)) {
    rt_expr.eval_func_ = ObExprReleaseAllLocks::release_all_locks;
  }

  return ret;
}

int ObExprReleaseAllLocks::release_all_locks(const ObExpr &expr,
                                             ObEvalCtx &ctx,
                                             ObDatum &result)
{
  int ret = OB_SUCCESS;
  int64_t release_cnt = 0;
  ObReleaseAllLockExecutor executor;
  if (!proxy_is_support(ctx.exec_ctx_)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("obproxy is not support mysql lock function", K(ret));
  } else {
    ret = executor.execute(ctx.exec_ctx_, release_cnt);
  }

  if (OB_SUCC(ret)) {
    if (release_cnt < 0) {
      result.set_null();
    } else {
      result.set_int(release_cnt);
    }
  } else {
    result.set_null();
  }
  return ret;
}



}
}

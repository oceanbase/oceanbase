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

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/expr/ob_expr_last_refresh_scn.h"
#include "sql/engine/expr/ob_expr_util.h"
#include "sql/engine/ob_physical_plan_ctx.h"
#include "sql/engine/ob_exec_context.h"

namespace oceanbase
{
using namespace common;

namespace sql
{
ObExprLastRefreshScn::ObExprLastRefreshScn(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_LAST_REFRESH_SCN, N_SYS_LAST_REFRESH_SCN, 0, NOT_VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}
ObExprLastRefreshScn::~ObExprLastRefreshScn()
{
}

int ObExprLastRefreshScn::calc_result_type0(ObExprResType &type, ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  if (is_oracle_mode()) {
    const ObAccuracy &acc = ObAccuracy::DDL_DEFAULT_ACCURACY2[common::ORACLE_MODE][common::ObNumberType];
    type.set_number();
    type.set_scale(acc.get_scale());
    type.set_precision(acc.get_precision());
  } else {
    const ObAccuracy &acc = common::ObAccuracy::DDL_DEFAULT_ACCURACY[common::ObUInt64Type];
    type.set_uint64();
    type.set_scale(acc.get_scale());
    type.set_precision(acc.get_precision());
    type.set_result_flag(NOT_NULL_FLAG);
  }
  return OB_SUCCESS;
}

int ObExprLastRefreshScn::eval_last_refresh_scn(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  uint64_t scn = OB_INVALID_SCN_VAL;
  const ObExprLastRefreshScn::LastRefreshScnExtraInfo *info =
    static_cast<const ObExprLastRefreshScn::LastRefreshScnExtraInfo*>(expr.extra_info_);
  const ObPhysicalPlanCtx *phy_plan_ctx = NULL;
  if (OB_ISNULL(info) || OB_ISNULL(phy_plan_ctx = ctx.exec_ctx_.get_physical_plan_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("phy_plan_ctx is null", K(ret), K(info), K(phy_plan_ctx));
  } else if (OB_UNLIKELY(OB_INVALID_SCN_VAL == (scn = phy_plan_ctx->get_last_refresh_scn(info->mview_id_)))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("failed to get valid last_refresh_scn for mview id", K(ret), K(info->mview_id_), K(lbt()));
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "materialized view id in last_refresh_scn");
  } else if (ObUInt64Type == expr.datum_meta_.type_) {
    expr_datum.set_uint(scn);
  } else {
    ObNumStackOnceAlloc tmp_alloc;
    number::ObNumber num;
    if (OB_FAIL(num.from(scn, tmp_alloc))) {
      LOG_WARN("copy number fail", K(ret));
    } else {
      expr_datum.set_number(num);
    }
  }
  return ret;
}

int ObExprLastRefreshScn::cg_expr(ObExprCGCtx &op_cg_ctx, const ObRawExpr &raw_expr, ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  LastRefreshScnExtraInfo *extra_info = NULL;
  void *buf = NULL;
  if (OB_ISNULL(op_cg_ctx.allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("allocator is null", K(ret));
  } else if (OB_ISNULL(buf = op_cg_ctx.allocator_->alloc(sizeof(LastRefreshScnExtraInfo)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", K(ret));
  } else {
    extra_info = new(buf) LastRefreshScnExtraInfo(*op_cg_ctx.allocator_, type_);
    extra_info->mview_id_ = static_cast<const ObSysFunRawExpr&>(raw_expr).get_mview_id();
    rt_expr.eval_func_ = ObExprLastRefreshScn::eval_last_refresh_scn;
    rt_expr.extra_info_ = extra_info;
  }
  return OB_SUCCESS;
}

OB_SERIALIZE_MEMBER(ObExprLastRefreshScn::LastRefreshScnExtraInfo, mview_id_);

int ObExprLastRefreshScn::LastRefreshScnExtraInfo::deep_copy(common::ObIAllocator &allocator,
                                                             const ObExprOperatorType type,
                                                             ObIExprExtraInfo *&copied_info) const
{
  int ret = OB_SUCCESS;
  OZ(ObExprExtraInfoFactory::alloc(allocator, type, copied_info));
  LastRefreshScnExtraInfo &other = *static_cast<LastRefreshScnExtraInfo *>(copied_info);
  if (OB_SUCC(ret)) {
    other = *this;
  }
  return ret;
}

int ObExprLastRefreshScn::set_last_refresh_scns(const ObIArray<uint64_t> &src_mview_ids,
                                                ObMySQLProxy *sql_proxy,
                                                ObSQLSessionInfo *session,
                                                const share::SCN &scn,
                                                ObIArray<uint64_t> &mview_ids,
                                                ObIArray<uint64_t> &last_refresh_scns)
{
  int ret = OB_SUCCESS;
  mview_ids.reuse();
  last_refresh_scns.reuse();
  ObSEArray<uint64_t, 2> res_ids;
  ObSEArray<uint64_t, 2> res_scns;
  if (OB_ISNULL(sql_proxy) || OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      ObSqlString sql;
      sqlclient::ObMySQLResult *mysql_result = NULL;
      if (OB_FAIL(get_last_refresh_scn_sql(scn, src_mview_ids, sql))) {
        LOG_WARN("failed to get last refresh scn sql", K(ret));
      } else if (OB_FAIL(sql_proxy->read(res, session->get_effective_tenant_id(), sql.ptr()))) {
        LOG_WARN("execute sql failed", K(ret), K(sql));
      } else if (OB_ISNULL(mysql_result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("execute sql fail", K(ret));
      } else {
        ObSEArray<uint64_t, 2> res_ids;
        ObSEArray<uint64_t, 2> res_scns;
        const int64_t col_idx0 = 0;
        const int64_t col_idx1 = 1;
        while (OB_SUCC(ret) && OB_SUCC(mysql_result->next())) {
          int64_t mview_id = OB_INVALID_ID;
          uint64_t last_refresh_scn = OB_INVALID_SCN_VAL;
          if (OB_FAIL(mysql_result->get_int(col_idx0, mview_id))
              || OB_FAIL(mysql_result->get_uint(col_idx1, last_refresh_scn))) {
            LOG_WARN("fail to get int/uint value", K(ret));
          } else if (OB_FAIL(res_ids.push_back(mview_id))
                     || OB_FAIL(res_scns.push_back(last_refresh_scn))) {
            LOG_WARN("fail to push back", K(ret));
          }
        }
        if (OB_LIKELY(OB_SUCCESS == ret || OB_ITER_END == ret)
            && (OB_FAIL(mview_ids.assign(res_ids)) || OB_FAIL(last_refresh_scns.assign(res_scns)))) {
          LOG_WARN("fail to assign array", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObExprLastRefreshScn::get_last_refresh_scn_sql(const share::SCN &scn,
                                                   const ObIArray<uint64_t> &mview_ids,
                                                   ObSqlString &sql)
{
  int ret = OB_SUCCESS;
  ObSqlString mview_id_array;
  if (OB_UNLIKELY(mview_ids.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect empty array", K(ret), K(mview_ids));
  } else if (OB_UNLIKELY(mview_ids.count() > 100)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("more than 100 different materialized view id used in last_refresh_scn", K(ret), K(mview_ids.count()));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "more than 100 different materialized view id used in last_refresh_scn is");
  } else {
    for (int i = 0; OB_SUCC(ret) && i < mview_ids.count(); ++i) {
      if (OB_FAIL(mview_id_array.append_fmt(0 == i ? "%ld" : ",%ld", mview_ids.at(i)))) {
        LOG_WARN("fail to append fmt", KR(ret));
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (SCN::invalid_scn() == scn) {
    if (OB_FAIL(sql.assign_fmt("SELECT MVIEW_ID, LAST_REFRESH_SCN FROM `%s`.`%s` WHERE TENANT_ID = 0 AND MVIEW_ID IN (%.*s)",
                                    OB_SYS_DATABASE_NAME, OB_ALL_MVIEW_TNAME,
                                    (int)mview_id_array.length(), mview_id_array.ptr()))) {
      LOG_WARN("fail to assign sql", KR(ret));
    }
  } else if (OB_FAIL(sql.assign_fmt("SELECT MVIEW_ID, LAST_REFRESH_SCN FROM `%s`.`%s` AS OF SNAPSHOT %ld WHERE TENANT_ID = 0 AND MVIEW_ID IN (%.*s)",
                                    OB_SYS_DATABASE_NAME, OB_ALL_MVIEW_TNAME, scn.get_val_for_sql(),
                                    (int)mview_id_array.length(), mview_id_array.ptr()))) {
    LOG_WARN("fail to assign sql", KR(ret), K(scn));
  }
  return ret;
}

} //namespace sql
} //namespace oceanbase

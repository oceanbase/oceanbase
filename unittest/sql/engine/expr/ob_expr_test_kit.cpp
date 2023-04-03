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

#define USING_LOG_PREFIX SQL

#include <gtest/gtest.h>
#define private public
#define protected public
#include "ob_expr_test_kit.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::common::number;
using namespace oceanbase::share;
using namespace oceanbase::sql;
using namespace oceanbase::observer;

ObExprTestKit::ObExprTestKit()
      : allocator_(ObModIds::TEST),
      calc_buf_(ObModIds::TEST),
      eval_tmp_alloc_(ObModIds::TEST),
      expr_factory_(allocator_),
      exec_ctx_(allocator_),
      expr_ctx_(NULL, &session_, &exec_ctx_, &calc_buf_),
      eval_ctx_(exec_ctx_, calc_buf_, eval_tmp_alloc_)
{
  oceanbase::common::ObClusterVersion::get_instance().init(CLUSTER_CURRENT_VERSION);
  ObServer::get_instance().init_tz_info_mgr();
  ObServer::get_instance().init_global_context();
}

int ObExprTestKit::init()
{
  int ret = OB_SUCCESS;
  ObTenantManager::get_instance().init(10);
  ObTenantManager::get_instance().add_tenant(tenant_id_);
  ObTenantManager::get_instance().add_tenant(OB_SERVER_TENANT_ID);
  ObTenantManager::get_instance().set_tenant_mem_limit(
      tenant_id_, 10L * (1L << 30), 10 * (1L << 30));
  ObTenantManager::get_instance().set_tenant_mem_limit(
      OB_SERVER_TENANT_ID, 10L * (1L << 30), 10 * (1L << 30));

  OZ(ObPreProcessSysVars::init_sys_var());
  ObString tenant("test_tenant");
  OZ(session_.init_tenant(tenant, tenant_id_));
  OZ(session_.test_init(0, 0, 0, NULL));
  OZ(session_.load_default_sys_variable(true, true));
  session_.set_user(OB_SYS_USER_NAME, OB_SYS_HOST_NAME, OB_SYS_USER_ID);
  session_.set_user_priv_set(OB_PRIV_ALL | OB_PRIV_GRANT | OB_PRIV_BOOTSTRAP);
  session_.set_default_database(OB_SYS_DATABASE_NAME, CS_TYPE_UTF8MB4_GENERAL_CI);

  exec_ctx_.set_sql_ctx(&sql_ctx_);
  exec_ctx_.set_my_session(&session_);
  exec_ctx_.get_task_executor_ctx()->set_min_cluster_version(CLUSTER_CURRENT_VERSION);
  exec_ctx_.frames_ = (char **)allocator_.alloc(2 * 8);
  exec_ctx_.frames_[0] = (char *)allocator_.alloc(4096);
  memset(exec_ctx_.frames_[0], 0, 4096);
  exec_ctx_.frames_[1] = (char *)allocator_.alloc(4096);
  memset(exec_ctx_.frames_[1], 0, 4096);
  exec_ctx_.frame_cnt_ = 2;
  OZ(exec_ctx_.create_physical_plan_ctx());
  expr_ctx_.phy_plan_ctx_ = exec_ctx_.get_physical_plan_ctx();
  eval_ctx_.frames_ = exec_ctx_.frames_;

  init_sql_factories();
  init_sql_expr_static_var();

  return ret;
}

void ObExprTestKit::destroy()
{
  session_.~ObSQLSessionInfo();
  new (&session_) ObSQLSessionInfo();

  exec_ctx_.~ObExecContext();
  new (&exec_ctx_) ObExecContext(allocator_);
  ObTenantManager::get_instance().destroy();
}

int ObExprTestKit::resolve_const_expr(const char *str, ObRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  const ParseNode *node = NULL;
  OZ(ObRawExprUtils::parse_expr_node_from_str(ObString(str), allocator_, node));

  ObSchemaChecker schema_checker;
  schema::ObSchemaGetterGuard schema_guard;
  OZ(schema_checker.init(schema_guard));
  ObResolverParams param;
  param.allocator_ = &allocator_;
  param.session_info_ = &session_;
  param.expr_factory_ = &expr_factory_;
  param.schema_checker_ = &schema_checker;

  ObArray<ObVarInfo> var_array;
  OZ(ObResolverUtils::resolve_const_expr(param, *node, expr, &var_array));

  return ret;
}

int ObExprTestKit::create_expr(const char *expr_str, ObObj *args, int arg_cnt,
                               ObRawExpr *raw_expr, ObSqlExpression &old_expr, ObExpr *&expr)
{
  int ret = OB_SUCCESS;
  // resolve raw expr
  OZ(resolve_const_expr(expr_str, raw_expr));

  if (raw_expr->get_param_count() != arg_cnt) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("arg count mismatch", K(raw_expr->get_param_count()), K(arg_cnt));
  }

  // replace argument with %args
  ObConstRawExpr *const_expr = NULL;
  for (int64_t i = 0; OB_SUCC(ret) && i < raw_expr->get_param_count(); i++) {
    const_expr = dynamic_cast<ObConstRawExpr *>(raw_expr->get_param_expr(i));
    if (NULL == const_expr) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("not count expr", K(ret), K(*raw_expr->get_param_expr(i)));
    } else {
      const_expr->set_value(args[i]);
    }
  }
  OZ(raw_expr->formalize(&session_));

  // generate old expr
  uint32_t next_id = 0;
  RowDesc row_desc;
  ObExprOperatorFactory expr_factory(allocator_);
  ObExprGeneratorImpl expr_generator(expr_factory, 0, 0, &next_id, row_desc);
  OZ(expr_generator.generate(*raw_expr, old_expr));

  // generate new expr
  int64_t expr_cnt = arg_cnt + 1;
  ObObjMeta metas[expr_cnt];
  metas[0] = raw_expr->get_result_type();
  for (int64_t i = 0; i < arg_cnt; i++) {
    metas[i + 1] = args[i].get_meta();
  }

  ObExpr *exprs = (ObExpr *)allocator_.alloc(sizeof(ObExpr) * expr_cnt);
  expr = exprs;

  const int64_t datum_eval_info_size = sizeof(ObDatum) + sizeof(ObEvalInfo);
  for (int64_t i = 0; OB_SUCC(ret) && i < expr_cnt; i++) {
    ObExpr &e = exprs[i];
    new (&e) ObExpr();
    e.obj_meta_ = metas[i];
    e.datum_meta_ = ObDatumMeta(metas[i].get_type(),
                                metas[i].get_collation_type(), metas[i].get_scale());
    e.obj_datum_map_ = ObDatum::get_obj_datum_map_type(e.obj_meta_.get_type());
    e.frame_idx_ = 0;
    e.datum_off_ = (datum_eval_info_size + 40) * i;
    e.eval_info_off_ = e.datum_off_ + sizeof(ObDatum);
    e.res_buf_off_ = e.datum_off_ + datum_eval_info_size;
    e.res_buf_len_ = 40;
  }

  ObExpr &e = exprs[0];
  e.frame_idx_ = 1;
  if (arg_cnt > 0) {
    e.args_ = (ObExpr **)allocator_.alloc(8 * arg_cnt);
    e.arg_cnt_ = arg_cnt;
    for (int64_t i = 0; i < arg_cnt; i++) {
      e.args_[i] = &exprs[i + 1];
    }
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < arg_cnt; i++) {
    OZ(e.args_[i]->locate_datum_for_write(eval_ctx_).from_obj(args[i], e.obj_datum_map_));
  }

  return ret;
}

/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX RS

#include "rootserver/mview/ob_mview_pending_task_executor.h"
#include "rootserver/mview/ob_mview_pending_task_table_operator.h"
#include "share/ob_rpc_struct.h"
#include "lib/timezone/ob_time_convert.h"
#include "observer/ob_server_struct.h"
#include "share/ob_get_compat_mode.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_schema_struct.h"
#include "share/schema/ob_table_schema.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/ob_physical_plan_ctx.h"
#include "sql/ob_sql_context.h"
#include "sql/plan_cache/ob_plan_cache_util.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/session/ob_sql_session_mgr.h"
#include "storage/mview/ob_mview_refresh.h"

namespace oceanbase
{
namespace rootserver
{

using namespace common;
using namespace share;
using namespace share::schema;
using namespace sql;
using namespace storage;

ObMViewPendingTaskExecutor::ObMViewPendingTaskExecutor()
  : is_inited_(false),
    schema_service_(NULL)
{
}

ObMViewPendingTaskExecutor::~ObMViewPendingTaskExecutor()
{
}

int ObMViewPendingTaskExecutor::init(ObMultiVersionSchemaService *schema_service)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("pending task executor init twice", KR(ret));
  } else if (OB_ISNULL(schema_service)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("schema service is null", KR(ret));
  } else {
    schema_service_ = schema_service;
    is_inited_ = true;
  }
  return ret;
}

int ObMViewPendingTaskExecutor::create_session(uint64_t tenant_id,
                                               ObFreeSessionCtx &free_session_ctx,
                                               ObSQLSessionInfo *&session)
{
  int ret = OB_SUCCESS;
  uint32_t sid = ObSQLSessionInfo::INVALID_SESSID;
  uint64_t proxy_sid = 0;
  if (OB_ISNULL(GCTX.session_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session_mgr_ is null", KR(ret));
  } else if (OB_FAIL(GCTX.session_mgr_->create_sessid(sid))) {
    LOG_WARN("alloc session id failed", KR(ret));
  } else if (OB_FAIL(GCTX.session_mgr_->create_session(
                 tenant_id, sid, proxy_sid, ObTimeUtility::current_time(), session))) {
    LOG_WARN("create session failed", KR(ret), K(sid), K(tenant_id));
    GCTX.session_mgr_->mark_sessid_unused(sid);
    session = NULL;
  } else if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null session", KR(ret));
  } else {
    free_session_ctx.sessid_ = sid;
    free_session_ctx.proxy_sessid_ = proxy_sid;
    free_session_ctx.tenant_id_ = tenant_id;
    free_session_ctx.has_inc_active_num_ = true;
  }
  return ret;
}

int ObMViewPendingTaskExecutor::destroy_session(ObFreeSessionCtx &free_session_ctx,
                                                ObSQLSessionInfo *session)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(GCTX.session_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session_mgr_ is null", KR(ret));
  } else if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is null", KR(ret));
  } else {
    session->set_session_sleep();
    GCTX.session_mgr_->revert_session(session);
    GCTX.session_mgr_->free_session(free_session_ctx);
    GCTX.session_mgr_->mark_sessid_unused(free_session_ctx.sessid_);
  }
  return ret;
}

int ObMViewPendingTaskExecutor::get_exec_user_info(uint64_t tenant_id,
                                                   uint64_t mview_id,
                                                   bool is_oracle_mode,
                                                   ObSchemaGetterGuard &schema_guard,
                                                   const ObTableSchema *mview_schema,
                                                   const ObUserInfo *&user_info)
{
  int ret = OB_SUCCESS;
  ObSEArray<const ObUserInfo *, 1> user_infos;
  user_info = NULL;
  uint64_t define_user_id = mview_schema->get_define_user_id();
  if (OB_INVALID_ID != define_user_id) {
    user_info = schema_guard.get_user_info(tenant_id, define_user_id);
  }
  if (OB_ISNULL(user_info)) {
    LOG_INFO("mview owner not found, fall back to default user",
      K(tenant_id), K(mview_id), K(define_user_id));
    if (is_oracle_mode) {
      if (OB_FAIL(schema_guard.get_user_info(tenant_id, ObString("SYS"), user_infos))) {
        LOG_WARN("get user info failed", KR(ret), K(tenant_id));
      } else if (1 != user_infos.count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected user info count", KR(ret), K(tenant_id), K(user_infos.count()));
      } else if (OB_ISNULL(user_info = user_infos.at(0))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("user info is null", KR(ret), K(tenant_id));
      }
    } else if (OB_FAIL(schema_guard.get_user_info(tenant_id, ObString("root"), ObString("%"),
                                                  user_info))) {
      LOG_WARN("get user info failed", KR(ret), K(tenant_id));
    } else if (OB_ISNULL(user_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("user info is null", KR(ret), K(tenant_id));
    }
  }
  return ret;
}

int ObMViewPendingTaskExecutor::get_session_timeout_us(int64_t expire_ts,
                                                        int64_t &timeout_us)
{
  int ret = OB_SUCCESS;
  const int64_t DEFAULT_TIMEOUT_US = 24LL * 60 * 60 * 1000 * 1000;
  const int64_t MIN_TIMEOUT_US = 1LL * 1000 * 1000;
  timeout_us = DEFAULT_TIMEOUT_US;
  if (expire_ts > 0) {
    const int64_t now = ObTimeUtility::current_time();
    const int64_t remaining = expire_ts - now;
    if (remaining <= 0) {
      ret = OB_TIMEOUT;
      LOG_WARN("mview refresh deadline already crossed when entering executor",
               KR(ret), K(expire_ts), K(now));
    } else {
      timeout_us = MAX(remaining, MIN_TIMEOUT_US);
    }
  }
  return ret;
}

int ObMViewPendingTaskExecutor::init_env(uint64_t tenant_id,
                                         uint64_t mview_id,
                                         bool is_oracle_mode,
                                         int64_t expire_ts,
                                         ObSchemaGetterGuard &schema_guard,
                                         ObSQLSessionInfo &session)
{
  int ret = OB_SUCCESS;
  const ObTenantSchema *tenant_info = NULL;
  const ObTableSchema *mview_schema = NULL;
  const ObDatabaseSchema *database_schema = NULL;
  const ObUserInfo *user_info = NULL;
  ObPrivSet db_priv_set = OB_PRIV_SET_EMPTY;
  ObPCMemPctConf pc_mem_conf;
  ObObj compatibility_mode;
  ObObj sql_mode;
  ObObj query_timeout_obj;
  ObObj trx_timeout_obj;
  int64_t timeout_us = 0;
  if (is_oracle_mode) {
    compatibility_mode.set_int(1);
    sql_mode.set_uint(ObUInt64Type, DEFAULT_ORACLE_MODE);
  } else {
    compatibility_mode.set_int(0);
    sql_mode.set_uint(ObUInt64Type, DEFAULT_MYSQL_MODE);
  }
  if (OB_FAIL(get_session_timeout_us(expire_ts, timeout_us))) {
    LOG_WARN("calc session timeout us failed", KR(ret), K(tenant_id), K(mview_id), K(expire_ts));
  } else if (OB_FAIL(schema_guard.get_tenant_info(tenant_id, tenant_info))) {
    LOG_WARN("get tenant info failed", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(tenant_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant info is null", KR(ret), K(tenant_id));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, mview_id, mview_schema))) {
    LOG_WARN("get mview schema failed", KR(ret), K(tenant_id), K(mview_id));
  } else if (OB_ISNULL(mview_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("mview schema is null", KR(ret), K(tenant_id), K(mview_id));
  } else if (OB_FAIL(schema_guard.get_database_schema(tenant_id,
                                                       mview_schema->get_database_id(),
                                                       database_schema))) {
    LOG_WARN("get database schema failed", KR(ret), K(tenant_id), K(mview_id));
  } else if (OB_ISNULL(database_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("database schema is null", KR(ret), K(tenant_id), K(mview_id));
  } else if (OB_FAIL(get_exec_user_info(tenant_id, mview_id, is_oracle_mode, schema_guard,
                                         mview_schema, user_info))) {
    LOG_WARN("get exec user info failed", KR(ret), K(tenant_id), K(mview_id));
  }
  if (OB_SUCC(ret)) {
    session.set_inner_session();
    if (OB_FAIL(session.load_default_sys_variable(true, true))) {
      LOG_WARN("load default sys variable failed", KR(ret));
    } else if (OB_FAIL(session.update_max_packet_size())) {
      LOG_WARN("update max packet size failed", KR(ret));
    } else if (OB_FAIL(session.init_tenant(tenant_info->get_tenant_name(), tenant_id))) {
      LOG_WARN("init tenant failed", KR(ret), K(tenant_id));
    } else if (OB_FAIL(session.load_all_sys_vars(schema_guard))) {
      LOG_WARN("load all sys vars failed", KR(ret), K(tenant_id));
    } else if (OB_FAIL(session.update_sys_variable(share::SYS_VAR_SQL_MODE, sql_mode))) {
      LOG_WARN("update sql mode failed", KR(ret), K(tenant_id));
    } else if (OB_FAIL(session.update_sys_variable(share::SYS_VAR_OB_COMPATIBILITY_MODE, compatibility_mode))) {
      LOG_WARN("update compatibility mode failed", KR(ret), K(tenant_id));
    } else if (is_oracle_mode &&
               OB_FAIL(session.update_sys_variable(share::SYS_VAR_NLS_DATE_FORMAT,
                                                   ObTimeConverter::COMPAT_OLD_NLS_DATE_FORMAT))) {
      LOG_WARN("update nls date format failed", KR(ret), K(tenant_id));
    } else if (is_oracle_mode &&
               OB_FAIL(session.update_sys_variable(share::SYS_VAR_NLS_TIMESTAMP_FORMAT,
                                                   ObTimeConverter::COMPAT_OLD_NLS_TIMESTAMP_FORMAT))) {
      LOG_WARN("update nls timestamp format failed", KR(ret), K(tenant_id));
    } else if (is_oracle_mode &&
               OB_FAIL(session.update_sys_variable(share::SYS_VAR_NLS_TIMESTAMP_TZ_FORMAT,
                                                   ObTimeConverter::COMPAT_OLD_NLS_TIMESTAMP_TZ_FORMAT))) {
      LOG_WARN("update nls timestamp tz format failed", KR(ret), K(tenant_id));
    } else if (OB_FAIL(session.set_default_database(database_schema->get_database_name()))) {
      LOG_WARN("set default database failed", KR(ret), K(tenant_id));
    } else if (OB_FAIL(session.get_pc_mem_conf(pc_mem_conf))) {
      LOG_WARN("get pc mem conf failed", KR(ret), K(tenant_id));
    } else if (OB_FALSE_IT(session.set_database_id(database_schema->get_database_id()))) {
    } else if (OB_FAIL(session.set_user(user_info->get_user_name(),
                                        user_info->get_host_name_str(),
                                        user_info->get_user_id()))) {
      LOG_WARN("set user failed", KR(ret), K(tenant_id));
    } else if (OB_FALSE_IT(session.set_priv_user_id(user_info->get_user_id()))) {
    } else if (OB_FALSE_IT(session.set_user_priv_set(user_info->get_priv_set()))) {
    } else if (OB_FALSE_IT(session.init_use_rich_format())) {
    } else if (OB_FAIL(schema_guard.get_db_priv_set(tenant_id, user_info->get_user_id(),
                                                    database_schema->get_database_name(), db_priv_set))) {
      LOG_WARN("get db priv set failed", KR(ret), K(tenant_id));
    } else if (OB_FALSE_IT(session.set_db_priv_set(db_priv_set))) {
    } else if (OB_FALSE_IT(session.get_enable_role_array().reuse())) {
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < user_info->get_role_id_array().count(); ++i) {
        if (user_info->get_disable_option(user_info->get_role_id_option_array().at(i)) == 0) {
          if (OB_FAIL(session.get_enable_role_array().push_back(user_info->get_role_id_array().at(i)))) {
            LOG_WARN("push back enable role failed", KR(ret), K(tenant_id), K(i));
          }
        }
      }
      if (OB_SUCC(ret)) {
        session.set_shadow(false);
        session.gen_gtt_session_scope_unique_id();
        session.gen_gtt_trans_scope_unique_id();
        session.set_client_sessid(session.get_sid());
        query_timeout_obj.set_int(timeout_us);
        trx_timeout_obj.set_int(timeout_us);
        if (OB_FAIL(session.update_sys_variable(share::SYS_VAR_OB_QUERY_TIMEOUT, query_timeout_obj))) {
          LOG_WARN("update query timeout failed", KR(ret), K(tenant_id));
        } else if (OB_FAIL(session.update_sys_variable(share::SYS_VAR_OB_TRX_TIMEOUT, trx_timeout_obj))) {
          LOG_WARN("update trx timeout failed", KR(ret), K(tenant_id));
        } else {
          session.set_query_start_time(ObTimeUtility::current_time());
          session.set_current_trace_id(ObCurTraceId::get_trace_id());
        }
      }
    }
  }
  return ret;
}

int ObMViewPendingTaskExecutor::run_pending_task(const obrpc::ObRunMViewPendingTaskArg &arg)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = arg.tenant_id_;
  const int64_t refresh_id = arg.refresh_id_;
  const uint64_t mview_id = arg.mview_id_;
  const share::schema::ObMVRefreshMethod refresh_method = arg.refresh_method_;
  const int64_t refresh_parallel = arg.refresh_parallel_;
  const int64_t expire_ts = arg.expire_ts_;
  bool is_oracle_mode = false;
  lib::Worker::CompatMode saved_mode = THIS_WORKER.get_compatibility_mode();
  const int64_t saved_timeout_ts = THIS_WORKER.get_timeout_ts();
  // Default worker deadline if no caller deadline was forwarded. Aligned with
  // the executor session's DEFAULT_TIMEOUT_US so SQL-layer timeout and worker
  // timeout converge on the same wall-clock point.
  const int64_t DEFAULT_WORKER_BUDGET_US = 24LL * 60 * 60 * 1000 * 1000;
  const int64_t now = ObTimeUtility::current_time();
  const int64_t worker_deadline = (expire_ts > 0) ? expire_ts : (now + DEFAULT_WORKER_BUDGET_US);
  THIS_WORKER.set_timeout_ts(worker_deadline);
  lib::Worker::CompatMode target_mode = saved_mode;
  bool need_restore_mode = false;
  ObArenaAllocator allocator("MVPendingExec", OB_MALLOC_NORMAL_BLOCK_SIZE, tenant_id);
  ObSchemaGetterGuard schema_guard;
  ObFreeSessionCtx free_session_ctx;
  ObSQLSessionInfo *session = NULL;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("pending task executor not init", KR(ret));
  } else if (OB_FAIL(ObCompatModeGetter::check_is_oracle_mode_with_tenant_id(tenant_id,
                                                                              is_oracle_mode))) {
    LOG_WARN("check compat mode failed", KR(ret), K(tenant_id));
  } else if (FALSE_IT(target_mode = is_oracle_mode ? lib::Worker::CompatMode::ORACLE
                                                   : lib::Worker::CompatMode::MYSQL)) {
  } else {
    need_restore_mode = (saved_mode != target_mode);
    if (OB_UNLIKELY(need_restore_mode)) {
      THIS_WORKER.set_compatibility_mode(target_mode);
    }
    if (OB_FAIL(schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
      LOG_WARN("get tenant schema guard failed", KR(ret), K(tenant_id));
    } else if (OB_FAIL(create_session(tenant_id, free_session_ctx, session))) {
      LOG_WARN("create session failed", KR(ret), K(tenant_id));
    } else if (OB_FAIL(ObMViewPendingTaskTableOperator::update_task_session_id(
                           GCTX.sql_proxy_, tenant_id, refresh_id, mview_id,
                           free_session_ctx.sessid_))) {
      LOG_WARN("update task session id failed",
               KR(ret), K(tenant_id), K(refresh_id), K(mview_id),
               K(free_session_ctx.sessid_));
    } else if (OB_FAIL(init_env(tenant_id, mview_id, is_oracle_mode, expire_ts, schema_guard, *session))) {
      LOG_WARN("init env failed", KR(ret), K(tenant_id), K(mview_id));
    } else {
      ObExecContext exec_ctx(allocator);
      ObPhysicalPlanCtx phy_plan_ctx(allocator);
      ObSqlCtx sql_ctx;
      session->set_session_sleep();                    // SESSION_INIT → SESSION_SLEEP
      session->set_thread_id(GETTID());                // bind executing thread
      exec_ctx.set_physical_plan_ctx(&phy_plan_ctx);
      exec_ctx.set_my_session(session);
      exec_ctx.set_sql_ctx(&sql_ctx);
      exec_ctx.set_sql_proxy(GCTX.sql_proxy_);
      exec_ctx.set_mem_attr(ObMemAttr(tenant_id,
                                      ObModIds::OB_SQL_EXEC_CONTEXT,
                                      ObCtxIds::EXECUTE_CTX_ID));
      sql_ctx.session_info_ = session;
      sql_ctx.schema_guard_ = &schema_guard;
      session->set_cur_exec_ctx(&exec_ctx);
      ObMViewRefreshParam refresh_param(tenant_id, mview_id, refresh_id, refresh_method, refresh_parallel);
      ObMViewRefresher refresher(exec_ctx, refresh_param);
      refresh_param.retry_id_ = arg.retry_count_;
      refresh_param.is_consistent_refresh_ = arg.is_consistent_refresh_;
      refresh_param.target_data_sync_scn_.reset();
      if (OB_FAIL(refresh_param.target_data_sync_scn_.convert_for_gts(
                     static_cast<int64_t>(arg.target_data_sync_scn_)))) {
        LOG_WARN("convert target data sync scn failed", KR(ret), K(tenant_id), K(mview_id), K(arg));
      } else if (OB_FAIL(session->set_session_active())) { // → QUERY_ACTIVE
        LOG_WARN("set session active failed", KR(ret), K(tenant_id), K(mview_id));
      } else if (OB_FAIL(refresher.refresh())) {
        LOG_WARN("fail to do refresh", KR(ret), K(tenant_id), K(mview_id), K(refresh_id));
      }
      session->set_cur_exec_ctx(NULL);
      exec_ctx.set_physical_plan_ctx(NULL);
      exec_ctx.set_sql_ctx(NULL);
    }
  }
  if (OB_LIKELY(NULL != session)) {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(destroy_session(free_session_ctx, session))) {
      LOG_WARN("destroy session failed", KR(tmp_ret));
      ret = OB_SUCC(ret) ? tmp_ret : ret;
    } else {
      session = NULL;
    }
  }
  if (OB_UNLIKELY(need_restore_mode)) {
    THIS_WORKER.set_compatibility_mode(saved_mode);
  }
  THIS_WORKER.set_timeout_ts(saved_timeout_ts);
  return ret;
}

} // namespace rootserver
} // namespace oceanbase

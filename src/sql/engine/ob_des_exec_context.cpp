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
#include "sql/ob_sql.h"
#include "sql/engine/ob_des_exec_context.h"
#include "sql/engine/ob_physical_plan_ctx.h"
#include "sql/session/ob_sql_session_info.h"
#include "observer/ob_server_struct.h"
#include "sql/session/ob_sql_session_mgr.h"
#include "lib/stat/ob_session_stat.h"

using namespace oceanbase::common;
namespace oceanbase
{
namespace sql
{

ObDesExecContext::ObDesExecContext(ObIAllocator &allocator, ObSQLSessionMgr *session_mgr)
    : ObExecContext(allocator)
{
  UNUSED(session_mgr);
  free_session_ctx_.sessid_ = ObSQLSessionInfo::INVALID_SESSID;
  set_sql_ctx(&sql_ctx_);
}

ObDesExecContext::~ObDesExecContext()
{
  cleanup_session();
  if (NULL != phy_plan_ctx_) {
    phy_plan_ctx_->~ObPhysicalPlanCtx();
    phy_plan_ctx_ = NULL;
  }
}

void ObDesExecContext::cleanup_session()
{
  if (NULL != my_session_) {
    if (ObSQLSessionInfo::INVALID_SESSID == free_session_ctx_.sessid_) {
      my_session_->~ObSQLSessionInfo();
      my_session_ = NULL;
    } else if (NULL != GCTX.session_mgr_) {
      my_session_->set_session_sleep();
      GCTX.session_mgr_->revert_session(my_session_);
      GCTX.session_mgr_->free_session(free_session_ctx_);
      my_session_ = NULL;
      GCTX.session_mgr_->mark_sessid_unused(free_session_ctx_.sessid_);
    }
  }
  ObActiveSessionGuard::setup_default_ash(); // enforce cleanup for future RPC cases
}

void ObDesExecContext::show_session()
{
  if (NULL != my_session_) {
    my_session_->set_shadow(false);
  }
}

void ObDesExecContext::hide_session()
{
  if (NULL != my_session_) {
    my_session_->set_shadow(true);
  }
}

int ObDesExecContext::create_my_session(uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *local_session = NULL;
  if (OB_UNLIKELY(my_session_ != NULL)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("my_session is not null.");
  } else if (NULL == GCTX.session_mgr_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session manager is NULL", K(ret));
  } else {
    uint32_t sid = ObSQLSessionInfo::INVALID_SESSID;
    uint64_t proxy_sid = 0;
    if (OB_FAIL(GCTX.session_mgr_->create_sessid(sid))) {
      LOG_WARN("alloc session id failed", K(ret));
    } else if (OB_FAIL(GCTX.session_mgr_->create_session(tenant_id, sid, proxy_sid,
                                                    ObTimeUtility::current_time(),
                                                    my_session_))) {
      LOG_WARN("create session failed", K(ret), K(sid));
      GCTX.session_mgr_->mark_sessid_unused(sid);
      my_session_ = NULL;
    } else {
      free_session_ctx_.sessid_ = sid;
      free_session_ctx_.proxy_sessid_ = proxy_sid;
    }
    if (OB_FAIL(ret)) {
      // fail back to local session allocating, avoid remote/distribute executing fail
      // if server session overflow.
      ret = OB_SUCCESS;
      if (OB_UNLIKELY(NULL == (local_session = static_cast<ObSQLSessionInfo*>(
          allocator_.alloc(sizeof(ObSQLSessionInfo)))))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("no more memory to create sql session info");
      } else {
        local_session = new (local_session) ObSQLSessionInfo();
        uint32_t tmp_sid = 0;
        uint64_t tmp_proxy_sessid = proxy_sid;
        bool session_in_mgr = false;
        if (OB_FAIL(GCTX.session_mgr_->create_sessid(tmp_sid, session_in_mgr))) {
          LOG_WARN("failed to mock session id", K(ret));
        } else if (OB_FAIL(local_session->init(tmp_sid, tmp_proxy_sessid, NULL))) {
          LOG_WARN("my session init failed", K(ret));
          local_session->~ObSQLSessionInfo();
        } else {
          my_session_ = local_session;
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    //notice: can't unlink exec context and session info here
    typedef ObSQLSessionInfo::ExecCtxSessionRegister MyExecCtxSessionRegister;
    MyExecCtxSessionRegister ctx_register(*my_session_, *this);
    sql_ctx_.session_info_ = my_session_;
  }
  return ret;
}

DEFINE_DESERIALIZE(ObDesExecContext)
{
  int ret = OB_SUCCESS;
  uint64_t ser_version = get_ser_version();
  int64_t index = 0;
  ObPhyOperatorType phy_op_type;
  int64_t tmp_phy_op_type = 0;
  uint64_t phy_op_size = 0;
  uint64_t tenant_id = OB_INVALID_TENANT_ID;
  if (ser_version == SER_VERSION_1) {
    OB_UNIS_DECODE(tenant_id);
  }
  OB_UNIS_DECODE(phy_op_size);
  //now to init ObExecContext container
  if (OB_SUCC(ret)) {
    if (OB_FAIL(create_physical_plan_ctx())) {
      LOG_WARN("create physical plan context failed", K(ret));
    } else if (OB_ISNULL(phy_plan_ctx_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("succ to create phy plan ctx, but phy plan ctx is NULL", K(ret));
    } else if (OB_FAIL(create_my_session(tenant_id))) {
      LOG_WARN("create my session failed", K(ret));
    } else if (OB_ISNULL(my_session_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("succ to create session, but session is NULL", K(ret));
    } else {
      OB_UNIS_DECODE(*phy_plan_ctx_);
      ObSQLSessionInfo::LockGuard query_guard(my_session_->get_query_lock());
      ObSQLSessionInfo::LockGuard data_guard(my_session_->get_thread_data_lock());
      OB_UNIS_DECODE(*my_session_);
      my_session_->set_is_remote(true);
      my_session_->set_session_type_with_flag();
      if (OB_FAIL(ret)) {
        LOG_WARN("session deserialize failed", K(ret));
      } else if (OB_FAIL(my_session_->set_session_active(
          ObString::make_string("REMOTE/DISTRIBUTE PLAN EXECUTING"),
          obmysql::COM_QUERY))) {
        LOG_WARN("set remote session active failed", K(ret));
      }
      // alloc from session manager, increase active session number
      if (OB_SUCC(ret) && free_session_ctx_.sessid_ != ObSQLSessionInfo::INVALID_SESSID) {
        free_session_ctx_.tenant_id_ = my_session_->get_effective_tenant_id();
        ObTenantStatEstGuard g(free_session_ctx_.tenant_id_);
        EVENT_INC(ACTIVE_SESSIONS);
        free_session_ctx_.has_inc_active_num_ = true;
      }
    }
  }

  if (OB_SUCC(ret)) {
    set_mem_attr(ObMemAttr(tenant_id, ObModIds::OB_SQL_EXEC_CONTEXT, ObCtxIds::EXECUTE_CTX_ID));
    // init operator context need session info, initialized after session deserialized.
    if (OB_FAIL(init_phy_op(phy_op_size))) {
      LOG_WARN("init exec context phy op failed", K(ret), K_(phy_op_size));
    }
  }

  OB_UNIS_DECODE(task_executor_ctx_);
  OB_UNIS_DECODE(das_ctx_);
  OB_UNIS_DECODE(sql_ctx_);
  if (OB_SUCC(ret)) {
    if (OB_FAIL(init_expr_op(phy_plan_ctx_->get_expr_op_size()))) {
      LOG_WARN("init exec context expr op failed", K(ret));
    } else {
      das_ctx_.get_location_router().set_retry_info(&my_session_->get_retry_info());
    }
  }
  use_temp_expr_ctx_cache_ = true;
  return ret;
}
}/* ns sql*/
}/* ns oceanbase */

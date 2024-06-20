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

#include "sql/engine/dml/ob_link_dml_op.h"
#include "sql/engine/ob_exec_context.h"
#include "observer/ob_server_struct.h"
#include "share/schema/ob_dblink_mgr.h"
#include "lib/mysqlclient/ob_mysql_connection.h"
#include "lib/mysqlclient/ob_mysql_connection_pool.h"
#include "sql/ob_sql_utils.h"
#include "sql/dblink/ob_tm_service.h"
#include "share/config/ob_server_config.h"
namespace oceanbase
{
using namespace common;
using namespace common::sqlclient;
using namespace share;
using namespace share::schema;

namespace sql
{
ObLinkDmlSpec::ObLinkDmlSpec(common::ObIAllocator &alloc, const ObPhyOperatorType type)
  : ObLinkSpec(alloc, type)
{}

OB_SERIALIZE_MEMBER((ObLinkDmlSpec, ObLinkSpec));

ObLinkDmlOp::ObLinkDmlOp(ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input)
  : ObLinkOp(exec_ctx, spec, input),
    affected_rows_(0)
{}

void ObLinkDmlOp::reset()
{
  reset_link_sql();
  reset_dblink();
  affected_rows_ = 0;
  dblink_schema_ = NULL;
}

int ObLinkDmlOp::send_reverse_link_info(transaction::ObTransID &tx_id)
{
  int ret = OB_SUCCESS;
  LOG_DEBUG("send_reverse_link_info", K(dblink_conn_->get_reverse_link_creadentials()), KP(dblink_conn_), K(link_type_));
  if (dblink_conn_->get_reverse_link_creadentials()) {
    // do nothing
  } else if (common::sqlclient::DblinkDriverProto::DBLINK_DRV_OB != link_type_) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("dblink connect to non-OceanBase database can't support reverse link", K(ret));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "dblink write to non-OceanBase database with reverse link(@! or @dblink!)");
  } else if (OB_ISNULL(dblink_schema_) || OB_ISNULL(dblink_conn_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL", K(ret), KP(dblink_schema_), KP(dblink_conn_));
  } else {
    const oceanbase::common::ObString &user_name = dblink_schema_->get_reverse_user_name();
    const oceanbase::common::ObString &tenant_name = dblink_schema_->get_reverse_tenant_name();
    const oceanbase::common::ObString &cluster_name = dblink_schema_->get_reverse_cluster_name();
    const oceanbase::common::ObString &passwd = dblink_schema_->get_plain_reverse_password();
    const oceanbase::common::ObAddr &addr = dblink_schema_->get_reverse_host_addr();
    if (user_name.empty() ||
        tenant_name.empty() ||
        passwd.empty() ||
        !addr.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("reverse link has invalid credentials", K(ret), K(user_name), K(tenant_name), K(passwd.empty()), K(addr));
      LOG_USER_ERROR(OB_ERR_UNEXPECTED, "check if the database link was created with local credentials");
    } else {
      ObReverseLink reverse_link_info;
      reverse_link_info.set_user(user_name);
      reverse_link_info.set_tenant(tenant_name);
      reverse_link_info.set_cluster(cluster_name);
      reverse_link_info.set_passwd(passwd);
      reverse_link_info.set_addr(addr);
      common::ObAddr local_addr = GCTX.self_addr();
      local_addr.set_port(ObServerConfig::get_instance().mysql_port);
      reverse_link_info.set_self_addr(local_addr);
      reverse_link_info.set_tm_sessid(sessid_);
      reverse_link_info.set_tx_id(tx_id.get_id());
      int64_t seri_pos = 0;
      int64_t seri_size = reverse_link_info.get_serialize_size();
      char * seri_buff = NULL;
      ObString hex_str;
      if (OB_ISNULL(seri_buff = static_cast<char*>(MY_SPEC.allocator_.alloc(seri_size)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc memory", K(seri_size), K(ret));
      } else if (OB_FAIL(reverse_link_info.serialize(seri_buff, seri_size, seri_pos))) {
          LOG_WARN("failed to serialize", KP(seri_buff), K(seri_size), K(seri_pos), K(ret));
      } else if (OB_FAIL(ObHexUtilsBase::hex(hex_str, MY_SPEC.allocator_, seri_buff, seri_size))) {
        LOG_WARN("failed to covert serialized binary to hex string", KP(seri_buff), K(seri_size), K(seri_pos), K(hex_str), K(ret));
      } else if (OB_FAIL(static_cast<common::sqlclient::ObMySQLConnection*>(dblink_conn_)->set_session_variable(
                                                      reverse_link_info.SESSION_VARIABLE_STRING, hex_str))) {
        LOG_WARN("failed to set session variable", K(ret), KP(dblink_conn_), K(hex_str),
                                                  K(reverse_link_info.SESSION_VARIABLE_STRING));
      } else {
        dblink_conn_->set_reverse_link_creadentials(true);
        LOG_DEBUG("succ send reverse_link_info", K(dblink_conn_), K(hex_str), K(reverse_link_info));
      }
    }
  }
  return ret;
}

int ObLinkDmlOp::inner_execute_link_stmt(const char *link_stmt)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session = ctx_.get_my_session();
  if (OB_ISNULL(dblink_proxy_) || OB_ISNULL(dblink_conn_) || OB_ISNULL(link_stmt) || OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dblink_proxy or link_stmt is NULL", K(ret), KP(dblink_proxy_), KP(link_stmt), KP(dblink_conn_), KP(session));
  } else if (MY_SPEC.is_reverse_link_ && OB_FAIL(send_reverse_link_info(session->get_dblink_context().get_tx_id()))) {
    LOG_WARN("failed to send reverse link info", K(ret), K(link_stmt));
  } else if (OB_FAIL(dblink_proxy_->dblink_write(dblink_conn_, affected_rows_, link_stmt))) {
    LOG_WARN("write failed", K(ret), K(link_stmt));
  } else {
    LOG_DEBUG("LINKDMLOP succ to dblink write", K(affected_rows_), K(link_stmt), KP(dblink_conn_), K(ret));
  }
  return ret;
}

void ObLinkDmlOp::reset_dblink()
{
  int tmp_ret = OB_SUCCESS;
  // when it is oci connection, does not need terminate stmt like read, because write will terminate stmt after execute_update immediately
  ObDblinkCtxInSession::revert_dblink_conn(dblink_conn_); // release rlock locked by get_dblink_conn
  tenant_id_ = OB_INVALID_ID;
  dblink_id_ = OB_INVALID_ID;
  dblink_proxy_ = NULL;
  dblink_conn_ = NULL;
}

int ObLinkDmlOp::inner_open()
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session = ctx_.get_my_session();
  ObPhysicalPlanCtx *plan_ctx = ctx_.get_physical_plan_ctx();
  stmt_buf_len_ += head_comment_length_;
  in_xa_transaction_ = true; // link dml aways in xa trasaction
  dblink_id_ = MY_SPEC.dblink_id_;
  dblink_proxy_ = GCTX.dblink_proxy_;
  if (OB_ISNULL(session) || OB_ISNULL(plan_ctx) || OB_ISNULL(dblink_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session or plan_ctx or dblink_proxy_ is NULL", K(ret), KP(session), KP(plan_ctx), KP(dblink_proxy_));
  } else if (FALSE_IT(tenant_id_ = session->get_effective_tenant_id())) {
  } else if (FALSE_IT(sessid_ = session->get_sessid())) {
  } else if (OB_FAIL(set_next_sql_req_level())) {
    LOG_WARN("failed to set next sql req level", K(ret));
  } else if (OB_FAIL(init_dblink())) {
    LOG_WARN("failed to init dblink", K(ret), K(dblink_id_));
  } else if (OB_ISNULL(stmt_buf_ = static_cast<char *>(allocator_.alloc(stmt_buf_len_)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to init stmt buf", K(ret), K(stmt_buf_len_));
  } else {
    LOG_DEBUG("succ to open link dml", K(lbt()));
  }
  return ret;
}

int ObLinkDmlOp::inner_get_next_row()
{
  int ret = OB_SUCCESS;
  const ObString &stmt_fmt = MY_SPEC.stmt_fmt_;
  const ObIArray<ObParamPosIdx> &param_infos = MY_SPEC.param_infos_;
  ObPhysicalPlanCtx *plan_ctx = ctx_.get_physical_plan_ctx();
  if (OB_ISNULL(plan_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get plan ctx", K(ret));
  } else if (common::sqlclient::DblinkDriverProto::DBLINK_DRV_OB != link_type_ && MY_SPEC.is_reverse_link_) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("dblink connect to non-OceanBase database can't support reverse link", K(ret));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "dblink write to non-OceanBase database with reverse link(@! or @dblink!)");
  } else if (OB_FAIL(execute_link_stmt(stmt_fmt, param_infos, plan_ctx->get_param_store()))) {
    LOG_WARN("failed to execute link stmt", K(ret), K(stmt_fmt), K(param_infos));
  } else {
    plan_ctx->add_affected_rows(affected_rows_);
    ret = OB_ITER_END;
    LOG_DEBUG("succ to exec link dml", K(affected_rows_), K(ret));
  }
  return ret;
}

int ObLinkDmlOp::inner_close()
{
  int ret = OB_SUCCESS;
  reset();
  return ret;
}

int ObLinkDmlOp::inner_rescan()
{
  reset_link_sql();
  affected_rows_ = 0;
  return ObOperator::inner_rescan();
}

} // end namespace sql
} // end namespace oceanbase

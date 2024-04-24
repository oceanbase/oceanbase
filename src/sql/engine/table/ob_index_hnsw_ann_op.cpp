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

#include "sql/engine/table/ob_index_hnsw_ann_op.h"
#include "observer/ob_inner_sql_connection_pool.h"
#include "sql/engine/ob_exec_context.h"

namespace oceanbase
{
using namespace common::sqlclient;
using namespace observer;
namespace sql
{

int ObIndexHnswAnnOp::open_inner_conn()
{
    int ret = OB_SUCCESS;
    ObInnerSQLConnectionPool *pool = NULL;
    ObSQLSessionInfo *session = NULL;
    ObISQLConnection *conn;
    if (OB_ISNULL(sql_proxy_ = ctx_.get_sql_proxy())) {
        ret = OB_NOT_INIT;
        LOG_WARN("sql proxy is NULL", K(ret));
    } else if (OB_ISNULL(session = ctx_.get_my_session())) {
        ret = OB_NOT_INIT;
        LOG_WARN("session is NULL", K(ret));
    } else if (NULL != session->get_inner_conn()) {
        // do nothing.
    } else if (OB_ISNULL(pool = static_cast<ObInnerSQLConnectionPool*>(sql_proxy_->get_pool()))) {
        ret = OB_NOT_INIT;
        LOG_WARN("connection pool is NULL", K(ret));
    } else if (INNER_POOL != pool->get_type()) {
        LOG_WARN("connection pool type is not inner", K(ret), K(pool->get_type()));
    } else if (OB_FAIL(pool->acquire(session, conn))) {
        LOG_WARN("failed to acquire inner connection", K(ret));
    } else {
        /**
         * session is the only data struct which can pass through multi layer nested sql,
         * so we put inner conn in session to share it within multi layer nested sql.
         */
        session->set_inner_conn(conn);
    }
    if (OB_SUCC(ret)) {
        inner_conn_ = static_cast<ObInnerSQLConnection *>(session->get_inner_conn());
        tenant_id_ = session->get_effective_tenant_id();
    }
    return ret;
}

int ObIndexHnswAnnOp::close_inner_conn()
{
  /**
   * we can call it even if open_inner_conn() failed, because only the one who call
   * open_inner_conn() succeed first will do close job by "if (need_close_conn_)".
   */
    int ret = OB_SUCCESS;
    ObSQLSessionInfo *session = ctx_.get_my_session();
    if (OB_ISNULL(sql_proxy_) || OB_ISNULL(session)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("sql_proxy of session is NULL", K(ret), KP(sql_proxy_), KP(session));
    } else {
        OZ(sql_proxy_->close(static_cast<ObInnerSQLConnection *>(session->get_inner_conn()), true));
        OX(session->set_inner_conn(NULL));
    }
    sql_proxy_ = NULL;
    inner_conn_ = NULL;
    return ret;
}

}
}
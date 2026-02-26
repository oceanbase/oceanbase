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

#define USING_LOG_PREFIX STORAGE

#include "storage/vector_index/ob_vector_refresh_idx_transaction.h"
#include "observer/ob_inner_sql_connection_pool.h"

namespace oceanbase
{
namespace storage
{
using namespace observer;
using namespace share;
using namespace sql;
using namespace common::sqlclient;


ObVectorRefreshIdxTransaction::ObSessionParamSaved::ObSessionParamSaved()
  : session_info_(nullptr), is_inner_(false), autocommit_(false)
{
}

ObVectorRefreshIdxTransaction::ObSessionParamSaved::~ObSessionParamSaved()
{
  int ret = OB_SUCCESS;
  if (nullptr != session_info_) {
    if (OB_FAIL(restore())) {
      LOG_WARN("fail to restore session param", KR(ret));
    }
  }
}

int ObVectorRefreshIdxTransaction::ObSessionParamSaved::save(ObSQLSessionInfo *session_info)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("already save one session param", KR(ret), KP(session_info_), KP(session_info));
  } else if (OB_ISNULL(session_info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(session_info));
  } else {
    bool autocommit = false;
    if (OB_FAIL(session_info->get_autocommit(autocommit))) {
      LOG_WARN("fail to get autocommit", KR(ret));
    } else {
      session_info_ = session_info;
      is_inner_ = session_info->is_inner();
      autocommit_ = autocommit;
      session_info->set_inner_session();
      session_info->set_autocommit(false);
      session_info->get_ddl_info().set_is_dummy_ddl_for_inner_visibility(true);
    }
  }
  return ret;
}

int ObVectorRefreshIdxTransaction::ObSessionParamSaved::restore()
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(session_info_)) {
    if (is_inner_) {
      session_info_->set_inner_session();
    } else {
      session_info_->set_user_session();
    }
    session_info_->set_autocommit(autocommit_);
    session_info_->get_ddl_info().set_is_dummy_ddl_for_inner_visibility(false);
    session_info_ = nullptr;
  }
  return ret;
}

ObVectorRefreshIdxTransaction::ObVectorRefreshIdxTransaction()
  : session_info_(nullptr), start_time_(OB_INVALID_TIMESTAMP), in_trans_(false)
{
}

ObVectorRefreshIdxTransaction::~ObVectorRefreshIdxTransaction()
{
  int ret = OB_SUCCESS;
  if (in_trans_) {
    if (OB_FAIL(end(OB_SUCCESS == get_errno()))) {
      LOG_WARN("fail to end", KR(ret));
    }
  }
}

int ObVectorRefreshIdxTransaction::connect(ObSQLSessionInfo *session_info, ObISQLClient *sql_client)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr != pool_ || nullptr != conn_)) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("transaction can only be started once", KR(ret), K(pool_), K(conn_));
  } else if (OB_UNLIKELY(nullptr == session_info || nullptr == sql_client)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(session_info), KP(sql_client));
  } else {
    ObInnerSQLConnectionPool *pool = nullptr;
    ObInnerSQLConnection *conn = nullptr;
    if (OB_ISNULL(pool = static_cast<ObInnerSQLConnectionPool *>(sql_client->get_pool()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected connection pool", KR(ret));
    } else if (OB_FAIL(pool->acquire_spi_conn(session_info, conn))) {
      LOG_WARN("acquire connection failed", KR(ret), K(pool), K(session_info));
    } else if (OB_ISNULL(conn)) {
      ret = OB_INNER_STAT_ERROR;
      LOG_WARN("connection can not be NULL", KR(ret), K_(pool));
    } else if (!sql_client->is_active()) {
      ret = OB_INACTIVE_SQL_CLIENT;
      LOG_WARN("inactive sql client", KR(ret));
      int tmp_ret = pool->release(conn, OB_SUCCESS == ret);
      if (OB_SUCCESS != tmp_ret) {
        LOG_WARN("release connection failed", K(tmp_ret));
      }
      conn = nullptr;
    } else {
      sql_client_ = sql_client;
      pool_ = pool;
      conn_ = conn;
      oracle_mode_ = session_info->is_oracle_compatible();
    }
  }
  return ret;
}

int ObVectorRefreshIdxTransaction::start_transaction(uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObISQLConnection *conn = nullptr;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(conn = get_connection())) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("conn_ is NULL", KR(ret));
  } else {
    if (OB_FAIL(conn->start_transaction(tenant_id, false /*with_snapshot*/))) {
      LOG_WARN("fail to start transaction", KR(ret), K(tenant_id));
    }
    if (OB_SUCCESS == get_errno()) {
      set_errno(ret);
    }
  }
  return ret;
}

int ObVectorRefreshIdxTransaction::end_transaction(const bool commit)
{
  int ret = OB_SUCCESS;
  ObISQLConnection *conn = nullptr;
  if (OB_ISNULL(conn = get_connection())) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("conn_ is NULL", KR(ret));
  } else {
    if (commit) {
      if (OB_FAIL(conn->commit())) {
        LOG_WARN("fail to do commit", KR(ret));
      }
    } else {
      if (OB_FAIL(conn->rollback())) {
        LOG_WARN("fail to do rollback", KR(ret));
      }
    }
    if (OB_SUCCESS == get_errno()) {
      set_errno(ret);
    }
  }
  return ret;
}

int ObVectorRefreshIdxTransaction::start(ObSQLSessionInfo *session_info, ObISQLClient *sql_client)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(in_trans_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("already in trans", KR(ret));
  } else if (OB_UNLIKELY(nullptr == session_info || nullptr == sql_client)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(session_info), KP(sql_client));
  } else if (OB_UNLIKELY(session_info->is_in_transaction())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected session is in trans", KR(ret));
  } else if (OB_FAIL(session_param_saved_.save(session_info))) {
    LOG_WARN("fail to save session param", KR(ret));
  } else if (OB_FAIL(connect(session_info, sql_client))) {
    LOG_WARN("fail to connect", KR(ret));
  } else {
    const uint64_t tenant_id = session_info->get_effective_tenant_id();
    start_time_ = ObTimeUtility::current_time();
    if (OB_FAIL(start_transaction(tenant_id))) {
      LOG_WARN("failed to start transaction", KR(ret), K(tenant_id));
    } else {
      session_info_ = session_info;
      in_trans_ = true;
      LOG_DEBUG("start transaction success", K(tenant_id));
    }
  }
  if (OB_FAIL(ret)) {
    int tmp_ret = OB_SUCCESS;
    close();
    if (OB_TMP_FAIL(session_param_saved_.restore())) {
      LOG_WARN("fail to restore session param", KR(tmp_ret));
    }
  }
  return ret;
}

int ObVectorRefreshIdxTransaction::end(const bool commit)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (in_trans_) {
    if (OB_FAIL(end_transaction(commit))) {
      LOG_WARN("fail to end transation", KR(ret));
    } else {
      LOG_DEBUG("end transaction success", K(commit));
    }
    in_trans_ = false;
  }
  close();
  if (OB_TMP_FAIL(session_param_saved_.restore())) {
    LOG_WARN("fail to restore session param", KR(tmp_ret));
    ret = COVER_SUCC(tmp_ret);
  }
  return ret;
}

}
}
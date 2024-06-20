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

#include "storage/mview/ob_mview_transaction.h"
#include "observer/ob_inner_sql_connection.h"
#include "observer/ob_inner_sql_connection_pool.h"

namespace oceanbase
{
namespace storage
{
using namespace observer;
using namespace share;
using namespace sql;
using namespace common::sqlclient;

/**
 * ObSessionParamSaved
 */

ObMViewTransaction::ObSessionParamSaved::ObSessionParamSaved()
  : session_info_(nullptr), is_inner_(false), autocommit_(false)
{
}

ObMViewTransaction::ObSessionParamSaved::~ObSessionParamSaved()
{
  int ret = OB_SUCCESS;
  if (nullptr != session_info_) {
    if (OB_FAIL(restore())) {
      LOG_WARN("fail to restore session param", KR(ret));
    }
  }
}

int ObMViewTransaction::ObSessionParamSaved::save(ObSQLSessionInfo *session_info)
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
      session_info_->get_ddl_info().set_refreshing_mview(true);
    }
  }
  return ret;
}

int ObMViewTransaction::ObSessionParamSaved::restore()
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(session_info_)) {
    if (is_inner_) {
      session_info_->set_inner_session();
    } else {
      session_info_->set_user_session();
    }
    session_info_->set_autocommit(autocommit_);
    session_info_->get_ddl_info().set_refreshing_mview(false);
    session_info_ = nullptr;
  }
  return ret;
}

/**
 * ObSessionSavedForInner
 */

ObMViewTransaction::ObSessionSavedForInner::ObSessionSavedForInner()
  : allocator_("MVSessionSaved"),
    session_info_(nullptr),
    session_saved_value_(nullptr),
    database_id_(OB_INVALID_ID),
    database_name_(nullptr)
{
}

ObMViewTransaction::ObSessionSavedForInner::~ObSessionSavedForInner()
{
  int ret = OB_SUCCESS;
  if (nullptr != session_info_) {
    if (OB_FAIL(restore())) {
      LOG_WARN("fail to restore session param", KR(ret));
    }
  }
}

int ObMViewTransaction::ObSessionSavedForInner::save(ObSQLSessionInfo *session_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr != session_info_ || nullptr != session_saved_value_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("already save one session param", KR(ret), KP(session_info_),
             KP(session_saved_value_));
  } else if (OB_ISNULL(session_info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(session_info));
  } else {
    const int64_t max_database_name_len = OB_MAX_DATABASE_NAME_BUF_LENGTH * OB_MAX_CHAR_LEN;
    const uint64_t tenant_id = session_info->get_effective_tenant_id();
    const uint64_t database_id = session_info->get_database_id();
    const ObString database_name = session_info->get_database_name();
    ObSQLSessionInfo::StmtSavedValue *session_saved_value = nullptr;
    char *database_name_buf = nullptr;
    allocator_.reuse();
    allocator_.set_tenant_id(tenant_id);
    if (OB_ISNULL(session_saved_value = OB_NEWx(ObSQLSessionInfo::StmtSavedValue, &allocator_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to new ObSQLSessionInfo::StmtSavedValue", KR(ret));
    } else if (OB_ISNULL(database_name_buf =
                           static_cast<char *>(allocator_.alloc(max_database_name_len)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memory", KR(ret));
    }
    // save session
    if (OB_SUCC(ret)) {
      if (OB_FAIL(session_info->save_session(*session_saved_value))) {
        LOG_WARN("fail to save session", K(ret));
      } else {
        session_saved_value_ = session_saved_value;
      }
    }
    // change default database
    if (OB_SUCC(ret)) {
      MEMCPY(database_name_buf, database_name.ptr(), database_name.length());
      database_name_buf[database_name.length()] = '\0';
      if (OB_FAIL(session_info->set_default_database(OB_SYS_DATABASE_NAME))) {
        LOG_WARN("fail to set default database", KR(ret));
      } else {
        session_info->set_database_id(OB_SYS_DATABASE_ID);
        database_id_ = database_id;
        database_name_ = database_name_buf;
      }
    }
    if (OB_SUCC(ret)) {
      session_info_ = session_info;
    }
    if (OB_FAIL(ret)) {
      int tmp_ret = OB_SUCCESS;
      // restore session
      if (nullptr != session_saved_value_) {
        if (OB_TMP_FAIL(session_info->restore_session(*session_saved_value_))) {
          LOG_WARN("failed to restore session", KR(tmp_ret));
        } else {
          session_saved_value_ = nullptr;
        }
      }
      // restore default database
      if (nullptr != database_name_) {
        if (OB_FAIL(session_info->set_default_database(database_name_))) {
          LOG_WARN("fail to set default database", KR(ret));
        } else {
          session_info->set_database_id(database_id_);
          database_id_ = OB_INVALID_ID;
          database_name_ = nullptr;
        }
      }
      if (nullptr != session_saved_value) {
        session_saved_value->ObSQLSessionInfo::StmtSavedValue::~StmtSavedValue();
        allocator_.free(session_saved_value);
        session_saved_value = nullptr;
      }
      if (nullptr != database_name_buf) {
        allocator_.free(database_name_buf);
        database_name_buf = nullptr;
      }
    }
  }
  return ret;
}

int ObMViewTransaction::ObSessionSavedForInner::restore()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (OB_NOT_NULL(session_info_)) {
    // restore session
    if (nullptr != session_saved_value_) {
      if (OB_TMP_FAIL(session_info_->restore_session(*session_saved_value_))) {
        LOG_WARN("failed to restore session", KR(tmp_ret));
        ret = COVER_SUCC(tmp_ret);
      } else {
        session_saved_value_->ObSQLSessionInfo::StmtSavedValue::~StmtSavedValue();
        allocator_.free(session_saved_value_);
        session_saved_value_ = nullptr;
      }
    }
    // restore default database
    if (nullptr != database_name_) {
      if (OB_TMP_FAIL(session_info_->set_default_database(database_name_))) {
        LOG_WARN("fail to set default database", KR(tmp_ret));
        ret = COVER_SUCC(tmp_ret);
      } else {
        session_info_->set_database_id(database_id_);
        database_id_ = OB_INVALID_ID;
        allocator_.free(database_name_);
        database_name_ = nullptr;
      }
    }
    session_info_ = nullptr;
  }
  return ret;
}

/**
 * ObMViewTransaction
 */

ObMViewTransaction::ObMViewTransaction()
  : session_info_(nullptr), start_time_(OB_INVALID_TIMESTAMP), in_trans_(false)
{
}

ObMViewTransaction::~ObMViewTransaction()
{
  int ret = OB_SUCCESS;
  if (in_trans_) {
    if (OB_FAIL(end(OB_SUCCESS == get_errno()))) {
      LOG_WARN("fail to end", KR(ret));
    }
  }
}

int ObMViewTransaction::connect(ObSQLSessionInfo *session_info, ObISQLClient *sql_client)
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
    } else if (!sql_client->is_active()) { // check client active after connection acquired
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

int ObMViewTransaction::start_transaction(uint64_t tenant_id)
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

int ObMViewTransaction::end_transaction(const bool commit)
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

int ObMViewTransaction::start(ObSQLSessionInfo *session_info, ObISQLClient *sql_client)
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

int ObMViewTransaction::end(const bool commit)
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

int ObMViewTransaction::save_session_for_inner()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(session_info_)) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("session info can not be NULL", KR(ret), KP(session_info_));
  } else if (OB_FAIL(session_saved_for_inner_.save(session_info_))) {
    LOG_WARN("fail to save session for inner", KR(ret));
  }
  return ret;
}

int ObMViewTransaction::restore_session_for_inner()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(session_info_)) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("session info can not be NULL", KR(ret), KP(session_info_));
  } else if (OB_FAIL(session_saved_for_inner_.restore())) {
    LOG_WARN("fail to restore session for inner", KR(ret));
  }
  return ret;
}

int ObMViewTransaction::set_compact_mode(ObCompatibilityMode compact_mode)
{
  int ret = OB_SUCCESS;
  ObISQLConnection *conn = nullptr;
  if (OB_ISNULL(conn = get_connection())) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("conn_ is NULL", KR(ret));
  } else if (OB_UNLIKELY(ObCompatibilityMode::OCEANBASE_MODE == compact_mode)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(compact_mode));
  } else {
    if (OB_FAIL(conn->set_session_variable(OB_SV_COMPATIBILITY_MODE,
                                           static_cast<int64_t>(compact_mode)))) {
      LOG_WARN("fail to set inner connection compact mode", KR(ret), K(compact_mode));
    } else {
      if (is_oracle_compatible(compact_mode)) {
        conn->set_oracle_compat_mode();
      } else {
        conn->set_mysql_compat_mode();
      }
    }
  }
  return ret;
}

/**
 * ObMViewTransactionInnerMySQLGuard
 */

ObMViewTransactionInnerMySQLGuard::ObMViewTransactionInnerMySQLGuard(ObMViewTransaction &trans)
  : trans_(trans),
    old_compact_mode_(ObCompatibilityMode::OCEANBASE_MODE),
    error_ret_(OB_SUCCESS),
    need_reset_(false),
    first_loop_(true)
{
  int &ret = error_ret_;
  ObSQLSessionInfo *session_info = nullptr;
  if (OB_UNLIKELY(!trans_.is_started())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("trans is not started", KR(ret));
  } else {
    need_reset_ = true;
    old_compact_mode_ = trans_.get_compatibility_mode();
    if (OB_FAIL(trans_.save_session_for_inner())) {
      LOG_WARN("fail to save session for inner", KR(ret));
    } else if (OB_FAIL(trans_.set_compact_mode(ObCompatibilityMode::MYSQL_MODE))) {
      LOG_WARN("fail to set mysql compact mode", KR(ret));
    }
  }
}

ObMViewTransactionInnerMySQLGuard::~ObMViewTransactionInnerMySQLGuard()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (!need_reset_) {
    // do nothing
  } else {
    if (OB_TMP_FAIL(trans_.restore_session_for_inner())) {
      LOG_WARN("fail to restore session for inner", KR(tmp_ret));
      ret = COVER_SUCC(tmp_ret);
    }
    if (OB_TMP_FAIL(trans_.set_compact_mode(old_compact_mode_))) {
      LOG_WARN("fail to set compact mode", KR(tmp_ret), K(old_compact_mode_));
      ret = COVER_SUCC(tmp_ret);
    }
  }
  if (OB_SUCCESS == error_ret_) {
    error_ret_ = ret;
  }
}

} // namespace storage
} // namespace oceanbase

// Copyright (c) 2021 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.

#include "storage/tx/ob_dblink_client.h"
#ifdef OB_BUILD_ORACLE_PL
#include "pl/ob_pl_stmt.h"
#endif

namespace oceanbase
{
using namespace common;
using namespace common::sqlclient;
using namespace share;
using namespace pl;

namespace transaction
{
void ObDBLinkClient::reset()
{
  index_ = 0;
  xid_.reset();
  state_ = ObDBLinkClientState::IDLE;
  dblink_type_ = sqlclient::DblinkDriverProto::DBLINK_UNKNOWN;
  dblink_conn_ = NULL;
  if (NULL != impl_) {
    impl_->~ObXAQuery();
    mtl_free(impl_);
    impl_ = NULL;
  }
  tx_timeout_us_ = -1;
  savepoint_array_.reset();
  dblink_statistics_ = NULL;
  is_inited_ = false;
}

int ObDBLinkClient::init(const uint32_t index,
                         const DblinkDriverProto dblink_type,
                         const int64_t tx_timeout_us,
                         ObISQLConnection *dblink_conn,
                         ObDBLinkTransStatistics *dblink_statistics)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    TRANS_LOG(WARN, "init twice", K(ret), K(*this));
  } else if (DblinkDriverProto::DBLINK_UNKNOWN == dblink_type
      || NULL == dblink_conn
      || 0 > tx_timeout_us
      || 0 == index
      || NULL == dblink_statistics) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid arguments", K(ret), K(dblink_type), KP(dblink_conn),
        K(tx_timeout_us), K(index), KP(dblink_statistics));
  } else {
    index_ = index;
    dblink_conn_ = dblink_conn;
    dblink_type_ = dblink_type;
    tx_timeout_us_ = tx_timeout_us;
    dblink_statistics_ = dblink_statistics;
    is_inited_ = true;
    TRANS_LOG(INFO, "dblink client init", K(*this));
  }
  return ret;
}

// execute xa start for dblink client
// 1. if START, return success directly
// 2. if IDLE, execute xa start
//    2.1 create dblink default savepoint
// @param[in] xid
int ObDBLinkClient::rm_xa_start(const ObXATransID &xid, const ObTxIsolationLevel isolation)
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(lock_);
  const int64_t start_ts = ObTimeUtility::current_time();

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "dblink client is not inited", K(ret), K(xid), K(*this));
  } else if (NULL == dblink_statistics_) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "unexpected dblink client", K(ret), K(xid), K(*this), KP_(dblink_statistics));
  } else if (!xid.is_valid() || xid.empty() || ObTxIsolationLevel::INVALID == isolation) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), K(xid), K(isolation));
  } else if (ObDBLinkClientState::IDLE != state_) {
    if (ObDBLinkClientState::START == state_
        && xid.all_equal_to(xid_)) {
      // return OB_SUCCESS
    } else {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "unexpected dblink client", K(ret), K(xid), K(*this));
    }
  // TODO, check connection
  } else {
    int64_t flag = ObXAFlag::OBTMNOFLAGS;
    if (ObTxIsolationLevel::RR == isolation || ObTxIsolationLevel::SERIAL == isolation) {
      flag = ObXAFlag::OBTMSERIALIZABLE;
    }
    if (OB_FAIL(init_query_impl_(isolation))) {
      TRANS_LOG(WARN, "fail to init query impl", K(ret), K(xid), K(isolation), K(*this));
    } else if (NULL == impl_) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "unexpected query impl", K(ret), K(xid), K(*this));
    } else if (OB_FAIL(impl_->xa_start(xid, flag))) {
      TRANS_LOG(WARN, "fail to execute query", K(ret), K(xid), K(flag), K(*this));
    } else {
      xid_ = xid;
      state_ = ObDBLinkClientState::START;
    }
    // create default savepoint when xa branch start
    if (OB_SUCC(ret)) {
      if (OB_FAIL(rm_create_savepoint_(common::ObString(DBLINK_DEFAULT_SAVEPOINT)))) {
        TRANS_LOG(WARN, "fail to execute create dblink default savepoint", K(ret), K(xid), K(flag), KPC(this));
      }
    }
    TRANS_LOG(INFO, "rm xa start for dblink", K(ret), K(xid), K(isolation), K(flag));
  }
  // for statistics
  const int64_t used_time_us = ObTimeUtility::current_time() - start_ts;
  dblink_statistics_->inc_dblink_trans_xa_start_count();
  dblink_statistics_->add_dblink_trans_xa_start_used_time(used_time_us);
  if (OB_FAIL(ret)) {
    dblink_statistics_->inc_dblink_trans_xa_start_fail_count();
  }
  return ret;
}

// execute xa end for dblink client
// 1. if END, return success directly
// 2. if START, execute xa end
int ObDBLinkClient::rm_xa_end()
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(lock_);
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "dblink client is not inited", K(ret), K(*this));
  } else if (NULL == dblink_statistics_) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "unexpected dblink client", K(ret), K(*this), KP_(dblink_statistics));
  } else if (!xid_.is_valid() || xid_.empty()) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "invalid xid", K(ret), K(*this));
  } else if (NULL == impl_) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "invalid query impl", K(ret), K(*this));
  // TODO, check connection
  } else {
    ret = rm_xa_end_();
  }
  return ret;
}

// execute xa prepare for dblink client
// 1. if START, execute xa end first
// 2. if END, execute xa prepare
// 3. if PREPARED or RDONLY_PREPARED, return success directly
int ObDBLinkClient::rm_xa_prepare()
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(lock_);

  // step 1, execute xa end if necessary
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "dblink client is not inited", K(ret), K(*this));
  } else if (NULL == dblink_statistics_) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "unexpected dblink client", K(ret), K(*this), KP_(dblink_statistics));
  } else if (!xid_.is_valid() || xid_.empty()) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "invalid xid", K(ret), K(*this));
  } else if (NULL == impl_) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "invalid query impl", K(ret), K(*this));
  // TODO, check connection
  } else if (ObDBLinkClientState::START == state_) {
    if (OB_FAIL(rm_xa_end_())) {
      TRANS_LOG(WARN, "fail to execute xa end", K(ret), K(*this));
    }
  }

  // step 2, execute xa prepare
  if (OB_SUCCESS != ret) {
  } else if (ObDBLinkClientState::END != state_) {
    if (ObDBLinkClientState::PREPARED == state_
        || ObDBLinkClientState::RDONLY_PREPARED == state_) {
      // return OB_SUCCESS
    } else {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "unexpected dblink client", K(ret), K(*this));
    }
  } else {
    const int64_t start_ts = ObTimeUtility::current_time();
    state_ = ObDBLinkClientState::PREPARING;
    if (OB_FAIL(impl_->xa_prepare(xid_))) {
      if (OB_TRANS_XA_RDONLY != ret) {
        TRANS_LOG(WARN, "fail to execute query", K(ret), K(*this));
      }
    }
    if (OB_SUCCESS == ret) {
      state_ = ObDBLinkClientState::PREPARED;
    } else if (OB_TRANS_XA_RDONLY == ret) {
      state_ = ObDBLinkClientState::RDONLY_PREPARED;
    } else {
      // TODO, handle exceptions
    }
    // for statistics
    const int64_t used_time_us = ObTimeUtility::current_time() - start_ts;
    dblink_statistics_->inc_dblink_trans_xa_prepare_count();
    dblink_statistics_->add_dblink_trans_xa_prepare_used_time(used_time_us);
    if (OB_FAIL(ret) && OB_TRANS_XA_RDONLY != ret) {
      dblink_statistics_->inc_dblink_trans_xa_prepare_fail_count();
    }
    TRANS_LOG(INFO, "rm xa prepare for dblink", K(ret), K_(xid), K(used_time_us));
  }
  return ret;
}

// execute xa commit for dblink client
// NOTE that this function can be called only if all participants are prepared successfully
// 1. if COMMITTED or RDONLY_PREPARED, return success directly
// 2. if PREPARED, execute xa commit
int ObDBLinkClient::rm_xa_commit()
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(lock_);
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "dblink client is not inited", K(ret), K(*this));
  } else if (NULL == dblink_statistics_) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "unexpected dblink client", K(ret), K(*this), KP_(dblink_statistics));
  } else if (!xid_.is_valid() || xid_.empty()) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "invalid xid", K(ret), K(xid_));
  } else if (NULL == impl_) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "invalid query impl", K(ret), K(*this));
  // TODO, check connection
  } else if (ObDBLinkClientState::PREPARED != state_) {
    if (ObDBLinkClientState::COMMITTED == state_
        || ObDBLinkClientState::RDONLY_PREPARED == state_) {
      // return OB_SUCCESS
    } else {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "unexpected dblink client", K(ret), K(*this));
    }
  } else {
    // two phase commit
    const int64_t start_ts = ObTimeUtility::current_time();
    const int64_t flags = ObXAFlag::OBTMNOFLAGS;
    state_ = ObDBLinkClientState::COMMITTING;
    if (OB_FAIL(impl_->xa_commit(xid_, flags))) {
      TRANS_LOG(WARN, "fail to execute query", K(ret), K(*this));
    } else {
      state_ = ObDBLinkClientState::COMMITTED;
    }
    // for statistics
    const int64_t used_time_us = ObTimeUtility::current_time() - start_ts;
    dblink_statistics_->inc_dblink_trans_xa_commit_count();
    dblink_statistics_->add_dblink_trans_xa_commit_used_time(used_time_us);
    if (OB_FAIL(ret)) {
      dblink_statistics_->inc_dblink_trans_xa_commit_fail_count();
    }
    TRANS_LOG(INFO, "rm xa commit for dblink", K(ret), K_(xid), K(used_time_us));
  }
  return ret;
}

// execute xa rollback for dblink client
// 1. if START, execute xa end first
// 2. if END, execute xa rollback
// 3. if RDONLY_PREPARED, return success directly
// 4. if PREPARED, execute xa rollback
int ObDBLinkClient::rm_xa_rollback()
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(lock_);

  // step 1, execute xa end if necessary
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "dblink client is not inited", K(ret), K(*this));
  } else if (NULL == dblink_statistics_) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "unexpected dblink client", K(ret), K(*this), KP_(dblink_statistics));
  } else if (!xid_.is_valid() || xid_.empty()) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "invalid xid", K(ret), K(xid_));
  } else if (NULL == impl_) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "invalid query impl", K(ret), K(*this));
  // TODO, check connection
  } else if (ObDBLinkClientState::START == state_) {
    if (OB_FAIL(rm_xa_end_())) {
      TRANS_LOG(WARN, "fail to execute xa end", K(ret), K(*this));
    }
  }

  // step 2, execute xa rollback
  if (OB_SUCCESS != ret) {
  } else if (ObDBLinkClientState::PREPARED != state_
      && ObDBLinkClientState::END != state_
      && ObDBLinkClientState::PREPARING != state_) {
    if (ObDBLinkClientState::ROLLBACKED == state_
        || ObDBLinkClientState::RDONLY_PREPARED == state_) {
      // return OB_SUCCESS
    } else {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "unexpected dblink client", K(ret), K(*this));
    }
  } else {
    const int64_t start_ts = ObTimeUtility::current_time();
    state_ = ObDBLinkClientState::ROLLBACKING;
    if (OB_FAIL(impl_->xa_rollback(xid_))) {
      TRANS_LOG(WARN, "fail to execute query", K(ret), K(*this));
    } else {
      state_ = ObDBLinkClientState::ROLLBACKED;
    }
    // for statistics
    const int64_t used_time_us = ObTimeUtility::current_time() - start_ts;
    dblink_statistics_->inc_dblink_trans_xa_rollback_count();
    dblink_statistics_->add_dblink_trans_xa_rollback_used_time(used_time_us);
    if (OB_FAIL(ret)) {
      dblink_statistics_->inc_dblink_trans_xa_rollback_fail_count();
    }
    TRANS_LOG(INFO, "rm xa rollback for dblink", K(ret), K_(xid), K(used_time_us));
  }
  return ret;
}

int ObDBLinkClient::rm_xa_end_()
{
  int ret = OB_SUCCESS;
  const int64_t start_ts = ObTimeUtility::current_time();
  if (ObDBLinkClientState::START != state_) {
    if (ObDBLinkClientState::END == state_) {
      // return OB_SUCCESS
    } else {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "unexpected dblink client", K(ret), K(*this));
    }
  } else {
    if (OB_FAIL(impl_->xa_end(xid_, ObXAFlag::OBTMSUCCESS))) {
      TRANS_LOG(WARN, "fail to do xa end", K(ret), K(*this));
    } else {
      state_ = ObDBLinkClientState::END;
    }
    if (OB_SUCCESS != ret) {
      // TODO, handle exceptions
    }
  }
  // for statistics
  const int64_t used_time_us = ObTimeUtility::current_time() - start_ts;
  dblink_statistics_->inc_dblink_trans_xa_end_count();
  dblink_statistics_->add_dblink_trans_xa_end_used_time(used_time_us);
  if (OB_FAIL(ret)) {
    dblink_statistics_->inc_dblink_trans_xa_end_fail_count();
  }
  TRANS_LOG(INFO, "rm xa end for dblink", K(ret), K_(xid), K(used_time_us));
  return ret;
}

int ObDBLinkClient::rm_create_savepoint_(const ObString &savepoint_name)
{
  int ret = OB_SUCCESS;

  // TODO::check client state
  if (OB_ISNULL(impl_)) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "query impl not init", K(ret), KPC(this));
  } else {
#ifdef OB_BUILD_DBLINK
    if (ObDBLinkClientState::START == state_) {
      const ObString *sp_name = &savepoint_name;
      if (savepoint_name == PL_IMPLICIT_SAVEPOINT) {
        sp_name = &PL_DBLINK_DEFAULT_SAVEPOINT;
      } else {
        sp_name = &savepoint_name;
      }
      if (OB_FAIL(create_explicit_savepoint_(*sp_name))) {
        TRANS_LOG(WARN, "fail to exec create savepoint statement",
                  K(ret), KPC(this), K(sp_name), K(savepoint_name));
      } else {
        bool hit = false;
        ARRAY_FOREACH_X(savepoint_array_, i, cnt, !hit) {
          if (savepoint_array_.at(i) == *sp_name) {
            hit = true;
          }
        }
        if (!hit) {
          if (OB_FAIL(savepoint_array_.push_back(*sp_name))) {
            TRANS_LOG(WARN, "fail to push back savepoint into savepoint array",
                      K(ret), KPC(this), KPC(sp_name), K(savepoint_name));
          }
        }
      }
      TRANS_LOG(INFO, "rm create savepoint", K(ret), KPC(this), KPC(sp_name), K(savepoint_name));
    } else {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "fail to create savepoint, dblink client in unexpected state",
                K(ret), KPC(this), K(savepoint_name));
    }
#else
  ret = OB_NOT_SUPPORTED;
  TRANS_LOG(WARN, "fail to create savepoint", K(ret));
#endif
  }

  return ret;
}

int ObDBLinkClient::rm_rollback_savepoint_(const ObString &savepoint_name)
{
  int ret = OB_SUCCESS;

  // TODO::check client state
  if (OB_ISNULL(impl_)) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "query impl not init", K(ret), KPC(this));
  } else {
#ifdef OB_BUILD_DBLINK
    if (ObDBLinkClientState::START == state_) {
      if (savepoint_array_.count() <=0){
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(WARN, "savepoint array should not empty", K(ret), KPC(this));
      } else {
        const ObString *sp_name = &savepoint_name;
        if (savepoint_name == PL_IMPLICIT_SAVEPOINT) {
          sp_name = &PL_DBLINK_DEFAULT_SAVEPOINT;
        } else {
          sp_name = &savepoint_name;
        }

        bool hit = false;
        ARRAY_FOREACH_X(savepoint_array_, i, cnt, !hit) {
          if (savepoint_array_.at(i) == *sp_name) {
            hit = true;
          }
        }
        if (!hit) {
          if (OB_FAIL(rollback_to_explicit_savepoint_(DBLINK_DEFAULT_SAVEPOINT))) {
            TRANS_LOG(WARN, "fail to rollback to rm base savepoint",
                      K(ret), KPC(this), K(savepoint_name));
          }
        } else {
          if (OB_FAIL(rollback_to_explicit_savepoint_(*sp_name))) {
            TRANS_LOG(WARN, "fail to rollback to rm savepoint",
                      K(ret), KPC(this), K(savepoint_name));
          }
        }
        TRANS_LOG(INFO, "rm rollback to savepoint", K(ret), KPC(this), KPC(sp_name), K(savepoint_name));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "fail to rollback savepoint, dblink client in unexpected state",
                K(ret), KPC(this), K(savepoint_name));
    }
#else
  ret = OB_NOT_SUPPORTED;
  TRANS_LOG(WARN, "fail to rollback savepoint", K(ret));
#endif
  }

  return ret;
}

int ObDBLinkClient::rm_create_savepoint(const ObString &savepoint_name)
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(lock_);

  if (OB_FAIL(rm_create_savepoint_(savepoint_name))) {
    TRANS_LOG(WARN, "fail to create savepoint",
          K(ret), KPC(this), K(savepoint_name));
  }

  return ret;
}

int ObDBLinkClient::rm_rollback_savepoint(const ObString &savepoint_name)
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(lock_);

  if (OB_FAIL(rm_rollback_savepoint_(savepoint_name))) {
    TRANS_LOG(WARN, "fail to rollback savepoint",
              K(ret), KPC(this), K(savepoint_name));
  }

  return ret;
}

bool ObDBLinkClient::is_started(const ObXATransID &xid)
{
  // TODO, check xid
  return ObDBLinkClientState::START == state_;
}

bool ObDBLinkClient::equal(ObISQLConnection *dblink_conn)
{
  return dblink_conn_ == dblink_conn;
}

int ObDBLinkClient::init_query_impl_(const ObTxIsolationLevel isolation)
{
  int ret = OB_SUCCESS;
  if (NULL == impl_) {
    if (DblinkDriverProto::DBLINK_DRV_OB == dblink_type_) {
      void *ptr = NULL;
      if (NULL == (ptr = mtl_malloc(sizeof(ObXAQueryObImpl), SET_IGNORE_MEM_VERSION("ObXAQuery")))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        TRANS_LOG(WARN, "fail to allocate memory", K(ret), K(*this));
      } else {
        impl_ = new(ptr) ObXAQueryObImpl();
        ObXAQueryObImpl *ob_impl = NULL;
        if (NULL == (ob_impl = dynamic_cast<ObXAQueryObImpl*>(impl_))) {
          ret = OB_ERR_UNEXPECTED;
          TRANS_LOG(WARN, "unexpected query impl for ob", K(ret), K(*this));
        } else if (OB_FAIL(ob_impl->init(dblink_conn_))) {
          TRANS_LOG(WARN, "fail to init query impl", K(ret), K(*this));
        } else {
          // set tx variables
          static const int64_t MIN_TIMEOUT_US = 20 * 1000 * 1000;  // 20s
          const int64_t timeout_us = tx_timeout_us_ + MIN_TIMEOUT_US;
          ObMySQLConnection *mysql_conn = dynamic_cast<ObMySQLConnection*>(dblink_conn_);
          const ObString isolation_str = get_tx_isolation_str(isolation);
          if (nullptr == mysql_conn) {
            ret = OB_ERR_UNEXPECTED;
            TRANS_LOG(WARN, "unexpected mysql connection", K(ret), K(*this));
          } else if (OB_FAIL(mysql_conn->set_session_variable("ob_trx_timeout", timeout_us))) {
            TRANS_LOG(WARN, "fail to set transaction timeout", K(ret), K(timeout_us), K(*this));
          } else if (OB_FAIL(mysql_conn->set_session_variable("tx_isolation", isolation_str))) {
            TRANS_LOG(WARN, "fail to set transaction isolation level in session", K(ret),
                K(timeout_us), K(*this));
          }
        }
        if (OB_SUCCESS != ret) {
          impl_->~ObXAQuery();
          mtl_free(impl_);
          impl_ = NULL;
        }
      }
    } else if (DblinkDriverProto::DBLINK_DRV_OCI == dblink_type_) {
      void *ptr = NULL;
      if (NULL == (ptr = mtl_malloc(sizeof(ObXAQueryOraImpl), SET_IGNORE_MEM_VERSION("ObXAQuery")))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        TRANS_LOG(WARN, "fail to allocate memory", K(ret), K(*this));
      } else {
        impl_ = new(ptr) ObXAQueryOraImpl();
        ObXAQueryOraImpl *ora_impl = NULL;
        if (NULL == (ora_impl = dynamic_cast<ObXAQueryOraImpl*>(impl_))) {
          ret = OB_ERR_UNEXPECTED;
          TRANS_LOG(WARN, "unexpected query impl for oracle", K(ret), K(*this));
        } else if (OB_FAIL(ora_impl->init(dblink_conn_))) {
          TRANS_LOG(WARN, "fail to init query impl", K(ret), K(*this));
        } else {
          // do nothing
        }
        if (OB_SUCCESS != ret) {
          impl_->~ObXAQuery();
          mtl_free(impl_);
          impl_ = NULL;
        }
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "unexpected dblink type", K(ret), K(*this));
    }
  }
  return ret;
}

#define CREATE_SAVEPOINT_SQL "savepoint %.*s"
int ObDBLinkClient::create_explicit_savepoint_(const ObString &savepoint_name)
{
  int ret = OB_SUCCESS;

  if (OB_NOT_NULL(dblink_conn_)) {
    ObSqlString sql;
    const uint64_t tenant_id = OB_INVALID_TENANT_ID;
    int64_t affect_rows = 0;
    if (OB_FAIL(sql.assign_fmt(CREATE_SAVEPOINT_SQL,
                               savepoint_name.length(), savepoint_name.ptr()))) {
      TRANS_LOG(WARN,"fail to assign sql", K(ret), K(sql));
    } else if (OB_FAIL(dblink_conn_->execute_write(tenant_id, sql.ptr(), affect_rows))) {
      TRANS_LOG(WARN,"fail to exec create savepoint sql", K(ret), K(sql), K(savepoint_name));
    }
    TRANS_LOG(DEBUG, "dblink client create savepoint", K(ret), K(savepoint_name));
  }

  return ret;
}

#define ROLLBACK_SAVEPOINT_SQL "rollback to %.*s"
int ObDBLinkClient::rollback_to_explicit_savepoint_(const ObString &savepoint_name)
{
  int ret = OB_SUCCESS;

  if (OB_NOT_NULL(dblink_conn_)) {
    ObSqlString sql;
    const uint64_t tenant_id = OB_INVALID_TENANT_ID;
    int64_t affect_rows = 0;
    if (OB_FAIL(sql.assign_fmt(ROLLBACK_SAVEPOINT_SQL,
                               savepoint_name.length(), savepoint_name.ptr()))) {
      TRANS_LOG(WARN,"fail to assign sql", K(ret), K(sql));
    } else if (OB_FAIL(dblink_conn_->execute_write(tenant_id, sql.ptr(), affect_rows))) {
      TRANS_LOG(WARN,"fail to exec rollback savepoint sql", K(ret), K(sql), K(savepoint_name));
    }
    TRANS_LOG(DEBUG, "dblink client rollback savepoint", K(ret), K(savepoint_name));
  }

  return ret;
}

bool ObDBLinkClient::is_valid_dblink_type(const DblinkDriverProto dblink_type)
{
  bool ret_bool = true;
  if (DblinkDriverProto::DBLINK_DRV_OB != dblink_type
      && DblinkDriverProto::DBLINK_DRV_OCI != dblink_type) {
    ret_bool = false;
  }
  return ret_bool;
}

} // transaction
} // oceanbase

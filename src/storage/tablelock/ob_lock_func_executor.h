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

#ifndef OCEANBASE_OB_LOCK_FUNC_EXECUTOR_H_
#define OCEANBASE_OB_LOCK_FUNC_EXECUTOR_H_

#include "sql/session/ob_basic_session_info.h"

namespace oceanbase
{
namespace sql
{
class ObSQLSessionInfo;
class ObExecContext;
}

namespace pl
{
class PlPackageLock;
}

namespace common
{
class ObMySQLProxy;
}

namespace observer
{
class ObInnerSQLConnection;
}

namespace transaction
{
class ObTransID;
class ObTxDesc;

namespace tablelock
{
struct ObLockObjsRequest;
struct ObUnLockObjsRequest;
class ObLockFuncContext
{
public:
  ObLockFuncContext() { reset(); }
  virtual ~ObLockFuncContext() { reset(); }
  void reset()
  {
    reset_autocommit_ = false;
    has_inner_dml_write_ = false;
    need_close_conn_ = false;
    have_saved_session_ = false;
    has_autonomous_tx_ = false;
    old_worker_timeout_ts_ = 0;
    old_phy_plan_timeout_ts_ = 0;
    last_insert_id_ = 0;
    tenant_id_ = 0;
    database_id_ = OB_INVALID_ID;
    database_name_.reset();
    sql_proxy_ = nullptr;
    inner_conn_ = nullptr;
    session_info_ = nullptr;
    my_exec_ctx_ = nullptr;
    saved_session_.reset();
  }

  int init(sql::ObSQLSessionInfo &session_info,
           sql::ObExecContext &ctx,
           const int64_t timeout_us = 0);
  int destroy(sql::ObExecContext &ctx,
              sql::ObSQLSessionInfo &session_info,
              bool is_rollback);
  bool is_inited() { return session_info_ != NULL; }

  static int valid_execute_context(sql::ObExecContext &ctx);
  int execute_write(const ObSqlString &sql, int64_t &affected_rows);
  int execute_read(const ObSqlString &sql, common::ObMySQLProxy::MySQLResult &res);
private:
  int open_inner_conn_();
  int close_inner_conn_();
  static void register_for_deadlock_(sql::ObSQLSessionInfo &session_info,
                                     const transaction::ObTransID &parent_tx_id);
  static int implicit_end_trans_(sql::ObSQLSessionInfo &session,
                                 sql::ObExecContext &ctx,
                                 bool is_rollback,
                                 bool can_async = false);
private:
  friend class ObLockFuncExecutor;
  friend class ObGetLockExecutor;
  bool reset_autocommit_;
  bool has_inner_dml_write_;
  bool need_close_conn_;
  bool have_saved_session_;
  bool has_autonomous_tx_;
  int64_t old_worker_timeout_ts_;
  int64_t old_phy_plan_timeout_ts_;
  uint64_t last_insert_id_;
  uint64_t tenant_id_;
  uint64_t database_id_;
  ObSqlString database_name_;
  common::ObMySQLProxy *sql_proxy_;
  observer::ObInnerSQLConnection *inner_conn_;
  sql::ObSQLSessionInfo *session_info_;
  sql::ObExecContext *my_exec_ctx_; //my exec context
  sql::ObBasicSessionInfo::TransSavedValue saved_session_;
};

class ObLockFuncExecutor
{
public:
  static constexpr int64_t MAX_FULL_TABLE_NAME_LENGTH = (OB_MAX_TABLE_NAME_LENGTH
                                                         + OB_MAX_DATABASE_NAME_LENGTH
                                                         + 1);
  static constexpr int64_t MAX_LOCK_HANDLE_LEGNTH = 65;
  static constexpr int64_t WHERE_CONDITION_BUFFER_SIZE = 512;
  static constexpr int64_t LOCK_ID_LENGTH = 10;
  static constexpr int64_t MIN_LOCK_HANDLE_ID = 0x40000000;
  static constexpr int64_t MAX_LOCK_HANDLE_ID = 1999999999;
  static constexpr int64_t DEFAULT_EXPIRATION_US = 60 * 1000 * 1000L; // 1min

  static constexpr int64_t INVALID_RELEASE_CNT = -2;
  static constexpr int64_t LOCK_NOT_EXIST_RELEASE_CNT = -1;
  static constexpr int64_t LOCK_NOT_OWN_RELEASE_CNT = 0;

public:
  int check_lock_exist_(const uint64_t &lock_id);
  int check_lock_exist_(ObLockFuncContext &ctx,
                        const int64_t raw_owner_id,
                        const uint64_t &lock_id,
                        bool &exist);
  int check_lock_exist_(ObLockFuncContext &ctx,
                        const uint64_t &lock_id,
                        bool &exist);
  int check_owner_exist_(ObLockFuncContext &ctx,
                         const uint32_t client_session_id,
                         const uint64_t client_session_create_ts,
                         bool &exist);
  int check_lock_exist_(ObLockFuncContext &ctx,
                        ObSqlString &where_cond,
                        bool &exist);
  int check_client_ssid_(ObLockFuncContext &ctx,
                         const uint32_t client_session_id,
                         const uint64_t client_session_create_ts);
  int remove_lock_id_(ObLockFuncContext &ctx, const int64_t raw_owner_id, const uint64_t &lock_id);
  int remove_lock_id_(ObLockFuncContext &ctx,
                      const int64_t lock_id);
  int remove_session_record_(ObLockFuncContext &ctx,
                             const uint32_t client_session_id,
                             const uint64_t client_session_create_ts);
  int unlock_obj_(transaction::ObTxDesc *tx_desc,
                  const transaction::ObTxParam &tx_param,
                  const ObUnLockObjsRequest &arg);
  int query_lock_id_(const ObString &lock_name,
                     uint64_t &lock_id);
  int query_lock_id_and_lock_handle_(const ObString &lock_name,
                                     uint64_t &lock_id,
                                     char *lock_handle_buf);
  int query_lock_owner_(const uint64_t &lock_id,
                        int64_t &owner_id);
  int extract_lock_id_(const ObString &lock_handle,
                       uint64_t &lock_id);
  void mark_lock_session_(sql::ObSQLSessionInfo *session,
                          const bool is_lock_session);
  int remove_expired_lock_id();
  int remove_expired_lock_id_(sql::ObExecContext &ctx);
};

class ObGetLockExecutor : public ObLockFuncExecutor
{
public:
  friend class pl::PlPackageLock;
  int execute(sql::ObExecContext &ctx,
              const ObString &lock_name,
              const int64_t timeout_us);
private:
  int generate_lock_id_(ObLockFuncContext &ctx,
                        const ObString &lock_name,
                        const int64_t timeout_us,
                        uint64_t &lock_id);
  int update_session_table_(ObLockFuncContext &ctx,
                            const uint32_t client_session_id,
                            const uint64_t client_session_create_ts,
                            const uint32_t server_session_id);
  int check_need_reroute_(ObLockFuncContext &ctx,
                          sql::ObSQLSessionInfo *session,
                          const uint32_t client_session_id,
                          const uint64_t client_session_create_ts);
  int get_lock_session_(ObLockFuncContext &ctx,
                        const uint32_t client_session_id,
                        const uint64_t client_session_create_ts,
                        ObAddr &lock_session_addr,
                        uint32_t &lock_session_id);
  int get_first_session_info_(common::sqlclient::ObMySQLResult &res, ObAddr &session_addr, uint32_t &server_session_id);
  int get_sql_port_(ObLockFuncContext &ctx, const ObAddr &svr_addr, int32_t &sql_port);
  int lock_obj_(sql::ObSQLSessionInfo *session,
                const transaction::ObTxParam &tx_param,
                const uint32_t client_session_id,
                const uint64_t client_session_create_ts,
                const int64_t obj_id,
                const int64_t timeout_us);
  int generate_lock_id_(const ObString &lock_name,
                        uint64_t &lock_id,
                        char *lock_handle);
  int write_lock_id_(ObLockFuncContext &ctx,
                     const ObString &lock_name,
                     const int64_t timeout_us,
                     const uint64_t &lock_id,
                     const char *lock_handle_buf);
};

class ObReleaseLockExecutor : public ObLockFuncExecutor
{
public:
  int execute(sql::ObExecContext &ctx,
              const ObString &lock_name,
              int64_t &release_cnt);
};

class ObReleaseAllLockExecutor : public ObLockFuncExecutor
{
public:
  int execute(sql::ObExecContext &ctx,
              int64_t &release_cnt);
  // used internal, release all the lock that required by the session.
  int execute(const int64_t raw_owner_id);
private:
  int execute_(sql::ObExecContext &ctx,
               const uint32_t client_session_id,
               const uint64_t client_session_create_ts,
               int64_t &release_cnt);
  int execute_(sql::ObExecContext &ctx,
               const ObTableLockOwnerID &owner_id,
               int64_t &release_cnt);
  int release_all_locks_(ObLockFuncContext &ctx,
                         sql::ObSQLSessionInfo *session,
                         const transaction::ObTxParam &tx_param,
                         const ObTableLockOwnerID &owner_id,
                         int64_t &release_cnt);
  int release_all_locks_(ObLockFuncContext &ctx,
                         const ObIArray<ObUnLockObjsRequest> &arg_list,
                         sql::ObSQLSessionInfo *session,
                         const transaction::ObTxParam &tx_param,
                         int64_t &release_cnt);
  int parse_lock_request_(common::sqlclient::ObMySQLResult &res,
                          ObLockObjsRequest &arg,
                          int64_t &cnt);
  int get_unlock_request_list_(common::sqlclient::ObMySQLResult *res,
                               ObIArray<ObUnLockObjsRequest> &arg_list);
  int get_unlock_request_(const ObLockObjsRequest &arg,
                          ObUnLockObjsRequest &unlock_arg);
};

class ObISFreeLockExecutor : public ObLockFuncExecutor
{
public:
  int execute(sql::ObExecContext &ctx,
              const ObString &lock_name);
};

class ObISUsedLockExecutor : public ObLockFuncExecutor
{
public:
  int execute(sql::ObExecContext &ctx,
              const ObString &lock_name,
              uint32_t &sess_id);
};

} // tablelock
} // transaction
} // oceanbase
#endif

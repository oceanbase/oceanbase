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

#ifndef OCEANBASE_OB_LOCK_EXECUTOR_H_
#define OCEANBASE_OB_LOCK_EXECUTOR_H_

#include "sql/session/ob_basic_session_info.h"
#include "storage/tablelock/ob_table_lock_rpc_struct.h"

namespace oceanbase
{
namespace sql
{
class ObSQLSessionInfo;
class ObExecContext;
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
struct ObLockRequest;
struct ObLockObjsRequest;
struct ObUnLockObjsRequest;
struct ObUnLockTableRequest;

class ObLockContext
{
public:
  ObLockContext() { reset(); }
  virtual ~ObLockContext() { reset(); }
  void reset()
  {
    reset_autocommit_ = false;
    has_inner_dml_write_ = false;
    have_saved_session_ = false;
    has_autonomous_tx_ = false;
    is_inner_session_ = false;
    old_worker_timeout_ts_ = 0;
    old_phy_plan_timeout_ts_ = 0;
    last_insert_id_ = 0;
    tenant_id_ = 0;
    database_id_ = OB_INVALID_ID;
    database_name_.reset();
    inner_conn_ = nullptr;
    store_inner_conn_ = nullptr;
    my_exec_ctx_ = nullptr;
    saved_session_.reset();
  }

  int init(sql::ObExecContext &ctx,
           const int64_t timeout_us = 0);
  int destroy(sql::ObExecContext &ctx,
              bool is_rollback);
  bool is_inited() { return my_exec_ctx_ != NULL; }

  static int valid_execute_context(sql::ObExecContext &ctx);
  int execute_write(const ObSqlString &sql,
                    int64_t &affected_rows);
  int execute_read(const ObSqlString &sql,
                   common::ObMySQLProxy::MySQLResult &res);
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
  friend class ObLockExecutor;
  bool reset_autocommit_;
  bool has_inner_dml_write_;
  bool have_saved_session_;
  bool has_autonomous_tx_;
  bool is_inner_session_;
  int64_t old_worker_timeout_ts_;
  int64_t old_phy_plan_timeout_ts_;
  uint64_t last_insert_id_;
  uint64_t tenant_id_;
  uint64_t database_id_;
  ObSqlString database_name_;
  observer::ObInnerSQLConnection *inner_conn_;
  observer::ObInnerSQLConnection *store_inner_conn_;
  sql::ObExecContext *my_exec_ctx_; //my exec context
  sql::ObBasicSessionInfo::TransSavedValue saved_session_;
};

class ObLockExecutor
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
  static bool proxy_is_support(sql::ObExecContext &exec_ctx);
  static bool proxy_is_support(sql::ObSQLSessionInfo *session);
  int remove_expired_lock_id();
  int check_client_ssid(ObLockContext &ctx,
                        const uint32_t client_session_id,
                        const uint64_t client_session_create_ts);
  int remove_session_record(ObLockContext &ctx,
                            const uint32_t client_session_id,
                            const uint64_t client_session_create_ts);

protected:
  int unlock_obj_(transaction::ObTxDesc *tx_desc,
                  const transaction::ObTxParam &tx_param,
                  const ObUnLockObjsRequest &arg);
  int unlock_table_(transaction::ObTxDesc *tx_desc,
                    const transaction::ObTxParam &tx_param,
                    const ObUnLockTableRequest &arg);
  int query_lock_id_(const ObString &lock_name,
                     uint64_t &lock_id);
  int query_lock_id_and_lock_handle_(const ObString &lock_name,
                                     uint64_t &lock_id,
                                     char *lock_handle_buf);
  int extract_lock_id_(const ObString &lock_handle,
                       uint64_t &lock_id);
  void mark_lock_session_(sql::ObSQLSessionInfo *session,
                          const bool is_lock_session);
  int get_lock_session_(ObLockContext &ctx,
                        const uint32_t client_session_id,
                        const uint64_t client_session_create_ts,
                        ObAddr &lock_session_addr,
                        uint32_t &lock_session_id);
  int get_first_session_info_(common::sqlclient::ObMySQLResult &res,
                              ObAddr &session_addr,
                              uint32_t &server_session_id);
  int update_session_table_(ObLockContext &ctx,
                            const uint32_t client_session_id,
                            const uint64_t client_session_create_ts,
                            const uint32_t server_session_id);
  int get_sql_port_(ObLockContext &ctx,
                    const ObAddr &svr_addr,
                    int32_t &sql_port);
  int check_need_reroute_(ObLockContext &ctx,
                          const uint32_t client_session_id,
                          const uint64_t client_session_create_ts);
};

class ObUnLockExecutor : public ObLockExecutor
{
public:
  // new one
  enum ReleaseType
  {
    RELEASE_OBJ_LOCK,
    RELEASE_TABLE_LOCK,
    RELEASE_ALL_LOCKS,
  };
  int execute(sql::ObExecContext &ctx,
              const ReleaseType release_type,
              int64_t &release_cnt);
  // used internal, release all the lock that required by the session.
  int execute(const ObTableLockOwnerID &owner_id);
private:
  int execute_(sql::ObExecContext &ctx,
               const ObTableLockOwnerID &owner_id,
               int64_t &release_cnt);
  int execute_(sql::ObExecContext &ctx,
               const uint32_t client_session_id,
               const uint64_t client_session_create_ts,
               const ReleaseType release_type,
               int64_t &release_cnt);
  int release_all_locks_(ObLockContext &ctx,
                         sql::ObSQLSessionInfo *session,
                         const transaction::ObTxParam &tx_param,
                         const ObTableLockOwnerID &owner_id,
                         const ReleaseType release_type,
                         int64_t &release_cnt);
  int release_all_locks_(ObLockContext &ctx,
                         const ObIArray<ObLockRequest *> &arg_list,
                         sql::ObSQLSessionInfo *session,
                         const transaction::ObTxParam &tx_param,
                         int64_t &cnt);
  int get_task_type_by_release_type_(const ReleaseType &release_type, ObTableLockTaskType &task_type);
};
} // tablelock
} // transaction
} // oceanbase
#endif

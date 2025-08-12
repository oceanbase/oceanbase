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
#ifndef OCEANBASE_STORAGE_TABLELOCK_OB_TABLE_LOCK_LIVE_DETECTOR_H
#define OCEANBASE_STORAGE_TABLELOCK_OB_TABLE_LOCK_LIVE_DETECTOR_H
#include "storage/tablelock/ob_table_lock_live_detect_func.h"
#include "storage/tablelock/ob_table_lock_common.h"
#include "storage/tablelock/ob_table_lock_rpc_struct.h"
#include "deps/oblib/src/lib/mysqlclient/ob_isql_client.h"
#include "sql/session/ob_sql_session_mgr.h"
namespace oceanbase
{
namespace observer
{
class ObInnerSQLConnection;
}
namespace obrpc
{
class Bool;
}
namespace common
{
class ObAddr;
class ObSqlString;
template <typename T>
class ObIArray;
template <typename... T>
class ObTuple;
}  // namespace common
namespace sql
{
class ObSQLSessionInfo;
}
namespace share
{
class ObDMLSqlSplicer;
}
namespace transaction
{
namespace tablelock
{
class ObTableLockDetectFuncList
{
  static const int64_t RETRY_RPC_TIMES = 10;
  static const int64_t RPC_INTERVAL_TIME_US = 100L * 1000L;       // 100ms
  static const int64_t DEFAULT_TIMEOUT_US = 10L * 1000L * 1000L;  // 10s

public:
  static int detect_session_alive(const uint32_t session_id, bool &is_alive);
  static int detect_session_alive_for_rpc(const uint32_t session_id, obrpc::Bool &is_alive);
  static int do_session_alive_detect();
  static int do_session_at_least_one_alive_detect(const uint64_t tenant_id, const uint32_t session_id, const ObArray<ObAddr> *dest_server, bool &is_alive);
  static int do_session_at_least_one_alive_detect_for_rpc(const uint32_t session_id, obrpc::Bool &is_alive);
  static int get_tenant_servers(const uint64_t tenant_id, common::ObIArray<common::ObAddr> &server_array);

private:
  static int do_session_alive_detect_for_a_client_session_(const int64_t &client_session_id, bool &all_alive);
  static int do_session_alive_detect_for_a_server_session_(const ObAddr &addr,
                                                           const int64_t &server_session_id,
                                                           bool &is_alive);
  static int get_active_server_session_list_(const int64_t &client_session_id,
                                             ObIArray<ObTuple<ObAddr, int64_t>> &server_session_list,
                                             bool &all_alive);
  static int check_server_is_online_(const ObString &svr_ip, const int64_t svr_port, bool &is_online);
  static int get_owner_id_list_from_table_(ObIAllocator &allocator, ObArray<ObTableLockOwnerID *> &owner_ids);
  static int get_owner_id_list_from_table_(ObIAllocator &allocator,
                                           const bool is_new_table,
                                           ObArray<ObTableLockOwnerID *> &owner_ids);
};

class ObTableLockDetector
{
  static const int64_t DETECT_INFO_COLUMN_SIZE = 8;

  // We have to set the type to int64_t here, because
  // the type of the columns in the inner_table is int
  typedef common::ObTuple<int64_t,        /* 0. task_type */
                          int64_t,        /* 1. obj_type */
                          int64_t,        /* 2. obj_id */
                          int64_t,        /* 3. lock_mode */
                          int64_t,        /* 4. owner_id */
                          int64_t,        /* 5. cnt */
                          int64_t,        /* 6. detect_func_no */
                          ObStringHolder> /* 7. detect_func_param */
    ObTableLockDetectReadInfo;
  typedef common::ObTuple<ObTableLockTaskType,   /* 0. task_type */
                          ObLockOBJType,         /* 1. obj_type */
                          uint64_t,              /* 2. obj_id */
                          ObTableLockMode,       /* 3. lock_mode */
                          ObTableLockOwnerID,    /* 4. owner_id */
                          uint64_t,              /* 5. cnt */
                          ObTableLockDetectType, /* 6. detect_func_no */
                          ObString>              /* 7. detect_func_param */
    ObTableLockDetectInfo;

public:
  static int record_detect_info_to_inner_table(sql::ObSQLSessionInfo *session_info,
                                               const ObTableLockTaskType &task_type,
                                               const ObLockRequest &lock_req,
                                               const bool for_dbms_lock,
                                               bool &need_record_to_lock_table);
  static int remove_detect_info_from_inner_table(sql::ObSQLSessionInfo *session_info,
                                                 const ObTableLockTaskType &task_type,
                                                 const ObLockRequest &lock_req,
                                                 bool &need_remove_from_lock_table);
  static int remove_detect_info_from_inner_table(sql::ObSQLSessionInfo *session_info,
                                                 const ObTableLockTaskType &task_type,
                                                 const ObLockRequest &lock_req,
                                                 int64_t &cnt);
  static int do_detect_and_clear();
  static int remove_lock_by_owner_id(const ObTableLockOwnerID &owner_id);
  static int remove_expired_lock_id();

  static int check_lock_id_exist_in_inner_table(sql::ObSQLSessionInfo *session_info,
                                                const uint64_t &obj_id,
                                                const ObLockOBJType &obj_type,
                                                bool &exist);
  static int check_lock_owner_exist_in_inner_table(sql::ObSQLSessionInfo *session_info,
                                                   const uint32_t client_session_id,
                                                   const uint64_t client_session_create_ts,
                                                   bool &exist);
  static int check_lock_exist_in_inner_table(sql::ObSQLSessionInfo *session_info,
                                             const ObTableLockTaskType &task_type,
                                             const ObLockRequest &lock_req,
                                             bool &exist);
  static int check_and_set_old_detect_table_is_empty();
  static int get_table_name(const bool is_new_table, char *table_name);
  static int get_lock_mode_by_owner_id_and_lock_id(const int64_t tenant_id,
                                                   const ObTableLockOwnerID &owner_id,
                                                   const uint64_t lock_id,
                                                   ObTableLockMode &lock_mode);
  static int get_lock_owner_by_lock_id(const uint64_t &lock_id, ObTableLockOwnerID &lock_owner);
  static int get_unlock_request_list(sql::ObSQLSessionInfo *session,
                                     const ObTableLockOwnerID &owner_id,
                                     const ObTableLockTaskType task_type,
                                     ObIAllocator &allocator,
                                     ObIArray<ObLockRequest *> &arg_list);
  static bool old_detect_table_is_empty();

private:
  static int record_detect_info_to_inner_table_(observer::ObInnerSQLConnection *inner_conn,
                                                const ObTableLockTaskType &task_type,
                                                const ObLockRequest &lock_req,
                                                bool &need_record_to_lock_table);
  static int check_lock_exist_(observer::ObInnerSQLConnection *inner_conn,
                               const ObSqlString &where_cond,
                               const ObTableLockOwnerID &lock_owner,
                               bool &exist);
  static int check_lock_exist_(observer::ObInnerSQLConnection *inner_conn,
                               const ObSqlString &where_cond,
                               bool &exist);
  static int check_lock_exist_in_table_(observer::ObInnerSQLConnection *inner_conn,
                                        const char *table_name,
                                        const ObSqlString &where_cond,
                                        bool &exist);
  static int check_lock_exist_in_table_(observer::ObInnerSQLConnection *inner_conn,
                                        const ObSqlString &where_cond,
                                        const ObTableLockOwnerID lock_owner,
                                        const bool is_new_table,
                                        bool &exist);
  static int generate_insert_dml_(const ObTableLockTaskType &task_type,
                                  const ObLockRequest &lock_req,
                                  const bool is_new_table,
                                  share::ObDMLSqlSplicer &dml);
  static int add_pk_column_to_dml_(const ObTableLockTaskType &task_type,
                                   const ObLockRequest &lock_req,
                                   const bool is_new_table,
                                   share::ObDMLSqlSplicer &dml);
  static int generate_update_sql_(const char *table_name, const share::ObDMLSqlSplicer &dml, ObSqlString &sql);
  static int generate_select_sql_(const char *table_name, const share::ObDMLSqlSplicer &dml, ObSqlString &sql);
  static int delete_record_(const char *table_name,
                            observer::ObInnerSQLConnection *conn,
                            const share::ObDMLSqlSplicer &dml);
  static int update_cnt_of_lock_(const char *table_name,
                                 observer::ObInnerSQLConnection *conn,
                                 const share::ObDMLSqlSplicer &dml);
  static int get_cnt_of_lock_(observer::ObInnerSQLConnection *conn,
                              const ObTableLockTaskType &task_type,
                              const ObLockRequest &lock_req,
                              const bool &only_check_old_table,
                              int64_t &cnt,
                              bool &is_from_old_table);
  static int get_cnt_of_lock_(observer::ObInnerSQLConnection *conn,
                              const ObTableLockTaskType &task_type,
                              const ObLockRequest &lock_req,
                              int64_t &cnt_in_new_table,
                              int64_t &cnt_in_old_table);
  static int get_lock_cnt_in_table_(observer::ObInnerSQLConnection *conn,
                                    const bool is_new_table,
                                    const ObTableLockTaskType &task_type,
                                    const ObLockRequest &lock_req,
                                    int64_t &cnt);
  static int get_lock_cnt_in_table_(observer::ObInnerSQLConnection *conn,
                                    const char *table_name,
                                    const share::ObDMLSqlSplicer &dml,
                                    int64_t &cnt);
  static int remove_detect_info_from_table_(observer::ObInnerSQLConnection *conn,
                                            const ObTableLockTaskType &task_type,
                                            const ObLockRequest &lock_req,
                                            const int64_t cnt_in_old_table,
                                            const int64_t cnt_in_new_table,
                                            int64_t &real_del_cnt);
  static int remove_detect_info_from_table_(observer::ObInnerSQLConnection *conn,
                                            const ObTableLockTaskType &task_type,
                                            const ObLockRequest &lock_req,
                                            const bool is_new_table);
  static int get_table_name_and_dml_with_pk_column_(const bool is_new_table,
                                                    const ObTableLockTaskType &task_type,
                                                    const ObLockRequest &lock_req,
                                                    char *table_name,
                                                    share::ObDMLSqlSplicer &dml);
  static int get_unlock_request_list_(common::sqlclient::ObMySQLResult *res,
                                      ObIAllocator &allocator,
                                      ObIArray<ObLockRequest *> &arg_list);
  static int generate_get_unlock_request_sql_(const ObTableLockOwnerID &owner_id,
                                              const ObTableLockTaskType task_type,
                                              ObSqlString &sql);
  static int parse_unlock_request_(common::sqlclient::ObMySQLResult &res, ObIAllocator &allocator, ObLockRequest *&arg);
  static int fill_owner_id_for_unlock_request_(const ObTableLockOwnerID &owner_id, ObIArray<ObLockRequest *> &arg_list);
  static int get_lock_owner_where_cond_(const ObTableLockOwnerID lock_owner,
                                        const bool is_new_table,
                                        ObSqlString &where_cond);
  static bool is_unlock_task_(const ObTableLockTaskType &task_type);
public:
  static const char *detect_columns[DETECT_INFO_COLUMN_SIZE];
  // detect session alive
  static ObTableLockDetectFunc<> func1;
};

class ObSessionAliveChecker
{
public:
  ObSessionAliveChecker() = delete;
  ~ObSessionAliveChecker() {}
  explicit ObSessionAliveChecker(uint32_t session_id) : session_id_(session_id),
                                                        is_alive_(false) {}
  bool operator()(sql::ObSQLSessionMgr::Key key, sql::ObSQLSessionInfo *sess_info);
  static bool is_proxy_sid(const uint32_t session_id) {return session_id <= 0x7FFFFFFFU;};
  uint32_t get_sid() const {return session_id_;}
  inline void set_alive(const bool is_alive) {is_alive_ = is_alive;}
  inline bool is_alive() const {return is_alive_;}
  TO_STRING_KV(K_(session_id), K_(is_alive));
private:
  uint32_t session_id_;
  bool is_alive_;
  DISALLOW_COPY_AND_ASSIGN(ObSessionAliveChecker);
};

}  // namespace tablelock
}  // namespace transaction
}  // namespace oceanbase
#endif

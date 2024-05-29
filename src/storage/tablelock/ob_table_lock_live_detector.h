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

private:
  static int do_session_alive_detect_for_a_client_session_(const int64_t &client_session_id, bool &all_alive);
  static int do_session_alive_detect_for_a_server_session_(const ObAddr &addr,
                                                           const int64_t &server_session_id,
                                                           bool &is_alive);
  static int get_active_server_session_list_(const int64_t &client_session_id,
                                             ObIArray<ObTuple<ObAddr, int64_t>> &server_session_list,
                                             bool &all_alive);
  static int check_server_is_online_(const ObString &svr_ip, const int64_t svr_port, bool &is_online);
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
  static int remove_lock_by_owner_id(const int64_t raw_owner_id);
  static int remove_expired_lock_id();

private:
  static int record_detect_info_to_inner_table_(observer::ObInnerSQLConnection *inner_conn,
                                                const char *table_name,
                                                const ObTableLockTaskType &task_type,
                                                const ObLockRequest &lock_req,
                                                bool &need_record_to_lock_table);
  static int check_dbms_lock_record_exist_(observer::ObInnerSQLConnection *inner_conn,
                                           const char *table_name,
                                           const ObTableLockTaskType &task_type,
                                           const ObLockRequest &lock_req,
                                           bool &is_existed);
  static int generate_insert_dml_(const ObTableLockTaskType &task_type,
                                  const ObLockRequest &lock_req,
                                  share::ObDMLSqlSplicer &dml);
  static int add_pk_column_to_dml_(const ObTableLockTaskType &task_type,
                                   const ObLockRequest &lock_req,
                                   share::ObDMLSqlSplicer &dml);
  static int generate_update_sql_(const char *table_name, const share::ObDMLSqlSplicer &dml, ObSqlString &sql);
  static int generate_select_sql_(const char *table_name, const share::ObDMLSqlSplicer &dml, ObSqlString &sql);
  static int delete_record_(const char *table_name,
                            observer::ObInnerSQLConnection *conn,
                            const share::ObDMLSqlSplicer &dml);
  static int update_cnt_of_lock_(const char *table_name,
                                 observer::ObInnerSQLConnection *conn,
                                 const share::ObDMLSqlSplicer &dml);
  static int get_cnt_of_lock_(const char *table_name,
                              observer::ObInnerSQLConnection *conn,
                              const share::ObDMLSqlSplicer &dml,
                              int64_t &cnt);
  static bool is_unlock_task_(const ObTableLockTaskType &task_type);

public:
  static const char *detect_columns[DETECT_INFO_COLUMN_SIZE];
  // detect session alive
  static ObTableLockDetectFunc<> func1;
};
}  // namespace tablelock
}  // namespace transaction
}  // namespace oceanbase
#endif

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

#ifndef OB_DDL_ERROR_MESSAGE_REPORTER_H
#define OB_DDL_ERROR_MESSAGE_REPORTER_H

#include "lib/net/ob_addr.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "common/sql_mode/ob_sql_mode_utils.h"
#include "common/rowkey/ob_store_rowkey.h"
#include "share/ob_errno.h"
#include "share/schema/ob_schema_struct.h"
#include "share/ob_ddl_common.h"
#include "share/schema/ob_table_schema.h"
#include "storage/blocksstable/ob_datum_rowkey.h"

namespace oceanbase
{
namespace share
{
class ObDDLErrorMessageTableOperator
{
public:
  struct ObBuildDDLErrorMessage
  {
  public:
    ObBuildDDLErrorMessage()
      : ret_code_(common::OB_NOT_INIT), ddl_type_(ObDDLType::DDL_INVALID), affected_rows_(0),
        user_message_(nullptr), dba_message_("\0"), allocator_()
    {}
    virtual ~ObBuildDDLErrorMessage();
    int prepare_user_message_buf(const int64_t len);
    bool operator==(const ObBuildDDLErrorMessage &other) const;
    bool operator!=(const ObBuildDDLErrorMessage &other) const;
    TO_STRING_KV(K_(ret_code), K_(ddl_type), K_(affected_rows), K_(user_message), K_(dba_message));
  public:
    int ret_code_;
    ObDDLType ddl_type_;
    int64_t affected_rows_;
    char *user_message_;
    char dba_message_[common::OB_MAX_ERROR_MSG_LEN];
    common::ObArenaAllocator allocator_;
  };

  //for add_column in ddl_error_message
  struct ObDDLErrorInfo final
  {
  public:
    ObDDLErrorInfo()
      : parent_task_id_(0), task_id_(0), trace_id_()
    {
      memset(trace_id_str_, 0, sizeof(trace_id_str_));
    }
    ~ObDDLErrorInfo() = default;
    int set_parent_task_id(const int64_t parent_task_id)
    {
      parent_task_id_ = parent_task_id;
      return common::OB_SUCCESS;
    }
    int set_task_id(const int64_t task_id)
    {
      task_id_ = task_id;
      return common::OB_SUCCESS;
    }
    int set_trace_id(const ObString &trace_id)
    {
      common::ObDataBuffer allocator(trace_id_str_, OB_MAX_TRACE_ID_BUFFER_SIZE);
      return common::ob_write_string(allocator, trace_id, trace_id_);
    }

    TO_STRING_KV(K(task_id_), K(parent_task_id_), K(trace_id_str_), K(trace_id_));
  public:
    int64_t parent_task_id_;
    int64_t task_id_;
    common::ObString trace_id_;
    char trace_id_str_[OB_MAX_TRACE_ID_BUFFER_SIZE];
  };

  ObDDLErrorMessageTableOperator();
  virtual ~ObDDLErrorMessageTableOperator();
  static int get_index_task_info(ObMySQLProxy &sql_proxy, const share::schema::ObTableSchema &index_schema, ObDDLErrorInfo &info);
  static int extract_index_key(const share::schema::ObTableSchema &index_schema, const blocksstable::ObDatumRowkey &index_key,
    char *buffer, const int64_t buffer_len);
  static int load_ddl_user_error(const uint64_t tenant_id, const int64_t task_id, const uint64_t table_id, 
      common::ObMySQLProxy &sql_proxy, ObBuildDDLErrorMessage &error_message);
  static int get_ddl_error_message(const uint64_t tenant_id, const int64_t task_id, const int64_t target_object_id,
      const common::ObAddr &addr, const bool is_ddl_retry_task, common::ObMySQLProxy &sql_proxy, ObBuildDDLErrorMessage &error_message, 
      int64_t &forward_user_msg_len);
  static int get_ddl_error_message(
      const uint64_t tenant_id,
      const int64_t task_id,
      const int64_t target_object_id,
      const int64_t object_id,
      common::ObMySQLProxy &sql_proxy,
      ObBuildDDLErrorMessage &error_message,
      int64_t &forward_user_msg_len);
  static int report_ddl_error_message(const ObBuildDDLErrorMessage &error_message, const uint64_t tenant_id,
      const char *trace_id, const int64_t task_id, const int64_t parent_task_id, const uint64_t table_id,
      const int64_t schema_version, const int64_t object_id, const common::ObAddr &addr, common::ObMySQLProxy &sql_proxy);
  static int report_ddl_error_message(const ObBuildDDLErrorMessage &error_message, const uint64_t tenant_id,
      const ObCurTraceId::TraceId &trace_id, const int64_t task_id, const int64_t parent_task_id, const uint64_t table_id,
      const int64_t schema_version, const int64_t object_id, const common::ObAddr &addr, common::ObMySQLProxy &sql_proxy);
  static int report_ddl_error_message(const ObBuildDDLErrorMessage &error_message, const uint64_t tenant_id,
      const int64_t task_id, const uint64_t table_id, const int64_t schema_version, const int64_t object_id,
      const int64_t parent_task_id, const common::ObAddr &addr, common::ObMySQLProxy &sql_proxy);
  static int build_ddl_error_message(const int ret_code, const uint64_t tenant_id, const uint64_t table_id,
      ObBuildDDLErrorMessage &error_message, const common::ObString index_name,
      const uint64_t index_id, const ObDDLType ddl_type, const char *message, int &report_ret_code);
  static int generate_index_ddl_error_message(const int ret_code, const share::schema::ObTableSchema &index_schema, 
      const char *trace_id, const int64_t task_id, const int64_t parent_task_id,
      const int64_t object_id, const common::ObAddr &addr, common::ObMySQLProxy &sql_proxy, const char *index_key, int &report_ret_code);
};
}  // end namespace share
}  // end namespace oceanbase

#endif  // OB_DDL_ERROR_MESSAGE_REPORTER_H

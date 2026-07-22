/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SHARE_AI_SERVICE_OB_AI_SYSTEM_TABLE_MANAGER_H_
#define OCEANBASE_SHARE_AI_SERVICE_OB_AI_SYSTEM_TABLE_MANAGER_H_

#include "lib/ob_define.h"
#include "lib/string/ob_string.h"
#include "lib/container/ob_iarray.h"
#include "lib/allocator/ob_allocator.h"
#include "lib/mysqlclient/ob_isql_client.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "share/ai_service/ob_ai_exec_struct.h"
#include "share/ai_service/ob_ai_batch_file_manager.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "share/ob_dml_sql_splicer.h"

namespace oceanbase
{
namespace common
{
class ObMySQLProxy;
class ObSqlString;
}

namespace share
{

// Table names for AI batch task system tables
// Note: OB_ALL_AI_BATCH_TASK_TNAME is defined in share/inner_table/ob_inner_table_schema_constants.h

// Column name constants for __all_ai_batch_task table
struct ObAiTaskTableColumns
{
  static const char *const TASK_ID;
  static const char *const TENANT_ID;
  static const char *const MODEL_NAME;
  static const char *const COMMAND_TYPE;
  static const char *const STATUS;
  static const char *const REQUESTS_HANDLED;
  static const char *const TOTAL_REQUESTS;
  static const char *const TASK_CREATE_TIME;
  static const char *const TASK_UPDATE_TIME;
  static const char *const BATCH_ID;
  static const char *const REMOTE_FILE_IDS;
  static const char *const LOCAL_FILE_METADATA;
  static const char *const ERROR_DETAIL;
  static const char *const DDL_TASK_ID;
};

// Maximum lengths for various fields
constexpr int64_t OB_AI_MAX_TASK_ID_LENGTH = 256;
constexpr int64_t OB_AI_MAX_BATCH_ID_LENGTH = 256;

/**
 * @class ObAiSystemTableManager
 * @brief Manager for AI task system table (__all_ai_task)
 *
 * This class provides CRUD operations for AI task records stored in
 * OceanBase system tables. After removing __all_ai_internal_task,
 * all task state is managed in __all_ai_task with extended fields.
 */
class ObAiSystemTableManager
{
public:
  ObAiSystemTableManager();
  ~ObAiSystemTableManager();

  /**
   * @brief Initialize the manager
   * @param tenant_id The tenant ID for the manager
   * @param allocator Memory allocator for internal allocations
   * @return OB_SUCCESS on success, error code otherwise
   */
  int init(uint64_t tenant_id, common::ObIAllocator &allocator);

  /**
   * @brief Destroy the manager and release resources
   */
  void destroy();

  /**
   * @brief Register a new task
   * @param task_info Task information to register
   * @param sql_client SQL client for database operations
   * @return OB_SUCCESS on success, error code otherwise
   */
  int register_task(const ObAiTaskInfo &task_info, common::ObISQLClient &sql_client);

  /**
   * @brief Update task status and extended fields
   * @param task_id Task ID to update
   * @param status New status
   * @param requests_handled Number of processed requests (default 0)
   * @param error_code Error code if failed (default OB_SUCCESS)
   * @param error_message Error message if failed (optional)
   * @param http_error_code HTTP error code from provider (optional)
   * @param remote_file_ids JSON: {"input_file_id":"...","output_file_id":"...","error_file_id":"..."} (optional)
   * @param sql_client SQL client for database operations
   * @param batch_id Provider-side batch ID (optional)
   * @return OB_SUCCESS on success, error code otherwise
   */
  int update_task_status(const ObString &task_id,
                         ObAiTaskStatus status,
                         int64_t requests_handled,
                         int error_code,
                         const ObString &error_message,
                         int64_t http_error_code,
                         const ObString &remote_file_ids,
                         common::ObISQLClient &sql_client,
                         const ObString &batch_id = ObString());

  /**
   * @brief Query task status
   * @param task_id Task ID to query
   * @param task_info Output parameter for task info
   * @param sql_client SQL client for database operations
   * @return OB_SUCCESS on success, OB_ENTRY_NOT_EXIST if not found
   */
  int query_task_status(const ObString &task_id,
                        ObAiTaskInfo &task_info,
                        common::ObISQLClient &sql_client);

  /**
   * @brief Delete a task
   * @param task_id Task ID to delete
   * @param sql_client SQL client for database operations
   * @return OB_SUCCESS on success, error code otherwise
   */
  int delete_task(const ObString &task_id, common::ObISQLClient &sql_client);

  /**
   * @brief Update file metadata (JSON) for a task
   * @param task_id Task ID to update
   * @param local_file_metadata JSON string containing fd/size/line_count info
   * @param sql_client SQL client for database operations
   * @return OB_SUCCESS on success
   */
  int update_task_file_metadata(const ObString &task_id,
                                const ObString &local_file_metadata,
                                common::ObISQLClient &sql_client);

  /**
   * @brief Archive a terminal task to history table and delete from main table.
   *        INSERT and DELETE are wrapped in a transaction for atomicity.
   * @param task_id Task ID to archive
   * @param token_usage JSON: {"completion_tokens":N,"prompt_tokens":N,"total_tokens":N}
   * @param provider_timeline JSON: {"created_at":N,...} provider-side stage timestamps
   * @param sql_proxy SQL proxy for database operations (used to start transaction)
   * @return OB_SUCCESS on success
   */
  int archive_task_to_history(const ObString &task_id,
                              const ObString &token_usage,
                              const ObString &provider_timeline,
                              common::ObMySQLProxy &sql_proxy);

  /**
   * @brief Check if manager is initialized
   */
  bool is_inited() const { return is_inited_; }

  /**
   * @brief Get tenant ID
   */
  uint64_t get_tenant_id() const { return tenant_id_; }

private:
  int copy_string_field_(const common::ObString &src, common::ObString &dest);
  // Get current timestamp in microseconds
  int64_t get_current_time_us_() const;

private:
  bool is_inited_;
  uint64_t tenant_id_;
  common::ObIAllocator *allocator_;

  DISALLOW_COPY_AND_ASSIGN(ObAiSystemTableManager);
};

} // namespace share
} // namespace oceanbase

#endif // OCEANBASE_SHARE_AI_SERVICE_OB_AI_SYSTEM_TABLE_MANAGER_H_
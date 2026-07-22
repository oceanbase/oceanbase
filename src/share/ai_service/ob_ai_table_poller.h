/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SHARE_AI_SERVICE_OB_AI_TABLE_POLLER_H_
#define OCEANBASE_SHARE_AI_SERVICE_OB_AI_TABLE_POLLER_H_

#include "lib/ob_define.h"
#include "lib/string/ob_string.h"
#include "lib/container/ob_se_array.h"
#include "lib/allocator/ob_allocator.h"
#include "lib/lock/ob_spin_lock.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "share/vector_index/ob_ai_access_service.h"
#include "share/ai_service/ob_ai_exec_struct.h"
#include "share/ai_service/ob_ai_system_table_manager.h"

namespace oceanbase
{
// Forward declarations for types in vector_index namespace
namespace vector_index
{
class ObAiAccessService;
} // namespace vector_index

namespace share
{

/**
 * @brief System table poller for AI execution tasks
 *
 * ObSystemTablePoller periodically polls the AI task system table
 * for pending tasks, claims them atomically, and submits them
 * to the execution scheduler for processing.
 *
 * The poller implements self-scheduling using the task scheduler's
 * schedule_after mechanism, allowing it to run at regular intervals
 * without blocking a dedicated thread.
 *
 * Key responsibilities (pure GC after Writer-based refactor):
 * 1. Recover stale RUNNING tasks after restart (mark FAILED)
 * 2. Cleanup terminal tasks from memory (task_map_)
 * 3. Remote file cleanup placeholder logging
 * 4. Archive expired terminal tasks to history table
 */
class ObSystemTablePoller : public vector_index::ObAiSchedulableTask
{
public:
  // Constants
  static const int64_t DEFAULT_POLL_INTERVAL_US = 1000000;  // 1 second
  static const int64_t MAX_TASK_ID_LENGTH = 128;
  static const int64_t MAX_BATCH_ID_LENGTH = 128;

  ObSystemTablePoller();
  virtual ~ObSystemTablePoller();

  /**
   * @brief Initialize the poller with specific parameters
   * @param allocator Memory allocator for internal allocations
   * @param service Reference to the execution service
   * @param table_manager Reference to the system table manager
   * @param sql_client SQL client for database operations
   * @param poll_interval_us Polling interval in microseconds (default: 1 second)
   * @return OB_SUCCESS on success
   */
  int init(common::ObIAllocator &allocator,
           vector_index::ObAiAccessService &service,
           ObAiSystemTableManager &table_manager,
           common::ObMySQLProxy &sql_proxy,
           int64_t poll_interval_us = DEFAULT_POLL_INTERVAL_US);

  /**
   * @brief Reset poller state
   */
  virtual void reset() override;

  /**
   * @brief Main work method - called by scheduler
   *
   * This method:
   * 1. Checks if stopped
   * 2. Recovers stale RUNNING tasks on first round
   * 3. Periodically cleans up remote files (placeholder)
   * 4. Periodically archives expired terminal tasks to history
   * 5. Cleans up terminal tasks from memory (task_map_)
   *
   * @return OB_SUCCESS on success
   */
  virtual int do_work() override;

  /**
   * @brief Check if poller needs rescheduling
   * @return true - poller always needs rescheduling for continuous polling
   */
  virtual bool need_reschedule() const override;

  /**
   * @brief Get reschedule delay in microseconds
   * @return poll_interval_us_
   */
  virtual int64_t get_reschedule_delay_us() const override;

  /**
   * @brief Stop the poller
   *
   * After calling stop(), the poller will not schedule itself again
   * and will skip any pending work.
   */
  void stop();

  /**
   * @brief Check if poller is stopped
   */
  bool is_stopped() const { return stopped_; }

  /**
   * @brief Start the poller
   *
   * Schedules the first poll task.
   * @return OB_SUCCESS on success
   */
  int start();

  /**
   * @brief Get poll interval in microseconds
   */
  int64_t get_poll_interval_us() const { return poll_interval_us_; }

  /**
   * @brief Set poll interval in microseconds
   */
  void set_poll_interval_us(int64_t interval_us);

  TO_STRING_KV("is_inited", is_inited(), K_(stopped), K_(poll_interval_us));

private:
  // Remote file cleanup: log files that should be deleted for terminal tasks
  int cleanup_remote_files_();

  // Recovery: mark stale RUNNING tasks as FAILED after observer restart
  // (TmpFileManager fds are lost, PENDING/UPLOADING tasks cannot continue)
  int recover_running_tasks_after_restart_();

  // Handle tasks abandoned by DDL:
  // Phase 1 (RUNNING): initiate_cancel() to stop execution thread
  // Phase 2 (terminal): archive to history + destroy
  int handle_abandoned_tasks_();

  // Check if DDL task is still alive (running) in __all_ddl_task_status.
  // Returns:
  //   OB_SUCCESS + ddl_alive=true  : DDL record exists and is running
  //   OB_SUCCESS + ddl_alive=false : DDL record not found or in terminal state (FAIL/SUCCESS)
  //   other error                  : query failed, caller should skip and retry next round
  int check_ddl_alive_(uint64_t tenant_id, int64_t ddl_task_id, bool &ddl_alive);

private:
  // Configuration
  int64_t poll_interval_us_;

  // State
  bool stopped_;
  int64_t poll_round_;

  // References to external components (not owned)
  vector_index::ObAiAccessService *service_;
  ObAiSystemTableManager *table_manager_;
  common::ObMySQLProxy *sql_proxy_;
  common::ObIAllocator *allocator_;

  DISALLOW_COPY_AND_ASSIGN(ObSystemTablePoller);
};

/**
 * @brief Factory for creating system table poller
 */
class ObSystemTablePollerFactory
{
public:
  /**
   * @brief Create a system table poller
   * @param allocator Memory allocator
   * @param service Execution service reference
   * @param table_manager System table manager reference
   * @param sql_client SQL client for database operations
   * @param poll_interval_us Polling interval in microseconds
   * @param poller Output poller pointer
   * @return OB_SUCCESS on success
   */
  static int create_poller(common::ObIAllocator &allocator,
                          vector_index::ObAiAccessService &service,
                          ObAiSystemTableManager &table_manager,
                          common::ObMySQLProxy &sql_proxy,
                          int64_t poll_interval_us,
                          ObSystemTablePoller *&poller);

  /**
   * @brief Destroy a system table poller
   * @param allocator Memory allocator used for creation
   * @param poller Poller to destroy
   */
  static void destroy_poller(common::ObIAllocator &allocator,
                            ObSystemTablePoller *poller);

private:
  ObSystemTablePollerFactory() = delete;
  DISALLOW_COPY_AND_ASSIGN(ObSystemTablePollerFactory);
};

} // namespace share
} // namespace oceanbase

#endif // OCEANBASE_SHARE_AI_SERVICE_OB_AI_TABLE_POLLER_H_
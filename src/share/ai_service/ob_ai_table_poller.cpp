/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SHARE

#include "share/ai_service/ob_ai_table_poller.h"
#include "share/ai_service/ob_ai_service_proxy.h"
#include "share/vector_index/ob_ai_access_service.h"
#include "share/ai_service/ob_ai_service_struct.h"
#include "lib/time/ob_time_utility.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/allocator/ob_malloc.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "common/storage/ob_io_device.h"
#include "lib/json_type/ob_json_base.h"
#include "lib/json_type/ob_json_parse.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "lib/mysqlclient/ob_mysql_result.h"
#include "share/ob_ddl_common.h"

namespace oceanbase
{
namespace share
{

namespace
{

int mark_restart_failed_task_(ObAiSystemTableManager &table_manager,
                              common::ObISQLClient &sql_client,
                              const ObAiTaskInfo &task_info,
                              int error_code,
                              const common::ObString &error_message,
                              const common::ObString &output_file_id = common::ObString())
{
  common::ObString remote_files_json = task_info.remote_file_ids_;
  common::ObArenaAllocator tmp_alloc("BatchRestart", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  if (!output_file_id.empty()) {
    ObAiRemoteFilesView view;
    if (OB_SUCCESS == ObAiTaskInfo::parse_remote_files_json(task_info.remote_file_ids_, view)) {
      view.output_file_id_ = output_file_id;
      ObAiTaskInfo::build_remote_files_json(tmp_alloc,
                                            view.input_file_id_,
                                            view.output_file_id_,
                                            view.error_file_id_,
                                            remote_files_json);
    }
  }
  return table_manager.update_task_status(task_info.task_id_,
                                          OB_AI_TASK_STATUS_FAILED,
                                          task_info.requests_handled_,
                                          error_code,
                                          error_message,
                                          0,
                                          remote_files_json,
                                          sql_client);
}

int mark_restart_failed_with_reason_(ObAiSystemTableManager &table_manager,
                                     common::ObISQLClient &sql_client,
                                     const ObAiTaskInfo &task_info,
                                     int error_code,
                                     const char *error_message,
                                     const common::ObString &output_file_id = common::ObString())
{
  return mark_restart_failed_task_(table_manager,
                                   sql_client,
                                   task_info,
                                   error_code,
                                   common::ObString::make_string(error_message),
                                   output_file_id);
}

} // namespace

// Import vector_index namespace types for base class
using vector_index::ObAiSchedulableTask;
using vector_index::ObAiAccessService;
using vector_index::OB_AI_TASK_PRIORITY_HIGH;
using vector_index::OB_AI_SCHEDULABLE_TASK_TYPE_TABLE_POLLER;

ObSystemTablePoller::ObSystemTablePoller()
    : poll_interval_us_(DEFAULT_POLL_INTERVAL_US),
      stopped_(true),
      poll_round_(0),
      service_(nullptr),
      table_manager_(nullptr),
      sql_proxy_(nullptr),
      allocator_(nullptr)
{
}

ObSystemTablePoller::~ObSystemTablePoller()
{
  reset();
}

int ObSystemTablePoller::init(common::ObIAllocator &allocator,
                               ObAiAccessService &service,
                               ObAiSystemTableManager &table_manager,
                               common::ObMySQLProxy &sql_proxy,
                               int64_t poll_interval_us)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObSystemTablePoller already initialized", K(ret));
  } else if (OB_UNLIKELY(poll_interval_us <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid poll_interval_us", K(ret), K(poll_interval_us));
  } else {
    // Initialize base class
    if (OB_FAIL(ObAiSchedulableTask::init(allocator,
                                           OB_AI_TASK_PRIORITY_HIGH,
                                           OB_AI_SCHEDULABLE_TASK_TYPE_TABLE_POLLER))) {
      LOG_WARN("failed to init base class", K(ret));
    } else {
      allocator_ = &allocator;
      service_ = &service;
      table_manager_ = &table_manager;
      sql_proxy_ = &sql_proxy;
      poll_interval_us_ = poll_interval_us;
      stopped_ = true;
      poll_round_ = 0;
      is_inited_ = true;
      LOG_INFO("ObSystemTablePoller initialized", K_(poll_interval_us));
    }
  }
  return ret;
}

void ObSystemTablePoller::reset()
{
  if (is_inited_) {
    stopped_ = true;
    service_ = nullptr;
    table_manager_ = nullptr;
    sql_proxy_ = nullptr;
    allocator_ = nullptr;
    poll_round_ = 0;
    is_inited_ = false;
    ObAiSchedulableTask::reset();
  }
}

int ObSystemTablePoller::do_work()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSystemTablePoller not initialized", K(ret));
  } else if (stopped_) {
    // Skip work if stopped
    LOG_DEBUG("[BATCH-FILE] ObSystemTablePoller is stopped, skip work");
  } else {
    poll_round_++;

    // On first poll round after startup, recover stale RUNNING tasks
    // whose TmpFileManager fds are no longer valid.
    // RUNNING tasks without batch_id → mark FAILED (data lost, user rebuilds)
    // RUNNING tasks with batch_id → can continue polling
    // RUNNING tasks with output_file_id → can re-download result
    if (1 == poll_round_ && OB_NOT_NULL(table_manager_) && OB_NOT_NULL(sql_proxy_)) {
      int tmp_ret = recover_running_tasks_after_restart_();
      if (OB_SUCCESS != tmp_ret) {
        LOG_WARN("failed to recover running tasks after restart", K(tmp_ret));
        // Non-fatal: continue normal polling
      }
    }

    // Periodic: handle abandoned tasks (every 60 rounds ~= 1 min)
    // Phase 1: initiate cancel on RUNNING abandoned tasks
    // Phase 2: archive + destroy terminal abandoned tasks
    if (poll_round_ % 60 == 0 && OB_NOT_NULL(service_) && OB_NOT_NULL(table_manager_) && OB_NOT_NULL(sql_proxy_)) {
      int tmp_ret = handle_abandoned_tasks_();
      if (OB_SUCCESS != tmp_ret) {
        LOG_WARN("failed to handle abandoned tasks", K(tmp_ret));
      }
    }

    // Periodic: remote file cleanup placeholder (every 60 rounds ~= 1 min)
    if (poll_round_ % 60 == 0 && OB_NOT_NULL(table_manager_) && OB_NOT_NULL(sql_proxy_)) {
      int tmp_ret = cleanup_remote_files_();
      if (OB_SUCCESS != tmp_ret) {
        LOG_WARN("failed to cleanup remote files", K(tmp_ret));
      }
    }

    // Lazy cleanup: remove terminal tasks from task_map_
    if (OB_NOT_NULL(service_)) {
      int tmp_ret = service_->cleanup_terminal_tasks_();
      if (OB_SUCCESS != tmp_ret) {
        LOG_WARN("failed to cleanup terminal tasks", K(tmp_ret));
      }
    }
  }
  return ret;
}

bool ObSystemTablePoller::need_reschedule() const
{
  return !stopped_;
}

int64_t ObSystemTablePoller::get_reschedule_delay_us() const
{
  return poll_interval_us_;
}

void ObSystemTablePoller::stop()
{
  stopped_ = true;
  LOG_INFO("[BATCH-FILE] ObSystemTablePoller stopped");
}

int ObSystemTablePoller::start()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSystemTablePoller not initialized", K(ret));
  } else if (!stopped_) {
    LOG_INFO("[BATCH-FILE] ObSystemTablePoller already running");
  } else {
    stopped_ = false;
    LOG_INFO("[BATCH-FILE] ObSystemTablePoller started", K_(poll_interval_us));
  }
  return ret;
}

void ObSystemTablePoller::set_poll_interval_us(int64_t interval_us)
{
  if (interval_us > 0) {
    poll_interval_us_ = interval_us;
  }
}

int ObSystemTablePoller::cleanup_remote_files_()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(table_manager_) || OB_ISNULL(sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table_manager or sql_client is null", K(ret));
  } else {
    uint64_t tenant_id = table_manager_->get_tenant_id();

    // Phase 1: collect file cleanup tasks from SQL result set, then close the connection.
    // HTTP deletions are deferred to Phase 2 so the MySQL connection is not held
    // open during potentially long-running curl calls (up to DOWNLOAD_HTTP_TIMEOUT_US each).
    struct FileCleanupTask {
      common::ObString task_id_;
      common::ObString model_name_;
      common::ObString input_file_id_;
      common::ObString output_file_id_;
      common::ObString error_file_id_;
      TO_STRING_KV(K(task_id_), K(model_name_), K(input_file_id_), K(output_file_id_), K(error_file_id_));
    };
    common::ObArenaAllocator collect_alloc("BatchFileGC", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
    common::ObSEArray<FileCleanupTask, 8> cleanup_tasks;
    cleanup_tasks.set_attr(ObMemAttr(MTL_ID(), "BatchFileGC"));

    SMART_VAR(common::ObMySQLProxy::MySQLResult, res) {
      common::sqlclient::ObMySQLResult *result = nullptr;
      ObSqlString select_sql;
      select_sql.set_attr(ObMemAttr(MTL_ID(), "BatchFileGC"));
      if (OB_FAIL(select_sql.assign_fmt(
          "SELECT task_id, model_name, remote_file_ids, batch_id FROM %s "
          "WHERE tenant_id = %lu",
          OB_ALL_AI_BATCH_TASK_HISTORY_TNAME,
          tenant_id))) {
        LOG_WARN("failed to build select sql", K(ret));
      } else if (OB_FAIL(sql_proxy_->read(res, tenant_id, select_sql.ptr()))) {
        LOG_WARN("failed to execute select", K(ret));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null result set from remote file cleanup select", K(ret));
      } else {
        while (OB_SUCC(ret)) {
          if (OB_FAIL(result->next())) {
            if (OB_ITER_END == ret) {
              ret = OB_SUCCESS;
            }
            break;
          }
          common::ObString task_id, model_name, remote_file_ids, batch_id;
          EXTRACT_VARCHAR_FIELD_MYSQL(*result, "task_id", task_id);
          if (OB_FAIL(ret)) { break; }
          EXTRACT_VARCHAR_FIELD_MYSQL_WITH_DEFAULT_VALUE(*result, "model_name", model_name, true, true, "");
          if (OB_FAIL(ret)) { break; }
          EXTRACT_VARCHAR_FIELD_MYSQL_WITH_DEFAULT_VALUE(*result, "remote_file_ids", remote_file_ids, true, true, "");
          if (OB_FAIL(ret)) { break; }
          EXTRACT_VARCHAR_FIELD_MYSQL_WITH_DEFAULT_VALUE(*result, "batch_id", batch_id, true, true, "");
          if (OB_FAIL(ret)) { break; }

          ObAiRemoteFilesView rf_view;
          if (!remote_file_ids.empty()) {
            ObAiTaskInfo::parse_remote_files_json(remote_file_ids, rf_view);
          }
          if (!rf_view.input_file_id_.empty() && !model_name.empty()) {
            FileCleanupTask ct;
            int tmp_ret = OB_SUCCESS;
            if (OB_SUCCESS != (tmp_ret = ob_write_string(collect_alloc, task_id, ct.task_id_))) {
              LOG_WARN("[BATCH-FILE-GC] failed to copy task_id", K(tmp_ret), K(task_id));
            } else if (OB_SUCCESS != (tmp_ret = ob_write_string(collect_alloc, model_name, ct.model_name_))) {
              LOG_WARN("[BATCH-FILE-GC] failed to copy model_name", K(tmp_ret), K(task_id));
            } else if (OB_SUCCESS != (tmp_ret = ob_write_string(collect_alloc, rf_view.input_file_id_, ct.input_file_id_))) {
              LOG_WARN("[BATCH-FILE-GC] failed to copy input_file_id", K(tmp_ret), K(task_id));
            } else if (OB_SUCCESS != (tmp_ret = ob_write_string(collect_alloc, rf_view.output_file_id_, ct.output_file_id_))) {
              LOG_WARN("[BATCH-FILE-GC] failed to copy output_file_id", K(tmp_ret), K(task_id));
            } else if (OB_SUCCESS != (tmp_ret = ob_write_string(collect_alloc, rf_view.error_file_id_, ct.error_file_id_))) {
              LOG_WARN("[BATCH-FILE-GC] failed to copy error_file_id", K(tmp_ret), K(task_id));
            } else if (OB_SUCCESS != (tmp_ret = cleanup_tasks.push_back(ct))) {
              LOG_WARN("[BATCH-FILE-GC] failed to push cleanup task", K(tmp_ret), K(task_id));
            }
            // Non-fatal: a failed copy just skips this entry in Phase 2
          } else if (!rf_view.input_file_id_.empty()) {
            LOG_WARN("[BATCH-FILE-GC] skip remote input file cleanup because model_name is empty",
                     K(task_id), K(rf_view.input_file_id_));
          }
        }
      }
    } // SMART_VAR: MySQL result set and connection released here

    // Phase 2: HTTP deletions with no MySQL connection held open.
    for (int64_t i = 0; i < cleanup_tasks.count(); ++i) {
      const FileCleanupTask &ct = cleanup_tasks.at(i);
      common::ObArenaAllocator http_alloc("BatchFileGC", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
      ObAiModelEndpointInfo endpoint_info;
      common::ObString api_key;
      ObAiBatchFileManager batch_file_manager;
      int tmp_ret = ObAiServiceProxy::select_ai_endpoint(
          gen_meta_tenant_id(tenant_id), http_alloc, *sql_proxy_, ct.model_name_, endpoint_info);
      if (OB_SUCCESS != tmp_ret) {
        LOG_WARN("[BATCH-FILE-GC] failed to load endpoint info for remote file cleanup",
                 K(tmp_ret), K(ct.task_id_), K(ct.model_name_));
      } else if (OB_SUCCESS != (tmp_ret = endpoint_info.get_unencrypted_access_key(http_alloc, api_key))) {
        LOG_WARN("[BATCH-FILE-GC] failed to decrypt endpoint api key for remote file cleanup",
                 K(tmp_ret), K(ct.task_id_), K(ct.model_name_));
      } else if (OB_SUCCESS != (tmp_ret = batch_file_manager.init(
                     http_alloc, endpoint_info.get_batch_file_url(), api_key))) {
        LOG_WARN("[BATCH-FILE-GC] failed to init batch file manager for remote file cleanup",
                 K(tmp_ret), K(ct.task_id_), K(ct.model_name_));
      } else if (OB_SUCCESS != (tmp_ret = batch_file_manager.delete_file(ct.input_file_id_))) {
        LOG_WARN("[BATCH-FILE-GC] failed to delete remote input file",
                 K(tmp_ret), K(ct.task_id_), K(ct.input_file_id_), K(ct.model_name_));
      } else {
        LOG_INFO("[BATCH-FILE-GC] deleted remote input file",
                 K(ct.task_id_), K(ct.input_file_id_), K(ct.model_name_));
      }
      if (!ct.error_file_id_.empty() && batch_file_manager.is_inited()) {
        int del_ret = batch_file_manager.delete_file(ct.error_file_id_);
        if (OB_SUCCESS != del_ret) {
          LOG_WARN("[BATCH-FILE-GC] failed to delete remote error file",
                   K(del_ret), K(ct.task_id_), K(ct.error_file_id_), K(ct.model_name_));
        } else {
          LOG_INFO("[BATCH-FILE-GC] deleted remote error file",
                   K(ct.task_id_), K(ct.error_file_id_), K(ct.model_name_));
        }
      }
      if (!ct.output_file_id_.empty() && batch_file_manager.is_inited()) {
        int del_ret = batch_file_manager.delete_file(ct.output_file_id_);
        if (OB_SUCCESS != del_ret) {
          LOG_WARN("[BATCH-FILE-GC] failed to delete remote output file",
                   K(del_ret), K(ct.task_id_), K(ct.output_file_id_), K(ct.model_name_));
        } else {
          LOG_INFO("[BATCH-FILE-GC] deleted remote output file",
                   K(ct.task_id_), K(ct.output_file_id_), K(ct.model_name_));
        }
      }
    }
  }
  return ret;
}

int ObSystemTablePoller::handle_abandoned_tasks_()
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(service_) || OB_ISNULL(table_manager_) || OB_ISNULL(sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("handle_abandoned_tasks_: null pointers", K(ret));
  } else {
    // Collect all tasks with abandoned_ == true under lock, pinning each.
    common::ObSEArray<vector_index::ObAiAccessTask*, 8> abandoned_tasks;
    common::ObSEArray<common::ObString, 8> abandoned_task_ids;
    abandoned_tasks.set_attr(ObMemAttr(MTL_ID(), "AbandonedTasks"));
    abandoned_task_ids.set_attr(ObMemAttr(MTL_ID(), "AbandonedTaskId"));

    if (OB_FAIL(service_->collect_abandoned_tasks(abandoned_tasks, abandoned_task_ids))) {
      LOG_WARN("handle_abandoned_tasks_: failed to collect abandoned tasks", K(ret));
    } else {
      common::ObArenaAllocator archive_alloc("AiPollerArch", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
      int64_t idx = 0;
      for (; OB_SUCC(ret) && idx < abandoned_tasks.count(); ++idx) {
        vector_index::ObAiAccessTask *task = abandoned_tasks.at(idx);
        const common::ObString &task_id = abandoned_task_ids.at(idx);
        if (OB_ISNULL(task)) { continue; }

        if (task->is_running_state()) {
          // Phase 1: issue cancel to execution thread
          {
            int tmp_ret = task->initiate_cancel();
            if (OB_SUCCESS != tmp_ret) {
              LOG_WARN("[BATCH-FILE-GC] initiate_cancel failed for abandoned task, will retry",
                       K(tmp_ret), K(task_id));
            } else {
              LOG_INFO("[BATCH-FILE-GC] issued cancel for abandoned running task", K(task_id));
            }
          }
          task->unpin();
        } else {
          if (!task->is_terminal_state()) {
            char abandon_msg[128];
            int64_t msg_pos = snprintf(abandon_msg, sizeof(abandon_msg),
                "Task abandoned in non-terminal state (state=%d), marked FAILED by GC",
                static_cast<int>(task->get_state()));
            if (msg_pos < 0 || msg_pos >= static_cast<int64_t>(sizeof(abandon_msg))) {
              abandon_msg[sizeof(abandon_msg) - 1] = '\0';
            }
            int tmp_ret = table_manager_->update_task_status(
                task_id, OB_AI_TASK_STATUS_FAILED, 0, OB_CANCELED,
                ObString::make_string(abandon_msg),
                0, ObString(), *sql_proxy_);
            if (OB_SUCCESS != tmp_ret && OB_ENTRY_NOT_EXIST != tmp_ret) {
              LOG_WARN("[BATCH-FILE-GC] failed to fail abandoned unscheduled task, will retry",
                       K(tmp_ret), K(task_id), "state", task->get_state());
              task->unpin();
              continue;
            } else {
              task->set_state(OB_AI_TASK_STATUS_FAILED);
            }
          }
          if (task->is_archived()) {
            // Already archived in a previous round; cleanup_terminal_tasks_() will free.
            task->unpin();
            continue;
          }
          // Remote batch cancel is handled by complete_with_cancel_() in the
          // Scheduler thread.  initiate_cancel() on a terminal task is a no-op.
          // Phase 2: archive + destroy
          // token_usage: {"completion_tokens":%ld,"prompt_tokens":%ld,"total_tokens":%ld}
          // provider_timeline: {"created_at":%ld,"in_progress_at":%ld,"finalizing_at":%ld,
          //   "completed_at":%ld,"failed_at":%ld,"expired_at":%ld,"expires_at":%ld,
          //   "cancelling_at":%ld,"cancelled_at":%ld}  DB column max 1024
          const int64_t token_usage_len = 128;
          const int64_t provider_timeline_len = 1024;
          char *token_usage = static_cast<char*>(archive_alloc.alloc(token_usage_len));
          char *provider_timeline = static_cast<char*>(archive_alloc.alloc(provider_timeline_len));
          if (OB_ISNULL(token_usage) || OB_ISNULL(provider_timeline)) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_ERROR("failed to alloc archive buffers from local arena, tenant may be under memory pressure",
                      K(ret));
            // Do not unpin here: the tail loop below unpins from idx so each task
            // is unpinned exactly once (collect_abandoned_tasks pins once per task).
            break;
          }
          int64_t completion_tokens = task->get_accumulated_completion_tokens();
          int64_t prompt_tokens = task->get_accumulated_prompt_tokens();
          int64_t total_tokens = task->get_accumulated_total_tokens();
          int64_t model_wait_us = task->get_model_wait_time_us();
          token_usage[0] = '\0';
          provider_timeline[0] = '\0';
          snprintf(token_usage, token_usage_len,
                   "{\"completion_tokens\":%ld,\"prompt_tokens\":%ld,\"total_tokens\":%ld}",
                   completion_tokens, prompt_tokens, total_tokens);
          if (model_wait_us > 0) {
            snprintf(provider_timeline, provider_timeline_len,
                     "{\"model_wait_time_us\":%ld}", model_wait_us);
          }
          int archive_ret = table_manager_->archive_task_to_history(
              task_id, ObString::make_string(token_usage),
              ObString::make_string(provider_timeline), *sql_proxy_);
          if (OB_SUCCESS != archive_ret) {
            if (OB_ENTRY_NOT_EXIST == archive_ret) {
              // Record already gone from main table (archived by another path such as
              // release_task whose transaction committed but returned an error).
              // Treat as already archived so cleanup_terminal_tasks_() can free the object.
              if (task->set_archived()) {
                LOG_INFO("[BATCH-FILE-GC] abandoned task already archived, marked for cleanup",
                         K(task_id));
              }
            } else {
              // Single-task failure must not abort the loop; the next round of
              // collect_abandoned_tasks() will pick this task up again for retry.
              LOG_WARN("[BATCH-FILE-GC] failed to archive abandoned task, will retry next round",
                       K(archive_ret), K(task_id));
            }
            task->unpin();
          } else {
            if (task->set_archived()) {
              LOG_INFO("[BATCH-FILE-GC] archived abandoned task to history", K(task_id));
            } else {
              LOG_INFO("[BATCH-FILE-GC] task archived concurrently by another path", K(task_id));
            }
            task->unpin();
            // cleanup_terminal_tasks_() will erase from map and free once pin_count==0
          }
          archive_alloc.reuse();
        }
      }
      // Best-effort unpin any tasks that were collected but not unpinned yet due to
      // early loop exit (e.g. archive_alloc OOM).
      for (; idx < abandoned_tasks.count(); ++idx) {
        if (OB_NOT_NULL(abandoned_tasks.at(idx))) {
          abandoned_tasks.at(idx)->unpin();
        }
      }
    }
  }
  return ret;
}

int ObSystemTablePoller::check_ddl_alive_(uint64_t tenant_id, int64_t ddl_task_id, bool &ddl_alive)
{
  int ret = OB_SUCCESS;
  ddl_alive = false;
  if (ddl_task_id <= 0) {
    // No associated DDL task — treat as dead so orphan tasks can be cleaned up
  } else {
    ObSqlString sql;
    SMART_VAR(common::ObMySQLProxy::MySQLResult, res) {
      common::sqlclient::ObMySQLResult *result = nullptr;
      if (OB_FAIL(sql.assign_fmt(
              "SELECT status FROM %s WHERE tenant_id = %lu AND task_id = %ld",
              OB_ALL_DDL_TASK_STATUS_TNAME, tenant_id, ddl_task_id))) {
        LOG_WARN("failed to build DDL alive check SQL", K(ret), K(ddl_task_id));
      } else if (OB_FAIL(sql_proxy_->read(res, tenant_id, sql.ptr()))) {
        LOG_WARN("failed to query DDL task status", K(ret), K(ddl_task_id));
      } else if (nullptr == (result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null result for DDL alive check", K(ret), K(ddl_task_id));
      } else if (OB_FAIL(result->next())) {
        if (OB_ITER_END == ret) {
          // No record — DDL is gone, treat as dead
          ret = OB_SUCCESS;
          ddl_alive = false;
        }
      } else {
        int64_t status = 0;
        EXTRACT_INT_FIELD_MYSQL(*result, "status", status, int64_t);
        if (OB_SUCC(ret)) {
          // FAIL=99 and SUCCESS=100 are terminal DDL states; anything else is still running
          ddl_alive = (status != static_cast<int64_t>(share::ObDDLTaskStatus::FAIL) &&
                       status != static_cast<int64_t>(share::ObDDLTaskStatus::SUCCESS));
        }
      }
    }
  }
  return ret;
}

int ObSystemTablePoller::recover_running_tasks_after_restart_()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(table_manager_) || OB_ISNULL(sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table_manager or sql_client is null", K(ret));
  } else {
    uint64_t tenant_id = table_manager_->get_tenant_id();
    int64_t failed_count = 0;
    // Batch-file tasks are not recoverable after observer restart because DDL-side
    // in-memory state is lost. Reconcile all stale RUNNING tasks:
    // - no batch_id: remote batch was never created, fail locally
    // - has batch_id: inspect remote status, best-effort cancel if still running,
    //   and always fail locally so DDL polling can converge.
    SMART_VAR(common::ObMySQLProxy::MySQLResult, res) {
      common::sqlclient::ObMySQLResult *result = nullptr;
      ObSqlString select_sql;
      select_sql.set_attr(ObMemAttr(MTL_ID(), "AiRecover"));
      if (OB_FAIL(select_sql.assign_fmt(
          "SELECT task_id FROM %s "
          "WHERE tenant_id = %lu AND status = %d",
          OB_ALL_AI_BATCH_TASK_TNAME,
          tenant_id,
          static_cast<int>(OB_AI_TASK_STATUS_RUNNING)))) {
        LOG_WARN("failed to build recovery select SQL", K(ret));
      } else if (OB_FAIL(sql_proxy_->read(res, tenant_id, select_sql.ptr()))) {
        LOG_WARN("failed to execute recovery select", K(ret));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null result set from recovery select", K(ret));
      } else {
        while (OB_SUCC(ret)) {
          if (OB_FAIL(result->next())) {
            if (OB_ITER_END == ret) {
              ret = OB_SUCCESS;
            }
            break;
          }
          common::ObString task_id;
          EXTRACT_VARCHAR_FIELD_MYSQL(*result, "task_id", task_id);
          if (OB_FAIL(ret)) { break; }

          // Skip tasks that already have a live in-memory object: they were
          // created after this restart and their TmpFile is still valid.
          vector_index::ObAiAccessTask *task_obj = nullptr;
          if (OB_NOT_NULL(service_)) {
            int tmp = service_->get_task_object(task_id, task_obj);
            if (OB_SUCCESS == tmp && OB_NOT_NULL(task_obj)) {
              continue;
            }
          }

          ObAiTaskInfo task_info;
          int tmp_ret = table_manager_->query_task_status(task_id, task_info, *sql_proxy_);
          if (OB_SUCCESS != tmp_ret) {
            LOG_WARN("failed to load task info for restart reconciliation", K(tmp_ret), K(task_id));
          } else {
            // DDL liveness gate (task 8.2): skip if DDL is still alive (another observer's DDL
            // manages this task); also skip on query failure and retry next round.
            bool ddl_alive = false;
            int ddl_check_ret = (task_info.ddl_task_id_ > 0)
                ? check_ddl_alive_(tenant_id, task_info.ddl_task_id_, ddl_alive)
                : OB_SUCCESS;
            if (OB_SUCCESS != ddl_check_ret) {
              LOG_WARN("[BATCH-FILE] restart recovery C2: DDL alive check failed, skip RUNNING task",
                       K(ddl_check_ret), K(task_id), K(task_info.ddl_task_id_));
            } else if (ddl_alive) {
              LOG_INFO("[BATCH-FILE] restart recovery C2: DDL still alive, skip RUNNING task",
                       K(task_id), K(task_info.ddl_task_id_));
            } else {
            tmp_ret = OB_SUCCESS;
            common::ObString output_file_id;

            // batch_id is now a top-level column
            if (task_info.batch_id_.empty()) {
              tmp_ret = mark_restart_failed_with_reason_(
                  *table_manager_, *sql_proxy_, task_info, OB_ERR_UNEXPECTED,
                  "Task failed after observer restart before remote batch submission");
            } else if (task_info.model_name_.empty()) {
              tmp_ret = mark_restart_failed_with_reason_(
                  *table_manager_, *sql_proxy_, task_info, OB_ERR_UNEXPECTED,
                  "Task failed after observer restart: endpoint info missing for remote reconciliation");
            } else {
              common::ObArenaAllocator restart_alloc("BatchRestart", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
              ObAiModelEndpointInfo endpoint_info;
              common::ObString api_key;
              ObAiBatchFileManager batch_file_manager;
              ObAiBatchSubmitResult poll_result;

              if (OB_FAIL(ObAiServiceProxy::select_ai_endpoint(
                      gen_meta_tenant_id(tenant_id), restart_alloc, *sql_proxy_, task_info.model_name_, endpoint_info))) {
                LOG_WARN("failed to load endpoint info for restart reconciliation",
                         K(ret), K(task_id), K(task_info.model_name_));
                tmp_ret = mark_restart_failed_with_reason_(
                    *table_manager_, *sql_proxy_, task_info, ret,
                    "Task failed after observer restart: remote endpoint lookup failed");
              } else if (OB_FAIL(endpoint_info.get_unencrypted_access_key(restart_alloc, api_key))) {
                LOG_WARN("failed to decrypt endpoint api key for restart reconciliation",
                         K(ret), K(task_id), K(task_info.model_name_));
                tmp_ret = mark_restart_failed_with_reason_(
                    *table_manager_, *sql_proxy_, task_info, ret,
                    "Task failed after observer restart: remote endpoint authentication lookup failed");
              } else if (OB_FAIL(batch_file_manager.init(
                      restart_alloc, endpoint_info.get_batch_file_url(), api_key))) {
                LOG_WARN("failed to init batch file manager for restart reconciliation",
                         K(ret), K(task_id), K(task_info.model_name_));
                tmp_ret = mark_restart_failed_with_reason_(
                    *table_manager_, *sql_proxy_, task_info, ret,
                    "Task failed after observer restart: remote batch manager init failed");
              } else if (OB_FAIL(batch_file_manager.poll_batch_status(task_info.batch_id_, poll_result))) {
                LOG_WARN("failed to poll remote batch status during restart reconciliation",
                         K(ret), K(task_id), K(task_info.batch_id_));
                tmp_ret = mark_restart_failed_with_reason_(
                    *table_manager_, *sql_proxy_, task_info, ret,
                    "Task failed after observer restart: remote batch status check failed");
              } else {
                output_file_id = poll_result.output_file_id_;
                if (!poll_result.is_terminal()) {
                  int cancel_ret = batch_file_manager.cancel_batch(task_info.batch_id_);
                  if (OB_SUCCESS != cancel_ret) {
                    LOG_WARN("failed to cancel remote batch during restart reconciliation",
                             K(cancel_ret), K(task_id), K(task_info.batch_id_));
                    tmp_ret = mark_restart_failed_with_reason_(
                        *table_manager_, *sql_proxy_, task_info, cancel_ret,
                        "Task failed after observer restart: local DDL state lost and remote batch cancel failed",
                        output_file_id);
                  } else {
                    tmp_ret = mark_restart_failed_with_reason_(
                        *table_manager_, *sql_proxy_, task_info, OB_ERR_UNEXPECTED,
                        "Task failed after observer restart: local DDL state lost and remote batch was cancelled",
                        output_file_id);
                  }
                } else if (OB_AI_BATCH_FILE_STATUS_COMPLETED == poll_result.status_) {
                  tmp_ret = mark_restart_failed_with_reason_(
                      *table_manager_, *sql_proxy_, task_info, OB_ERR_UNEXPECTED,
                      "Task failed after observer restart: remote batch already completed but local DDL state is unrecoverable",
                      output_file_id);
                } else {
                  tmp_ret = mark_restart_failed_with_reason_(
                      *table_manager_, *sql_proxy_, task_info, OB_ERR_UNEXPECTED,
                      "Task failed after observer restart: remote batch reached terminal state and local DDL state is unrecoverable",
                      output_file_id);
                }
              }
            }

            if (OB_SUCCESS != tmp_ret) {
              LOG_WARN("failed to reconcile stale running task after restart",
                       K(tmp_ret), K(task_id), K(task_info.batch_id_));
            } else {
              failed_count++;
              LOG_INFO("[BATCH-FILE] reconciled stale RUNNING task after restart",
                       K(task_id), K(task_info.batch_id_));
            }
            } // close: DDL dead → reconcile
            ret = OB_SUCCESS;
          }
        }
      }
    }
    if (OB_SUCC(ret) && failed_count > 0) {
      LOG_INFO("[BATCH-FILE] recovered stale RUNNING tasks after restart",
               K(failed_count), K(tenant_id));
    }

    // C3 path (tasks 8.1 + 8.3): scan terminal tasks not in task_map_ and archive
    // those whose DDL is no longer alive.  This handles the "restart orphan" case
    // where a task reached FINISHED/FAILED/CANCELLED before the restart but was
    // never consumed by DDL (e.g. DDL crashed before calling release_task).
    if (OB_SUCC(ret)) {
      int64_t c3_archived_count = 0;
      SMART_VAR(common::ObMySQLProxy::MySQLResult, c3_res) {
        common::sqlclient::ObMySQLResult *c3_result = nullptr;
        ObSqlString c3_sql;
        c3_sql.set_attr(ObMemAttr(MTL_ID(), "AiTermOrphan"));
        if (OB_FAIL(c3_sql.assign_fmt(
                "SELECT task_id FROM %s "
                "WHERE tenant_id = %lu AND status IN (%d, %d, %d)",
                OB_ALL_AI_BATCH_TASK_TNAME,
                tenant_id,
                static_cast<int>(OB_AI_TASK_STATUS_FINISHED),
                static_cast<int>(OB_AI_TASK_STATUS_FAILED),
                static_cast<int>(OB_AI_TASK_STATUS_CANCELLED)))) {
          LOG_WARN("failed to build C3 recovery select SQL", K(ret));
        } else if (OB_FAIL(sql_proxy_->read(c3_res, tenant_id, c3_sql.ptr()))) {
          LOG_WARN("failed to execute C3 recovery select", K(ret));
        } else if (OB_ISNULL(c3_result = c3_res.get_result())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("null result set from C3 recovery select", K(ret));
        } else {
          while (OB_SUCC(ret)) {
            if (OB_FAIL(c3_result->next())) {
              if (OB_ITER_END == ret) {
                ret = OB_SUCCESS;
              }
              break;
            }
            common::ObString task_id;
            EXTRACT_VARCHAR_FIELD_MYSQL(*c3_result, "task_id", task_id);
            if (OB_FAIL(ret)) { break; }

            // Skip tasks with a live in-memory object (actively managed post-restart)
            vector_index::ObAiAccessTask *task_obj = nullptr;
            if (OB_NOT_NULL(service_)) {
              int tmp = service_->get_task_object(task_id, task_obj);
              if (OB_SUCCESS == tmp && OB_NOT_NULL(task_obj)) {
                continue;
              }
            }

            ObAiTaskInfo task_info;
            int tmp_ret = table_manager_->query_task_status(task_id, task_info, *sql_proxy_);
            if (OB_SUCCESS != tmp_ret) {
              LOG_WARN("[BATCH-FILE] C3 recovery: failed to load task info, skipping",
                       K(tmp_ret), K(task_id));
            } else {
              bool ddl_alive = false;
              int ddl_check_ret = (task_info.ddl_task_id_ > 0)
                  ? check_ddl_alive_(tenant_id, task_info.ddl_task_id_, ddl_alive)
                  : OB_SUCCESS;
              if (OB_SUCCESS != ddl_check_ret) {
                LOG_WARN("[BATCH-FILE] C3 recovery: DDL alive check failed, skip task this round",
                         K(ddl_check_ret), K(task_id), K(task_info.ddl_task_id_));
              } else if (ddl_alive) {
                LOG_INFO("[BATCH-FILE] C3 recovery: DDL still alive, skip terminal task",
                         K(task_id), K(task_info.ddl_task_id_));
              } else {
                // DDL gone — archive to history (DB only, no local fd)
                tmp_ret = table_manager_->archive_task_to_history(
                    task_id, ObString(), ObString(), *sql_proxy_);
                if (OB_SUCCESS != tmp_ret) {
                  LOG_WARN("[BATCH-FILE] C3 recovery: failed to archive terminal orphan task",
                           K(tmp_ret), K(task_id), K(task_info.ddl_task_id_));
                } else {
                  c3_archived_count++;
                  LOG_INFO("[BATCH-FILE] C3 recovery: archived terminal orphan task",
                           K(task_id), K(task_info.ddl_task_id_), "status", task_info.status_);
                }
              }
            }
            ret = OB_SUCCESS;
          }
        }
      }
      if (OB_SUCC(ret) && c3_archived_count > 0) {
        LOG_INFO("[BATCH-FILE] C3 recovery: archived terminal orphan tasks after restart",
                 K(c3_archived_count), K(tenant_id));
      }
    }
  }
  return ret;
}

// ObSystemTablePollerFactory implementation

int ObSystemTablePollerFactory::create_poller(common::ObIAllocator &allocator,
                                               vector_index::ObAiAccessService &service,
                                               ObAiSystemTableManager &table_manager,
                                               common::ObMySQLProxy &sql_proxy,
                                               int64_t poll_interval_us,
                                               ObSystemTablePoller *&poller)
{
  int ret = OB_SUCCESS;
  poller = nullptr;

  void *buf = allocator.alloc(sizeof(ObSystemTablePoller));
  if (OB_ISNULL(buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory for ObSystemTablePoller", K(ret));
  } else {
    poller = new (buf) ObSystemTablePoller();
    if (OB_FAIL(poller->init(allocator, service, table_manager, sql_proxy, poll_interval_us))) {
      LOG_WARN("failed to init ObSystemTablePoller", K(ret));
      poller->~ObSystemTablePoller();
      allocator.free(buf);
      poller = nullptr;
    }
  }
  return ret;
}

void ObSystemTablePollerFactory::destroy_poller(common::ObIAllocator &allocator,
                                                  ObSystemTablePoller *poller)
{
  if (OB_NOT_NULL(poller)) {
    poller->~ObSystemTablePoller();
    allocator.free(poller);
  }
}

} // namespace share
} // namespace oceanbase
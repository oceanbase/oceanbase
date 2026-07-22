/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SHARE_AI_SERVICE_OB_AI_BATCH_FILE_WRITER_H_
#define OCEANBASE_SHARE_AI_SERVICE_OB_AI_BATCH_FILE_WRITER_H_

#include "lib/ob_define.h"
#include "lib/string/ob_string.h"
#include "lib/allocator/page_arena.h"
#include "share/ai_service/ob_ai_exec_struct.h"
#include "share/ai_service/ob_ai_func_provider.h"
#include "share/ai_service/ob_batch_file_jsonl_writer.h"

namespace oceanbase
{
namespace vector_index
{
class ObAiAccessService;
} // namespace vector_index

namespace share
{

/**
 * @brief RAII Writer for batch task data collection
 *
 * ObAiBatchTaskWriter wraps data collection and task creation into a single
 * RAII object. Data is written directly to TmpFile via append(), and commit()
 * synchronously registers the task in RUNNING state.
 *
 * Usage:
 *   ObAiBatchTaskWriter writer;
 *   service->open_batch_task(endpoint, cmd, ddl_task_id, writer);
 *   while (has_data) {
 *     ret = writer.append(line);
 *     if (OB_ITER_END == ret) {
 *       writer.commit(task_id);  // commit current, re-open new writer
 *       service->open_batch_task(endpoint, cmd, ddl_task_id, writer);
 *       ret = writer.append(line);  // retry same line
 *     }
 *   }
 *   if (!writer.is_empty()) writer.commit(task_id);
 */
class ObAiBatchTaskWriter
{
public:
  ObAiBatchTaskWriter();
  ~ObAiBatchTaskWriter();

  /**
   * @brief Initialize writer (called by ObAiAccessService::open_batch_task)
   * @param service Back-pointer to access service (for commit)
   * @param endpoint_info Endpoint info for the task
   * @param command_type AI command type
   * @param ddl_task_id Associated DDL task ID
   * @param dir_id TmpFileManager directory ID
   * @param tenant_id Tenant ID
   * @return OB_SUCCESS on success
   */
  int init(vector_index::ObAiAccessService *service,
           const ObAiModelEndpointInfo &endpoint_info,
           ObAiCommandType command_type,
           int64_t ddl_task_id,
           int64_t dir_id,
           uint64_t tenant_id,
           bool allow_null_on_failure = false);

  /**
   * @brief Append a line to the batch file
   * @param line Line to append
   * @return OB_SUCCESS on success,
   *         OB_ITER_END if threshold reached (line NOT written),
   *         other error codes on failure
   */
  int append(const common::ObAiBatchFileLine &line);

  /**
   * @brief Commit: register task as RUNNING, create Task object, schedule
   * @param task_id Output: generated task ID
   * @return OB_SUCCESS on success
   */
  int commit(common::ObString &task_id);

  void reset();

  bool is_empty() const { return current_line_count_ == 0; }
  bool is_committed() const { return is_committed_; }
  bool is_inited() const { return is_inited_; }

  int64_t get_line_count() const { return current_line_count_; }
  int64_t get_file_size() const { return jsonl_writer_.is_inited() ? jsonl_writer_.get_size() : 0; }
  int64_t get_jsonl_fd() const { return jsonl_writer_.is_inited() ? jsonl_writer_.get_fd() : -1; }

  TO_STRING_KV(K_(is_inited), K_(is_committed), K_(current_line_count),
               K_(ddl_task_id), K_(dir_id));

private:
  bool is_inited_;
  bool is_committed_;

  // Configuration from init()
  vector_index::ObAiAccessService *service_;
  int64_t ddl_task_id_;
  int64_t dir_id_;
  uint64_t tenant_id_;
  ObAiCommandType command_type_;

  // Pointer into caller-owned endpoint_info; caller must outlive this writer
  const ObAiModelEndpointInfo *endpoint_info_;

  bool allow_null_on_failure_;

  // File state
  int64_t current_line_count_;

  // JSONL writer (writes directly to TmpFile)
  ObBatchFileJsonlWriter jsonl_writer_;

  // Reusable allocator for per-line JSON serialization
  common::ObArenaAllocator line_alloc_;

  DISALLOW_COPY_AND_ASSIGN(ObAiBatchTaskWriter);
};

} // namespace share
} // namespace oceanbase

#endif // OCEANBASE_SHARE_AI_SERVICE_OB_AI_BATCH_FILE_WRITER_H_
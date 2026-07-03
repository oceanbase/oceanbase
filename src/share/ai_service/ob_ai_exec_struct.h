/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_SHARE_AI_SERVICE_OB_AI_EXEC_STRUCT_H_
#define OCEANBASE_SHARE_AI_SERVICE_OB_AI_EXEC_STRUCT_H_

#include "lib/ob_define.h"
#include "lib/string/ob_string.h"
#include "lib/container/ob_iarray.h"
#include "lib/allocator/ob_allocator.h"
#include "share/ai_service/ob_ai_service_struct.h"

namespace oceanbase
{
namespace share
{

// error_detail column is varchar(4096):
// {"ob_error_code":N,"model_http_code":N,"message":"..."}
constexpr int64_t OB_AI_MAX_ERROR_DETAIL_LENGTH = 4096;
constexpr int64_t OB_AI_MAX_ERROR_MESSAGE_LENGTH = 512;

// Command type for AI execution
enum ObAiCommandType
{
  OB_AI_COMMAND_INVALID = 0,
  OB_AI_COMMAND_EMBED = 1,       // Embedding command
  OB_AI_COMMAND_COMPLETE = 2,    // Completion command (future)
  OB_AI_COMMAND_RERANK = 3,      // Rerank command (future)
  OB_AI_COMMAND_MAX
};

// Service tier for AI task execution
enum ObAiServiceTier
{
  OB_AI_SERVICE_TIER_STANDARD = 0,
  OB_AI_SERVICE_TIER_BATCH    = 1,
  OB_AI_SERVICE_TIER_FLEX     = 2,
  OB_AI_SERVICE_TIER_MAX
};

// Task status for scheduling
enum ObAiTaskStatus
{
  OB_AI_TASK_STATUS_INVALID = 0,
  OB_AI_TASK_STATUS_PENDING = 3,
  OB_AI_TASK_STATUS_RUNNING = 4,
  OB_AI_TASK_STATUS_FINISHED = 5,
  OB_AI_TASK_STATUS_FAILED = 6,
  OB_AI_TASK_STATUS_CANCELLED = 7
};

// Task phase for internal state machine
enum ObAiTaskPhase
{
  OB_AI_TASK_PHASE_INIT = 0,
  OB_AI_TASK_PHASE_HTTP_SENT = 1,
  OB_AI_TASK_PHASE_HTTP_COMPLETED = 2,
  OB_AI_TASK_PHASE_PARSED = 3,
  OB_AI_TASK_PHASE_DONE = 5,
  // Batch file specific phases
  OB_AI_TASK_PHASE_FILE_UPLOADING = 11,    // Uploading file to provider
  OB_AI_TASK_PHASE_BATCH_SUBMITTING = 12,  // Submitting batch job
  OB_AI_TASK_PHASE_BATCH_POLLING = 13,     // Polling batch status
  OB_AI_TASK_PHASE_RESULT_DOWNLOADING = 14  // Downloading result file
};

// Source type for input data
enum ObAiSourceType
{
  OB_AI_SOURCE_TYPE_INVALID = 0,
  OB_AI_SOURCE_TYPE_MEMORY = 1,     // In-memory data (sync path)
  OB_AI_SOURCE_TYPE_FILE = 2,       // Temporary file (batch file path)
  OB_AI_SOURCE_TYPE_TABLE = 3,      // Direct table scan (future)
  OB_AI_SOURCE_TYPE_MAX
};

// Input item for embedding/completion
struct ObAiInputItem
{
public:
  ObAiInputItem() { reset(); }
  ~ObAiInputItem() = default;

  void reset()
  {
    logical_index_ = OB_INVALID_INDEX;
    embed_text_.reset();
    dimension_ = 0;
  }

  bool is_valid() const
  {
    return logical_index_ != OB_INVALID_INDEX && !embed_text_.empty();
  }

  TO_STRING_KV(K_(logical_index), K_(dimension), K_(embed_text));

public:
  int64_t logical_index_;      // Logical index in the overall input sequence
  ObString embed_text_;        // Text content for embedding
  int64_t dimension_;          // Expected embedding dimension
};

// Result item for embedding/completion
struct ObAiResultItem
{
public:
  ObAiResultItem() : allocator_(nullptr) { reset(); }
  explicit ObAiResultItem(common::ObIAllocator *allocator) : allocator_(allocator) { reset(); }
  ~ObAiResultItem() = default;

  void reset()
  {
    logical_index_ = OB_INVALID_INDEX;
    ret_code_ = OB_SUCCESS;
    error_message_.reset();
    embedding_vector_ = nullptr;
    vector_size_ = 0;
  }

  bool is_valid() const
  {
    return logical_index_ != OB_INVALID_INDEX;
  }

  bool is_success() const { return OB_SUCCESS == ret_code_; }

  int deep_copy_vector(common::ObIAllocator &allocator, const float *src, int64_t size);

  TO_STRING_KV(K_(logical_index), K_(ret_code), K_(error_message), K_(vector_size));

public:
  int64_t logical_index_;       // Logical index matching ObAiInputItem
  int ret_code_;                // Per-item error code
  ObString error_message_;      // Per-item error message
  float *embedding_vector_;     // Embedding result (owned by allocator)
  int64_t vector_size_;         // Vector dimension

private:
  common::ObIAllocator *allocator_;
};

// Result row for batch file processing
// Used by AiAccessService to return ordered results to DDL layer
struct ObAiResultRow
{
public:
  ObAiResultRow() { reset(); }
  ~ObAiResultRow() = default;

  void reset()
  {
    original_index_ = OB_INVALID_INDEX;
    ret_code_ = OB_SUCCESS;
    error_detail_.reset();
    command_type_ = OB_AI_COMMAND_INVALID;
    embedding_vector_ = nullptr;
    vector_dim_ = 0;
    extra_cols_ptr_ = nullptr;
    extra_cols_count_ = 0;
  }

  bool is_valid() const { return original_index_ != OB_INVALID_INDEX; }
  bool is_success() const { return OB_SUCCESS == ret_code_; }

  bool operator<(const ObAiResultRow &other) const {
    return original_index_ < other.original_index_;
  }

  TO_STRING_KV(K_(original_index), K_(ret_code), K_(command_type), K_(vector_dim), K_(extra_cols_count));

public:
  int64_t original_index_;      // From custom_id in JSONL
  int ret_code_;                // Per-row error code
  ObString error_detail_;       // JSON: {"ob_error_code":N,"model_http_code":N,"message":"..."}
  ObAiCommandType command_type_; // Command type that produced this result
  float *embedding_vector_;     // Embedding result (owned by Task allocator)
  int64_t vector_dim_;          // Vector dimension
  void *extra_cols_ptr_;        // Pointer to extra columns array (blocksstable::ObStorageDatum*)
  int64_t extra_cols_count_;    // Number of extra columns
};

typedef ObAiResultRow ObAiEmbeddingResultRow;

// Task info for main task (user-visible)
// Parsed remote_files view for runtime access.
// ObAiTaskInfo stores the raw JSON string (remote_files_), this struct
// provides typed access to individual fields after parsing.
struct ObAiRemoteFilesView
{
public:
  ObAiRemoteFilesView() { reset(); }
  ~ObAiRemoteFilesView() = default;

  void reset()
  {
    input_file_id_.reset();
    output_file_id_.reset();
    error_file_id_.reset();
  }

  bool is_empty() const
  {
    return input_file_id_.empty()
        && output_file_id_.empty() && error_file_id_.empty();
  }

  TO_STRING_KV(K_(input_file_id), K_(output_file_id), K_(error_file_id));

public:
  ObString input_file_id_;
  ObString output_file_id_;
  ObString error_file_id_;
};

struct ObAiTaskInfo
{
public:
  ObAiTaskInfo() { reset(); }
  ~ObAiTaskInfo() = default;

  void reset()
  {
    task_id_.reset();
    tenant_id_ = OB_INVALID_TENANT_ID;
    ddl_task_id_ = 0;
    model_name_.reset();
    command_type_ = OB_AI_COMMAND_INVALID;
    status_ = OB_AI_TASK_STATUS_INVALID;
    requests_handled_ = 0;
    total_requests_ = 0;
    task_create_time_ = 0;
    task_update_time_ = 0;
    batch_id_.reset();
    remote_file_ids_.reset();
    local_file_metadata_.reset();
    error_detail_.reset();
    // TmpFileManager fd metadata (in-memory, serialized to/from local_file_metadata JSON)
    jsonl_fd_ = -1;
    jsonl_size_ = 0;
    jsonl_line_count_ = 0;
    result_fd_ = -1;
    result_size_ = 0;
    result_line_count_ = 0;
    // History-only fields
    token_usage_.reset();
    provider_timeline_.reset();
  }

  bool is_valid() const
  {
    return !task_id_.empty() && tenant_id_ != OB_INVALID_TENANT_ID;
  }

  int generate_task_id(char *buf, int64_t buf_len);

  // Serialize TmpFileManager fd metadata to JSON for system table storage
  int serialize_file_metadata(char *buf, int64_t buf_len, int64_t &pos) const;
  // Deserialize TmpFileManager fd metadata from JSON
  int deserialize_file_metadata(const ObString &json);

  // Build remote_files JSON string from individual fields (writes into allocator)
  static int build_remote_files_json(common::ObIAllocator &alloc,
                                     const ObString &input_file_id,
                                     const ObString &output_file_id,
                                     const ObString &error_file_id,
                                     ObString &json_out);
  // Parse remote_files JSON string into structured view
  static int parse_remote_files_json(const ObString &json, ObAiRemoteFilesView &out);

  TO_STRING_KV(K_(task_id), K_(tenant_id), K_(ddl_task_id),
               K_(model_name),
               K_(command_type), K_(status),
               K_(requests_handled), K_(total_requests),
               K_(task_create_time), K_(task_update_time),
               K_(batch_id),
               K_(remote_file_ids),
               K_(jsonl_fd), K_(jsonl_size), K_(jsonl_line_count),
               K_(result_fd), K_(result_size), K_(result_line_count),
               K_(error_detail), K_(token_usage), K_(provider_timeline));

public:
  ObString task_id_;               // Unique task identifier (UUID)
  uint64_t tenant_id_;             // Tenant ID
  int64_t ddl_task_id_;            // Associated DDL task ID
  ObString model_name_;            // AI model name
  ObAiCommandType command_type_;   // Command type
  ObAiTaskStatus status_;          // Task status
  int64_t requests_handled_;       // Number of processed requests
  int64_t total_requests_;         // Total number of requests
  int64_t task_create_time_;       // Task creation timestamp (us)
  int64_t task_update_time_;       // Last update timestamp (us)
  ObString batch_id_;              // Provider-side Batch task ID
  ObString remote_file_ids_;       // JSON: {"input_file_id":"...","output_file_id":"...","error_file_id":"..."}
  ObString local_file_metadata_;   // JSON: fd/size/line_count metadata for system table
  // TmpFileManager fd metadata (in-memory, serialized to/from local_file_metadata JSON)
  int64_t jsonl_fd_;               // TmpFileManager fd for JSONL input data
  int64_t jsonl_size_;             // JSONL data size
  int64_t jsonl_line_count_;       // JSONL line count
  int64_t result_fd_;              // TmpFileManager fd for result data
  int64_t result_size_;            // Result data size
  int64_t result_line_count_;      // Result line count
  ObString error_detail_;          // JSON: {"ob_error_code":N,"model_http_code":N,"message":"..."}
  // History-only fields
  ObString token_usage_;           // JSON: {"completion_tokens":N,"prompt_tokens":N,"total_tokens":N}
  ObString provider_timeline_;     // JSON: {"created_at":N,...} provider-side timestamps
};

// Utility functions for enum conversion
class ObAiExecUtils
{
public:
  static const char *get_command_type_str(ObAiCommandType type);
  static const char *get_ai_service_tier_str(ObAiServiceTier tier);
  static const char *get_task_status_str(ObAiTaskStatus status);
  static const char *get_task_phase_str(ObAiTaskPhase phase);
  static const char *get_source_type_str(ObAiSourceType type);

  static ObAiCommandType str_to_command_type(const ObString &str);
  static ObAiServiceTier str_to_ai_service_tier(const ObString &str);

  static int build_error_detail_json(int ob_error_code,
                                     int64_t model_http_code,
                                     const common::ObString &message,
                                     char *buf, int64_t buf_len);

private:
  ObAiExecUtils() = delete;
  DISALLOW_COPY_AND_ASSIGN(ObAiExecUtils);
};

} // namespace share
} // namespace oceanbase

#endif // OCEANBASE_SHARE_AI_SERVICE_OB_AI_EXEC_STRUCT_H_
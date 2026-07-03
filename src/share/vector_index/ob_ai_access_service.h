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

#ifndef OCEANBASE_SHARE_VECTOR_INDEX_OB_AI_ACCESS_SERVICE_H_
#define OCEANBASE_SHARE_VECTOR_INDEX_OB_AI_ACCESS_SERVICE_H_

#include "lib/ob_define.h"
#include "lib/string/ob_string.h"
#include "lib/container/ob_se_array.h"
#include "lib/container/ob_iarray.h"
#include "lib/container/ob_array.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/lock/ob_spin_lock.h"
#include "lib/lock/ob_thread_cond.h"
#include "lib/lock/ob_latch.h"
#include "lib/allocator/ob_allocator.h"
#include "lib/allocator/ob_malloc.h"
#include "share/ai_service/ob_ai_exec_struct.h"
#include "share/ai_service/ob_ai_service_struct.h"
#include "share/ai_service/ob_ai_system_table_manager.h"
#include "share/ai_service/ob_ai_batch_file_manager.h"
#include "share/ai_service/ob_ai_batch_file_writer.h"
#include "share/ai_service/ob_ai_func_provider.h"
#include "share/ob_occam_thread_pool.h"
#include "share/ob_occam_timer.h"

namespace oceanbase
{
// Forward declarations for types in share namespace
namespace share
{
class ObSystemTablePoller;
} // namespace share

namespace vector_index
{

// Forward declarations
class ObAiTaskScheduler;
class ObAiAccessService;

// ============================================================================
// Section 1: Error handling from ob_ai_retry_manager.h
// ============================================================================

// Error category for AI service operations
enum ObAiErrorCategory
{
  OB_AI_ERROR_CATEGORY_INVALID = 0,
  OB_AI_ERROR_CATEGORY_NETWORK = 1,        // Network errors (timeout, connection failed)
  OB_AI_ERROR_CATEGORY_RATE_LIMIT = 2,     // Rate limit errors (429)
  OB_AI_ERROR_CATEGORY_INVALID_REQUEST = 3, // Invalid request (400, 401, 403)
  OB_AI_ERROR_CATEGORY_SERVER_ERROR = 4,   // Server errors (500, 502, 503, 504)
  OB_AI_ERROR_CATEGORY_UNKNOWN = 5
};

// Error context for retry decision
struct ObAiErrorContext
{
public:
  ObAiErrorContext() { reset(); }
  ~ObAiErrorContext() = default;

  void reset()
  {
    http_status_code_ = 0;
    error_category_ = OB_AI_ERROR_CATEGORY_INVALID;
    internal_error_code_ = OB_SUCCESS;
    error_message_.reset();
    provider_.reset();
    retry_count_ = 0;
  }

  bool is_valid() const
  {
    return error_category_ != OB_AI_ERROR_CATEGORY_INVALID;
  }

  TO_STRING_KV(K_(http_status_code), K_(error_category), K_(internal_error_code),
               K_(error_message), K_(provider), K_(retry_count));

public:
  int64_t http_status_code_;        // HTTP status code (0 for non-HTTP errors)
  ObAiErrorCategory error_category_; // Error category
  int internal_error_code_;          // OceanBase internal error code
  common::ObString error_message_;   // Error message from provider
  common::ObString provider_;        // Provider name (OPENAI, DASHSCOPE, etc.)
  int64_t retry_count_;              // Current retry count

private:
  DISALLOW_COPY_AND_ASSIGN(ObAiErrorContext);
};

// Abstract interface for error mapping from different providers
class ObAiErrorMapper
{
public:
  ObAiErrorMapper() {}
  virtual ~ObAiErrorMapper() = default;

  // Map HTTP status code and response body to error category
  virtual int map_error(int64_t http_status,
                        const common::ObString &response_body,
                        ObAiErrorContext &error_ctx) const = 0;

  // Check if the error is retryable for this provider
  virtual bool is_retryable(const ObAiErrorContext &error_ctx) const = 0;

protected:
  // Shared implementation for map_error — handles all common HTTP status codes
  int map_error_impl_(int64_t http_status,
                      const common::ObString &response_body,
                      const char *provider_name,
                      ObAiErrorContext &error_ctx) const;

  // Shared implementation for is_retryable
  // retryable_on_413: if true, 413 (payload too large) triggers retry with smaller batch
  bool is_retryable_impl_(const ObAiErrorContext &error_ctx,
                           bool retryable_on_413) const;

private:
  DISALLOW_COPY_AND_ASSIGN(ObAiErrorMapper);
};

// OpenAI-specific error mapper
class ObAiOpenAiErrorMapper : public ObAiErrorMapper
{
public:
  ObAiOpenAiErrorMapper() {}
  virtual ~ObAiOpenAiErrorMapper() = default;

  virtual int map_error(int64_t http_status,
                        const common::ObString &response_body,
                        ObAiErrorContext &error_ctx) const override;

  virtual bool is_retryable(const ObAiErrorContext &error_ctx) const override;

private:
  DISALLOW_COPY_AND_ASSIGN(ObAiOpenAiErrorMapper);
};

// DashScope-specific error mapper
class ObAiDashScopeErrorMapper : public ObAiErrorMapper
{
public:
  ObAiDashScopeErrorMapper() {}
  virtual ~ObAiDashScopeErrorMapper() = default;

  virtual int map_error(int64_t http_status,
                        const common::ObString &response_body,
                        ObAiErrorContext &error_ctx) const override;

  virtual bool is_retryable(const ObAiErrorContext &error_ctx) const override;

private:
  DISALLOW_COPY_AND_ASSIGN(ObAiDashScopeErrorMapper);
};

// Retry manager for AI service operations
class ObAiRetryManager
{
public:
  static const int64_t RETRY_BASE_INTERVAL_US = 1000 * 1000;      // 1 second
  static const int64_t RETRY_MAX_INTERVAL_US = 60 * 1000 * 1000;  // 60 seconds
  static const int64_t RETRY_MULTIPLIER = 2;
  static const int64_t MAX_RETRY_COUNT = 10;

  ObAiRetryManager();
  ~ObAiRetryManager();

  int init();
  void reset();
  bool is_retryable_error(const ObAiErrorContext &error_ctx) const;
  int64_t calculate_retry_interval(int64_t retry_count) const;
  static int64_t calculate_retry_interval(int64_t retry_count,
                                           int64_t base_interval_us,
                                           int64_t max_interval_us,
                                           int64_t multiplier);
  const ObAiErrorMapper* get_error_mapper(const common::ObString &provider) const;
  bool is_inited() const { return is_inited_; }

  TO_STRING_KV(K_(is_inited));

private:
  bool is_inited_;
  ObAiOpenAiErrorMapper openai_mapper_;
  ObAiDashScopeErrorMapper dashscope_mapper_;

  DISALLOW_COPY_AND_ASSIGN(ObAiRetryManager);
};

// Utility class for retry-related operations
class ObAiRetryUtils
{
public:
  static int map_http_status_to_internal_error(int64_t http_status);

private:
  ObAiRetryUtils() = delete;
  DISALLOW_COPY_AND_ASSIGN(ObAiRetryUtils);
};

// ============================================================================
// Section 2: Batch processing from ob_ai_batch_processor.h
// ============================================================================

static const int64_t OB_MAX_MODEL_NAME_LENGTH = 256;

// Dimension key for batch size management
struct ObAiDimensionKey
{
public:
  ObAiDimensionKey() { reset(); }
  explicit ObAiDimensionKey(const common::ObString &model_name,
                             const common::ObString &provider,
                             share::ObAiAccessMode access_mode)
    : model_name_(model_name),
      provider_(provider),
      ai_execution_mode_(access_mode)
  {}
  ~ObAiDimensionKey() = default;

  void reset()
  {
    model_name_.reset();
    provider_.reset();
    ai_execution_mode_ = share::OB_AI_ACCESS_MODE_INVALID;
  }

  bool is_valid() const
  {
    return !model_name_.empty() && !provider_.empty() &&
           ai_execution_mode_ != share::OB_AI_ACCESS_MODE_INVALID;
  }

  uint64_t hash() const
  {
    uint64_t hash_val = 0;
    hash_val = common::murmurhash(model_name_.ptr(), model_name_.length(), hash_val);
    hash_val = common::murmurhash(provider_.ptr(), provider_.length(), hash_val);
    hash_val = common::murmurhash(&ai_execution_mode_, sizeof(ai_execution_mode_), hash_val);
    return hash_val;
  }

  bool operator==(const ObAiDimensionKey &other) const
  {
    return model_name_ == other.model_name_ &&
           provider_ == other.provider_ &&
           ai_execution_mode_ == other.ai_execution_mode_;
  }

  TO_STRING_KV(K_(model_name), K_(provider), K_(ai_execution_mode));

public:
  common::ObString model_name_;
  common::ObString provider_;
  share::ObAiAccessMode ai_execution_mode_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObAiDimensionKey);
};

// Batch size state for a single endpoint dimension
struct ObAiBatchSizeState
{
public:
  ObAiBatchSizeState() { reset(); }
  ~ObAiBatchSizeState() = default;

  void reset()
  {
    current_batch_size_ = 0;
    min_batch_size_ = 1;
    max_batch_size_ = 0;
    successful_requests_ = 0;
    failed_requests_ = 0;
    consecutive_failures_ = 0;
    last_adjust_time_us_ = 0;
  }

  bool is_valid() const
  {
    return current_batch_size_ > 0 && max_batch_size_ > 0;
  }

  TO_STRING_KV(K_(current_batch_size), K_(min_batch_size), K_(max_batch_size),
               K_(successful_requests), K_(failed_requests), K_(consecutive_failures));

public:
  int64_t current_batch_size_;
  int64_t min_batch_size_;
  int64_t max_batch_size_;
  int64_t successful_requests_;
  int64_t failed_requests_;
  int64_t consecutive_failures_;
  int64_t last_adjust_time_us_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObAiBatchSizeState);
};

// Adaptive batch processor for AI service
class ObAdaptiveBatchProcessor
{
public:
  static const int64_t DEFAULT_MIN_BATCH_SIZE = 1;
  static const int64_t DEFAULT_MAX_BATCH_SIZE = 2048;
  static const int64_t DEFAULT_INITIAL_BATCH_SIZE = 10;
  static const int64_t SUCCESS_THRESHOLD_FOR_INCREASE = 3;
  static const int64_t MAX_CONSECUTIVE_FAILURES = 3;
  static const int64_t BATCH_SIZE_INCREASE_STEP = 1;
  static const int64_t BATCH_SIZE_DECREASE_FACTOR = 2;

  ObAdaptiveBatchProcessor();
  ~ObAdaptiveBatchProcessor();

  int init(common::ObIAllocator &allocator);
  void reset();
  int64_t get_suggested_batch_size(const ObAiDimensionKey &key,
                                    int64_t default_batch_size = DEFAULT_INITIAL_BATCH_SIZE);
  int report_success(const ObAiDimensionKey &key);
  int report_failure(const ObAiDimensionKey &key, bool is_batch_size_related = true);
  int set_max_batch_size(const ObAiDimensionKey &key, int64_t max_batch_size);
  int get_batch_size_state(const ObAiDimensionKey &key, ObAiBatchSizeState &state) const;
  bool is_inited() const { return is_inited_; }

  TO_STRING_KV(K_(is_inited));

private:
  struct BatchSizeEntry
  {
    ObAiBatchSizeState state_;
    char model_name_buf_[OB_MAX_MODEL_NAME_LENGTH];
    char provider_buf_[64];
    ObAiDimensionKey key_;

    BatchSizeEntry() { reset(); }
    void reset()
    {
      state_.reset();
      MEMSET(model_name_buf_, 0, sizeof(model_name_buf_));
      MEMSET(provider_buf_, 0, sizeof(provider_buf_));
      key_.reset();
    }
    TO_STRING_KV(K_(state), K_(key));
  };

  BatchSizeEntry* find_entry_by_key_(const ObAiDimensionKey &key) const;
  int get_or_create_entry_(const ObAiDimensionKey &key,
                            int64_t default_batch_size,
                            BatchSizeEntry *&entry);
  void try_increase_batch_size_(BatchSizeEntry &entry);
  void try_decrease_batch_size_(BatchSizeEntry &entry, bool is_batch_size_related);

private:
  bool is_inited_;
  common::ObIAllocator *allocator_;
  common::ObSpinLock lock_;
  common::ObArray<BatchSizeEntry*> entries_;

  DISALLOW_COPY_AND_ASSIGN(ObAdaptiveBatchProcessor);
};


// ============================================================================
// Section 3: Task scheduler from ob_ai_task_scheduler.h
// ============================================================================

// Forward declarations
class ObAiTaskScheduler;

// Task type for scheduling
enum ObAiSchedulableTaskType
{
  OB_AI_SCHEDULABLE_TASK_TYPE_INVALID = 0,
  OB_AI_SCHEDULABLE_TASK_TYPE_EXECUTION_TASK = 1,
  OB_AI_SCHEDULABLE_TASK_TYPE_TABLE_POLLER = 2,
  OB_AI_SCHEDULABLE_TASK_TYPE_MAX
};

// Task priority levels
enum ObAiTaskPriority
{
  OB_AI_TASK_PRIORITY_INVALID = 0,
  OB_AI_TASK_PRIORITY_HIGH = 1,
  OB_AI_TASK_PRIORITY_NORMAL = 2,
  OB_AI_TASK_PRIORITY_LOW = 3,
  OB_AI_TASK_PRIORITY_MAX
};

// Abstract base class for schedulable tasks
class ObAiSchedulableTask
{
public:
  ObAiSchedulableTask()
    : state_(share::OB_AI_TASK_STATUS_PENDING),
      priority_(OB_AI_TASK_PRIORITY_NORMAL),
      task_type_(OB_AI_SCHEDULABLE_TASK_TYPE_INVALID),
      is_inited_(false),
      scheduler_(nullptr)
  {}

  virtual ~ObAiSchedulableTask() = default;

  virtual int init(common::ObIAllocator &allocator,
                   ObAiTaskPriority priority,
                   ObAiSchedulableTaskType task_type);

  virtual void reset()
  {
    state_ = share::OB_AI_TASK_STATUS_PENDING;
    priority_ = OB_AI_TASK_PRIORITY_NORMAL;
    task_type_ = OB_AI_SCHEDULABLE_TASK_TYPE_INVALID;
    is_inited_ = false;
    scheduler_ = nullptr;
  }

  share::ObAiTaskStatus get_state() const { return ATOMIC_LOAD(&state_); }
  void set_state(share::ObAiTaskStatus state) { ATOMIC_STORE(&state_, state); }
  ObAiTaskPriority get_priority() const { return priority_; }
  ObAiSchedulableTaskType get_task_type() const { return task_type_; }
  bool is_inited() const { return is_inited_; }
  void set_scheduler(ObAiTaskScheduler *scheduler) { scheduler_ = scheduler; }

  virtual int do_work() = 0;
  virtual bool need_reschedule() const { return false; }
  virtual int64_t get_reschedule_delay_us() const { return 0; }

  virtual int on_cancelled()
  {
    set_state(share::OB_AI_TASK_STATUS_CANCELLED);
    return OB_SUCCESS;
  }

  virtual int on_finished()
  {
    set_state(share::OB_AI_TASK_STATUS_FINISHED);
    return OB_SUCCESS;
  }

  virtual int on_failed(int error_code)
  {
    UNUSED(error_code);
    set_state(share::OB_AI_TASK_STATUS_FAILED);
    return OB_SUCCESS;
  }

  bool is_running_state() const
  {
    return get_state() == share::OB_AI_TASK_STATUS_RUNNING;
  }

  bool is_terminal_state() const
  {
    const share::ObAiTaskStatus s = get_state();
    return s == share::OB_AI_TASK_STATUS_FINISHED ||
           s == share::OB_AI_TASK_STATUS_FAILED ||
           s == share::OB_AI_TASK_STATUS_CANCELLED;
  }

  bool can_execute() const
  {
    const share::ObAiTaskStatus s = get_state();
    return s == share::OB_AI_TASK_STATUS_PENDING ||
           s == share::OB_AI_TASK_STATUS_RUNNING;
  }

  TO_STRING_KV(K_(state), K_(priority), K_(task_type), K_(is_inited));

protected:
  share::ObAiTaskStatus state_;
  ObAiTaskPriority priority_;
  ObAiSchedulableTaskType task_type_;
  bool is_inited_;
  ObAiTaskScheduler *scheduler_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObAiSchedulableTask);
};

// Task scheduler using ObOccamThreadPool and ObOccamTimer
class ObAiTaskScheduler
{
public:
  ObAiTaskScheduler();
  ~ObAiTaskScheduler();

  int init(int64_t thread_num, int64_t queue_size_pow2 = 10);
  int start();
  void stop();
  void wait();
  void destroy();

  int schedule_task(ObAiSchedulableTask &task);
  int schedule_after(ObAiSchedulableTask &task, int64_t delay_us);

  bool is_inited() const { return is_inited_; }
  bool is_running() const { return is_running_; }
  int64_t get_running_task_count() const;

public:
  static const int64_t DEFAULT_THREAD_NUM = 4;
  static const int64_t DEFAULT_QUEUE_SIZE_POW2 = 10;
  static const int64_t TIMER_PRECISION_US = 100000;

private:
  common::occam::TASK_PRIORITY map_priority_(ObAiTaskPriority priority) const;
  bool execute_task_(ObAiSchedulableTask *task);

  class TaskWrapper
  {
  public:
    TaskWrapper(ObAiSchedulableTask *task, ObAiTaskScheduler *scheduler)
      : task_(task), scheduler_(scheduler)
    {}

    bool operator()();

  private:
    ObAiSchedulableTask *task_;
    ObAiTaskScheduler *scheduler_;
  };

private:
  bool is_inited_;
  bool is_running_;
  common::ObOccamThreadPool thread_pool_;
  common::ObOccamTimer timer_;
  common::ObSpinLock lock_;

  DISALLOW_COPY_AND_ASSIGN(ObAiTaskScheduler);
};

// Utility functions for enum conversion
class ObAiSchedulerUtils
{
public:
  static const char *get_task_type_str(ObAiSchedulableTaskType type);
  static const char *get_priority_str(ObAiTaskPriority priority);

private:
  ObAiSchedulerUtils() = delete;
  DISALLOW_COPY_AND_ASSIGN(ObAiSchedulerUtils);
};

// ============================================================================
// Section 4: Execution task from ob_ai_execution_task.h
// ============================================================================

// Phase transition validation helper
class ObAiAccessTaskPhaseManager
{
public:
  static bool is_valid_transition(share::ObAiTaskPhase from_phase, share::ObAiTaskPhase to_phase);
  static const char* get_phase_str(share::ObAiTaskPhase phase);
  static share::ObAiTaskStatus map_phase_to_status(share::ObAiTaskPhase phase, int result_code);

private:
  static const share::ObAiTaskPhase VALID_TRANSITIONS_FROM_INIT[];
  static const share::ObAiTaskPhase VALID_TRANSITIONS_FROM_HTTP_SENT[];
  static const share::ObAiTaskPhase VALID_TRANSITIONS_FROM_HTTP_COMPLETED[];
  static const share::ObAiTaskPhase VALID_TRANSITIONS_FROM_PARSED[];
  static const share::ObAiTaskPhase VALID_TRANSITIONS_FROM_DONE[];
};

// HTTP response data holder
struct ObAiHttpResponseData
{
public:
  ObAiHttpResponseData(common::ObIAllocator &allocator)
    : allocator_(allocator), data_(nullptr), size_(0), http_status_code_(0)
  {}

  ~ObAiHttpResponseData() { reset(); }

  void reset()
  {
    if (OB_NOT_NULL(data_)) {
      allocator_.free(data_);
      data_ = nullptr;
    }
    size_ = 0;
    http_status_code_ = 0;
  }

  int allocate(int64_t size);
  int append_data(const char *src, int64_t len);

  common::ObIAllocator &allocator_;
  char *data_;
  int64_t size_;
  int64_t http_status_code_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObAiHttpResponseData);
};

// AI Execution Task
class ObAiAccessTask : public ObAiSchedulableTask
{
public:
  static const int64_t DEFAULT_HTTP_TIMEOUT_US = 60 * 1000 * 1000;
  static const int64_t DEFAULT_BATCH_SIZE = 10;
  static const int64_t MAX_RESPONSE_SIZE = 100 * 1024 * 1024;
  ObAiAccessTask();
  virtual ~ObAiAccessTask();

  // Guard override: this base-class init is never the right one for ObAiAccessTask.
  // Body is in the .cpp so we don't depend on the caller's USING_LOG_PREFIX from a header.
  virtual int init(common::ObIAllocator &allocator,
                   ObAiTaskPriority priority,
                   ObAiSchedulableTaskType task_type) override;

  int init(common::ObIAllocator &allocator,
           share::ObAiAccessMode access_mode,
           share::ObAiCommandType command_type,
           int64_t total_count,
           const share::ObAiModelEndpointInfo &endpoint_info,
           const common::ObString &task_id,
           share::ObAiSystemTableManager *table_manager = nullptr,
           common::ObISQLClient *sql_client = nullptr,
           bool allow_null_on_failure = false);

  // Get next batch of results from result file (streaming read with parse_offset_)
  int get_next_result(int64_t batch_size,
                      common::ObIArray<share::ObAiResultRow> &results,
                      bool &has_more);

  virtual void reset() override;
  virtual int do_work() override;
  virtual bool need_reschedule() const override;
  virtual int64_t get_reschedule_delay_us() const override;

  share::ObAiTaskPhase get_phase() const { return ATOMIC_LOAD(&phase_); }
  share::ObAiAccessMode get_ai_execution_mode() const { return ai_execution_mode_; }
  share::ObAiCommandType get_command_type() const { return command_type_; }
  bool is_all_processed() const { return current_offset_ >= total_count_; }
  int64_t get_processed_count() const { return current_offset_; }
  int64_t get_total_count() const { return total_count_; }
  int get_last_error_code() const { return last_error_ctx_.internal_error_code_; }
  int64_t get_terminal_ts() const { return terminal_ts_; }
  const common::ObString &get_task_id() const { return task_id_; }
  int64_t get_accumulated_prompt_tokens() const { return accumulated_prompt_tokens_; }
  int64_t get_accumulated_completion_tokens() const { return accumulated_completion_tokens_; }
  int64_t get_accumulated_total_tokens() const { return accumulated_total_tokens_; }
  int64_t get_model_wait_time_us() const
  {
    return (terminal_ts_ > 0 && batch_submit_time_us_ > 0)
           ? terminal_ts_ - batch_submit_time_us_ : 0;
  }
  bool is_archived() const { return ATOMIC_LOAD(&is_archived_); }
  bool set_archived() { return ATOMIC_BCAS(&is_archived_, false, true); }
  void pin() { ATOMIC_AAF(&pin_count_, 1); }
  int64_t unpin() { return ATOMIC_SAF(&pin_count_, 1); }
  int64_t get_pin_count() const { return ATOMIC_LOAD(&pin_count_); }

  // Signal cancel without HTTP: set cancel_requested_ flag only.
  // Safe to call from any thread (including DAG Worker).
  void signal_cancel() { ATOMIC_STORE(&cancel_requested_, true); }
  bool is_cancel_requested() const { return ATOMIC_LOAD(&cancel_requested_); }

  // Cancel this task: calls remote batch cancel API (if batch_id exists) and sets cancel flag.
  // Non-blocking. Must only be called from Poller thread (makes HTTP calls).
  int initiate_cancel();

  // Abandon flag: set by DDL layer when it exits before task completes.
  // Poller will detect this and perform async archive + destroy.
  bool is_abandoned() const { return ATOMIC_LOAD(&abandoned_); }
  void set_abandoned() { ATOMIC_STORE(&abandoned_, true); }

  TO_STRING_KV(K_(phase), K_(is_inited), K_(current_offset), K_(total_count),
               K_(batch_size), K_(http_timeout_us), K_(ai_execution_mode), K_(reschedule_delay_us));

private:
  struct RemoteAssetView
  {
    common::ObString input_file_id_;
    common::ObString batch_id_;
    common::ObString output_file_id_;
    common::ObString error_file_id_;
  };

  struct LocalMaterializationView
  {
    int64_t jsonl_fd_;
    int64_t jsonl_size_;
    int64_t jsonl_line_count_;
    int64_t result_fd_;
    int64_t result_size_;
    int64_t result_line_count_;
  };

  int set_phase_(share::ObAiTaskPhase new_phase);
  void log_phase_transition_(share::ObAiTaskPhase from_phase, share::ObAiTaskPhase to_phase);

  int handle_done_phase_();
  int advance_current_mode_();
  int advance_batch_file_mode_();

  int complete_terminal_(share::ObAiTaskStatus status, int error_code_for_table,
                         const common::ObString &error_msg);
  int complete_with_error_(int error_code, const common::ObString &error_msg);
  int complete_with_degraded_finish_(int error_code, const common::ObString &error_msg);
  int complete_with_cancel_();
  int complete_successfully_();
  int ensure_batch_file_manager_();
  int transition_to_phase_(share::ObAiTaskPhase phase, const char *phase_name);
  int update_system_task_status_(share::ObAiTaskStatus status,
                                 int64_t progress,
                                 int error_code,
                                 const common::ObString &error_msg);
  int sync_running_task_state_(int64_t progress,
                               const char *action_desc,
                               bool set_running_state = false);
  int persist_task_file_metadata_(int64_t result_fd,
                                  int64_t result_size,
                                  int64_t result_line_count);
  int transition_to_terminal_state_(share::ObAiTaskStatus status);
  int cleanup_jsonl_tmp_file_();

  int handle_batch_file_init_phase_();
  int handle_file_uploading_phase_();
  int handle_batch_submitting_phase_();
  int handle_batch_polling_phase_();
  int handle_result_downloading_phase_();

  int upload_batch_file_();
  int submit_batch_job_();
  int poll_batch_status_();
  int download_result_file_();
  int get_batch_submit_spec_(common::ObString &endpoint,
                             common::ObString &completion_window) const;
  int process_result_line_(const share::ObAiBatchLineResult &line_result,
                           share::ObAiResultRow &row);
  RemoteAssetView build_remote_asset_view_() const;
  LocalMaterializationView build_local_materialization_view_() const;

  bool is_batch_file_mode_() const { return share::OB_AI_ACCESS_MODE_BATCH_FILE == ai_execution_mode_; }
  void parse_and_accumulate_tokens_(const common::ObString &response_body);
  void cleanup_tmp_files_();  // Clean up TmpFileManager fds on terminal state

  bool is_retryable_error_(int error_code) const;
  int64_t calculate_retry_delay_us_(int error_code) const;

private:
  share::ObAiAccessMode ai_execution_mode_;
  share::ObAiCommandType command_type_;
  int64_t http_timeout_us_;
  int64_t batch_size_;
  common::ObAIFuncBase *provider_;

  share::ObAiTaskPhase phase_;

  share::ObAiSystemTableManager *table_manager_;
  common::ObISQLClient *sql_client_;

  common::ObString task_id_;

  common::ObString batch_file_url_;  // URL for BatchFile API
  common::ObString api_key_;
  common::ObString model_name_;
  common::ObString provider_name_;

  int64_t current_offset_;
  int64_t total_count_;

  ObAiErrorContext last_error_ctx_;

  ObAiHttpResponseData *http_response_;
  common::ObArenaAllocator local_allocator_;

  ObAiAccessService *service_;

  share::ObAiBatchFileManager *batch_file_manager_;
  common::ObString current_file_id_;
  common::ObString current_batch_id_;
  common::ObString output_file_id_;
  common::ObString error_file_id_;

  // TmpFileManager-based storage (replaces physical file paths)
  int64_t jsonl_fd_;            // TmpFileManager fd for JSONL input data
  int64_t jsonl_size_;          // JSONL data size
  int64_t jsonl_line_count_;    // JSONL line count
  int64_t result_fd_;           // TmpFileManager fd for downloaded result data
  int64_t result_size_;         // Result data size
  int64_t result_line_count_;   // Result line count
  int64_t last_poll_time_us_;
  share::ObAiBatchFileStatus batch_status_;

  // Streaming result read members (TmpFileManager-based)
  share::ObBatchFileJsonlIterator *result_iter_;  // Iterator for streaming result reads
  int64_t parse_offset_;  // Tracks how many lines have been read from result file

  // Token usage accumulators (summed during get_next_result)
  int64_t accumulated_prompt_tokens_;
  int64_t accumulated_completion_tokens_;
  int64_t accumulated_total_tokens_;

  // Reschedule delay for polling/retry phases
  int64_t reschedule_delay_us_;

  // Retry count for current phase (reset on phase transition)
  int64_t retry_count_;

  // Timestamp when task entered terminal state (0 = not terminal yet)
  int64_t terminal_ts_;

  int64_t batch_submit_time_us_;              // Timestamp when batch was submitted (for model_wait_time)

  // Set to true after the task has been successfully archived to history table.
  // cleanup_terminal_tasks_() will not free this task until is_archived_ is true.
  bool is_archived_;

  // Temporary pins held by callers that need to use the task outside task_map_lock_.
  // cleanup_terminal_tasks_() must not free a pinned task.
  int64_t pin_count_;

  // Set by DDL layer when the DDL exits before this task completes.
  // Causes do_work() to stop processing and transition to CANCELLED state.
  volatile bool cancel_requested_;

  // Set by DDL layer (via abandon_task()) when it exits before this task finishes.
  // Poller will detect abandoned_ == true and perform async archive + destroy.
  volatile bool abandoned_;

  // When true, exhausted retries degrade to FINISHED+NULL instead of FAILED.
  bool allow_null_on_failure_;

  // Number of rows submitted to the batch job; used for D5 missing-row detection.
  int64_t submitted_count_;

  friend class ObAiAccessService;
  DISALLOW_COPY_AND_ASSIGN(ObAiAccessTask);
};


// ============================================================================
// Section 5: Execution service from ob_ai_execution_service.h
// ============================================================================

// Synchronous execution context for waiting task completion
struct ObAiSyncExecutionContext
{
public:
  ObAiSyncExecutionContext()
    : is_completed_(false),
      error_code_(OB_SUCCESS),
      cond_(),
      lock_(common::ObLatchIds::OB_EMBEDDING_TASK_HANDLER_SPIN_LOCK)
  {}

  void reset()
  {
    is_completed_ = false;
    error_code_ = OB_SUCCESS;
  }

  bool is_completed() const { return is_completed_; }
  int get_error_code() const { return error_code_; }

  TO_STRING_KV(K_(is_completed), K_(error_code));

public:
  bool is_completed_;
  int error_code_;
  common::ObThreadCond cond_;
  mutable common::ObSpinLock lock_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObAiSyncExecutionContext);
};

// Main service for AI execution in OceanBase
class ObAiAccessService
{
public:
  static const int64_t DEFAULT_SCHEDULER_THREAD_NUM = 4;
  static const int64_t DEFAULT_SCHEDULER_QUEUE_SIZE_POW2 = 12;

  ObAiAccessService();
  ~ObAiAccessService();

  int init(common::ObIAllocator &allocator,
           uint64_t tenant_id,
           int64_t thread_num = DEFAULT_SCHEDULER_THREAD_NUM);

  int start();
  void stop();
  void wait();
  void destroy();

  int query_task_status(const common::ObString &task_id,
                        share::ObAiTaskInfo &task_info);

  /**
   * @brief Release a FINISHED task after DDL has consumed all results.
   * Synchronously archives task to history table, removes from task_map_,
   * closes result_fd_, and destroys the task object.
   * Returns OB_SUCCESS if task not found (already released).
   * Fails fast on archive error — caller (DDL) should propagate the error.
   */
  int release_task(const common::ObString &task_id);

  /**
   * @brief Abandon a task when DDL exits before consuming results.
   * Terminal tasks (FINISHED/FAILED/CANCELLED): synchronously archive + destroy.
   * RUNNING tasks: set abandoned_ flag; Poller handles async cancel + archive.
   * Returns OB_SUCCESS if task not found (already released/archived).
   */
  int abandon_task(const common::ObString &task_id);

  /**
   * @brief Open a new batch task writer
   * @param endpoint_info Endpoint info for the task
   * @param command_type AI command type
   * @param ddl_task_id Associated DDL task ID
   * @param writer Output: initialized writer
   * @param allow_null_on_failure If true, exhausted retries degrade to FINISHED+NULL
   * @return OB_SUCCESS on success
   */
  int open_batch_task(const share::ObAiModelEndpointInfo &endpoint_info,
                      share::ObAiCommandType command_type,
                      int64_t ddl_task_id,
                      share::ObAiBatchTaskWriter &writer,
                      bool allow_null_on_failure = false);

  // Get next batch of results (DDL layer calls this after task is FINISHED)
  int get_next_results(const common::ObString &task_id,
                       int64_t batch_size,
                       common::ObIArray<share::ObAiResultRow> &results,
                       bool &has_more);

private:
  // Execute an HTTP request with curl (stateless, handle created/destroyed per call)
  int http_execute_(common::ObIAllocator &allocator,
                    const common::ObString &url,
                    const common::ObIArray<common::ObString> &headers,
                    const char *body,
                    int64_t body_len,
                    int64_t timeout_us,
                    ObAiHttpResponseData &response);

  static size_t curl_write_callback_(void *contents, size_t size, size_t nmemb, void *userp);
  static size_t curl_header_callback_(void *contents, size_t size, size_t nmemb, void *userp);

public:

  // ========== Internal Methods for Poller/Task/Writer ==========

  // Called by ObAiBatchTaskWriter::commit() to finalize and submit a batch task
  int commit_batch_task_(const share::ObBatchFileDataSegment &segment,
                         int64_t ddl_task_id,
                         const share::ObAiModelEndpointInfo &endpoint_info,
                         share::ObAiCommandType command_type,
                         common::ObString &task_id,
                         bool allow_null_on_failure = false);

  // Register Task object after Poller creates it.
  // task_in_map: on return, true iff task remains in task_map_ (success or rollback failed).
  // Caller must NOT destroy task when task_in_map is true.
  int register_task_object(const common::ObString &task_id,
                           ObAiAccessTask *task,
                           bool &task_in_map);

  // Get Task object by task_id
  int get_task_object(const common::ObString &task_id,
                      ObAiAccessTask *&task);

  // Get Task object and pin it atomically under task_map_lock_.
  // Caller MUST call task->unpin() after use. Returns OB_HASH_NOT_EXIST if not found.
  int get_and_pin_task_object(const common::ObString &task_id,
                               ObAiAccessTask *&task);

  // Collect all tasks with abandoned_ == true, pinning each.
  // Caller MUST call task->unpin() on each returned task after processing.
  int collect_abandoned_tasks(common::ObIArray<ObAiAccessTask*> &tasks,
                               common::ObIArray<common::ObString> &task_ids);

  // Remove Task object from registry
  int unregister_task_object(const common::ObString &task_id);

  // Lazy cleanup: free terminal tasks from task_map_ once they have been archived to history table.
  // Called by Poller in each do_work() cycle.
  int cleanup_terminal_tasks_();

  int register_table_poller_task(common::ObMySQLProxy &sql_proxy,
                                  int64_t poll_interval_us = 100000);

  void stop_table_poller();
  bool is_table_poller_running() const;

  bool is_inited() const { return is_inited_; }
  bool is_running() const { return is_running_; }
  uint64_t get_tenant_id() const { return tenant_id_; }

  ObAiTaskScheduler &get_scheduler() { return scheduler_; }
  share::ObAiSystemTableManager &get_table_manager() { return table_manager_; }

  void set_sql_client(common::ObISQLClient *sql_client) { sql_client_ = sql_client; }

  TO_STRING_KV(K_(is_inited), K_(is_running), K_(tenant_id));

private:
  bool is_inited_;
  bool is_running_;
  uint64_t tenant_id_;
  common::ObIAllocator *allocator_;
  common::ObISQLClient *sql_client_;
  // Stored by register_table_poller_task(); needed by release_task/abandon_task
  // for archive_task_to_history() which requires ObMySQLProxy.
  common::ObMySQLProxy *sql_proxy_;

  ObAiTaskScheduler scheduler_;
  share::ObAiSystemTableManager table_manager_;

  share::ObSystemTablePoller *table_poller_;

  // Map from task_id to Task object
  common::hash::ObHashMap<common::ObString, ObAiAccessTask*> task_map_;
  // Spin lock for map access
  common::ObSpinLock task_map_lock_;

  DISALLOW_COPY_AND_ASSIGN(ObAiAccessService);
};

// Tenant-level singleton for ObAiAccessService
class ObAiAccessServiceManager
{
public:
  static ObAiAccessService* get_instance(uint64_t tenant_id);
  static int create_instance(uint64_t tenant_id,
                             common::ObIAllocator &allocator,
                             int64_t thread_num = ObAiAccessService::DEFAULT_SCHEDULER_THREAD_NUM);
  static void destroy_instance(uint64_t tenant_id);
  static bool has_instance(uint64_t tenant_id);

private:
  ObAiAccessServiceManager() = delete;
  DISALLOW_COPY_AND_ASSIGN(ObAiAccessServiceManager);
};

} // namespace vector_index
} // namespace oceanbase

#endif // OCEANBASE_SHARE_VECTOR_INDEX_OB_AI_ACCESS_SERVICE_H_

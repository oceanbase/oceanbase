/**
* Copyright (c) 2023 OceanBase
* OceanBase CE is licensed under Mulan PubL v2.
* You can use this software according to the terms and conditions of the Mulan PubL v2.
* You may obtain a copy of Mulan PubL v2 at:
*          http://license.coscl.org.cn/MulanPubL-2.0
* THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
* EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
* MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
* See the Mulan PubL v2 for more details.
*/

#ifndef OCEANBASE_SHARE_VECTOR_INDEX_OB_VECTOR_EMBEDDING_HANDLER_H_
#define OCEANBASE_SHARE_VECTOR_INDEX_OB_VECTOR_EMBEDDING_HANDLER_H_

#include "ob_vector_index_util.h"
#include "lib/lock/ob_thread_cond.h"
#include "lib/lock/ob_latch.h"
#include "lib/thread/thread_mgr_interface.h"
#include "lib/allocator/ob_allocator.h"
#include "share/io/ob_io_define.h"


namespace oceanbase
{

namespace storage
{
class ObEmbeddingIOCallbackHandle;
}

namespace share
{

// Internal task processing phase
enum ObEmbeddingTaskPhase {
  OB_EMBEDDING_TASK_INIT = 0,
  OB_EMBEDDING_TASK_HTTP_SENT = 1,
  OB_EMBEDDING_TASK_HTTP_COMPLETED = 2,
  OB_EMBEDDING_TASK_PARSED = 3,
  OB_EMBEDDING_TASK_DONE = 4
};


// Embedding task status enum
enum ObEmbeddingTaskStatus {
  OB_EMBEDDING_TASK_PENDING = 0,
  OB_EMBEDDING_TASK_RUNNING = 1,
  OB_EMBEDDING_TASK_FINISHED = 2,
  OB_EMBEDDING_TASK_FAILED = 3,
  OB_EMBEDDING_TASK_CANCELLED = 4,
  OB_EMBEDDING_TASK_INVALID = 5
};

struct ObEmbeddingTaskInfo {
  uint64_t tenant_id_;
  int64_t task_id_;
  ObEmbeddingTaskStatus status_;
  int64_t task_start_time_;
  int64_t task_end_time_;
  ObString model_name_;
  ObString model_url_;
  int64_t processed_chunks_;
  int64_t total_chunks_;
  int64_t error_code_;
  ObString error_message_;
  ObString http_error_message_;
  int64_t http_error_code_;
  int64_t processing_time_us_;
  int64_t retry_count_;

  ObEmbeddingTaskInfo() {
    reset();
  }

  void reset() {
    tenant_id_ = OB_INVALID_TENANT_ID;
    task_id_ = OB_INVALID_ID;
    status_ = OB_EMBEDDING_TASK_INVALID;
    task_start_time_ = 0;
    task_end_time_ = 0;
    model_name_.reset();
    model_url_.reset();
    processed_chunks_ = 0;
    total_chunks_ = 0;
    error_code_ = 0;
    error_message_.reset();
    processing_time_us_ = 0;
    retry_count_ = 0;
  }

  bool is_valid() const {
    return tenant_id_ != OB_INVALID_TENANT_ID &&
          task_id_ != OB_INVALID_ID &&
          status_ != OB_EMBEDDING_TASK_INVALID;
  }

  TO_STRING_KV(K_(tenant_id), K_(task_id), K_(status),
              K_(task_start_time), K_(task_end_time), K_(model_name),
              K_(model_url), K_(processed_chunks), K_(total_chunks),
              K_(error_code), K_(error_message), K_(http_error_message), K_(http_error_code),
              K_(processing_time_us), K_(retry_count));
};

// Unified phase management class
class ObEmbeddingTaskPhaseManager {
private:
  // Phase transition validation - strict state machine
  // Each array contains valid transitions FROM that phase
  // Note: DONE is allowed from any state for failure handling
  static const ObEmbeddingTaskPhase VALID_TRANSITIONS_FROM_INIT[];
  static const ObEmbeddingTaskPhase VALID_TRANSITIONS_FROM_HTTP_SENT[];
  static const ObEmbeddingTaskPhase VALID_TRANSITIONS_FROM_HTTP_COMPLETED[];
  static const ObEmbeddingTaskPhase VALID_TRANSITIONS_FROM_PARSED[];
  static const ObEmbeddingTaskPhase VALID_TRANSITIONS_FROM_DONE[];

public:
  // Internal phase machine validation
  static bool is_valid_transition(ObEmbeddingTaskPhase from_phase, ObEmbeddingTaskPhase to_phase);

  // Get string representation of internal phase
  static const char* get_phase_string(ObEmbeddingTaskPhase phase) {
    switch (phase) {
      case OB_EMBEDDING_TASK_INIT: return "INIT";
      case OB_EMBEDDING_TASK_HTTP_SENT: return "HTTP_SENT";
      case OB_EMBEDDING_TASK_HTTP_COMPLETED: return "HTTP_COMPLETED";
      case OB_EMBEDDING_TASK_PARSED: return "PARSED";
      case OB_EMBEDDING_TASK_DONE: return "DONE";
      default: return "UNKNOWN";
    }
  }

  // Map internal phase to system table status
  static ObEmbeddingTaskStatus map_phase_to_status(ObEmbeddingTaskPhase phase, bool is_finished, int result_code) {
    if (is_finished) {
      return (result_code == OB_SUCCESS) ? OB_EMBEDDING_TASK_FINISHED : OB_EMBEDDING_TASK_FAILED;
    }

    switch (phase) {
      case OB_EMBEDDING_TASK_INIT:
        return OB_EMBEDDING_TASK_PENDING;
      case OB_EMBEDDING_TASK_HTTP_SENT:
      case OB_EMBEDDING_TASK_HTTP_COMPLETED:
      case OB_EMBEDDING_TASK_PARSED:
        return OB_EMBEDDING_TASK_RUNNING;
      case OB_EMBEDDING_TASK_DONE:
        return (result_code == OB_SUCCESS) ? OB_EMBEDDING_TASK_FINISHED : OB_EMBEDDING_TASK_FAILED;
      default:
        return OB_EMBEDDING_TASK_INVALID;
    }
  }
};

class ObEmbeddingTaskHandler;

// Constants for field lengths
class ObEmbeddingTask
{
  public:
  ObEmbeddingTask();
  explicit ObEmbeddingTask(ObArenaAllocator &allocator);
  ~ObEmbeddingTask();
  int init(const ObString &model_url,
          const ObString &model_name,
          const ObString &provider,
          const ObString &user_key,
          const ObIArray<ObString> &input_chunks,
          int64_t dimension,
          int64_t http_timeout_us,
          storage::ObEmbeddingIOCallbackHandle *cb_handle = nullptr);
  template <typename ThreadPoolType>
  int do_work(ThreadPoolType *thread_pool);
  int64_t get_task_id() const { return task_id_; }
  // 线程安全的访问方法，用于虚拟表查询
  int get_task_info_for_virtual_table(ObEmbeddingTaskInfo &task_info);

  TO_STRING_KV(K_(is_inited),
                K_(task_id),
                K_(model_url),
                K_(model_name),
                K_(user_key),
                K(input_chunks_.count()),
                K(output_vectors_.count()),
                K_(dimension),
                K_(batch_size),
                K_(processed_chunks),
                K_(total_chunks),
                K_(process_callback_offset));
  bool is_completed();
  void retain_if_managed();
  void release_if_managed();
  int get_async_result(ObArray<float*> &output_vectors);
  // 公共方法用于外部设置任务失败
  int mark_task_failed(int error_code);
  int maybe_callback();
  int wait_for_completion(const int64_t timeout_ms = 0);
  int wake_up();
  void disable_callback();
  void set_callback_done();
  bool need_callback() { return cb_handle_ != nullptr ? true : false; };

public:
  static const ObString MODEL_URL_NAME;
  static const ObString MODEL_NAME_NAME;
  static const ObString ENCODING_FORMAT_NAME;
  static const ObString DATA_NAME;
  static const ObString EMBEDDING_NAME;
  static const ObString FLOAT_FORMAT;
  static const ObString BASE64_FORMAT;
  static const ObString USER_KEY_NAME;
  static const ObString INPUT_NAME;
  static const ObString DIMENSIONS_NAME;

  static const int64_t HTTP_REQUEST_TIMEOUT; // 20 seconds

  // Reschedule related constants
  static const int64_t MAX_RESCHEDULE_RETRY_CNT;
  static const int64_t RESCHEDULE_RETRY_INTERVAL_US;

  // HTTP retry related constants
  static const int64_t MAX_HTTP_RETRY_CNT;
  static const int64_t HTTP_RETRY_BASE_INTERVAL_US;
  static const int64_t HTTP_RETRY_MAX_INTERVAL_US;
  static const int64_t HTTP_RETRY_MULTIPLIER;

  // Callback related constants
  static const int64_t CALLBACK_BATCH_SIZE;

private:
  void reset();
  bool is_finished() const;  // Internal use only - no lock needed
  void set_stop();
  int set_phase(ObEmbeddingTaskPhase new_phase);
  int complete_task(ObEmbeddingTaskPhase new_phase, int result_code, bool finished = true);
  int start_async_work();
  int check_async_progress();

  int send_http_request_async(const char *json_data, int64_t json_len);
  int process_http_response();
  bool is_http_response_ready() const;

  int init_http_request(const char *json_data, int64_t json_len);
  int check_http_progress();
  void cleanup_async_http();
  void log_phase_transition(ObEmbeddingTaskPhase from_phase, ObEmbeddingTaskPhase to_phase);
  int reschedule(ObEmbeddingTaskHandler *thread_pool);
  int handle_reschedule_failure(ObEmbeddingTaskHandler *thread_pool, int error_code);
  bool is_http_sent() const { return curl_request_in_progress_; }
  void set_task_id(int64_t task_id) { task_id_ = task_id; }

  int parse_embedding_response(const char *response_data, size_t response_size);

  // Helper methods for retry logic
  bool should_retry_http_request(int64_t http_error_code) const;
  bool is_batch_size_related_error(int64_t http_error_code) const;
  int64_t calculate_retry_interval() const;
  int adjust_batch_size_for_retry();
  void reset_retry_state();
  int map_http_error_to_internal_error(int64_t http_error_code) const;
  void try_increase_batch_size();
  int init_curl_handler(const ObString &model_url, const ObString &user_key);

  struct HttpResponseData {
    HttpResponseData(ObIAllocator &allocator) : data(nullptr), size(0), allocator(allocator) {}
    ~HttpResponseData() { reset(); }
    void reset()
    {
      if (OB_NOT_NULL(data)) {
        allocator.free(data);
        data = nullptr;
        size = 0;
      }
    }
    char *data;
    size_t size;
    ObIAllocator &allocator;
  };

  static size_t WriteMemoryCallback(void *contents, size_t size, size_t nmemb, void *userp)
  {
    size_t realsize = size * nmemb;
    struct HttpResponseData *mem = (struct HttpResponseData *)userp;

    char *ptr = (char*)mem->allocator.alloc(mem->size + realsize + 1);
    if (OB_ISNULL(ptr)) {
      return 0;
    } else {
      if (OB_NOT_NULL(mem->data)) {
        memcpy(ptr, mem->data, mem->size);
      }
      memcpy(&(ptr[mem->size]), contents, realsize);
      mem->data = ptr;
      mem->size += realsize;
      mem->data[mem->size] = '\0';
    }

    return realsize;
  }

private:
  ObArenaAllocator local_allocator_;
  ObArenaAllocator &allocator_;

  // request model required fields
  ObString model_url_;
  ObString model_name_;
  ObString provider_;
  bool use_base64_format_;
  ObString user_key_;
  int64_t dimension_;
  ObArray<ObString> input_chunks_;
  ObArray<float*> output_vectors_;

  // IO callback for progress notification
  storage::ObEmbeddingIOCallbackHandle *cb_handle_;

  // Callback tracking variables
  int64_t process_callback_offset_;

  // task status and progress
  bool is_inited_;
  int64_t tenant_id_;
  int64_t task_id_;
  ObEmbeddingTaskPhase phase_;
  int64_t process_start_time_us_;
  int64_t process_end_time_us_;
  int64_t processed_chunks_;
  int64_t total_chunks_;
  int64_t http_send_count_;

  int64_t http_error_code_;
  ObString http_error_message_;
  int64_t internal_error_code_;
  ObString internal_error_message_;

  // flow control
  mutable ObLatch task_lock_;
  uint32_t batch_size_;
  int64_t current_batch_idx_;

  // Async processing related members
  int64_t http_timeout_us_;
  int64_t http_send_time_us_; // Unified HTTP send time
  int64_t http_response_data_size_;
  char *http_response_data_;

  // Async HTTP processing members
  CURLM *curl_multi_handle_;
  CURL *curl_easy_handle_;
  bool curl_request_in_progress_;
  HttpResponseData *curl_response_data_; // Store response data for async HTTP processing
  struct curl_slist *curl_headers_; // Store HTTP headers for cleanup

  // HTTP retry related members
  int64_t http_retry_count_;
  int64_t http_total_retry_count_;
  int64_t http_retry_start_time_us_;
  int64_t http_last_retry_time_us_;
  bool need_retry_flag_;

  // Batch size adjustment for retry
  uint32_t original_batch_size_;
  bool batch_size_adjusted_;
  uint32_t current_batch_size_;
  uint32_t successful_requests_count_;

  ObThreadCond task_cond_;
  bool callback_done_;

  // TODO(fanfangyao.ffy): use taskhandle to manage task reference count
  // ref_cnt_ is only used to track the reference count of the post create embedding task
  int64_t ref_cnt_;

  private:
    DISALLOW_COPY_AND_ASSIGN(ObEmbeddingTask);
};


class ObEmbeddingTaskHandler : public lib::TGTaskHandler
{
public:
  ObEmbeddingTaskHandler();
  virtual ~ObEmbeddingTaskHandler();
  int init();
  int start();
  void stop();
  void wait();
  void destroy();

  virtual void handle(void *task) override;
  virtual void handle_drop(void *task) override;

  int push_task(ObEmbeddingTask &task);
  int get_tg_id() { return tg_id_; }
  void inc_task_ref() { ATOMIC_INC(&task_ref_cnt_); }
  void dec_task_ref() { ATOMIC_DEC(&task_ref_cnt_); }
  void inc_dropped_task_cnt() { ATOMIC_INC(&dropped_task_cnt_); }
  int add_task_to_tracking(ObEmbeddingTask *task);
  int remove_task_from_tracking(ObEmbeddingTask *task);
  // wait for all tasks to finish with timeout
  int wait_all_tasks_finished(int64_t timeout_us = ObEmbeddingTask::HTTP_REQUEST_TIMEOUT); // default 20s timeout
  // force drop all remaining tasks and update system table
  int force_drop_all_remaining_tasks();
  // check if all tasks are finished
  bool is_all_tasks_finished() const { return ATOMIC_LOAD(&task_ref_cnt_) == 0; }
  // get current task count for monitoring
  int64_t get_current_task_count() const { return ATOMIC_LOAD(&task_ref_cnt_); }
  // get dropped task count for monitoring
  int64_t get_dropped_task_count() const { return ATOMIC_LOAD(&dropped_task_cnt_); }
  // get tracked task count for monitoring
  int64_t get_tracked_task_count() const {
    common::ObSpinLockGuard guard(const_cast<common::ObSpinLock&>(task_list_lock_));
    return active_tasks_.count();
  }
  // get all active tasks for virtual table query
  int get_all_active_tasks(common::ObArray<ObEmbeddingTask*> &task_list) const;
  // reset statistics counters
  void reset_statistics() {
    ATOMIC_STORE(&task_ref_cnt_, 0);
    ATOMIC_STORE(&dropped_task_cnt_, 0);
  }
  // get task reference count (alias for get_current_task_count for backward compatibility)
  int64_t get_task_ref() const { return ATOMIC_LOAD(&task_ref_cnt_); }
  bool is_inited() const { return is_inited_; }
public:
  common::ObSpinLock lock_; // lock for init
public:
  // push task max wait time: 1s * 10 = 10s
  const static int64_t MAX_RETRY_PUSH_TASK_CNT = 10;
  static const int64_t INVALID_TG_ID = -1;
  // dynamic thread cnt, max cnt is THREAD_FACTOR * tenent_cpu_cnt
  constexpr static const float THREAD_FACTOR = 0.6;
  // 1s
  const static int64_t WAIT_RETRY_PUSH_TASK_TIME = 1 * 1000 * 1000; // us
  static const int64_t MIN_THREAD_COUNT = 1;

private:
  bool is_inited_;
  int tg_id_;
  volatile int64_t task_ref_cnt_;
  // track dropped tasks for monitoring
  volatile int64_t dropped_task_cnt_;
  // task tracking for force drop functionality
  common::ObSpinLock task_list_lock_;
  common::ObArray<ObEmbeddingTask*> active_tasks_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObEmbeddingTaskHandler);
};

}
}

#endif

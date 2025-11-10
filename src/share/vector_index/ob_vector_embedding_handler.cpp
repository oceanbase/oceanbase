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

#define USING_LOG_PREFIX SHARE

#include "share/vector_index/ob_vector_embedding_handler.h"
#include "share/vector_index/ob_json_helper.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/json/ob_json.h"
#include "lib/json_type/ob_json_parse.h"
#include "lib/json_type/ob_json_tree.h"
#include "share/ob_thread_define.h"
#include "observer/ob_server_struct.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "sql/engine/expr/ob_expr_ai/ob_ai_func_utils.h"
#include "storage/ddl/ob_hnsw_embedmgr.h"
#include "lib/lock/ob_thread_cond.h"
#include "lib/allocator/ob_malloc.h"
#include "lib/atomic/ob_atomic.h"

#include <curl/curl.h>

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::sql;
using namespace oceanbase::json;

#define HANDLE_TASK_FAILURE_AND_CLEANUP(task, thread_pool, ret, error_msg) \
  do { \
    LOG_WARN(error_msg, K(ret), K(*task)); \
    task->set_stop(); \
    if (OB_NOT_NULL(thread_pool)) { \
      thread_pool->remove_task_from_tracking(task); \
      thread_pool->dec_task_ref(); \
    } \
    continue_processing = false; \
  } while(0)

#define HANDLE_TASK_COMPLETION_AND_CLEANUP(task, thread_pool, log_msg) \
  do { \
    LOG_INFO(log_msg, K(*task)); \
    if (OB_NOT_NULL(thread_pool)) { \
      thread_pool->remove_task_from_tracking(task); \
      thread_pool->dec_task_ref(); \
    } \
    continue_processing = false; \
  } while(0)

//=============================================== ObEmbeddingTask ================================================
const ObString ObEmbeddingTask::MODEL_URL_NAME = "model_url";
const ObString ObEmbeddingTask::MODEL_NAME_NAME = "model";
const ObString ObEmbeddingTask::ENCODING_FORMAT_NAME = "encoding_format";
const ObString ObEmbeddingTask::DATA_NAME = "data";
const ObString ObEmbeddingTask::EMBEDDING_NAME = "embedding";
const ObString ObEmbeddingTask::FLOAT_FORMAT = "float";
const ObString ObEmbeddingTask::BASE64_FORMAT = "base64";
const ObString ObEmbeddingTask::USER_KEY_NAME = "user_key";
const ObString ObEmbeddingTask::INPUT_NAME = "input";
const ObString ObEmbeddingTask::DIMENSIONS_NAME = "dimensions";

const int64_t ObEmbeddingTask::HTTP_REQUEST_TIMEOUT = 20 * 1000 * 1000; // default http request timeout. 20 seconds

// Reschedule related constants
const int64_t ObEmbeddingTask::MAX_RESCHEDULE_RETRY_CNT = 3;
const int64_t ObEmbeddingTask::RESCHEDULE_RETRY_INTERVAL_US = 100 * 1000; // 100ms

// HTTP retry related constants
const int64_t ObEmbeddingTask::MAX_HTTP_RETRY_CNT = 3;
const int64_t ObEmbeddingTask::HTTP_RETRY_BASE_INTERVAL_US = 1 * 1000 * 1000; // 1 second
const int64_t ObEmbeddingTask::HTTP_RETRY_MAX_INTERVAL_US = 10 * 1000 * 1000; // 10 seconds
const int64_t ObEmbeddingTask::HTTP_RETRY_MULTIPLIER = 2;

// Callback related constants
const int64_t ObEmbeddingTask::CALLBACK_BATCH_SIZE = 64;

//=============================================== ObEmbeddingTaskPhaseManager ================================================
// Phase transition validation - strict state machine
// Each array contains valid transitions FROM that phase
// Note: DONE is allowed from any state for failure handling
const ObEmbeddingTaskPhase ObEmbeddingTaskPhaseManager::VALID_TRANSITIONS_FROM_INIT[] = {OB_EMBEDDING_TASK_HTTP_SENT, OB_EMBEDDING_TASK_DONE};
const ObEmbeddingTaskPhase ObEmbeddingTaskPhaseManager::VALID_TRANSITIONS_FROM_HTTP_SENT[] = {OB_EMBEDDING_TASK_INIT, OB_EMBEDDING_TASK_HTTP_COMPLETED, OB_EMBEDDING_TASK_DONE};// add OB_EMBEDDING_TASK_INIT to support retry from sent state
const ObEmbeddingTaskPhase ObEmbeddingTaskPhaseManager::VALID_TRANSITIONS_FROM_HTTP_COMPLETED[] = {OB_EMBEDDING_TASK_PARSED, OB_EMBEDDING_TASK_DONE};
const ObEmbeddingTaskPhase ObEmbeddingTaskPhaseManager::VALID_TRANSITIONS_FROM_PARSED[] = {OB_EMBEDDING_TASK_INIT, OB_EMBEDDING_TASK_DONE};
const ObEmbeddingTaskPhase ObEmbeddingTaskPhaseManager::VALID_TRANSITIONS_FROM_DONE[] = {}; // No valid transitions from DONE (terminal state)

bool ObEmbeddingTaskPhaseManager::is_valid_transition(ObEmbeddingTaskPhase from_phase, ObEmbeddingTaskPhase to_phase) {
  bool bret = false;
  if (from_phase < OB_EMBEDDING_TASK_INIT || from_phase > OB_EMBEDDING_TASK_DONE || to_phase < OB_EMBEDDING_TASK_INIT || to_phase > OB_EMBEDDING_TASK_DONE) {
    bret = false;
  } else {
    // Check if transition is allowed based on from_phase
    switch (from_phase) {
      case OB_EMBEDDING_TASK_INIT: {
        for (int i = 0; !bret && i < sizeof(VALID_TRANSITIONS_FROM_INIT)/sizeof(VALID_TRANSITIONS_FROM_INIT[0]); i++) {
          if (VALID_TRANSITIONS_FROM_INIT[i] == to_phase) {
            bret = true;
          }
        }
        break;
      }
      case OB_EMBEDDING_TASK_HTTP_SENT: {
        for (int i = 0; !bret && i < sizeof(VALID_TRANSITIONS_FROM_HTTP_SENT)/sizeof(VALID_TRANSITIONS_FROM_HTTP_SENT[0]); i++) {
          if (VALID_TRANSITIONS_FROM_HTTP_SENT[i] == to_phase) {
            bret = true;
          }
        }
        break;
      }
      case OB_EMBEDDING_TASK_HTTP_COMPLETED: {
        for (int i = 0; !bret && i < sizeof(VALID_TRANSITIONS_FROM_HTTP_COMPLETED)/sizeof(VALID_TRANSITIONS_FROM_HTTP_COMPLETED[0]); i++) {
          if (VALID_TRANSITIONS_FROM_HTTP_COMPLETED[i] == to_phase) {
            bret = true;
          }
        }
        break;
      }
      case OB_EMBEDDING_TASK_PARSED: {
        for (int i = 0; !bret && i < sizeof(VALID_TRANSITIONS_FROM_PARSED)/sizeof(VALID_TRANSITIONS_FROM_PARSED[0]); i++) {
          if (VALID_TRANSITIONS_FROM_PARSED[i] == to_phase) {
            bret = true;
          }
        }
        break;
      }
      case OB_EMBEDDING_TASK_DONE: {
        // No valid transitions from DONE
        bret = false;
        break;
      }
      default:
        bret = false;
        break;
    }
  }
  return bret;
}

ObEmbeddingTask::ObEmbeddingTask() : local_allocator_("EmbeddingTask", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
                                    allocator_(local_allocator_),
                                    model_url_(),
                                    model_name_(),
                                    provider_(),
                                    use_base64_format_(false),
                                    user_key_(),
                                    dimension_(0),
                                    input_chunks_(),
                                    output_vectors_(),
                                    cb_handle_(nullptr),
                                    process_callback_offset_(0),
                                    is_inited_(false),
                                    tenant_id_(common::OB_INVALID_ID),
                                    task_id_(OB_INVALID_ID),
                                    phase_(OB_EMBEDDING_TASK_INIT),
                                    process_start_time_us_(0),
                                    process_end_time_us_(0),
                                    processed_chunks_(0),
                                    total_chunks_(0),
                                    http_send_count_(0),
                                    http_error_code_(0),
                                    http_error_message_(),
                                    internal_error_code_(0),
                                    internal_error_message_(),
                                    task_lock_(),
                                    batch_size_(10),
                                    current_batch_idx_(),
                                    http_send_time_us_(0),
                                    http_response_data_size_(0),
                                    http_response_data_(nullptr),
                                    curl_multi_handle_(nullptr),
                                    curl_easy_handle_(nullptr),
                                    curl_request_in_progress_(false),
                                    curl_response_data_(nullptr),
                                    curl_headers_(nullptr),
                                    http_retry_count_(0),
                                    http_total_retry_count_(0),
                                    http_retry_start_time_us_(0),
                                    http_last_retry_time_us_(0),
                                    need_retry_flag_(false),
                                    original_batch_size_(batch_size_),
                                    batch_size_adjusted_(false),
                                    current_batch_size_(10),
                                    successful_requests_count_(0),
                                    task_cond_(),
                                    callback_done_(false),
                                    ref_cnt_(0) {}
ObEmbeddingTask::ObEmbeddingTask(ObArenaAllocator &allocator) : local_allocator_(), allocator_(allocator),
                                     model_url_(),
                                     model_name_(),
                                     provider_(),
                                     use_base64_format_(false),
                                     user_key_(),
                                     dimension_(0),
                                     input_chunks_(),
                                     output_vectors_(),
                                     cb_handle_(nullptr),
                                     process_callback_offset_(0),
                                     is_inited_(false),
                                     tenant_id_(common::OB_INVALID_ID),
                                     task_id_(OB_INVALID_ID),
                                     phase_(OB_EMBEDDING_TASK_INIT),
                                     process_start_time_us_(0),
                                     process_end_time_us_(0),
                                     processed_chunks_(0),
                                     total_chunks_(0),
                                     http_send_count_(0),
                                     http_error_code_(0),
                                     http_error_message_(),
                                     internal_error_code_(0),
                                     internal_error_message_(),
                                     task_lock_(),
                                     batch_size_(10),
                                     current_batch_idx_(),
                                     http_send_time_us_(0),
                                     http_response_data_size_(0),
                                     http_response_data_(nullptr),
                                     curl_multi_handle_(nullptr),
                                     curl_easy_handle_(nullptr),
                                     curl_request_in_progress_(false),
                                     curl_response_data_(nullptr),
                                     curl_headers_(nullptr),
                                     http_retry_count_(0),
                                     http_total_retry_count_(0),
                                     http_retry_start_time_us_(0),
                                     http_last_retry_time_us_(0),
                                     need_retry_flag_(false),
                                     original_batch_size_(batch_size_),
                                     batch_size_adjusted_(false),
                                     current_batch_size_(10),
                                     successful_requests_count_(0),
                                     task_cond_(),
                                     callback_done_(false),
                                     ref_cnt_(0) {}
ObEmbeddingTask::~ObEmbeddingTask() {
  reset();
}

int ObEmbeddingTask::init(const ObString &model_url,
                          const ObString &model_name,
                          const ObString &provider,
                          const ObString &user_key,
                          const ObIArray<ObString> &input_chunks,
                          int64_t dimension,
                          int64_t http_timeout_us,
                          storage::ObEmbeddingIOCallbackHandle *cb_handle)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObEmbeddingTask already inited", K(ret), K(model_url), K(model_name), K(user_key), K(input_chunks));
  } else if (OB_FAIL(input_chunks_.assign(input_chunks))) {
    LOG_WARN("failed to assign input chunks", K(ret), K(input_chunks));
  } else if (OB_FAIL(init_curl_handler(model_url, user_key))) {
    LOG_WARN("failed to init curl handler", K(ret), K(model_url), K(user_key));
  } else if (OB_FAIL(task_cond_.init(ObWaitEventIds::DEFAULT_COND_WAIT))) {
    LOG_WARN("failed to init completion cond", K(ret));
  } else {
    tenant_id_ = MTL_ID();
    task_id_ = (static_cast<int64_t>(tenant_id_) << 32) | (ObTimeUtility::current_time() & 0xFFFFFFFF);
    model_url_ = model_url;
    model_name_ = model_name;
    use_base64_format_ = ObAIFuncUtils::is_provider_support_base64(provider);
    provider_ = provider;
    user_key_ = user_key;
    dimension_ = dimension;
    total_chunks_ = input_chunks.count();
    processed_chunks_ = 0;
    process_callback_offset_ = 0;
    is_inited_ = true;
    internal_error_code_ = OB_SUCCESS;
    internal_error_message_.reset();
    http_error_code_ = 200;
    http_error_message_.reset();
    http_send_count_ = 0;
    http_send_time_us_ = 0;
    http_response_data_size_ = 0;

    cb_handle_ = cb_handle;
    if (nullptr != cb_handle_) {
      cb_handle_->retain();
      ref_cnt_ = 1;
    }

    output_vectors_.prepare_allocate_and_keep_count(total_chunks_);
    if (http_timeout_us > 0) {
      http_timeout_us_ = http_timeout_us;
    } else {
      http_timeout_us_ = HTTP_REQUEST_TIMEOUT;
    }

    // Initialize retry related variables
    http_retry_start_time_us_ = 0;
    http_last_retry_time_us_ = 0;
    http_total_retry_count_ = 0;
    original_batch_size_ = batch_size_;
    batch_size_adjusted_ = false;
    current_batch_size_ = batch_size_;
    successful_requests_count_ = 0;

    LOG_DEBUG("task initialized successfully", K(user_key_), K(task_id_), K(dimension_));
  }

  return ret;
}

int ObEmbeddingTask::parse_embedding_response(const char *response_data, size_t response_size)
{
  int ret = OB_SUCCESS;
  ObJsonReaderHelper json_reader(allocator_);
  ObJsonNode *root = nullptr;

  if (OB_ISNULL(response_data) || response_size == 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid response data", K(ret), KP(response_data), K(response_size));
  } else {
    if (OB_FAIL(json_reader.parse(response_data, response_size, root))) {
      LOG_WARN("failed to parse json response", K(ret), KP(response_data), K(response_size));
    } else if (OB_ISNULL(root)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("root is null", K(ret));
    } else if (!ObJsonHelper::is_object_type(root)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("root is not a json object", K(ret));
    } else {
      ObIJsonBase *data_array = nullptr;
      if (OB_FAIL(json_reader.get_object_value(root, DATA_NAME, data_array))) {
        LOG_WARN("data array not found in response", K(ret), K(DATA_NAME));
      } else if (!ObJsonHelper::is_array_type(data_array)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("data field is not an array", K(ret), "data_type", ObJsonHelper::get_type_name(data_array));
      } else {
        uint64_t data_array_size = json_reader.get_array_size(data_array);
        for (uint64_t data_idx = 0; data_idx < data_array_size && OB_SUCC(ret); data_idx++) {
          ObIJsonBase *data_item = nullptr;
          if (OB_FAIL(json_reader.get_array_element(data_array, data_idx, data_item))) {
            LOG_WARN("failed to get data array element", K(ret), K(data_idx));
          } else if (!ObJsonHelper::is_object_type(data_item)) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("data item is not an object", K(ret), K(data_idx));
          } else {
            ObIJsonBase *embedding_jbase = nullptr;
            float *vector = nullptr;
            if (OB_FAIL(json_reader.get_object_value(data_item, EMBEDDING_NAME, embedding_jbase))) {
              LOG_WARN("embedding array not found", K(ret), K(EMBEDDING_NAME));
            } else if (OB_ISNULL(embedding_jbase)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("embedding jbase is null", K(ret));
            } else if (use_base64_format_ && OB_FAIL(ObAIFuncUtils::decode_base64_embedding_array(
                                                      *embedding_jbase, allocator_, dimension_, vector))) {
              LOG_WARN("failed to decode base64 embedding array", K(ret));
            } else if (!use_base64_format_ && OB_FAIL(ObAIFuncUtils::decode_float_embedding_array(
                                                        *embedding_jbase, allocator_, json_reader, dimension_, vector))) {
              LOG_WARN("failed to decode float embedding array", K(ret));
            } else {
              // no need to lock here, only access by other thread when task is done
              if (OB_FAIL(output_vectors_.push_back(vector))) {
                LOG_WARN("failed to push back vector", K(ret));
              }
            }
          }
        }
      }
    }
  }

  return ret;
}


bool ObEmbeddingTask::is_finished() const
{
  return phase_ == OB_EMBEDDING_TASK_DONE;
}

void ObEmbeddingTask::reset()
{
  is_inited_ = false;
  model_url_.reset();
  model_name_.reset();
  provider_.reset();
  use_base64_format_ = false;
  user_key_.reset();
  input_chunks_.reset();
  output_vectors_.reset();
  process_callback_offset_ = 0;
  dimension_ = 0;
  processed_chunks_ = 0;
  total_chunks_ = 0;
  task_id_ = OB_INVALID_ID;
  current_batch_idx_ = 0;
  http_send_time_us_ = 0;
  http_response_data_size_ = 0;
  http_response_data_ = nullptr;
  if (nullptr != cb_handle_) {
    cb_handle_->release();
    cb_handle_ = nullptr;
  }

  // Reset retry related variables
  http_retry_count_ = 0;
  http_retry_start_time_us_ = 0;
  http_last_retry_time_us_ = 0;
  http_total_retry_count_ = 0;
  original_batch_size_ = 10;
  batch_size_adjusted_ = false;
  callback_done_ = false;

  cleanup_async_http();
  local_allocator_.reset();
  task_cond_.destroy();
}

void ObEmbeddingTask::set_stop()
{
  int ret = set_phase(OB_EMBEDDING_TASK_DONE);
  if (OB_FAIL(ret)) {
    LOG_WARN("failed to set phase to DONE when stopping task", K(ret));
  }
}

int ObEmbeddingTask::reschedule(ObEmbeddingTaskHandler *thread_pool)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(thread_pool)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("thread_pool is null", K(ret));
  } else {
    bool is_push_succ = false;
    int64_t retry_cnt = 0;

    while (OB_SUCC(ret) && !is_push_succ && retry_cnt++ < MAX_RESCHEDULE_RETRY_CNT) {
      retain_if_managed();
      if (OB_FAIL(TG_PUSH_TASK(thread_pool->get_tg_id(), this))) {
        if (ret == OB_EAGAIN) { // task queue is full, will retry
          LOG_DEBUG("task queue is full, will retry", K(retry_cnt), K(MAX_RESCHEDULE_RETRY_CNT), K(*this));
          ob_usleep(RESCHEDULE_RETRY_INTERVAL_US);
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("failed to push task to thread pool", K(ret), K(retry_cnt), K(*this));
        }
        release_if_managed();
      } else {
        is_push_succ = true;
        LOG_DEBUG("task rescheduled successfully", K(retry_cnt), K(*this));
      }
    }

    if (OB_SUCC(ret) && !is_push_succ) {
      ret = OB_TIMEOUT;
      LOG_WARN("failed to reschedule task after max retries", K(ret), K(MAX_RESCHEDULE_RETRY_CNT), K(*this));
    }
  }

  return ret;
}

int ObEmbeddingTask::handle_reschedule_failure(ObEmbeddingTaskHandler *thread_pool, int error_code)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(complete_task(OB_EMBEDDING_TASK_DONE, error_code, true))) {
    LOG_WARN("failed to handle task failure after reschedule failure", K(ret));
  } else {
    LOG_WARN("task reschedule failed, marking as failed", K(error_code), K(*this));
  }

  return ret;
}

int ObEmbeddingTask::start_async_work()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObEmbeddingTask not inited", K(ret));
  } else {
    ObEmbeddingTaskPhase current_phase = phase_;
    if (current_phase == OB_EMBEDDING_TASK_INIT || current_phase == OB_EMBEDDING_TASK_PARSED) {
      if (current_phase == OB_EMBEDDING_TASK_INIT) {
        // 记录开始时间
        {
          ObThreadCondGuard guard(task_cond_);
          process_start_time_us_ = ObTimeUtility::current_time();
        }
      }
    } else {
      ret = OB_STATE_NOT_MATCH;
      LOG_WARN("task already started or not ready for next batch", K(ret), K(*this));
    }

    int64_t start_idx = current_batch_idx_ * batch_size_;
    int64_t end_idx = OB_MIN(start_idx + batch_size_, input_chunks_.count());
    if (OB_FAIL(ret)) {
    } else if (start_idx >= input_chunks_.count()
              && OB_FAIL(complete_task(OB_EMBEDDING_TASK_DONE, OB_SUCCESS, true))) {
      LOG_WARN("failed to complete task successfully", K(ret));
    } else {
      // TODO: Depending on the model type, different HTTP requests need to be generated
      ObJsonBuilder json_builder(allocator_);
      Value *root = nullptr;
      Value *input_array = nullptr;
      if (OB_FAIL(json_builder.create_object(root))) {
        LOG_WARN("failed to create json object", K(ret));
      } else if (OB_FAIL(json_builder.add_array_field(root, INPUT_NAME, input_array))) {
        LOG_WARN("failed to add input array field", K(ret));
      } else {
        for (int64_t i = start_idx; i < end_idx && OB_SUCC(ret); i++) {
          const ObString &text = input_chunks_.at(i);
          LOG_DEBUG("Adding text to input array", K(i), K(text));
          if (OB_FAIL(json_builder.array_add_string(input_array, text))) {
            LOG_WARN("failed to add text to input array", K(ret), K(i));
          }
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(json_builder.add_string_field(root, MODEL_NAME_NAME, model_name_))) {
          LOG_WARN("failed to add model field", K(ret));
        } else if (use_base64_format_ && OB_FAIL(json_builder.add_string_field(root, ENCODING_FORMAT_NAME, BASE64_FORMAT))) {
          LOG_WARN("failed to add encoding format field", K(ret));
        } else if (!use_base64_format_ && OB_FAIL(json_builder.add_string_field(root, ENCODING_FORMAT_NAME, FLOAT_FORMAT))) {
          LOG_WARN("failed to add encoding format field", K(ret));
        } else if (dimension_ > 0 && OB_FAIL(json_builder.add_int_field(root, DIMENSIONS_NAME, dimension_))) {
          LOG_WARN("failed to add dimensions field", K(ret));
        } else {
          const int64_t json_buf_len = 8192;
          char *json_buf = (char*)allocator_.alloc(json_buf_len);
          if (OB_ISNULL(json_buf)) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("failed to alloc json buffer", K(ret));
          } else {
            int64_t json_len = 0;
            if (OB_FAIL(json_builder.to_string(root, json_buf, json_buf_len, json_len))) {
              LOG_WARN("failed to convert json to string", K(ret));
            } else {
              if (OB_FAIL(send_http_request_async(json_buf, json_len))) {
                if (OB_FAIL(complete_task(OB_EMBEDDING_TASK_DONE, ret, true))) {
                  LOG_WARN("failed to handle task failure", K(ret));
                }
              } else {
                LOG_DEBUG("HTTP request sent successfully for batch", K(current_batch_idx_),
                                                                    K(start_idx),
                                                                    K(end_idx),
                                                                    K(*this));
              }
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObEmbeddingTask::check_async_progress()
{
  int ret = OB_SUCCESS;
  ObEmbeddingTaskPhase current_phase = phase_;
  LOG_DEBUG("check_async_progress", K(current_phase), K(curl_request_in_progress_));
  if (current_phase == OB_EMBEDDING_TASK_INIT) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("task not started yet", K(ret));
  } else if (current_phase == OB_EMBEDDING_TASK_DONE) {
    ret = OB_SUCCESS;
  } else if (current_phase == OB_EMBEDDING_TASK_HTTP_SENT) {
    if (OB_FAIL(check_http_progress())) {
      if (ret == OB_NEED_RETRY) {
        // handle retry logic
        int64_t retry_interval = calculate_retry_interval();
        int64_t current_time = ObTimeUtility::current_time();

        if (current_time - http_last_retry_time_us_ >= retry_interval) {
          // time to retry
          http_response_data_ = nullptr;
          http_response_data_size_ = 0;

          if (OB_FAIL(set_phase(OB_EMBEDDING_TASK_INIT))) {
            LOG_WARN("failed to reset phase for retry", K(ret));
          } else if (OB_FAIL(start_async_work())) {
            LOG_WARN("failed to retry HTTP request", K(ret));
            if (OB_FAIL(complete_task(OB_EMBEDDING_TASK_DONE, ret, true))) {
              LOG_WARN("failed to handle retry failure", K(ret));
            }
          }
        } else {
          // not time to retry yet, continue waiting
          ret = OB_SUCCESS;
        }
      } else {
        // non-retryable error
        if (OB_FAIL(complete_task(OB_EMBEDDING_TASK_DONE, ret, true))) {
          LOG_WARN("failed to handle task failure", K(ret));
        }
      }
    } else if (is_http_response_ready()) {
      // Successful response, reset retry state
      reset_retry_state();

      if (OB_FAIL(set_phase(OB_EMBEDDING_TASK_HTTP_COMPLETED))) {
        LOG_WARN("failed to set phase to HTTP_COMPLETED", K(ret));
      }
    } else {
      int64_t current_time = ObTimeUtility::current_time();
      int64_t elapsed_time = current_time - http_send_time_us_;
      if (elapsed_time > http_timeout_us_) {
        if (OB_FAIL(complete_task(OB_EMBEDDING_TASK_DONE, OB_TIMEOUT, true))) {
          LOG_WARN("failed to handle task failure", K(ret));
        }
        LOG_WARN("HTTP request timeout", K(elapsed_time), K(http_timeout_us_), K(*this));
      }
    }
  } else if (current_phase == OB_EMBEDDING_TASK_HTTP_COMPLETED) {
    if (OB_FAIL(process_http_response())) {
      if (OB_FAIL(complete_task(OB_EMBEDDING_TASK_DONE, ret, true))) {
        LOG_WARN("failed to handle task failure", K(ret), K(*this));
      }
    } else {
      int64_t actual_chunks_in_batch = OB_MIN(batch_size_, input_chunks_.count() - (current_batch_idx_ * batch_size_));
      {
        ObThreadCondGuard guard(task_cond_);
        processed_chunks_ += actual_chunks_in_batch;
      }
      if (OB_FAIL(set_phase(OB_EMBEDDING_TASK_PARSED))) {
        LOG_WARN("failed to set phase to PARSED", K(ret), K(*this));
      }
    }
  } else if (current_phase == OB_EMBEDDING_TASK_PARSED) {
    int64_t next_start_idx = (current_batch_idx_ + 1) * batch_size_;
    LOG_DEBUG("check_async_progress: PARSED phase", K(current_batch_idx_), K(next_start_idx), K(input_chunks_.count()), K(processed_chunks_), K(total_chunks_));
    if (next_start_idx < input_chunks_.count()) {
      current_batch_idx_++;
      if (OB_FAIL(set_phase(OB_EMBEDDING_TASK_INIT))) {
        LOG_WARN("failed to set phase to INIT for next batch", K(ret), K(*this));
      }
    } else {
      if (OB_FAIL(complete_task(OB_EMBEDDING_TASK_DONE, OB_SUCCESS, true))) {
        LOG_WARN("failed to handle task success", K(ret), K(*this));
      }
    }
  }
  return ret;
}

bool ObEmbeddingTask::is_completed()
{
  ObThreadCondGuard guard(task_cond_);
  return phase_ == OB_EMBEDDING_TASK_DONE;
}

int ObEmbeddingTask::get_async_result(ObArray<float*> &vectors)
{
  int ret = OB_SUCCESS;
  if (!is_completed()) {
    ret = OB_EAGAIN;
    LOG_WARN("async task not completed yet", K(ret), K(phase_));
  } else {
    ObThreadCondGuard guard(task_cond_);
    if (internal_error_code_ != OB_SUCCESS) {
      ret = internal_error_code_;
      LOG_WARN("async task failed", K(ret), K_(http_error_code), K_(http_error_message), K(*this));
    } else {
      vectors.reset();
      if (OB_FAIL(vectors.assign(output_vectors_))) {
        LOG_WARN("failed to assign output_vectors", K(ret), K(*this));
      }
    }
  }

  return ret;
}


int ObEmbeddingTask::send_http_request_async(const char *json_data, int64_t json_len)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(init_http_request(json_data, json_len))) {
    LOG_WARN("failed to init async http request", K(ret), K(*this));
  } else {
    if (OB_FAIL(set_phase(OB_EMBEDDING_TASK_HTTP_SENT))) {
      LOG_WARN("failed to set phase to HTTP_SENT", K(ret), K(*this));
    } else {
      http_send_time_us_ = ObTimeUtility::current_time();
      curl_request_in_progress_ = true;
    }
  }

  return ret;
}

int ObEmbeddingTask::init_http_request(const char *json_data, int64_t json_len)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(curl_multi_handle_) || OB_ISNULL(curl_easy_handle_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null curl handles", K(ret), KP(curl_multi_handle_), KP(curl_easy_handle_));
  } else {
    curl_multi_remove_handle(curl_multi_handle_, curl_easy_handle_);
    CURLMcode multi_res = curl_multi_add_handle(curl_multi_handle_, curl_easy_handle_);
    if (multi_res != CURLM_OK) {
      ret = OB_CURL_ERROR;
      LOG_WARN("curl_multi_add_handle failed", K(ret), K(multi_res));
    } else {
      http_response_data_ = nullptr;
      http_response_data_size_ = 0;
      curl_easy_setopt(curl_easy_handle_, CURLOPT_POSTFIELDS, json_data);
      curl_easy_setopt(curl_easy_handle_, CURLOPT_POSTFIELDSIZE, json_len);
    }
  }
  return ret;
}

int ObEmbeddingTask::check_http_progress()
{
  int ret = OB_SUCCESS;
  if (need_retry_flag_ && !curl_request_in_progress_ && http_response_data_ == nullptr) {
    ret = OB_NEED_RETRY;
    LOG_DEBUG("in retry backoff state, signaling retry", K(http_retry_count_), K(http_error_code_));
  } else if (!curl_request_in_progress_ || OB_ISNULL(curl_multi_handle_) || OB_ISNULL(curl_easy_handle_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("no async HTTP request in progress or handles are null", K(ret),
            K(curl_request_in_progress_), KP(curl_multi_handle_), KP(curl_easy_handle_));
  } else {
    int running_handles = 0;
    CURLMcode multi_res = curl_multi_perform(curl_multi_handle_, &running_handles);

    LOG_DEBUG("check_http_progress", K(running_handles), K(multi_res));

    if (multi_res != CURLM_OK) {
      ret = OB_CURL_ERROR;
      LOG_WARN("curl_multi_perform failed", K(ret), K(multi_res));
    } else if (running_handles == 0) {
      curl_request_in_progress_ = false;
      need_retry_flag_ = false;
      CURLMsg *msg = nullptr;
      int msgs_in_queue = 0;
      while (OB_SUCC(ret) && (msg = curl_multi_info_read(curl_multi_handle_, &msgs_in_queue))) {
        if (OB_ISNULL(msg)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("curl_multi_info_read returned null", K(ret));
        } else if (msg->msg == CURLMSG_DONE) {
          CURL *easy_handle = msg->easy_handle;
          CURLcode res = msg->data.result;

          if (res == CURLE_OK) {
            long response_code;
            curl_easy_getinfo(easy_handle, CURLINFO_RESPONSE_CODE, &response_code);
            if (response_code == 200 /* HTTP OK */) {
              if (OB_NOT_NULL(curl_response_data_) && OB_NOT_NULL(curl_response_data_->data)) {
                http_response_data_size_ = curl_response_data_->size;
                http_response_data_ = (char*)allocator_.alloc(http_response_data_size_ + 1);
                if (OB_ISNULL(http_response_data_)) {
                  ret = OB_ALLOCATE_MEMORY_FAILED;
                  LOG_WARN("failed to allocate response data storage", K(ret), K(*this));
                } else {
                  memcpy(http_response_data_, curl_response_data_->data, http_response_data_size_);
                  http_response_data_[http_response_data_size_] = '\0';
                }
              } else {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("no response data available", K(ret), K(*this));
              }
            } else {
              {
                ObThreadCondGuard guard(task_cond_);
                if (OB_NOT_NULL(curl_response_data_) && OB_NOT_NULL(curl_response_data_->data)) {
                  http_error_message_ = ObString(curl_response_data_->size, curl_response_data_->data);
                }
                http_error_code_ = response_code;
                need_retry_flag_ = should_retry_http_request(response_code);
                // Check if we should retry this http request
                if (need_retry_flag_) {
                  http_retry_count_++;
                  http_total_retry_count_++;

                  // Check if we need to adjust batch size for retry
                  if (is_batch_size_related_error(response_code) && OB_FAIL(adjust_batch_size_for_retry())) {
                    LOG_WARN("failed to adjust batch size for retry", K(ret), K(*this));
                  } else {
                    // Set retry flag to indicate we should retry
                    ret = OB_NEED_RETRY;
                  }
                } else {
                  // Map HTTP error to internal error code
                  internal_error_code_ = map_http_error_to_internal_error(response_code);
                  internal_error_message_ = ObString("HTTP request failed");
                  ret = internal_error_code_;
                  LOG_WARN("HTTP request failed, no retry", K(response_code), K_(internal_error_code), K_(http_error_message), K(*this));
                }
              }

              if (need_retry_flag_) {
                if (http_retry_count_ == 1) { // 第一次重试
                  http_retry_start_time_us_ = ObTimeUtility::current_time();
                }
                http_last_retry_time_us_ = ObTimeUtility::current_time();
              }
            }
          } else {
            ret = OB_CURL_ERROR;
            LOG_WARN("curl request failed", K(ret), K(res), K(*this));
          }
        }
      }

      if (OB_NOT_NULL(curl_response_data_)) {
        curl_response_data_->reset();
      }
    }
  }

  return ret;
}

void ObEmbeddingTask::cleanup_async_http()
{
  if (OB_UNLIKELY(curl_request_in_progress_)) {
    FLOG_INFO("cleanup_async_http while curl_request_in_progress_ is true", K(*this));
  }

  if (OB_NOT_NULL(curl_easy_handle_)) {
    if (OB_NOT_NULL(curl_multi_handle_)) {
      curl_multi_remove_handle(curl_multi_handle_, curl_easy_handle_);
    }
    curl_easy_cleanup(curl_easy_handle_);
    curl_easy_handle_ = nullptr;
  }

  if (OB_NOT_NULL(curl_multi_handle_)) {
    curl_multi_cleanup(curl_multi_handle_);
    curl_multi_handle_ = nullptr;
  }

  if (OB_NOT_NULL(curl_headers_)) {
    curl_slist_free_all(curl_headers_);
    curl_headers_ = nullptr;
    LOG_DEBUG("Freed HTTP headers");
  }

  if (OB_NOT_NULL(curl_response_data_)) {
    OB_DELETEx(HttpResponseData, &allocator_, curl_response_data_);
    curl_response_data_ = nullptr;
  }
}

void ObEmbeddingTask::log_phase_transition(ObEmbeddingTaskPhase from_phase, ObEmbeddingTaskPhase to_phase)
{
  const char* from_state_str = nullptr;
  const char* to_state_str = nullptr;

  switch (from_phase) {
    case OB_EMBEDDING_TASK_INIT: from_state_str = "INIT"; break;
    case OB_EMBEDDING_TASK_HTTP_SENT: from_state_str = "HTTP_SENT"; break;
    case OB_EMBEDDING_TASK_HTTP_COMPLETED: from_state_str = "HTTP_COMPLETED"; break;
    case OB_EMBEDDING_TASK_PARSED: from_state_str = "PARSED"; break;
    case OB_EMBEDDING_TASK_DONE: from_state_str = "DONE"; break;
    default: from_state_str = "UNKNOWN"; break;
  }

  switch (to_phase) {
    case OB_EMBEDDING_TASK_INIT: to_state_str = "INIT"; break;
    case OB_EMBEDDING_TASK_HTTP_SENT: to_state_str = "HTTP_SENT"; break;
    case OB_EMBEDDING_TASK_HTTP_COMPLETED: to_state_str = "HTTP_COMPLETED"; break;
    case OB_EMBEDDING_TASK_PARSED: to_state_str = "PARSED"; break;
    case OB_EMBEDDING_TASK_DONE: to_state_str = "DONE"; break;
    default: to_state_str = "UNKNOWN"; break;
  }
  char msg[1024];
  int64_t msg_len = 0;
  int64_t ret = snprintf(msg, sizeof(msg), "[TASK] Status From %s to %s", from_state_str, to_state_str);
  msg_len = strlen(msg);
  if (ret < 0) {
    LOG_WARN("failed to format message", K(ret));
  } else {
    LOG_DEBUG(msg, K(msg_len));
  }
}

int ObEmbeddingTask::process_http_response()
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(http_response_data_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("no HTTP response data available", K(ret), K(*this));
  } else {
    if (OB_FAIL(parse_embedding_response(http_response_data_, http_response_data_size_))) {
      LOG_WARN("failed to parse embedding response", K(ret), K(*this));
    }
  }

  return ret;
}

bool ObEmbeddingTask::is_http_response_ready() const
{
  return OB_NOT_NULL(http_response_data_);
}

void ObEmbeddingTask::retain_if_managed()
{
  if (need_callback()) { ATOMIC_AAF(&ref_cnt_, 1); }
}

void ObEmbeddingTask::release_if_managed()
{
  if (need_callback()) {
    int64_t v = ATOMIC_AAF(&ref_cnt_, -1);
    if (0 == v) {
      this->~ObEmbeddingTask();
      ob_free(this);
    }
  }
}

template <typename ThreadPoolType>
int ObEmbeddingTask::do_work(ThreadPoolType *thread_pool)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObEmbeddingTask not inited", K(ret), K(*this));
  } else if (is_finished()) {
    LOG_DEBUG("task already finished, no work needed", K(*this));
  } else {
    LOG_DEBUG("processing embedding task", K(*this), "internal_phase", phase_);

    bool continue_processing = true;
    while (OB_SUCC(ret) && continue_processing && !is_finished()) {
      switch (phase_) {
        case OB_EMBEDDING_TASK_INIT: {
          LOG_DEBUG("Starting async work for new task", K(*this));
          if (OB_FAIL(start_async_work())) {
            HANDLE_TASK_FAILURE_AND_CLEANUP(this, thread_pool, ret, "failed to start async work");
          } else {
            LOG_DEBUG("async work started, continuing to next state", K(*this));
          }
          break;
        }
        case OB_EMBEDDING_TASK_HTTP_SENT: {
          LOG_DEBUG("Checking async progress for HTTP_SENT task", K(*this));
          if (OB_FAIL(check_async_progress())) {
            HANDLE_TASK_FAILURE_AND_CLEANUP(this, thread_pool, ret, "failed to check async progress");
          } else if (is_finished()) {
            HANDLE_TASK_COMPLETION_AND_CLEANUP(this, thread_pool, "task completed during HTTP progress check");
          } else if (!is_http_response_ready()) {
            LOG_DEBUG("HTTP request still in progress, rescheduling", K(*this));
            if (OB_NOT_NULL(thread_pool)) {
              if (OB_FAIL(reschedule(thread_pool))) {
                if (OB_FAIL(handle_reschedule_failure(thread_pool, ret))) {
                  LOG_WARN("failed to handle reschedule failure", K(ret));
                }
                HANDLE_TASK_FAILURE_AND_CLEANUP(this, thread_pool, ret, "failed to reschedule task");
              }
            }
            continue_processing = false;
          } else {
            LOG_DEBUG("HTTP response ready, continuing to next state", K(*this));
          }
          break;
        }
        case OB_EMBEDDING_TASK_HTTP_COMPLETED: {
          LOG_DEBUG("Processing HTTP response for HTTP_COMPLETED task", K(*this));
          if (OB_FAIL(check_async_progress())) {
            HANDLE_TASK_FAILURE_AND_CLEANUP(this, thread_pool, ret, "failed to process HTTP response");
          } else if (is_finished()) {
            HANDLE_TASK_COMPLETION_AND_CLEANUP(this, thread_pool, "task completed during response processing");
          }
          break;
        }
        case OB_EMBEDDING_TASK_PARSED: {
          LOG_DEBUG("Finalizing PARSED task", K(*this));
          if (OB_FAIL(check_async_progress())) {
            if (OB_FAIL(complete_task(OB_EMBEDDING_TASK_DONE, ret, true))) {
              LOG_WARN("failed to mark task as failed", K(ret), K(*this));
            }
            HANDLE_TASK_FAILURE_AND_CLEANUP(this, thread_pool, ret, "failed to finalize task");
          } else if (is_finished()) {
            HANDLE_TASK_COMPLETION_AND_CLEANUP(this, thread_pool, "task finalized successfully");
          } else {
            LOG_DEBUG("task state changed, continuing to next batch", K(*this));
          }
          break;
        }
        case OB_EMBEDDING_TASK_DONE: {
          HANDLE_TASK_COMPLETION_AND_CLEANUP(this, thread_pool, "task already completed, cleaning up");
          break;
        }
        default: {
          HANDLE_TASK_FAILURE_AND_CLEANUP(this, thread_pool, OB_ERR_UNEXPECTED, "unknown task state");
          break;
        }
      }
    }
  }

  return ret;
}

// Unified state management methods with validation and automatic system table update
int ObEmbeddingTask::set_phase(ObEmbeddingTaskPhase new_phase)
{
  int ret = OB_SUCCESS;
  bool should_callback = false;
  {
    ObThreadCondGuard guard(task_cond_);
    ObEmbeddingTaskPhase current_phase = phase_;
    // Validate phase transition
    if (!ObEmbeddingTaskPhaseManager::is_valid_transition(current_phase, new_phase)) {
      ret = OB_STATE_NOT_MATCH;
      LOG_WARN("invalid phase transition", K(ret),
              "from", ObEmbeddingTaskPhaseManager::get_phase_string(current_phase),
              "to", ObEmbeddingTaskPhaseManager::get_phase_string(new_phase));
    } else {
      log_phase_transition(current_phase, new_phase);
      phase_ = new_phase;
      if (phase_ == OB_EMBEDDING_TASK_HTTP_SENT) {
        http_send_count_++;
      }
      if (phase_ == OB_EMBEDDING_TASK_DONE && need_callback()) {
        should_callback = true;
      }
    }
  }

  if (OB_SUCC(ret) && should_callback) {
    int cb_ret = maybe_callback();
    if (OB_SUCCESS != cb_ret) {
      LOG_WARN("failed to maybe callback", K(cb_ret));
    }
    int tmp_ret = wake_up();
    if (OB_SUCCESS != tmp_ret) {
      LOG_WARN("failed to wake up after callback", K(tmp_ret));
    }
    release_if_managed();
  }
  return ret;
}



int ObEmbeddingTask::complete_task(ObEmbeddingTaskPhase new_phase, int result_code, bool finished)
{
  int ret = OB_SUCCESS;

  {
    ObThreadCondGuard guard(task_cond_);
    internal_error_code_ = result_code;
  }

  if (OB_FAIL(set_phase(new_phase))) {
    LOG_WARN("invalid phase transition for task completion", K(ret),
            "to", ObEmbeddingTaskPhaseManager::get_phase_string(new_phase));
  } else {
    ObThreadCondGuard guard(task_cond_);
    if (finished && new_phase == OB_EMBEDDING_TASK_DONE) {
      process_end_time_us_ = ObTimeUtility::current_time();
    }
    LOG_DEBUG("task completion phase transition successful",
              "new_phase", ObEmbeddingTaskPhaseManager::get_phase_string(new_phase),
              K(result_code), K(finished));
  }

  return ret;
}

int ObEmbeddingTask::mark_task_failed(int error_code)
{
  return complete_task(OB_EMBEDDING_TASK_DONE, error_code, true);
}

// placeholder for virtual table
int ObEmbeddingTask::get_task_info_for_virtual_table(ObEmbeddingTaskInfo &task_info)
{
  int ret = OB_SUCCESS;
  ObThreadCondGuard guard(task_cond_);

  task_info.reset();
  task_info.tenant_id_ = MTL_ID();
  task_info.task_id_ = task_id_;
  task_info.model_name_ = model_name_;
  task_info.model_url_ = model_url_;
  task_info.processed_chunks_ = processed_chunks_;
  task_info.total_chunks_ = total_chunks_;
  task_info.task_start_time_ = process_start_time_us_;
  task_info.task_end_time_ = process_end_time_us_;
  task_info.status_ = ObEmbeddingTaskPhaseManager::map_phase_to_status(phase_, is_completed(), internal_error_code_);

  if (process_end_time_us_ > 0 && process_start_time_us_ > 0) {
    task_info.processing_time_us_ = process_end_time_us_ - process_start_time_us_;
  }

  task_info.error_code_ = internal_error_code_;
  task_info.error_message_ = internal_error_message_;
  task_info.http_error_code_ = http_error_code_;
  task_info.http_error_message_ = http_error_message_;
  task_info.retry_count_ = http_total_retry_count_;
  return ret;
}

//=============================================== ObEmbeddingTaskHandler ================================================

ObEmbeddingTaskHandler::ObEmbeddingTaskHandler() : is_inited_(false),
                                                  tg_id_(ObEmbeddingTaskHandler::INVALID_TG_ID),
                                                  task_ref_cnt_(0),
                                                  dropped_task_cnt_(0) {}

ObEmbeddingTaskHandler::~ObEmbeddingTaskHandler()
{
  stop();
  wait();
  destroy();
}

int ObEmbeddingTaskHandler::init()
{
  int ret = OB_SUCCESS;
  common::ObSpinLockGuard guard(lock_);
  if (OB_UNLIKELY(is_inited_)) {
    // already inited
  } else if (OB_FAIL(start())) {
    LOG_WARN("failed to start embedding task handler", KR(ret));
    wait();
    stop();
    destroy();
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObEmbeddingTaskHandler::start()
{
  int ret = OB_SUCCESS;
  int64_t max_thread_cnt = MTL_CPU_COUNT() * THREAD_FACTOR;
  max_thread_cnt = OB_MAX(max_thread_cnt, MIN_THREAD_COUNT);
  if (OB_FAIL(TG_CREATE_TENANT(lib::TGDefIDs::VectorTaskPool, tg_id_))) {
    LOG_WARN("TG_CREATE_TENANT failed for embedding thread pool", KR(ret));
  } else if (OB_FAIL(TG_START(tg_id_))) {
    LOG_WARN("TG_START failed for embedding thread pool", KR(ret));
  } else if (OB_FAIL(TG_SET_ADAPTIVE_THREAD(tg_id_, MIN_THREAD_COUNT,
                                            max_thread_cnt))) {  // must be call TG_SET_ADAPTIVE_THREAD
  } else if (OB_FAIL(TG_SET_HANDLER_AND_START(tg_id_, *this))) {
    LOG_WARN("TG_SET_HANDLER_AND_START failed", KR(ret), K_(tg_id));
  } else {
    LOG_INFO("succ to start embedding task handler", K_(tg_id));
  }
  return ret;
}

void ObEmbeddingTaskHandler::stop()
{
  LOG_INFO("embedding task handler start to stop", K_(tg_id));
  if (OB_LIKELY(INVALID_TG_ID != tg_id_)) {
    TG_STOP(tg_id_);
  }
}

void ObEmbeddingTaskHandler::wait()
{
  int ret = OB_SUCCESS;
  LOG_INFO("embedding task handler start to wait", K_(tg_id));
  if (OB_LIKELY(INVALID_TG_ID != tg_id_)) {
    TG_WAIT(tg_id_);
  }
  if (OB_FAIL(wait_all_tasks_finished())) {
    LOG_WARN("failed to wait all tasks finished", K(ret));
  }
}

int ObEmbeddingTaskHandler::wait_all_tasks_finished(int64_t timeout_us)
{
  int ret = OB_SUCCESS;
  int64_t start_time = ObTimeUtility::current_time();
  int64_t current_task_count = 0;
  int64_t last_log_time = start_time;
  int64_t dropped_count = 0;
  const int64_t LOG_INTERVAL = 5 * 1000 * 1000; // 5 seconds

  LOG_INFO("start waiting for all tasks to finish", K_(task_ref_cnt), K_(dropped_task_cnt), K(timeout_us));

  while (OB_SUCC(ret) && ATOMIC_LOAD(&task_ref_cnt_) > 0) {
    current_task_count = ATOMIC_LOAD(&task_ref_cnt_);
    dropped_count = ATOMIC_LOAD(&dropped_task_cnt_);
    int64_t elapsed_time = ObTimeUtility::current_time() - start_time;
    int64_t current_time = ObTimeUtility::current_time();

    if (current_time - last_log_time > LOG_INTERVAL) { // 5s
      LOG_INFO("waiting for tasks to finish", K(current_task_count), K(dropped_count), K(elapsed_time), K(timeout_us));
      last_log_time = current_time;
    }

    if (elapsed_time > timeout_us) {
      ret = OB_TIMEOUT;
      LOG_WARN("timeout waiting for tasks to finish", K(ret),
                                                      K(current_task_count),
                                                      K(dropped_count),
                                                      K(elapsed_time),
                                                      K(timeout_us));
      if (OB_FAIL(force_drop_all_remaining_tasks())) {
        LOG_ERROR("failed to force drop remaining tasks", K(ret));
      }
    }
    ob_usleep(100 * 1000); // 100ms
  }

  if (OB_SUCC(ret)) {
    dropped_count = ATOMIC_LOAD(&dropped_task_cnt_);
    LOG_INFO("all tasks finished successfully", K_(task_ref_cnt), K(dropped_count));
  } else {
    dropped_count = ATOMIC_LOAD(&dropped_task_cnt_);
    LOG_WARN("failed to wait all tasks finished", K(ret), K_(task_ref_cnt), K(dropped_count));
  }

  return ret;
}

int ObEmbeddingTaskHandler::force_drop_all_remaining_tasks()
{
  int ret = OB_SUCCESS;
  int64_t remaining_tasks = ATOMIC_LOAD(&task_ref_cnt_);

  if (remaining_tasks <= 0) {
  } else {
    common::ObSpinLockGuard guard(task_list_lock_);
    for (int64_t i = 0; i < active_tasks_.count(); i++) {
      ObEmbeddingTask *task = active_tasks_.at(i);
      if (OB_NOT_NULL(task)) {
        if (OB_FAIL(task->mark_task_failed(OB_TIMEOUT))) {
          LOG_WARN("failed to update task status to failed", K(ret), K(*task));
        } else {
          LOG_INFO("updated task status to failed", K(*task));
        }
      }
    }
    active_tasks_.reset();
    ATOMIC_STORE(&task_ref_cnt_, 0);
    for (int64_t i = 0; i < remaining_tasks; i++) {
      ATOMIC_INC(&dropped_task_cnt_);
    }
  }

  return ret;
}

int ObEmbeddingTaskHandler::get_all_active_tasks(common::ObArray<ObEmbeddingTask*> &task_list) const
{
  int ret = OB_SUCCESS;
  common::ObSpinLockGuard guard(const_cast<common::ObSpinLock&>(task_list_lock_));

  task_list.reset();
  for (int64_t i = 0; i < active_tasks_.count(); i++) {
    ObEmbeddingTask *task = active_tasks_.at(i);
    if (OB_NOT_NULL(task)) {
      if (OB_FAIL(task_list.push_back(task))) {
        LOG_WARN("failed to push back task", K(ret), K(i));
      }
    }
  }

  return ret;
}

void ObEmbeddingTaskHandler::destroy()
{
  LOG_INFO("embedding task handler start to destroy", K_(tg_id), K_(task_ref_cnt), K_(dropped_task_cnt));

  if (ATOMIC_LOAD(&task_ref_cnt_) > 0) {
    int ret = OB_SUCCESS;
    if (OB_FAIL(force_drop_all_remaining_tasks())) {
      LOG_WARN("failed to force drop remaining tasks during destroy", K(ret));
    }
  }

  if (OB_LIKELY(INVALID_TG_ID != tg_id_)) {
    TG_DESTROY(tg_id_);
  }
  tg_id_ = INVALID_TG_ID;
  is_inited_ = false;
  LOG_INFO("embedding task handler destroyed", K_(task_ref_cnt), K_(dropped_task_cnt));
}

int ObEmbeddingTaskHandler::push_task(ObEmbeddingTask &task)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("handler is not init", KR(ret));
  } else {
    bool is_push_succ = false;
    int64_t has_retry_cnt = 0;

    while (OB_SUCC(ret) && !is_push_succ && has_retry_cnt++ <= MAX_RETRY_PUSH_TASK_CNT) {
      task.retain_if_managed();
      if (OB_FAIL(TG_PUSH_TASK(tg_id_, &task))) {
        if (ret != OB_EAGAIN) {
          LOG_WARN("fail to TG_PUSH_TASK", KR(ret), K(task));
        } else {
          LOG_DEBUG("fail to TG_PUSH_TASK, queue is full will retry", KR(ret), K(task));
          ob_usleep(WAIT_RETRY_PUSH_TASK_TIME);
          ret = OB_SUCCESS;
        }
        task.release_if_managed();
      } else {
        is_push_succ = true;
        inc_task_ref();
        if (OB_FAIL(add_task_to_tracking(&task))) {
          LOG_WARN("failed to add task to tracking", K(ret), K(task));
          // push task failed, mark task as failed
          task.mark_task_failed(ret);
        }
      }
    }
  }

  return ret;
}

void ObEmbeddingTaskHandler::handle(void *task)
{
  int ret = OB_SUCCESS;
  ObCurTraceId::init(GCONF.self_addr_);
  ObEmbeddingTask *embedding_task = nullptr;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("handler is not init", KR(ret));
  } else if (OB_ISNULL(task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret));
  } else {
    embedding_task = static_cast<ObEmbeddingTask *>(task);
    LOG_INFO("handling embedding task", K_(task_ref_cnt), KPC(embedding_task));

    if (OB_FAIL(embedding_task->do_work(this))) {
      LOG_WARN("failed to do work for embedding task", K(ret), KPC(embedding_task));
    }
  }

  if (OB_NOT_NULL(embedding_task)) {
    embedding_task->release_if_managed();
  }
  LOG_INFO("task handling completed", K_(task_ref_cnt));
}

void ObEmbeddingTaskHandler::handle_drop(void *task)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("handler is not init", KR(ret));
  } else if (OB_ISNULL(task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret));
  } else {
    ObEmbeddingTask *embedding_task = nullptr;
    embedding_task = static_cast<ObEmbeddingTask *>(task);

    LOG_INFO("dropping embedding task due to thread pool stop", K_(task_ref_cnt), KPC(embedding_task));

    if (embedding_task->get_task_id() != OB_INVALID_ID) {
      if (OB_FAIL(embedding_task->mark_task_failed(OB_ERR_UNEXPECTED))) {
        LOG_WARN("failed to update task status to failed", K(ret), KPC(embedding_task));
      }
    }

    if (embedding_task->need_callback()) {
      embedding_task->release_if_managed();
    } else {
      embedding_task->~ObEmbeddingTask();
    }

    remove_task_from_tracking(embedding_task);

    inc_dropped_task_cnt();
    dec_task_ref();
    LOG_INFO("task dropped and cleaned up", K_(task_ref_cnt));
  }
}

int ObEmbeddingTaskHandler::add_task_to_tracking(ObEmbeddingTask *task)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(task)) {
    common::ObSpinLockGuard guard(task_list_lock_);
    if (OB_FAIL(active_tasks_.push_back(task))) {
      LOG_WARN("failed to add task to tracking", K(ret), KP(task));
    } else {
      LOG_DEBUG("task added to tracking", KP(task), K_(task_ref_cnt));
    }
  }
  return ret;
}

int ObEmbeddingTaskHandler::remove_task_from_tracking(ObEmbeddingTask *task)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret));
  } else {
    common::ObSpinLockGuard guard(task_list_lock_);
    bool found = false;
    for (int64_t i = 0; !found && i < active_tasks_.count(); i++) {
      if (active_tasks_.at(i) == task) {
        found = true;
        if (OB_FAIL(active_tasks_.remove(i))) {
          LOG_WARN("failed to remove task from tracking", K(ret), KP(task));
        } else {
          LOG_DEBUG("task removed from tracking", KP(task), K_(task_ref_cnt));
        }
      }
    }
    if (!found) {
      LOG_WARN("task not found in tracking", K(*task), K_(task_ref_cnt));
    }
  }
  return ret;
}

//=============================================== HTTP Retry Helper Methods ================================================

bool ObEmbeddingTask::should_retry_http_request(int64_t http_error_code) const
{
  // Retry for transient errors that might succeed on retry
  switch (http_error_code) {
    // TODO: implement this
    // case 500: // Internal Server Error
    //   return http_retry_count_ < MAX_HTTP_RETRY_CNT;
    case 429:
    case 500:
    case 502:
    case 503:
    case 504:
      return http_retry_count_ < MAX_HTTP_RETRY_CNT;
    default:
      return false;
  }
}

bool ObEmbeddingTask::is_batch_size_related_error(int64_t http_error_code) const
{
  // These errors might be related to batch size being too large
  switch (http_error_code) {
    // TODO: implement this
    default:
      return false;
  }
}

int64_t ObEmbeddingTask::calculate_retry_interval() const
{
  // Exponential backoff with jitter
  int64_t base_interval = HTTP_RETRY_BASE_INTERVAL_US;
  int64_t max_interval = HTTP_RETRY_MAX_INTERVAL_US;
  int64_t multiplier = HTTP_RETRY_MULTIPLIER;

  int64_t interval = base_interval;
  for (int64_t i = 0; i < http_retry_count_ && interval < max_interval; i++) {
    interval *= multiplier;
  }

  // Add some jitter to avoid thundering herd
  int64_t jitter = (interval * (rand() % 100)) / 100;
  interval = OB_MIN(interval + jitter, max_interval);

  return interval;
}

int ObEmbeddingTask::adjust_batch_size_for_retry()
{
  int ret = OB_SUCCESS;

  if (batch_size_ > 1) {
    batch_size_ = batch_size_ - 1;
  } else {
    // batch size is 1, but still need to decrease it, maybe remote server is not ready
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("batch size can not be decreased", K(ret), K(batch_size_));
  }

  return ret;
}

void ObEmbeddingTask::reset_retry_state()
{
  http_retry_count_ = 0;
  http_retry_start_time_us_ = 0;
  http_last_retry_time_us_ = 0;
  need_retry_flag_ = false;
  // Try to increase batch size after successful request
  try_increase_batch_size();
}

int ObEmbeddingTask::map_http_error_to_internal_error(int64_t http_error_code) const
{
  switch (http_error_code) {
    case 400: // Bad Request
      return OB_INVALID_ARGUMENT;
    case 401: // Unauthorized
    case 403: // Forbidden
      return OB_ERR_NO_PRIVILEGE;
    case 404: // Not Found
      return OB_ENTRY_NOT_EXIST;
    case 408: // Request Timeout
      return OB_TIMEOUT;
    case 413: // Request Entity Too Large
      return OB_SIZE_OVERFLOW;
    case 429: // Too Many Requests
      return OB_ERR_UNEXPECTED;
    case 500: // Internal Server Error
    case 502: // Bad Gateway
    case 503: // Service Unavailable
    case 504: // Gateway Timeout
      return OB_ERR_UNEXPECTED;
    default:
      return OB_ERR_UNEXPECTED;
  }
}

void ObEmbeddingTask::try_increase_batch_size()
{
  if (batch_size_adjusted_ && batch_size_ < original_batch_size_) {
    batch_size_ = batch_size_ + 1;
    if (batch_size_ >= original_batch_size_) {
      batch_size_adjusted_ = false;
      batch_size_ = original_batch_size_;
    }
  }
}

void ObEmbeddingTask::disable_callback()
{
  if (OB_NOT_NULL(cb_handle_)) {
    cb_handle_->disable();
    LOG_DEBUG("callback disabled for task", KP(this), KP(cb_handle_));
  }
}

void ObEmbeddingTask::set_callback_done()
{
  ObThreadCondGuard guard(task_cond_);
  callback_done_ = true;
}

int ObEmbeddingTask::maybe_callback()
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(cb_handle_)) {
    const int64_t current_vectors_count = output_vectors_.count();
    const int64_t total_chunks = input_chunks_.count();

    if (OB_UNLIKELY(internal_error_code_ != OB_SUCCESS)) {
      if (OB_FAIL(cb_handle_->process())) {
        LOG_WARN("Failed to process IO callback on error", K(ret));
      }
    } else {
      if (OB_SUCC(ret) && current_vectors_count > 0 &&
          current_vectors_count >= process_callback_offset_ + CALLBACK_BATCH_SIZE) {
        LOG_DEBUG("Calling IO callback after processing batch",
                  K(current_vectors_count), K(CALLBACK_BATCH_SIZE), K(total_chunks),
                  K(process_callback_offset_));
        if (OB_FAIL(cb_handle_->process())) {
          LOG_WARN("Failed to process IO callback", K(ret));
        } else {
          process_callback_offset_ = current_vectors_count;
          LOG_DEBUG("Updated process_callback_offset_", K(process_callback_offset_));
        }
      }
      if (OB_SUCC(ret) && current_vectors_count == total_chunks &&
          phase_ == OB_EMBEDDING_TASK_DONE &&
          process_callback_offset_ < total_chunks) {
        LOG_DEBUG("Calling IO callback on task completion",
                  K(current_vectors_count), K(total_chunks), K(process_callback_offset_));
        if (OB_FAIL(cb_handle_->process())) {
          LOG_WARN("Failed to process IO callback on completion", K(ret));
        } else {
          process_callback_offset_ = total_chunks;
          LOG_DEBUG("Updated process_callback_offset_ to completion", K(process_callback_offset_));
        }
      }
    }
    // Set callback_done flag after callback finished
    set_callback_done();
  }
  return ret;
}

int ObEmbeddingTask::init_curl_handler(const ObString &model_url, const ObString &user_key)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(curl_multi_handle_ || curl_easy_handle_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("curl handles already initialized", K(ret), KPC(this));
  }else if (OB_ISNULL(curl_multi_handle_ = curl_multi_init())) {
    ret = OB_CURL_ERROR;
    LOG_WARN("failed to init curl multi handle", K(ret));
  } else if (OB_ISNULL(curl_easy_handle_ = curl_easy_init())) {
    ret = OB_CURL_ERROR;
    LOG_WARN("failed to init curl easy handle", K(ret));
  } else {
    char auth_header[512];
    snprintf(auth_header, sizeof(auth_header), "Authorization: Bearer %.*s",
             user_key.length(), user_key.ptr());

    curl_headers_ = nullptr;
    curl_headers_ = curl_slist_append(curl_headers_, "Content-Type: application/json");
    curl_headers_ = curl_slist_append(curl_headers_, auth_header);
    char *model_url_cstr = nullptr;
    if (OB_FAIL(ob_dup_cstring(allocator_, model_url, model_url_cstr))) { // add \0
      LOG_WARN("failed to duplicate model url", K(ret));
    } else if (OB_ISNULL(model_url_cstr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to duplicate model url", K(ret));
    } else {
      curl_easy_setopt(curl_easy_handle_, CURLOPT_URL, model_url_cstr);
      curl_easy_setopt(curl_easy_handle_, CURLOPT_HTTPHEADER, curl_headers_);
      curl_easy_setopt(curl_easy_handle_, CURLOPT_WRITEFUNCTION, ObEmbeddingTask::WriteMemoryCallback);

      curl_response_data_ = OB_NEWx(HttpResponseData, &allocator_, allocator_);
      if (OB_ISNULL(curl_response_data_)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to create response data structure", K(ret));
      } else {
        curl_easy_setopt(curl_easy_handle_, CURLOPT_WRITEDATA, (void *)curl_response_data_);

        curl_easy_setopt(curl_easy_handle_, CURLOPT_TIMEOUT, HTTP_REQUEST_TIMEOUT / 1000);
        curl_easy_setopt(curl_easy_handle_, CURLOPT_CONNECTTIMEOUT, HTTP_REQUEST_TIMEOUT / 1000);

        CURLMcode multi_res = curl_multi_add_handle(curl_multi_handle_, curl_easy_handle_);
        if (multi_res != CURLM_OK) {
          ret = OB_CURL_ERROR;
          LOG_WARN("failed to add easy handle to multi handle", K(ret), K(multi_res), K(*this));
        }
      }
    }
  }
  return ret;

}

int ObEmbeddingTask::wait_for_completion(const int64_t timeout_ms)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("task is not inited", K(ret));
  } else {
    ObThreadCondGuard guard(task_cond_);
    if (callback_done_) {
      // do nothing
    } else {
      if (OB_FAIL(task_cond_.wait(timeout_ms))) {
        LOG_WARN("failed to wait for completion", K(ret));
      }
    }
  }
  return ret;
}

int ObEmbeddingTask::wake_up()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("task is not inited", K(ret));
  } else {
    ObThreadCondGuard guard(task_cond_);
    if (OB_FAIL(task_cond_.signal())) {
      LOG_WARN("failed to signal completion cond", K(ret));
    }
  }
  return ret;
}
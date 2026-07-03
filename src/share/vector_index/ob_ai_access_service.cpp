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

#define USING_LOG_PREFIX SHARE

#include "share/vector_index/ob_ai_access_service.h"
#include "share/ai_service/ob_ai_table_poller.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/time/ob_time_utility.h"
#include "lib/ob_errno.h"
#include "lib/random/ob_random.h"
#include <curl/curl.h>
#include <fcntl.h>
#include <unistd.h>
#include "lib/string/ob_string.h"
#include "common/object/ob_object.h"
#include "deps/oblib/src/lib/allocator/ob_malloc.h"
#include "sql/engine/expr/ob_expr_ai/ob_ai_func_utils.h"
#include "lib/json_type/ob_json_base.h"
#include "storage/tmp_file/ob_tmp_file_manager.h"
#include "share/ai_service/ob_batch_file_jsonl_writer.h"
#include "share/ai_service/ob_batch_file_jsonl_iterator.h"
#include "observer/omt/ob_tenant_config_mgr.h"

namespace oceanbase
{
namespace vector_index
{

using namespace share;

// Forward declaration
class ObAiTaskScheduler;

//=============================================== ObAiSchedulableTask Implementation ================================================

int ObAiSchedulableTask::init(common::ObIAllocator &allocator,
                              ObAiTaskPriority priority,
                              ObAiSchedulableTaskType task_type)
{
  int ret = OB_SUCCESS;
  UNUSED(allocator);
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
  } else if (OB_UNLIKELY(priority <= OB_AI_TASK_PRIORITY_INVALID ||
                         priority >= OB_AI_TASK_PRIORITY_MAX)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid priority", K(ret), K(priority));
  } else if (OB_UNLIKELY(task_type <= OB_AI_SCHEDULABLE_TASK_TYPE_INVALID ||
                         task_type >= OB_AI_SCHEDULABLE_TASK_TYPE_MAX)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid task_type", K(ret), K(task_type));
  } else {
    priority_ = priority;
    task_type_ = task_type;
    state_ = OB_AI_TASK_STATUS_PENDING;
  }
  return ret;
}

//=============================================== ObAiTaskScheduler Implementation ================================================

// String constants for enum conversion
static const char *TASK_TYPE_STR[] = {
  "INVALID",
  "EXECUTION_TASK",
  "TABLE_POLLER"
};

static const char *PRIORITY_STR[] = {
  "INVALID",
  "HIGH",
  "NORMAL",
  "LOW"
};

ObAiTaskScheduler::ObAiTaskScheduler()
  : is_inited_(false),
    is_running_(false),
    lock_(common::ObLatchIds::OB_EMBEDDING_TASK_HANDLER_SPIN_LOCK)
{
}

ObAiTaskScheduler::~ObAiTaskScheduler()
{
  destroy();
}

int ObAiTaskScheduler::init(int64_t thread_num, int64_t queue_size_pow2)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObAiTaskScheduler init twice", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(thread_num <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid thread_num", K(ret), K(thread_num));
  } else {
    // Initialize thread pool
    if (OB_FAIL(thread_pool_.init_and_start(thread_num, queue_size_pow2))) {
      LOG_WARN("failed to init thread pool", K(ret), K(thread_num), K(queue_size_pow2));
    } else if (OB_FAIL(timer_.init_and_start(thread_pool_, TIMER_PRECISION_US, "AiTaskTimer"))) {
      LOG_WARN("failed to init timer", K(ret));
      thread_pool_.destroy();
    } else {
      is_inited_ = true;
      is_running_ = true;
      LOG_INFO("ObAiTaskScheduler init success", K(thread_num), K(queue_size_pow2));
    }
  }
  return ret;
}

int ObAiTaskScheduler::start()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObAiTaskScheduler not init", K(ret), K(is_inited_));
  } else {
    is_running_ = true;
    LOG_INFO("ObAiTaskScheduler started");
  }
  return ret;
}

void ObAiTaskScheduler::stop()
{
  if (is_inited_) {
    is_running_ = false;
    timer_.stop();
    thread_pool_.stop();
    LOG_INFO("ObAiTaskScheduler stopped");
  }
}

void ObAiTaskScheduler::wait()
{
  if (is_inited_) {
    timer_.wait();
    thread_pool_.wait();
    LOG_INFO("ObAiTaskScheduler wait completed");
  }
}

void ObAiTaskScheduler::destroy()
{
  if (is_inited_) {
    stop();
    wait();
    timer_.destroy();
    thread_pool_.destroy();
    is_inited_ = false;
    is_running_ = false;
    LOG_INFO("ObAiTaskScheduler destroyed");
  }
}

int ObAiTaskScheduler::schedule_task(ObAiSchedulableTask &task)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObAiTaskScheduler not init", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(!is_running_)) {
    ret = OB_NOT_RUNNING;
    LOG_WARN("ObAiTaskScheduler not running", K(ret), K(is_running_));
  } else if (OB_UNLIKELY(!task.is_inited())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("task not initialized", K(ret));
  } else if (OB_UNLIKELY(task.is_terminal_state())) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("task already in terminal state", K(ret), K(task.get_state()));
  } else {
    // Set scheduler reference
    task.set_scheduler(this);

    // Update task state
    if (OB_AI_TASK_STATUS_PENDING == task.get_state()) {
      task.set_state(OB_AI_TASK_STATUS_RUNNING);
    }

    // Create task wrapper and submit to thread pool
    TaskWrapper wrapper(&task, this);
    common::ObFuture<bool> future;
    common::occam::TASK_PRIORITY pool_priority = map_priority_(task.get_priority());

    switch (pool_priority) {
      case common::occam::TASK_PRIORITY::EXTREMELY_HIGH:
      case common::occam::TASK_PRIORITY::HIGH:
        ret = thread_pool_.commit_task<common::occam::TASK_PRIORITY::HIGH>(future, wrapper);
        break;
      case common::occam::TASK_PRIORITY::NORMAL:
        ret = thread_pool_.commit_task<common::occam::TASK_PRIORITY::NORMAL>(future, wrapper);
        break;
      case common::occam::TASK_PRIORITY::LOW:
      case common::occam::TASK_PRIORITY::EXTREMELY_LOW:
        ret = thread_pool_.commit_task<common::occam::TASK_PRIORITY::LOW>(future, wrapper);
        break;
      default:
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid priority", K(ret), K(task.get_priority()));
        break;
    }

    if (OB_FAIL(ret)) {
      LOG_WARN("failed to schedule task", K(ret), K(task), K(task.get_priority()));
    }
  }
  return ret;
}

int ObAiTaskScheduler::schedule_after(ObAiSchedulableTask &task, int64_t delay_us)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObAiTaskScheduler not init", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(!is_running_)) {
    ret = OB_NOT_RUNNING;
    LOG_WARN("ObAiTaskScheduler not running", K(ret), K(is_running_));
  } else if (OB_UNLIKELY(!task.is_inited())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("task not initialized", K(ret));
  } else if (OB_UNLIKELY(task.is_terminal_state())) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("task already in terminal state", K(ret), K(task.get_state()));
  } else if (OB_UNLIKELY(delay_us < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid delay_us", K(ret), K(delay_us));
  } else {
    // Set scheduler reference
    task.set_scheduler(this);

    // Create a task wrapper function for delayed scheduling
    auto delayed_func = [&task, this]() -> bool {
      int ret = OB_SUCCESS;
      if (OB_UNLIKELY(!is_running_)) {
        LOG_WARN("scheduler not running when delayed task executes", K(is_running_));
        task.on_cancelled();
      } else if (task.is_terminal_state()) {
        LOG_DEBUG("task already in terminal state, skip execution", K(task.get_state()));
      } else {
        // Execute the task
        bool should_stop = execute_task_(&task);

        // If task needs reschedule, schedule it again
        if (!should_stop && task.need_reschedule() && !task.is_terminal_state()) {
          int64_t next_delay = task.get_reschedule_delay_us();
          if (OB_FAIL(schedule_after(task, next_delay))) {
            LOG_WARN("failed to reschedule task", K(ret), K(task));
            task.on_failed(ret);
          }
        }
      }
      return false;
    };

    // Map priority for timer scheduling
    // Use schedule_task_ignore_handle_after to avoid the task being cancelled
    // when the handle goes out of scope (ObOccamTimerTaskRAIIHandle destructor calls stop_and_wait)
    common::occam::TASK_PRIORITY timer_priority = map_priority_(task.get_priority());

    switch (timer_priority) {
      case common::occam::TASK_PRIORITY::EXTREMELY_HIGH:
      case common::occam::TASK_PRIORITY::HIGH:
        ret = timer_.schedule_task_ignore_handle_after<common::occam::TASK_PRIORITY::HIGH>(
            delay_us, delayed_func);
        break;
      case common::occam::TASK_PRIORITY::NORMAL:
        ret = timer_.schedule_task_ignore_handle_after<common::occam::TASK_PRIORITY::NORMAL>(
            delay_us, delayed_func);
        break;
      case common::occam::TASK_PRIORITY::LOW:
      case common::occam::TASK_PRIORITY::EXTREMELY_LOW:
        ret = timer_.schedule_task_ignore_handle_after<common::occam::TASK_PRIORITY::LOW>(
            delay_us, delayed_func);
        break;
      default:
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid priority", K(ret), K(task.get_priority()));
        break;
    }

    if (OB_SUCC(ret)) {
      LOG_DEBUG("task scheduled after delay", K(task), K(delay_us), K(task.get_priority()));
    } else {
      LOG_WARN("failed to schedule task after delay", K(ret), K(task), K(delay_us));
    }
  }
  return ret;
}

int64_t ObAiTaskScheduler::get_running_task_count() const
{
  return 0;
}

common::occam::TASK_PRIORITY ObAiTaskScheduler::map_priority_(ObAiTaskPriority priority) const
{
  common::occam::TASK_PRIORITY mapped = common::occam::TASK_PRIORITY::NORMAL;
  switch (priority) {
    case OB_AI_TASK_PRIORITY_HIGH:
      mapped = common::occam::TASK_PRIORITY::HIGH;
      break;
    case OB_AI_TASK_PRIORITY_NORMAL:
      mapped = common::occam::TASK_PRIORITY::NORMAL;
      break;
    case OB_AI_TASK_PRIORITY_LOW:
      mapped = common::occam::TASK_PRIORITY::LOW;
      break;
    default:
      mapped = common::occam::TASK_PRIORITY::NORMAL;
      break;
  }
  return mapped;
}

bool ObAiTaskScheduler::execute_task_(ObAiSchedulableTask *task)
{
  bool should_stop = false;
  int ret = OB_SUCCESS;

  if (OB_ISNULL(task)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("task is null in execute_task_", K(ret));
    should_stop = true;
  } else if (task->is_terminal_state()) {
    LOG_DEBUG("task already in terminal state", K(task->get_state()));
    should_stop = true;
  } else {
    if (OB_AI_TASK_STATUS_PENDING == task->get_state()) {
      task->set_state(OB_AI_TASK_STATUS_RUNNING);
    }

    ret = task->do_work();

    if (OB_SUCCESS == ret) {
      if (task->is_terminal_state()) {
        LOG_DEBUG("task completed with terminal state", K(task->get_state()));
      } else if (task->need_reschedule()) {
        LOG_DEBUG("task needs reschedule", K(*task));
      } else {
        task->on_finished();
        LOG_DEBUG("task finished successfully", K(*task));
      }
    } else {
      task->on_failed(ret);
      LOG_WARN("task failed", K(ret), K(*task));
    }

    if (OB_AI_TASK_STATUS_CANCELLED == task->get_state()) {
      should_stop = true;
    }
  }

  return should_stop;
}

// TaskWrapper implementation
bool ObAiTaskScheduler::TaskWrapper::operator()()
{
  int ret = OB_SUCCESS;
  bool should_stop = false;
  if (OB_ISNULL(task_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("task is null in TaskWrapper", K(ret));
    should_stop = true;
  } else if (OB_ISNULL(scheduler_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("scheduler is null in TaskWrapper", K(ret));
    task_->on_failed(ret);
    should_stop = true;
  } else {
    should_stop = scheduler_->execute_task_(task_);

    if (!should_stop && task_->need_reschedule() && !task_->is_terminal_state()) {
      int64_t next_delay = task_->get_reschedule_delay_us();
      if (OB_FAIL(scheduler_->schedule_after(*task_, next_delay))) {
        LOG_WARN("failed to reschedule task", K(ret), K(*task_));
        task_->on_failed(ret);
      }
    }
  }
  return should_stop;
}

// ObAiSchedulerUtils implementation
const char *ObAiSchedulerUtils::get_task_type_str(ObAiSchedulableTaskType type)
{
  if (type >= OB_AI_SCHEDULABLE_TASK_TYPE_INVALID && type < OB_AI_SCHEDULABLE_TASK_TYPE_MAX) {
    return TASK_TYPE_STR[type];
  }
  return "UNKNOWN";
}

const char *ObAiSchedulerUtils::get_priority_str(ObAiTaskPriority priority)
{
  if (priority >= OB_AI_TASK_PRIORITY_INVALID && priority < OB_AI_TASK_PRIORITY_MAX) {
    return PRIORITY_STR[priority];
  }
  return "UNKNOWN";
}

//=============================================== ObAiAccessTaskPhaseManager Implementation ================================================

// Valid phase transitions from each state
const ObAiTaskPhase ObAiAccessTaskPhaseManager::VALID_TRANSITIONS_FROM_INIT[] = {
  OB_AI_TASK_PHASE_HTTP_SENT,
  OB_AI_TASK_PHASE_FILE_UPLOADING, // For BATCH_FILE mode with pre-generated JSONL
  OB_AI_TASK_PHASE_DONE
};

const ObAiTaskPhase ObAiAccessTaskPhaseManager::VALID_TRANSITIONS_FROM_HTTP_SENT[] = {
  OB_AI_TASK_PHASE_HTTP_COMPLETED,
  OB_AI_TASK_PHASE_DONE
};

const ObAiTaskPhase ObAiAccessTaskPhaseManager::VALID_TRANSITIONS_FROM_HTTP_COMPLETED[] = {
  OB_AI_TASK_PHASE_PARSED,
  OB_AI_TASK_PHASE_DONE
};

const ObAiTaskPhase ObAiAccessTaskPhaseManager::VALID_TRANSITIONS_FROM_PARSED[] = {
  OB_AI_TASK_PHASE_INIT,
  OB_AI_TASK_PHASE_DONE
};

const ObAiTaskPhase ObAiAccessTaskPhaseManager::VALID_TRANSITIONS_FROM_DONE[] = {
};

// BATCH_FILE mode phase transitions
const ObAiTaskPhase VALID_TRANSITIONS_FROM_FILE_UPLOADING[] = {
  OB_AI_TASK_PHASE_BATCH_SUBMITTING,
  OB_AI_TASK_PHASE_DONE
};

const ObAiTaskPhase VALID_TRANSITIONS_FROM_BATCH_SUBMITTING[] = {
  OB_AI_TASK_PHASE_BATCH_POLLING,
  OB_AI_TASK_PHASE_DONE
};

const ObAiTaskPhase VALID_TRANSITIONS_FROM_BATCH_POLLING[] = {
  OB_AI_TASK_PHASE_RESULT_DOWNLOADING,
  OB_AI_TASK_PHASE_DONE
};

const ObAiTaskPhase VALID_TRANSITIONS_FROM_RESULT_DOWNLOADING[] = {
  OB_AI_TASK_PHASE_DONE
};

bool ObAiAccessTaskPhaseManager::is_valid_transition(ObAiTaskPhase from_phase, ObAiTaskPhase to_phase)
{
  bool valid = false;
  if (from_phase < OB_AI_TASK_PHASE_INIT || from_phase > OB_AI_TASK_PHASE_RESULT_DOWNLOADING ||
      to_phase < OB_AI_TASK_PHASE_INIT || to_phase > OB_AI_TASK_PHASE_RESULT_DOWNLOADING) {
    valid = false;
  } else {
    const ObAiTaskPhase *transitions = nullptr;
    int64_t count = 0;

    switch (from_phase) {
      case OB_AI_TASK_PHASE_INIT:
        transitions = VALID_TRANSITIONS_FROM_INIT;
        count = sizeof(VALID_TRANSITIONS_FROM_INIT) / sizeof(VALID_TRANSITIONS_FROM_INIT[0]);
        break;
      case OB_AI_TASK_PHASE_HTTP_SENT:
        transitions = VALID_TRANSITIONS_FROM_HTTP_SENT;
        count = sizeof(VALID_TRANSITIONS_FROM_HTTP_SENT) / sizeof(VALID_TRANSITIONS_FROM_HTTP_SENT[0]);
        break;
      case OB_AI_TASK_PHASE_HTTP_COMPLETED:
        transitions = VALID_TRANSITIONS_FROM_HTTP_COMPLETED;
        count = sizeof(VALID_TRANSITIONS_FROM_HTTP_COMPLETED) / sizeof(VALID_TRANSITIONS_FROM_HTTP_COMPLETED[0]);
        break;
      case OB_AI_TASK_PHASE_PARSED:
        transitions = VALID_TRANSITIONS_FROM_PARSED;
        count = sizeof(VALID_TRANSITIONS_FROM_PARSED) / sizeof(VALID_TRANSITIONS_FROM_PARSED[0]);
        break;
      case OB_AI_TASK_PHASE_DONE:
        transitions = VALID_TRANSITIONS_FROM_DONE;
        count = 0;
        break;
      case OB_AI_TASK_PHASE_FILE_UPLOADING:
        transitions = VALID_TRANSITIONS_FROM_FILE_UPLOADING;
        count = sizeof(VALID_TRANSITIONS_FROM_FILE_UPLOADING) / sizeof(VALID_TRANSITIONS_FROM_FILE_UPLOADING[0]);
        break;
      case OB_AI_TASK_PHASE_BATCH_SUBMITTING:
        transitions = VALID_TRANSITIONS_FROM_BATCH_SUBMITTING;
        count = sizeof(VALID_TRANSITIONS_FROM_BATCH_SUBMITTING) / sizeof(VALID_TRANSITIONS_FROM_BATCH_SUBMITTING[0]);
        break;
      case OB_AI_TASK_PHASE_BATCH_POLLING:
        transitions = VALID_TRANSITIONS_FROM_BATCH_POLLING;
        count = sizeof(VALID_TRANSITIONS_FROM_BATCH_POLLING) / sizeof(VALID_TRANSITIONS_FROM_BATCH_POLLING[0]);
        break;
      case OB_AI_TASK_PHASE_RESULT_DOWNLOADING:
        transitions = VALID_TRANSITIONS_FROM_RESULT_DOWNLOADING;
        count = sizeof(VALID_TRANSITIONS_FROM_RESULT_DOWNLOADING) / sizeof(VALID_TRANSITIONS_FROM_RESULT_DOWNLOADING[0]);
        break;
      default:
        break;
    }

    for (int64_t i = 0; i < count && !valid; ++i) {
      if (transitions[i] == to_phase) {
        valid = true;
      }
    }
  }
  return valid;
}

const char* ObAiAccessTaskPhaseManager::get_phase_str(ObAiTaskPhase phase)
{
  const char *str = "UNKNOWN";
  switch (phase) {
    case OB_AI_TASK_PHASE_INIT:
      str = "INIT";
      break;
    case OB_AI_TASK_PHASE_HTTP_SENT:
      str = "HTTP_SENT";
      break;
    case OB_AI_TASK_PHASE_HTTP_COMPLETED:
      str = "HTTP_COMPLETED";
      break;
    case OB_AI_TASK_PHASE_PARSED:
      str = "PARSED";
      break;
    case OB_AI_TASK_PHASE_DONE:
      str = "DONE";
      break;
    case OB_AI_TASK_PHASE_FILE_UPLOADING:
      str = "FILE_UPLOADING";
      break;
    case OB_AI_TASK_PHASE_BATCH_SUBMITTING:
      str = "BATCH_SUBMITTING";
      break;
    case OB_AI_TASK_PHASE_BATCH_POLLING:
      str = "BATCH_POLLING";
      break;
    case OB_AI_TASK_PHASE_RESULT_DOWNLOADING:
      str = "RESULT_DOWNLOADING";
      break;
    default:
      str = "UNKNOWN";
      break;
  }
  return str;
}

ObAiTaskStatus ObAiAccessTaskPhaseManager::map_phase_to_status(ObAiTaskPhase phase, int result_code)
{
  ObAiTaskStatus status = OB_AI_TASK_STATUS_INVALID;
  switch (phase) {
    case OB_AI_TASK_PHASE_INIT:
      status = OB_AI_TASK_STATUS_PENDING;
      break;
    case OB_AI_TASK_PHASE_HTTP_SENT:
    case OB_AI_TASK_PHASE_HTTP_COMPLETED:
    case OB_AI_TASK_PHASE_PARSED:
      status = OB_AI_TASK_STATUS_RUNNING;
      break;
    case OB_AI_TASK_PHASE_DONE:
      status = (result_code == OB_SUCCESS) ? OB_AI_TASK_STATUS_FINISHED : OB_AI_TASK_STATUS_FAILED;
      break;
    default:
      status = OB_AI_TASK_STATUS_INVALID;
      break;
  }
  return status;
}

//=============================================== ObAiHttpResponseData Implementation ================================================

int ObAiHttpResponseData::allocate(int64_t size)
{
  int ret = OB_SUCCESS;
  if (size <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid size for response allocation", K(ret), K(size));
  } else {
    if (OB_NOT_NULL(data_)) {
      allocator_.free(data_);
      data_ = nullptr;
      size_ = 0;
    }
    data_ = static_cast<char*>(allocator_.alloc(size + 1));
    if (OB_ISNULL(data_)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("Failed to allocate response buffer", K(ret), K(size));
    } else {
      MEMSET(data_, 0, size + 1);
      size_ = 0;
    }
  }
  return ret;
}

int ObAiHttpResponseData::append_data(const char *src, int64_t len)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(src) || len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid arguments for append data", K(ret), KP(src), K(len));
  } else if (OB_ISNULL(data_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Response data not allocated", K(ret));
  } else {
    MEMCPY(data_ + size_, src, len);
    size_ += len;
    data_[size_] = '\0';
  }
  return ret;
}

//=============================================== ObAiAccessTask Implementation ================================================

ObAiAccessTask::ObAiAccessTask()
  : ObAiSchedulableTask(),
    ai_execution_mode_(OB_AI_ACCESS_MODE_BATCH_FILE),
    command_type_(OB_AI_COMMAND_EMBED),
    http_timeout_us_(DEFAULT_HTTP_TIMEOUT_US),
    batch_size_(DEFAULT_BATCH_SIZE),
    provider_(nullptr),
    phase_(OB_AI_TASK_PHASE_INIT),
    batch_file_url_(),
    api_key_(),
    model_name_(),
    provider_name_(),
    current_offset_(0),
    total_count_(0),
    last_error_ctx_(),
    http_response_(nullptr),
    local_allocator_("AiExecTask", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
    service_(nullptr),
    batch_file_manager_(nullptr),
    current_file_id_(),
    current_batch_id_(),
    output_file_id_(),
    error_file_id_(),
    jsonl_fd_(-1),
    jsonl_size_(0),
    jsonl_line_count_(0),
    result_fd_(-1),
    result_size_(0),
    result_line_count_(0),
    last_poll_time_us_(0),
    batch_status_(OB_AI_BATCH_FILE_STATUS_INVALID),
    // Streaming result read members
    result_iter_(nullptr),
    parse_offset_(0),
    accumulated_prompt_tokens_(0),
    accumulated_completion_tokens_(0),
    accumulated_total_tokens_(0),
    reschedule_delay_us_(0),
    retry_count_(0),
    terminal_ts_(0),
    batch_submit_time_us_(0),
    is_archived_(false),
    pin_count_(0),
    cancel_requested_(false),
    abandoned_(false),
    allow_null_on_failure_(false),
    submitted_count_(0)
{
}

ObAiAccessTask::~ObAiAccessTask()
{
  reset();
}

// Guard override: caller passed wrong init signature; the real init is the 9-arg one below.
int ObAiAccessTask::init(common::ObIAllocator &allocator,
                         ObAiTaskPriority priority,
                         ObAiSchedulableTaskType task_type)
{
  UNUSED(allocator);
  UNUSED(priority);
  UNUSED(task_type);
  LOG_ERROR_RET(common::OB_ERR_UNEXPECTED,
                "wrong init() called on ObAiAccessTask; use the 9-arg init() instead");
  return common::OB_ERR_UNEXPECTED;
}

int ObAiAccessTask::init(common::ObIAllocator &allocator,
                            ObAiAccessMode access_mode,
                            ObAiCommandType command_type,
                            int64_t total_count,
                            const ObAiModelEndpointInfo &endpoint_info,
                            const common::ObString &task_id,
                            ObAiSystemTableManager *table_manager,
                            common::ObISQLClient *sql_client,
                            bool allow_null_on_failure)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObAiAccessTask already inited", K(ret));
  } else if (OB_UNLIKELY(total_count <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("total_count must be positive", K(ret), K(total_count));
  } else if (OB_FAIL(endpoint_info.check_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid endpoint info", K(ret), K(endpoint_info));
  } else {
    if (OB_FAIL(ObAiSchedulableTask::init(allocator,
                                           OB_AI_TASK_PRIORITY_NORMAL,
                                           OB_AI_SCHEDULABLE_TASK_TYPE_EXECUTION_TASK))) {
      LOG_WARN("Failed to init base class", K(ret));
    } else if (OB_FAIL(ob_write_string(local_allocator_, endpoint_info.get_batch_file_url(), batch_file_url_, true))) {
      LOG_WARN("Failed to copy batch file url", K(ret));
    } else if (OB_FAIL(endpoint_info.get_unencrypted_access_key(local_allocator_, api_key_))) {
      LOG_WARN("Failed to get unencrypted access key", K(ret));
    } else if (OB_FAIL(ob_write_string(local_allocator_, endpoint_info.get_ai_model_name(), model_name_, true))) {
      LOG_WARN("Failed to copy model name", K(ret));
    } else if (OB_FAIL(ob_write_string(local_allocator_, endpoint_info.get_provider(), provider_name_, true))) {
      LOG_WARN("Failed to copy provider name", K(ret));
    } else if (OB_FAIL(ob_write_string(local_allocator_, task_id, task_id_, true))) {
      LOG_WARN("Failed to copy task_id", K(ret));
    } else {
      table_manager_ = table_manager;
      sql_client_ = sql_client;

      ai_execution_mode_ = access_mode;
      allow_null_on_failure_ = allow_null_on_failure;
      command_type_ = command_type;
      total_count_ = total_count;
      submitted_count_ = 0;
      current_offset_ = 0;
      phase_ = OB_AI_TASK_PHASE_INIT;

      // Task self-owns its provider, allocated on local_allocator_.
      // Only EMBED command type needs a provider for batch-file mode.
      if (OB_AI_COMMAND_EMBED == command_type_) {
        common::ObAIFuncIEmbed *embed_provider = nullptr;
        if (OB_FAIL(common::ObAIFuncUtils::get_embed_provider(local_allocator_, provider_name_, embed_provider))) {
          LOG_WARN("Failed to create embed provider for task", K(ret), K_(provider_name));
        } else {
          provider_ = embed_provider;
        }
      }

      if (OB_SUCC(ret)) {
        void *buf = local_allocator_.alloc(sizeof(ObAiHttpResponseData));
        if (OB_ISNULL(buf)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("Failed to allocate http response data", K(ret));
        } else {
          http_response_ = new (buf) ObAiHttpResponseData(local_allocator_);
          is_inited_ = true;
        }
      }
    }
  }
  return ret;
}

int ObAiAccessTask::get_next_result(int64_t batch_size,
                                    common::ObIArray<share::ObAiResultRow> &results,
                                    bool &has_more)
{
  int ret = OB_SUCCESS;
  results.reset();
  has_more = false;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObAiAccessTask not inited", K(ret));
  } else if (ATOMIC_LOAD(&phase_) != OB_AI_TASK_PHASE_DONE) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("Task not in DONE phase", K(ret), K(phase_));
  } else if (result_fd_ < 0 || result_size_ <= 0) {
    // allow_null_on_failure_: task completed via degraded-finish without downloading
    // results. Empty results is expected — caller signed up for NULL vectors on failure.
    if (!allow_null_on_failure_) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("result_fd is invalid", K(ret), K_(result_fd), K_(result_size));
    }
  } else {
    // Create result iterator if not already created (lazy init, persists across calls)
    if (OB_ISNULL(result_iter_)) {
      uint64_t tenant_id = MTL_ID();
      void *buf = local_allocator_.alloc(sizeof(share::ObBatchFileJsonlIterator));
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("Failed to allocate result iterator", K(ret));
      } else {
        result_iter_ = new (buf) share::ObBatchFileJsonlIterator();
        if (OB_FAIL(result_iter_->init(result_fd_, 0, result_size_, tenant_id))) {
          LOG_WARN("Failed to init result iterator", K(ret),
                   K_(result_fd), K_(result_size));
          result_iter_->~ObBatchFileJsonlIterator();
          local_allocator_.free(result_iter_);
          result_iter_ = nullptr;
        }
      }
    }

    if (OB_FAIL(ret)) {
    } else {
      int64_t lines_read = 0;
      // Use a temporary allocator for per-line JSON parsing intermediates
      // (custom_id_, response_body_, etc.) to avoid accumulation in local_allocator_
      common::ObArenaAllocator line_parse_alloc("LineParseA", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());

      while (OB_SUCC(ret) && lines_read < batch_size) {
        line_parse_alloc.reuse();  // Free all intermediate parse data from previous iteration
        common::ObString line;
        ret = result_iter_->get_next_line(line);
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          has_more = false;
          break;
        } else if (OB_FAIL(ret)) {
          LOG_WARN("Failed to read line from result iterator", K(ret), K_(parse_offset));
        } else if (line.empty()) {
          continue;  // Skip empty lines
        } else {
          common::ObSEArray<share::ObAiBatchLineResult, 1> line_results;
          if (OB_FAIL(share::ObAiBatchFileManager::parse_jsonl_results_from_buffer(
                        line_parse_alloc, line.ptr(), line.length(), line_results))) {
            // Log first 256 bytes of the bad line to diagnose parse failure
            int64_t dump_len = MIN(line.length(), 256);
            LOG_WARN("Failed to parse result line", K(ret), K_(parse_offset), K(lines_read),
                     "line_len", line.length(),
                     "line_prefix", common::ObString(dump_len, line.ptr()));
          } else if (line_results.empty()) {
            continue;  // Skip unparseable line
          } else {
            const share::ObAiBatchLineResult &line_result = line_results.at(0);
            share::ObAiResultRow row;
          if (OB_FAIL(process_result_line_(line_result, row))) {
            LOG_WARN("Failed to process result line", K(ret), K_(command_type), K_(parse_offset));
            row.ret_code_ = ret;
            ret = OB_SUCCESS;  // Don't fail the whole batch
          }

            if (OB_SUCC(ret)) {
              if (OB_FAIL(results.push_back(row))) {
                LOG_WARN("Failed to push result row", K(ret));
              } else {
                lines_read++;
                parse_offset_++;
              }
            }
          }
        }
      }

      // Check if there's more data after reading batch_size lines
      if (OB_SUCC(ret) && lines_read == batch_size) {
        has_more = result_iter_->has_more();
      }

      // D5: detect missing result rows — provider returned fewer entries than submitted.
      if (OB_SUCC(ret) && !has_more && submitted_count_ > 0 && parse_offset_ < submitted_count_) {
        LOG_WARN("[BATCH-FILE] result JSONL has fewer rows than submitted (D5)",
                 K_(task_id), K_(parse_offset), K_(submitted_count),
                 "missing_count", submitted_count_ - parse_offset_);
      }

      // Token stats are accumulated in memory and written to history table by Poller GC.

      LOG_DEBUG("[BATCH-FILE] get_next_result", K_(task_id), K(lines_read),
               K_(parse_offset), K(has_more), K(batch_size),
               K_(accumulated_prompt_tokens), K_(accumulated_completion_tokens),
               K_(accumulated_total_tokens));
    }
  }

  return ret;
}

void ObAiAccessTask::reset()
{
  if (OB_NOT_NULL(batch_file_manager_)) {
    batch_file_manager_->~ObAiBatchFileManager();
    local_allocator_.free(batch_file_manager_);
    batch_file_manager_ = nullptr;
  }

  // Clean up TmpFileManager fds before destroying iterators/writers
  // Note: cleanup_tmp_files_ uses MTL_ID() which may not be valid in all contexts,
  // but fd cleanup is best-effort during reset
  if (jsonl_fd_ >= 0 || result_fd_ >= 0) {
    cleanup_tmp_files_();
  }

  // Destroy result iterator if allocated
  if (OB_NOT_NULL(result_iter_)) {
    result_iter_->~ObBatchFileJsonlIterator();
    local_allocator_.free(result_iter_);
    result_iter_ = nullptr;
  }

  current_file_id_.reset();
  current_batch_id_.reset();
  output_file_id_.reset();
  error_file_id_.reset();
  jsonl_fd_ = -1;
  jsonl_size_ = 0;
  jsonl_line_count_ = 0;
  result_fd_ = -1;
  result_size_ = 0;
  result_line_count_ = 0;
  last_poll_time_us_ = 0;
  batch_status_ = OB_AI_BATCH_FILE_STATUS_INVALID;
  reschedule_delay_us_ = 0;

  if (OB_NOT_NULL(http_response_)) {
    http_response_->~ObAiHttpResponseData();
    local_allocator_.free(http_response_);
    http_response_ = nullptr;
  }

  batch_file_url_.reset();
  api_key_.reset();
  model_name_.reset();
  provider_name_.reset();

  current_offset_ = 0;
  total_count_ = 0;
  last_error_ctx_.reset();
  phase_ = OB_AI_TASK_PHASE_INIT;
  ai_execution_mode_ = OB_AI_ACCESS_MODE_BATCH_FILE;
  command_type_ = OB_AI_COMMAND_EMBED;
  if (OB_NOT_NULL(provider_)) {
    provider_->~ObAIFuncBase();
    provider_ = nullptr;
  }
  parse_offset_ = 0;
  accumulated_prompt_tokens_ = 0;
  accumulated_completion_tokens_ = 0;
  accumulated_total_tokens_ = 0;
  terminal_ts_ = 0;
  batch_submit_time_us_ = 0;
  pin_count_ = 0;
  ATOMIC_STORE(&cancel_requested_, false);
  ATOMIC_STORE(&abandoned_, false);
  service_ = nullptr;

  local_allocator_.reset();
  ObAiSchedulableTask::reset();
}

int ObAiAccessTask::do_work()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObAiAccessTask not inited", K(ret));
  } else if (phase_ == OB_AI_TASK_PHASE_DONE) {
    LOG_DEBUG("Task already completed", K(phase_), K(current_offset_), K(total_count_));
  } else if (is_cancel_requested()) {
    // DDL exited — terminate immediately. Remote batch (if any) is cleaned up by Poller.
    LOG_INFO("[BATCH-FILE] cancel requested, terminating",
             K_(task_id), K_(phase), K_(current_batch_id));
    if (OB_FAIL(complete_with_cancel_())) {
      LOG_WARN("Failed to complete with cancel", K(ret), K_(task_id));
    }
  } else {
    // Clear any pending retry delay so stale OB_EAGAIN delay does not linger
    // across invocations.  If this attempt also hits OB_EAGAIN the value will
    // be set again in the retry path below.
    reschedule_delay_us_ = 0;
    // Execution phases: FILE_UPLOADING → BATCH_SUBMITTING → BATCH_POLLING → RESULT_DOWNLOADING → DONE
    if (phase_ != OB_AI_TASK_PHASE_DONE) {
      LOG_DEBUG("[BATCH-FILE] processing execution task", K(phase_), K(current_offset_), K(total_count_), K_(ai_execution_mode));

      bool continue_processing = true;
      while (OB_SUCC(ret) && continue_processing && phase_ != OB_AI_TASK_PHASE_DONE) {
        LOG_DEBUG("[BATCH-FILE] do_work loop iteration",
                 K_(task_id), K_(phase), K_(state),
                 "phase_str", ObAiAccessTaskPhaseManager::get_phase_str(phase_));
        if (is_cancel_requested()) {
          LOG_INFO("[BATCH-FILE] cancel requested in do_work loop, terminating",
                   K_(task_id), K_(phase), K_(current_batch_id));
          if (OB_FAIL(complete_with_cancel_())) {
            LOG_WARN("Failed to complete with cancel in do_work loop", K(ret), K_(task_id));
          }
          continue_processing = false;
        } else if (OB_FAIL(advance_current_mode_())) {
          LOG_WARN("failed to advance current access mode", K(ret), K_(ai_execution_mode), K_(phase));
        } else if (phase_ == OB_AI_TASK_PHASE_BATCH_POLLING) {
          continue_processing = false;
        } else if (phase_ == OB_AI_TASK_PHASE_DONE) {
          continue_processing = false;
        }
      }
    }

    if (OB_FAIL(ret)) {
      int64_t max_retry_count = ObAiRetryManager::MAX_RETRY_COUNT;
      {
        omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
        if (tenant_config.is_valid()) {
          int64_t cfg = tenant_config->model_max_retries;
          // cfg==0 means unconfigured; default to 60 to prevent infinite retry blocking
          max_retry_count = (cfg == 0) ? 60 : cfg;
        }
      }
      const bool is_rate_limit = (OB_EAGAIN == ret);
      if (is_retryable_error_(ret) && (is_rate_limit || retry_count_ < max_retry_count)) {
        if (!is_rate_limit) {
          retry_count_++;
        }
        reschedule_delay_us_ = calculate_retry_delay_us_(ret);
        LOG_WARN("[BATCH-FILE] retryable error, scheduling retry",
                 K(ret), K(retry_count_), K(reschedule_delay_us_), K(phase_), K(is_rate_limit));
        ret = OB_SUCCESS;
      } else if (phase_ != OB_AI_TASK_PHASE_DONE) {
        // Only call complete_with_error_ if not already in DONE phase.
        // Some phase handlers call complete_with_error_ internally
        // before returning the error code.
        if (retry_count_ > 0) {
          LOG_WARN("[BATCH-FILE] max retries exhausted, failing task",
                   K(ret), K(retry_count_), K(phase_));
        }
        {
          int tmp_ret = complete_with_error_(ret, "Task execution failed");
          if (OB_SUCCESS != tmp_ret) {
            LOG_WARN("Failed to complete task with error", K(ret), K(tmp_ret));
          }
        }
      }
    }
  }
  return ret;
}

bool ObAiAccessTask::need_reschedule() const
{
  bool bret = false;
  if (is_terminal_state() || phase_ == OB_AI_TASK_PHASE_DONE) {
    bret = false;
  } else if (is_cancel_requested()) {
    // Always reschedule so do_work() can call complete_with_cancel_().
    bret = true;
  } else if (phase_ == OB_AI_TASK_PHASE_BATCH_POLLING) {
    bret = true;
  } else if (retry_count_ > 0 || reschedule_delay_us_ > 0) {
    // Covers both regular retries (retry_count_ > 0) and OB_EAGAIN rate-limit
    // retries (retry_count_ stays 0 but reschedule_delay_us_ is set).
    bret = true;
  }
  return bret;
}

int64_t ObAiAccessTask::get_reschedule_delay_us() const
{
  int64_t delay = 0;
  if (is_cancel_requested()) {
    delay = 0;  // wake up immediately so do_work() can call complete_with_cancel_()
  } else if (reschedule_delay_us_ > 0) {
    delay = reschedule_delay_us_;
  } else if (phase_ == OB_AI_TASK_PHASE_BATCH_POLLING) {
    int64_t current_time = common::ObTimeUtility::current_time();
    int64_t poll_interval = ObAiBatchFileManagerUtils::calculate_poll_interval(batch_status_);
    int64_t elapsed = current_time - last_poll_time_us_;
    delay = (poll_interval > elapsed) ? (poll_interval - elapsed) : 0;
    delay = std::max(delay, static_cast<int64_t>(1000000));
  }
  return delay;
}

int ObAiAccessTask::set_phase_(ObAiTaskPhase new_phase)
{
  int ret = OB_SUCCESS;
  if (!ObAiAccessTaskPhaseManager::is_valid_transition(phase_, new_phase)) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("Invalid phase transition", K(ret), K(phase_), K(new_phase));
  } else {
    log_phase_transition_(phase_, new_phase);
    ATOMIC_STORE(&phase_, new_phase);
    retry_count_ = 0;
  }
  return ret;
}

void ObAiAccessTask::log_phase_transition_(ObAiTaskPhase from_phase, ObAiTaskPhase to_phase)
{
  LOG_DEBUG("Task phase transition",
            "from", ObAiAccessTaskPhaseManager::get_phase_str(from_phase),
            "to", ObAiAccessTaskPhaseManager::get_phase_str(to_phase),
            K(current_offset_), K(total_count_));
}

bool ObAiAccessTask::is_retryable_error_(int error_code) const
{
  if (!is_batch_file_mode_()) {
    return false;
  }
  // Retryability is determined by error_code alone.
  // 4xx client errors map to OB_INVALID_ARGUMENT (not retryable).
  // 5xx server errors map to OB_RPC_POST_ERROR (retryable).
  // 429 rate-limit maps to OB_EAGAIN (retryable, extended backoff).
  return OB_RPC_POST_ERROR == error_code
      || OB_TIMEOUT == error_code
      || OB_CONNECT_ERROR == error_code
      || OB_EAGAIN == error_code
      || OB_CURL_ERROR == error_code;
}

int64_t ObAiAccessTask::calculate_retry_delay_us_(int error_code) const
{
  // OB_EAGAIN (429 rate-limit): double the base interval so backoff starts at 2s instead of 1s.
  int64_t base = (OB_EAGAIN == error_code)
                 ? ObAiRetryManager::RETRY_BASE_INTERVAL_US * 2
                 : ObAiRetryManager::RETRY_BASE_INTERVAL_US;
  return ObAiRetryManager::calculate_retry_interval(retry_count_,
                                                     base,
                                                     ObAiRetryManager::RETRY_MAX_INTERVAL_US,
                                                     ObAiRetryManager::RETRY_MULTIPLIER);
}

int ObAiAccessTask::handle_done_phase_()
{
  return OB_SUCCESS;
}

int ObAiAccessTask::advance_current_mode_()
{
  int ret = OB_SUCCESS;
  switch (ai_execution_mode_) {
    case OB_AI_ACCESS_MODE_BATCH_FILE:
      ret = advance_batch_file_mode_();
      break;
    case OB_AI_ACCESS_MODE_SYNC_HTTP:
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("Access mode not implemented yet", K(ret), K_(ai_execution_mode), K_(command_type));
      break;
    default:
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected access mode", K(ret), K_(ai_execution_mode), K_(task_id));
      break;
  }
  return ret;
}

int ObAiAccessTask::advance_batch_file_mode_()
{
  int ret = OB_SUCCESS;
  switch (phase_) {
    case OB_AI_TASK_PHASE_INIT:
      ret = handle_batch_file_init_phase_();
      break;
    case OB_AI_TASK_PHASE_FILE_UPLOADING:
      ret = handle_file_uploading_phase_();
      break;
    case OB_AI_TASK_PHASE_BATCH_SUBMITTING:
      ret = handle_batch_submitting_phase_();
      break;
    case OB_AI_TASK_PHASE_BATCH_POLLING:
      ret = handle_batch_polling_phase_();
      break;
    case OB_AI_TASK_PHASE_RESULT_DOWNLOADING:
      ret = handle_result_downloading_phase_();
      break;
    case OB_AI_TASK_PHASE_DONE:
      break;
    default:
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected batch-file phase", K(ret), K_(phase), K_(task_id));
      break;
  }
  return ret;
}

// ============================================================================
// ObAiAccessService HTTP execution (curl management)
// ============================================================================

size_t ObAiAccessService::curl_write_callback_(void *contents, size_t size, size_t nmemb, void *userp)
{
  size_t total_size = size * nmemb;
  ObAiHttpResponseData *response = static_cast<ObAiHttpResponseData*>(userp);

  if (OB_ISNULL(response) || OB_ISNULL(contents)) {
    return 0;
  }

  if (response->size_ + total_size > ObAiAccessTask::MAX_RESPONSE_SIZE) {
    LOG_WARN_RET(OB_SIZE_OVERFLOW, "Response too large", K(response->size_), K(total_size));
    return 0;
  }

  if (OB_SUCCESS != response->append_data(static_cast<const char*>(contents), total_size)) {
    return 0;
  }

  return total_size;
}

size_t ObAiAccessService::curl_header_callback_(void *contents, size_t size, size_t nmemb, void *userp)
{
  return size * nmemb;
}

int ObAiAccessService::http_execute_(common::ObIAllocator &allocator,
                                     const common::ObString &url,
                                     const common::ObIArray<common::ObString> &headers,
                                     const char *body,
                                     int64_t body_len,
                                     int64_t timeout_us,
                                     ObAiHttpResponseData &response)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObAiAccessService not init", K(ret));
  } else if (OB_UNLIKELY(url.empty()) || OB_ISNULL(body)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid arguments for http_execute_", K(ret), K(url), KP(body));
  } else {
    CURL *curl = curl_easy_init();
    if (OB_ISNULL(curl)) {
      ret = OB_CURL_ERROR;
      LOG_WARN("Failed to init curl easy handle", K(ret));
    } else {
      response.reset();

      curl_easy_setopt(curl, CURLOPT_TIMEOUT, timeout_us / 1000000);
      curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT, 30L);
      curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1L);
      curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, curl_write_callback_);
      curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response);
      curl_easy_setopt(curl, CURLOPT_HEADERFUNCTION, curl_header_callback_);
      curl_easy_setopt(curl, CURLOPT_HEADERDATA, &response);
      curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, 1L);
      curl_easy_setopt(curl, CURLOPT_SSL_VERIFYHOST, 2L);

      struct curl_slist *curl_headers = nullptr;
      for (int64_t i = 0; i < headers.count() && OB_SUCC(ret); ++i) {
        const common::ObString &header = headers.at(i);
        curl_headers = curl_slist_append(curl_headers, header.ptr());
        if (OB_ISNULL(curl_headers)) {
          ret = OB_CURL_ERROR;
          LOG_WARN("Failed to append header", K(ret), K(header));
        }
      }

      if (OB_SUCC(ret)) {
        curl_easy_setopt(curl, CURLOPT_URL, url.ptr());
        curl_easy_setopt(curl, CURLOPT_HTTPHEADER, curl_headers);
        curl_easy_setopt(curl, CURLOPT_POST, 1L);
        curl_easy_setopt(curl, CURLOPT_POSTFIELDS, body);
        curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, body_len);

        if (OB_FAIL(response.allocate(ObAiAccessTask::MAX_RESPONSE_SIZE))) {
          LOG_WARN("Failed to allocate response buffer", K(ret));
        } else {
          CURLcode curl_ret = curl_easy_perform(curl);
          if (curl_ret != CURLE_OK) {
            ret = OB_CURL_ERROR;
            LOG_WARN("Curl request failed", K(ret), K(curl_ret),
                     "error", curl_easy_strerror(curl_ret));
            response.http_status_code_ = 0;
          } else {
            long http_code = 0;
            curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);
            response.http_status_code_ = http_code;
          }
        }
      }

      if (OB_NOT_NULL(curl_headers)) {
        curl_slist_free_all(curl_headers);
      }
      curl_easy_cleanup(curl);
    }
  }
  return ret;
}

int ObAiAccessTask::update_system_task_status_(ObAiTaskStatus status,
                                               int64_t progress,
                                               int error_code,
                                               const common::ObString &error_msg)
{
  int ret = OB_SUCCESS;
  if (is_batch_file_mode_() && OB_NOT_NULL(table_manager_) && OB_NOT_NULL(sql_client_)
      && !task_id_.empty()) {
    common::ObString remote_files_json;
    if (OB_FAIL(ObAiTaskInfo::build_remote_files_json(local_allocator_,
                                                       current_file_id_,
                                                       output_file_id_,
                                                       error_file_id_,
                                                       remote_files_json))) {
      LOG_WARN("failed to build remote_files json", K(ret));
    } else {
      ret = table_manager_->update_task_status(task_id_,
                                               status,
                                               progress,
                                               error_code,
                                               error_msg,
                                               0,
                                               remote_files_json,
                                               *sql_client_,
                                               current_batch_id_);
    }
  }
  return ret;
}

int ObAiAccessTask::transition_to_phase_(ObAiTaskPhase phase, const char *phase_name)
{
  int ret = set_phase_(phase);
  if (OB_FAIL(ret)) {
    LOG_WARN("Failed to set phase", K(ret), K_(task_id), "phase_name", phase_name);
  }
  return ret;
}

int ObAiAccessTask::sync_running_task_state_(int64_t progress,
                                             const char *action_desc,
                                             bool set_running_state)
{
  int ret = update_system_task_status_(OB_AI_TASK_STATUS_RUNNING,
                                       progress,
                                       OB_SUCCESS,
                                       common::ObString());
  if (OB_FAIL(ret)) {
    LOG_WARN("Failed to sync running task state",
             K(ret), K_(task_id), "action", action_desc);
  } else if (set_running_state) {
    set_state(OB_AI_TASK_STATUS_RUNNING);
  }
  return ret;
}

int ObAiAccessTask::transition_to_terminal_state_(ObAiTaskStatus status)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(set_phase_(OB_AI_TASK_PHASE_DONE))) {
    LOG_WARN("Failed to set phase to DONE", K(ret), K(status), K_(task_id));
  } else {
    set_state(status);
    terminal_ts_ = common::ObTimeUtility::current_time();
  }
  return ret;
}

int ObAiAccessTask::persist_task_file_metadata_(int64_t result_fd,
                                                int64_t result_size,
                                                int64_t result_line_count)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(table_manager_) && OB_NOT_NULL(sql_client_) && !task_id_.empty()) {
    char metadata_buf[256];
    int64_t metadata_pos = 0;
    ObAiTaskInfo tmp_info;
    LocalMaterializationView materialization = build_local_materialization_view_();
    tmp_info.jsonl_fd_ = materialization.jsonl_fd_;
    tmp_info.jsonl_size_ = materialization.jsonl_size_;
    tmp_info.jsonl_line_count_ = materialization.jsonl_line_count_;
    tmp_info.result_fd_ = result_fd;
    tmp_info.result_size_ = result_size;
    tmp_info.result_line_count_ = result_line_count;
    if (OB_FAIL(tmp_info.serialize_file_metadata(metadata_buf, sizeof(metadata_buf), metadata_pos))) {
      LOG_WARN("Failed to serialize file metadata", K(ret), K_(task_id));
    } else {
      ObString metadata_str(static_cast<int32_t>(metadata_pos), metadata_buf);
      ret = table_manager_->update_task_file_metadata(task_id_, metadata_str, *sql_client_);
      if (OB_FAIL(ret)) {
        LOG_WARN("Failed to persist file metadata", K(ret), K_(task_id),
                 K(result_fd), K(result_size), K(result_line_count));
      }
    }
  }
  return ret;
}

int ObAiAccessTask::cleanup_jsonl_tmp_file_()
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = MTL_ID();
  if (jsonl_fd_ >= 0) {
    LOG_INFO("[BATCH-FILE] cleaning up jsonl tmp fd",
             K_(task_id), K_(jsonl_fd), K_(jsonl_size));
    ret = FILE_MANAGER_INSTANCE_WITH_MTL_SWITCH.remove(tenant_id, jsonl_fd_);
    if (OB_SUCCESS != ret) {
      LOG_WARN("Failed to remove jsonl tmp fd", K(ret), K_(jsonl_fd));
    }
    jsonl_fd_ = -1;
  }
  return ret;
}

int ObAiAccessTask::complete_terminal_(ObAiTaskStatus status,
                                        int error_code_for_table,
                                        const common::ObString &error_msg)
{
  int ret = OB_SUCCESS;
  // 1. Release TmpFileManager resources first (idempotent, always safe).
  cleanup_tmp_files_();
  // 2. Persist terminal state to system table (source of truth).
  //    If this fails, memory state stays non-terminal so do_work() can retry.
  if (OB_FAIL(update_system_task_status_(status, current_offset_,
                                                error_code_for_table, error_msg))) {
    LOG_WARN("Failed to update system task status", K(ret), K_(task_id), K(status));
  } else if (OB_FAIL(transition_to_terminal_state_(status))) {
    LOG_WARN("Failed to transition to terminal state", K(ret), K_(task_id), K(status));
  }
  return ret;
}

int ObAiAccessTask::complete_with_error_(int error_code, const common::ObString &error_msg)
{
  int ret = OB_SUCCESS;
  RemoteAssetView assets = build_remote_asset_view_();
  LOG_WARN("[BATCH-FILE] complete_with_error_ called",
           K_(task_id), K(error_code), K(error_msg), K_(phase),
           "phase_str", ObAiAccessTaskPhaseManager::get_phase_str(phase_),
           K(assets.output_file_id_), K(assets.input_file_id_), K(assets.batch_id_),
           K_(allow_null_on_failure));
  last_error_ctx_.internal_error_code_ = error_code;
  if (allow_null_on_failure_) {
    return complete_with_degraded_finish_(error_code, error_msg);
  }
  return complete_terminal_(OB_AI_TASK_STATUS_FAILED, error_code, error_msg);
}

int ObAiAccessTask::complete_with_degraded_finish_(int error_code, const common::ObString &error_msg)
{
  int ret = OB_SUCCESS;
  LOG_WARN("[BATCH-FILE] complete_with_degraded_finish_ called (allow_null_on_failure=true)",
           K_(task_id), K(error_code), K(error_msg), K_(phase));
  // Write OB_SUCCESS to system table: user opted in, NULL vectors are expected behavior.
  return complete_terminal_(OB_AI_TASK_STATUS_FINISHED, OB_SUCCESS, error_msg);
}

int ObAiAccessTask::initiate_cancel()
{
  int ret = OB_SUCCESS;
  if (is_terminal_state()) {
    // Already done, nothing to cancel
  } else {
    // Only set the atomic flag here; remote batch cancel is done by
    // complete_with_cancel_() in the Scheduler thread to avoid racing
    // on non-atomic current_batch_id_ / batch_file_manager_.
    ATOMIC_STORE(&cancel_requested_, true);
    LOG_INFO("[BATCH-FILE] cancel requested", K_(task_id), K_(phase));
  }
  return ret;
}

int ObAiAccessTask::complete_with_cancel_()
{
  int ret = OB_SUCCESS;
  // Best-effort cancel remote batch — safe here in Scheduler thread context
  // (single writer for current_batch_id_ / batch_file_manager_).
  if (!current_batch_id_.empty() && OB_NOT_NULL(batch_file_manager_)) {
    int tmp_ret = batch_file_manager_->cancel_batch(current_batch_id_);
    if (OB_SUCCESS != tmp_ret) {
      LOG_WARN("[BATCH-FILE] remote cancel batch failed",
               K(tmp_ret), K_(task_id), K_(current_batch_id));
    } else {
      LOG_INFO("[BATCH-FILE] remote cancel batch issued", K_(task_id), K_(current_batch_id));
    }
  }
  RemoteAssetView assets = build_remote_asset_view_();
  LOG_INFO("[BATCH-FILE] complete_with_cancel_ called",
           K_(task_id), K_(phase), K(assets.batch_id_),
           K_(accumulated_prompt_tokens), K_(accumulated_completion_tokens),
           K_(accumulated_total_tokens));
  ret = complete_terminal_(OB_AI_TASK_STATUS_CANCELLED, OB_SUCCESS, common::ObString());
  LOG_INFO("[BATCH-FILE] task cancelled gracefully",
           K_(task_id), K_(accumulated_total_tokens));
  return ret;
}

void ObAiAccessTask::parse_and_accumulate_tokens_(const common::ObString &response_body)
{
  // Best-effort: parse usage tokens from response body JSON
  // Format: {"data":[...],"usage":{"prompt_tokens":N,"completion_tokens":N,"total_tokens":N},...}
  common::ObArenaAllocator tmp_alloc("TokenParse", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  common::ObJsonNode *root = nullptr;
  if (OB_SUCCESS != common::ObJsonParser::get_tree(&tmp_alloc, response_body, root)) {
    // Skip silently — token parsing failure is non-fatal
  } else if (OB_NOT_NULL(root) && root->json_type() == common::ObJsonNodeType::J_OBJECT) {
    common::ObJsonObject *obj = static_cast<common::ObJsonObject*>(root);
    common::ObJsonNode *usage_node = obj->get_value("usage");
    if (OB_NOT_NULL(usage_node) && usage_node->json_type() == common::ObJsonNodeType::J_OBJECT) {
      common::ObJsonObject *usage_obj = static_cast<common::ObJsonObject*>(usage_node);
      common::ObJsonNode *pt = usage_obj->get_value("prompt_tokens");
      common::ObJsonNode *ct = usage_obj->get_value("completion_tokens");
      common::ObJsonNode *tt = usage_obj->get_value("total_tokens");
      if (OB_NOT_NULL(pt) && pt->json_type() == common::ObJsonNodeType::J_INT) {
        accumulated_prompt_tokens_ += static_cast<common::ObJsonInt*>(pt)->value();
      }
      if (OB_NOT_NULL(ct) && ct->json_type() == common::ObJsonNodeType::J_INT) {
        accumulated_completion_tokens_ += static_cast<common::ObJsonInt*>(ct)->value();
      }
      if (OB_NOT_NULL(tt) && tt->json_type() == common::ObJsonNodeType::J_INT) {
        accumulated_total_tokens_ += static_cast<common::ObJsonInt*>(tt)->value();
      }
    }
  }
}

int ObAiAccessTask::complete_successfully_()
{
  int ret = OB_SUCCESS;
  RemoteAssetView assets = build_remote_asset_view_();

  // Clean up jsonl input fd (no longer needed after upload).
  // Keep result_fd_ alive for get_next_result() reads.
  if (OB_FAIL(cleanup_jsonl_tmp_file_())) {
    LOG_WARN("Failed to cleanup jsonl tmp fd before success completion", K(ret), K_(task_id));
  } else if (OB_FAIL(update_system_task_status_(OB_AI_TASK_STATUS_FINISHED,
                                                total_count_,
                                                OB_SUCCESS,
                                                common::ObString()))) {
    LOG_WARN("Failed to update task status", K(ret),
             K_(task_id), K_(output_file_id));
  } else if (OB_FAIL(transition_to_terminal_state_(OB_AI_TASK_STATUS_FINISHED))) {
    LOG_WARN("Failed to transition task to FINISHED", K(ret), K_(task_id));
  } else {
    LOG_INFO("Task completed successfully", K(current_offset_), K(total_count_),
             K_(task_id), K_(output_file_id));
  }
  return ret;
}

void ObAiAccessTask::cleanup_tmp_files_()
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = MTL_ID();

  if (jsonl_fd_ >= 0) {
    LOG_INFO("[BATCH-FILE] cleaning up jsonl tmp fd",
             K_(task_id), K_(jsonl_fd), K_(jsonl_size));
    ret = FILE_MANAGER_INSTANCE_WITH_MTL_SWITCH.remove(tenant_id, jsonl_fd_);
    if (OB_SUCCESS != ret) {
      LOG_WARN("Failed to remove jsonl tmp fd", K(ret), K_(jsonl_fd));
    }
    jsonl_fd_ = -1;
  }

  if (result_fd_ >= 0) {
    LOG_INFO("[BATCH-FILE] cleaning up result tmp fd",
             K_(task_id), K_(result_fd), K_(result_size));
    ret = FILE_MANAGER_INSTANCE_WITH_MTL_SWITCH.remove(tenant_id, result_fd_);
    if (OB_SUCCESS != ret) {
      LOG_WARN("Failed to remove result tmp fd", K(ret), K_(result_fd));
    }
    result_fd_ = -1;
  }
}

int ObAiAccessTask::ensure_batch_file_manager_()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(batch_file_manager_)) {
    void *buf = local_allocator_.alloc(sizeof(ObAiBatchFileManager));
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("Failed to allocate batch file manager", K(ret), K_(task_id));
    } else {
      batch_file_manager_ = new (buf) ObAiBatchFileManager();
      if (OB_FAIL(batch_file_manager_->init(local_allocator_, batch_file_url_, api_key_))) {
        LOG_WARN("Failed to init batch file manager", K(ret), K_(task_id), K_(batch_file_url));
        batch_file_manager_->~ObAiBatchFileManager();
        local_allocator_.free(batch_file_manager_);
        batch_file_manager_ = nullptr;
      }
    }
  }
  return ret;
}

//=============================================== BATCH_FILE mode handlers ================================================

int ObAiAccessTask::handle_batch_file_init_phase_()
{
  int ret = OB_SUCCESS;
  LOG_INFO("[BATCH-FILE] init phase start",
           K(model_name_), K(provider_name_),
           K(batch_file_manager_));

  if (OB_FAIL(ensure_batch_file_manager_())) {
    LOG_WARN("Failed to ensure batch file manager", K(ret), K_(task_id));
  } else if (jsonl_fd_ < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("jsonl_fd is not set", K(ret));
  } else if (OB_FAIL(sync_running_task_state_(0, "init phase start", true))) {
    LOG_WARN("Failed to update task status to RUNNING", K(ret), K_(task_id));
  } else if (OB_FAIL(transition_to_phase_(OB_AI_TASK_PHASE_FILE_UPLOADING, "FILE_UPLOADING"))) {
  } else {
    LOG_INFO("[BATCH-FILE] proceeding to FILE_UPLOADING phase", K_(jsonl_fd), K_(jsonl_size));
  }

  return ret;
}

int ObAiAccessTask::handle_file_uploading_phase_()
{
  int ret = OB_SUCCESS;

  // Initialize batch_file_manager_ if not yet created (prepare-data path skips INIT phase)
  if (OB_FAIL(ensure_batch_file_manager_())) {
    LOG_WARN("Failed to ensure batch file manager", K(ret), K_(task_id));
  } else if (jsonl_fd_ < 0 || jsonl_size_ <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("jsonl_fd or jsonl_size is invalid", K(ret), K_(jsonl_fd), K_(jsonl_size));
  } else if (OB_FAIL(upload_batch_file_())) {
    LOG_WARN("Failed to upload batch file", K(ret));
  } else if (OB_FAIL(sync_running_task_state_(current_offset_,
                                              "persist input_file_id"))) {
    LOG_WARN("Failed to update input_file_id to system table", K(ret), K_(current_file_id));
  } else if (OB_FAIL(persist_task_file_metadata_(-1, 0, 0))) {
    LOG_WARN("Failed to persist upload-stage file metadata", K(ret), K_(task_id));
  } else if (OB_FAIL(transition_to_phase_(OB_AI_TASK_PHASE_BATCH_SUBMITTING, "BATCH_SUBMITTING"))) {
    LOG_WARN("Failed to transition to BATCH_SUBMITTING phase", K(ret), K_(task_id));
  } else {
    LOG_DEBUG("[BATCH-FILE] file uploading phase start", K_(jsonl_fd), K_(jsonl_size));
    LOG_INFO("[BATCH-FILE] file uploaded and persisted", K_(current_file_id));
  }
  return ret;
}

int ObAiAccessTask::handle_batch_submitting_phase_()
{
  int ret = OB_SUCCESS;

  LOG_INFO("[BATCH-FILE] batch submitting phase start", K_(current_file_id));

  if (is_cancel_requested() && current_batch_id_.empty()) {
    LOG_INFO("[BATCH-FILE] cancel requested at batch submitting entry, terminating before submit",
             K_(task_id), K_(phase), K_(current_file_id));
    if (OB_FAIL(complete_with_cancel_())) {
      LOG_WARN("Failed to complete with cancel at batch submitting entry", K(ret), K_(task_id));
    }
  } else if (OB_FAIL(submit_batch_job_())) {
    LOG_WARN("failed to submit batch job", K(ret));
  } else if (OB_FAIL(sync_running_task_state_(current_offset_,
                                              "persist batch_id"))) {
    LOG_WARN("Failed to update batch_id to system table", K(ret), K_(current_batch_id));
  } else if (OB_FAIL(transition_to_phase_(OB_AI_TASK_PHASE_BATCH_POLLING, "BATCH_POLLING"))) {
    LOG_WARN("failed to transition to BATCH_POLLING phase", K(ret), K_(task_id));
  } else {
    batch_submit_time_us_ = common::ObTimeUtility::current_time();
    LOG_INFO("[BATCH-FILE] batch job submitted and persisted",
             K_(current_batch_id), K_(batch_status));
  }
  return ret;
}

int ObAiAccessTask::handle_batch_polling_phase_()
{
  int ret = OB_SUCCESS;

  int64_t current_time = common::ObTimeUtility::current_time();
  int64_t poll_interval = ObAiBatchFileManagerUtils::calculate_poll_interval(batch_status_);

  LOG_DEBUG("[BATCH-FILE] batch polling phase", K_(phase), K_(batch_status),
           K_(current_batch_id), K_(last_poll_time_us), K(poll_interval));

  if (current_time - last_poll_time_us_ < poll_interval) {
    reschedule_delay_us_ = poll_interval - (current_time - last_poll_time_us_);
  } else if (OB_FAIL(poll_batch_status_())) {
    // Each task polls its own batch status individually
    LOG_WARN("[BATCH-FILE] failed to poll batch status", K(ret), K_(task_id));
  } else {
    last_poll_time_us_ = current_time;

    const char *status_name = ObAiBatchFileManagerUtils::batch_status_to_str(batch_status_);
    LOG_INFO("[BATCH-FILE] batch status polled", K_(task_id), K_(batch_status),
             "status_name", status_name, K_(current_batch_id), K_(output_file_id));

    if (OB_AI_BATCH_FILE_STATUS_COMPLETED == batch_status_) {
      if (is_cancel_requested()) {
        // Cancel was requested before we had a chance to download — discard the result.
        LOG_INFO("[BATCH-FILE] batch completed but cancel requested, discarding result",
                 K_(task_id), K_(current_batch_id), K_(output_file_id));
        if (OB_FAIL(complete_with_cancel_())) {
          LOG_WARN("Failed to complete with cancel", K(ret));
        }
      } else {
        LOG_INFO("[BATCH-FILE] batch completed, proceeding to RESULT_DOWNLOADING",
                 K_(task_id), K_(output_file_id));
        if (OB_FAIL(transition_to_phase_(OB_AI_TASK_PHASE_RESULT_DOWNLOADING, "RESULT_DOWNLOADING"))) {
          LOG_WARN("failed to transition to RESULT_DOWNLOADING phase", K(ret), K_(task_id));
        }
      }
    } else if (OB_AI_BATCH_FILE_STATUS_CANCELLED == batch_status_) {
      LOG_INFO("[BATCH-FILE] batch cancelled by remote, completing gracefully",
               K_(task_id), K_(current_batch_id), K_(accumulated_total_tokens));
      if (OB_FAIL(complete_with_cancel_())) {
        LOG_WARN("Failed to complete with cancel", K(ret));
      }
    } else if (OB_AI_BATCH_FILE_STATUS_FAILED == batch_status_ ||
               OB_AI_BATCH_FILE_STATUS_EXPIRED == batch_status_) {
      LOG_WARN("[BATCH-FILE] batch job terminal failure", K_(task_id),
               K_(batch_status), "status_name", status_name,
               K_(current_batch_id), K_(output_file_id), K_(error_file_id),
               K(last_error_ctx_.error_message_));
      common::ObString fail_msg = last_error_ctx_.error_message_.empty()
          ? common::ObString("Batch job failed or expired")
          : last_error_ctx_.error_message_;
      if (OB_FAIL(complete_with_error_(OB_ERR_UNEXPECTED, fail_msg))) {
        LOG_WARN("Failed to complete with error", K(ret));
      }
    }
  }
  return ret;
}

int ObAiAccessTask::handle_result_downloading_phase_()
{
  int ret = OB_SUCCESS;

  if (is_cancel_requested()) {
    LOG_INFO("[BATCH-FILE] cancel requested at result downloading entry, discarding result",
             K_(task_id), K_(output_file_id));
    if (OB_FAIL(complete_with_cancel_())) {
      LOG_WARN("Failed to complete with cancel", K(ret));
    }
  } else {
    LOG_INFO("[BATCH-FILE] entering RESULT_DOWNLOADING phase",
             K_(task_id), K_(output_file_id),
             "output_file_id_empty", output_file_id_.empty(),
             "output_file_id_len", output_file_id_.length(),
             KP_(batch_file_manager));

    if (OB_FAIL(download_result_file_())) {
      LOG_WARN("[BATCH-FILE] Failed to download result file", K(ret),
               K_(task_id), K_(output_file_id));
    } else if (OB_FAIL(complete_successfully_())) {
      // complete_successfully_ already sets phase to DONE and state to FINISHED
      LOG_WARN("Failed to complete task", K(ret));
    } else {
      LOG_INFO("[BATCH-FILE] Result downloaded, task DONE",
               K_(task_id), K_(result_fd), K_(result_size));
    }
  }
  return ret;
}

int ObAiAccessTask::upload_batch_file_()
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(batch_file_manager_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("batch_file_manager_ is null", K(ret));
  } else if (jsonl_fd_ < 0 || jsonl_size_ <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("jsonl_fd or jsonl_size is invalid", K(ret), K_(jsonl_fd), K_(jsonl_size));
  } else {
    ObAiFileUploadResult upload_result;
    common::ObString purpose("batch");
    uint64_t tenant_id = MTL_ID();

    // Generate file name for upload
    char file_name_buf[256];
    int64_t pos = snprintf(file_name_buf, sizeof(file_name_buf),
                           "batch_%.*s.jsonl",
                           static_cast<int>(task_id_.length()), task_id_.ptr());
    common::ObString file_name(pos, file_name_buf);

    // Stream upload from TmpFileManager fd
    if (OB_FAIL(batch_file_manager_->upload_from_tmpfile(
            jsonl_fd_, jsonl_size_, tenant_id, file_name, purpose, upload_result))) {
      LOG_WARN("Failed to upload from tmpfile", K(ret), K_(jsonl_fd), K_(jsonl_size));
    } else if (!upload_result.is_success()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Upload failed", K(ret), K(upload_result.error_detail_));
    } else if (OB_FAIL(ob_write_string(local_allocator_, upload_result.file_id_, current_file_id_, true))) {
      LOG_WARN("Failed to copy file id", K(ret));
    } else {
      LOG_INFO("File uploaded successfully from TmpFile",
               K_(current_file_id), K_(jsonl_fd), K_(jsonl_size));
    }
#ifdef ERRSIM
    if (OB_SUCC(ret)) {
      ret = OB_E(common::EventTable::EN_AI_TASK_FILE_UPLOAD_ERR) OB_SUCCESS;
      if (OB_FAIL(ret)) {
        LOG_WARN("[ERRSIM] fail after file upload", KR(ret), K_(task_id), K_(current_file_id));
      }
    }
#endif
  }
  return ret;
}

int ObAiAccessTask::submit_batch_job_()
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(batch_file_manager_) || current_file_id_.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Invalid state for batch submission", K(ret));
  } else {
    ObAiBatchSubmitResult submit_result;
    common::ObString endpoint;
    common::ObString completion_window;

    if (OB_FAIL(get_batch_submit_spec_(endpoint, completion_window))) {
      LOG_WARN("failed to build batch submit spec", K(ret), K_(command_type));
    } else if (OB_FAIL(batch_file_manager_->submit_batch(current_file_id_, endpoint, completion_window, submit_result))) {
      LOG_WARN("Failed to submit batch", K(ret));
    } else if (!submit_result.is_success()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Batch submission failed", K(ret), K(submit_result.error_detail_));
    } else if (OB_FAIL(ob_write_string(local_allocator_, submit_result.batch_id_, current_batch_id_, true))) {
      LOG_WARN("Failed to copy batch id", K(ret));
    } else {
      batch_status_ = submit_result.status_;
      submitted_count_ = jsonl_line_count_;
      LOG_INFO("Batch submitted successfully", K_(current_batch_id), K_(batch_status), K_(submitted_count));
    }
#ifdef ERRSIM
    if (OB_SUCC(ret)) {
      ret = OB_E(common::EventTable::EN_AI_TASK_BATCH_SUBMIT_ERR) OB_SUCCESS;
      if (OB_FAIL(ret)) {
        LOG_WARN("[ERRSIM] fail after batch submit", KR(ret), K_(task_id), K_(current_batch_id));
      }
    }
#endif
  }
  return ret;
}

int ObAiAccessTask::get_batch_submit_spec_(common::ObString &endpoint,
                                           common::ObString &completion_window) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(provider_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("provider is null, cannot get batch submit spec", K(ret), K_(command_type));
  } else if (OB_FAIL(provider_->get_batch_submit_spec(endpoint, completion_window))) {
    LOG_WARN("provider get_batch_submit_spec failed", K(ret), K_(command_type));
  }
  return ret;
}

int ObAiAccessTask::process_result_line_(const share::ObAiBatchLineResult &line_result,
                                         share::ObAiResultRow &row)
{
  int ret = OB_SUCCESS;
  row.original_index_ = strtol(line_result.custom_id_.ptr(), nullptr, 10);
  row.ret_code_ = line_result.is_success() ? OB_SUCCESS : OB_ERR_UNEXPECTED;
  // Deep-copy error_message into local_allocator_ so the pointer survives
  // line_parse_alloc.reuse() across loop iterations in get_next_result().
  if (!line_result.error_detail_.empty()) {
    if (OB_FAIL(ob_write_string(local_allocator_, line_result.error_detail_, row.error_detail_, true))) {
      LOG_WARN("Failed to deep copy error_message", K(ret));
    }
  } else {
    row.error_detail_.reset();
  }
  row.embedding_vector_ = nullptr;
  row.vector_dim_ = 0;

  if (line_result.is_success() && OB_NOT_NULL(provider_)) {
    parse_and_accumulate_tokens_(line_result.response_body_);
    if (OB_FAIL(provider_->decode_result(local_allocator_, line_result.response_body_, row))) {
      LOG_WARN("provider decode_result failed", K(ret), K_(command_type));
    }
  }
  return ret;
}

ObAiAccessTask::RemoteAssetView ObAiAccessTask::build_remote_asset_view_() const
{
  RemoteAssetView view;
  view.input_file_id_ = current_file_id_;
  view.batch_id_ = current_batch_id_;
  view.output_file_id_ = output_file_id_;
  view.error_file_id_ = error_file_id_;
  return view;
}

ObAiAccessTask::LocalMaterializationView ObAiAccessTask::build_local_materialization_view_() const
{
  LocalMaterializationView view;
  view.jsonl_fd_ = jsonl_fd_;
  view.jsonl_size_ = jsonl_size_;
  view.jsonl_line_count_ = jsonl_line_count_;
  view.result_fd_ = result_fd_;
  view.result_size_ = result_size_;
  view.result_line_count_ = result_line_count_;
  return view;
}

int ObAiAccessTask::poll_batch_status_()
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(batch_file_manager_) || current_batch_id_.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Invalid state for polling", K(ret));
  } else {
    ObAiBatchSubmitResult poll_result;

    if (OB_FAIL(batch_file_manager_->poll_batch_status(current_batch_id_, poll_result))) {
      LOG_WARN("Failed to poll batch status", K(ret));
    } else if (!poll_result.is_success() && !poll_result.error_detail_.empty()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Poll failed", K(ret), K(poll_result.error_detail_));
    } else {
      batch_status_ = poll_result.status_;
      if (OB_FAIL(ob_write_string(local_allocator_, poll_result.output_file_id_, output_file_id_, true))) {
        LOG_WARN("Failed to copy output file id", K(ret));
      } else if (OB_FAIL(ob_write_string(local_allocator_, poll_result.error_file_id_, error_file_id_, true))) {
        LOG_WARN("Failed to copy error file id", K(ret));
      }
      // Save error detail from provider for failure reporting
      if (OB_FAIL(ret)) {
      } else if (!poll_result.error_detail_.empty()) {
        if (OB_FAIL(ob_write_string(local_allocator_, poll_result.error_detail_,
                                      last_error_ctx_.error_message_, true))) {
          LOG_WARN("failed to copy error detail from poll result", K(ret));
        }
      }
      LOG_DEBUG("[BATCH-FILE] batch status polled", K_(batch_status), K_(current_batch_id),
                K(poll_result.request_counts_completed_), K(poll_result.request_counts_failed_),
                K(poll_result.error_detail_));
    }
#ifdef ERRSIM
    if (OB_SUCC(ret)) {
      ret = OB_E(common::EventTable::EN_AI_TASK_BATCH_POLL_ERR) OB_SUCCESS;
      if (OB_FAIL(ret)) {
        LOG_WARN("[ERRSIM] fail after batch poll", KR(ret), K_(task_id), K_(batch_status));
      }
    }
#endif
  }
  return ret;
}

int ObAiAccessTask::download_result_file_()
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(batch_file_manager_) || output_file_id_.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[BATCH-FILE] Invalid state for downloading", K(ret),
             KP_(batch_file_manager), K_(output_file_id),
             "output_file_id_len", output_file_id_.length(),
             "output_file_id_empty", output_file_id_.empty());
  } else {
    // Download result file to TmpFileManager via streaming callback
    uint64_t tenant_id = MTL_ID();
    int64_t dir_id = -1;
    share::ObBatchFileJsonlWriter result_writer;
    share::ObBatchFileDataSegment segment;
    if (OB_FAIL(FILE_MANAGER_INSTANCE_WITH_MTL_SWITCH.alloc_dir(tenant_id, dir_id))) {
      LOG_WARN("Failed to alloc tmp file dir for result", K(ret), K(tenant_id));
    } else if (OB_FAIL(result_writer.init(dir_id, tenant_id, "BatchDL"))) {
      LOG_WARN("Failed to init result writer", K(ret));
    } else if (OB_FAIL(batch_file_manager_->download_to_tmpfile(
            output_file_id_, result_writer, segment))) {
      LOG_WARN("Failed to download result to tmpfile", K(ret), K_(output_file_id));
    } else {
      result_fd_ = segment.fd_;
      result_size_ = segment.size_;
      result_line_count_ = segment.line_count_;
      LOG_DEBUG("[BATCH-FILE] result file downloaded to TmpFile",
               K_(output_file_id), K_(result_fd), K_(result_size), K_(result_line_count));
      if (OB_FAIL(persist_task_file_metadata_(result_fd_, result_size_, result_line_count_))) {
        LOG_WARN("Failed to persist result-stage file metadata", K(ret), K_(task_id));
      }
    }
#ifdef ERRSIM
    if (OB_SUCC(ret)) {
      ret = OB_E(common::EventTable::EN_AI_TASK_RESULT_DOWNLOAD_ERR) OB_SUCCESS;
      if (OB_FAIL(ret)) {
        LOG_WARN("[ERRSIM] fail after result download", KR(ret), K_(task_id), K_(result_fd));
      }
    }
#endif
  }
  return ret;
}

//=============================================== ObAiRetryManager Implementation ================================================

//=============================================== ObAiErrorMapper Shared Implementation ================================================

int ObAiErrorMapper::map_error_impl_(int64_t http_status,
                                      const common::ObString &response_body,
                                      const char *provider_name,
                                      ObAiErrorContext &error_ctx) const
{
  int ret = OB_SUCCESS;
  error_ctx.reset();
  error_ctx.http_status_code_ = http_status;
  error_ctx.provider_ = provider_name;

  // internal_error_code_ uses the canonical mapping
  // (ObAiBatchFileManager::map_http_status_to_error_code).
  // error_category_ is set independently for retry-manager consumption.
  error_ctx.internal_error_code_ = ObAiBatchFileManager::map_http_status_to_error_code(http_status);

  if (http_status == 0) {
    error_ctx.error_category_ = OB_AI_ERROR_CATEGORY_NETWORK;
  } else if (http_status == 429) {
    error_ctx.error_category_ = OB_AI_ERROR_CATEGORY_RATE_LIMIT;
  } else if (http_status >= 400 && http_status < 500) {
    error_ctx.error_category_ = OB_AI_ERROR_CATEGORY_INVALID_REQUEST;
  } else if (http_status >= 500) {
    error_ctx.error_category_ = OB_AI_ERROR_CATEGORY_SERVER_ERROR;
  } else {
    error_ctx.error_category_ = OB_AI_ERROR_CATEGORY_UNKNOWN;
  }

  error_ctx.error_message_ = response_body;
  return ret;
}

bool ObAiErrorMapper::is_retryable_impl_(const ObAiErrorContext &error_ctx,
                                          bool retryable_on_413) const
{
  bool retryable = false;
  switch (error_ctx.error_category_) {
    case OB_AI_ERROR_CATEGORY_NETWORK:
    case OB_AI_ERROR_CATEGORY_RATE_LIMIT:
    case OB_AI_ERROR_CATEGORY_SERVER_ERROR:
      retryable = true;
      break;
    case OB_AI_ERROR_CATEGORY_INVALID_REQUEST:
      retryable = retryable_on_413 && (error_ctx.http_status_code_ == 413);
      break;
    default:
      retryable = false;
      break;
  }
  return retryable;
}

//=============================================== ObAiOpenAiErrorMapper Implementation ================================================

int ObAiOpenAiErrorMapper::map_error(int64_t http_status,
                                      const common::ObString &response_body,
                                      ObAiErrorContext &error_ctx) const
{
  return map_error_impl_(http_status, response_body, "OPENAI", error_ctx);
}

bool ObAiOpenAiErrorMapper::is_retryable(const ObAiErrorContext &error_ctx) const
{
  return is_retryable_impl_(error_ctx, true /* retryable_on_413 */);
}

//=============================================== ObAiDashScopeErrorMapper Implementation ================================================

int ObAiDashScopeErrorMapper::map_error(int64_t http_status,
                                         const common::ObString &response_body,
                                         ObAiErrorContext &error_ctx) const
{
  return map_error_impl_(http_status, response_body, "DASHSCOPE", error_ctx);
}

bool ObAiDashScopeErrorMapper::is_retryable(const ObAiErrorContext &error_ctx) const
{
  return is_retryable_impl_(error_ctx, false /* retryable_on_413 */);
}

//=============================================== ObAiRetryManager Implementation ================================================

ObAiRetryManager::ObAiRetryManager()
  : is_inited_(false),
    openai_mapper_(),
    dashscope_mapper_()
{
}

ObAiRetryManager::~ObAiRetryManager()
{
  reset();
}

int ObAiRetryManager::init()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObAiRetryManager already inited", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

void ObAiRetryManager::reset()
{
  is_inited_ = false;
}

bool ObAiRetryManager::is_retryable_error(const ObAiErrorContext &error_ctx) const
{
  bool retryable = false;
  if (!is_inited_) {
    LOG_WARN_RET(OB_NOT_INIT, "ObAiRetryManager not inited");
  } else if (!error_ctx.is_valid()) {
    LOG_WARN_RET(OB_INVALID_ARGUMENT, "Invalid error context");
  } else {
    const ObAiErrorMapper *mapper = get_error_mapper(error_ctx.provider_);
    if (OB_NOT_NULL(mapper)) {
      retryable = mapper->is_retryable(error_ctx);
    } else {
      switch (error_ctx.error_category_) {
        case OB_AI_ERROR_CATEGORY_NETWORK:
        case OB_AI_ERROR_CATEGORY_RATE_LIMIT:
        case OB_AI_ERROR_CATEGORY_SERVER_ERROR:
          retryable = true;
          break;
        default:
          retryable = false;
          break;
      }
    }
  }
  return retryable;
}

int64_t ObAiRetryManager::calculate_retry_interval(int64_t retry_count) const
{
  return calculate_retry_interval(retry_count,
                                   RETRY_BASE_INTERVAL_US,
                                   RETRY_MAX_INTERVAL_US,
                                   RETRY_MULTIPLIER);
}

int64_t ObAiRetryManager::calculate_retry_interval(int64_t retry_count,
                                                    int64_t base_interval_us,
                                                    int64_t max_interval_us,
                                                    int64_t multiplier)
{
  int64_t interval = base_interval_us;

  for (int64_t i = 0; i < retry_count && interval < max_interval_us; ++i) {
    interval *= multiplier;
  }

  interval = OB_MIN(interval, max_interval_us);

  int64_t half_interval = interval / 2;
  int64_t jitter = 0;
  if (half_interval > 0) {
    jitter = static_cast<int64_t>(common::ObRandom::rand(0, static_cast<int32_t>(half_interval)));
  }
  interval = half_interval + jitter;

  return interval;
}

const ObAiErrorMapper* ObAiRetryManager::get_error_mapper(const common::ObString &provider) const
{
  const ObAiErrorMapper *mapper = nullptr;
  if (provider.case_compare("OPENAI") == 0) {
    mapper = &openai_mapper_;
  } else if (provider.case_compare("DASHSCOPE") == 0) {
    mapper = &dashscope_mapper_;
  }
  return mapper;
}

//=============================================== ObAiRetryUtils Implementation ================================================

int ObAiRetryUtils::map_http_status_to_internal_error(int64_t http_status)
{
  int internal_error = OB_ERR_UNEXPECTED;
  switch (http_status) {
    case 400:
      internal_error = OB_INVALID_ARGUMENT;
      break;
    case 401:
    case 403:
      internal_error = OB_ERR_NO_PRIVILEGE;
      break;
    case 404:
      internal_error = OB_ENTRY_NOT_EXIST;
      break;
    case 408:
      internal_error = OB_TIMEOUT;
      break;
    case 413:
      internal_error = OB_SIZE_OVERFLOW;
      break;
    case 429:
      internal_error = OB_ERR_WAIT_REMOTE_SCHEMA_REFRESH;
      break;
    case 500:
    case 502:
    case 503:
    case 504:
      internal_error = OB_ERR_UNEXPECTED;
      break;
    default:
      internal_error = OB_ERR_UNEXPECTED;
      break;
  }
  return internal_error;
}

//=============================================== ObAdaptiveBatchProcessor Implementation ================================================

ObAdaptiveBatchProcessor::ObAdaptiveBatchProcessor()
  : is_inited_(false),
    allocator_(nullptr),
    lock_(common::ObLatchIds::OB_EMBEDDING_TASK_HANDLER_SPIN_LOCK),
    entries_()
{
}

ObAdaptiveBatchProcessor::~ObAdaptiveBatchProcessor()
{
  reset();
}

int ObAdaptiveBatchProcessor::init(common::ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObAdaptiveBatchProcessor already inited", K(ret));
  } else {
    allocator_ = &allocator;
    entries_.set_tenant_id(MTL_ID());
    is_inited_ = true;
  }
  return ret;
}

void ObAdaptiveBatchProcessor::reset()
{
  if (OB_NOT_NULL(allocator_)) {
    for (int64_t i = 0; i < entries_.count(); ++i) {
      if (OB_NOT_NULL(entries_.at(i))) {
        entries_.at(i)->~BatchSizeEntry();
        allocator_->free(entries_.at(i));
      }
    }
    entries_.reset();
  }
  is_inited_ = false;
  allocator_ = nullptr;
}

ObAdaptiveBatchProcessor::BatchSizeEntry*
ObAdaptiveBatchProcessor::find_entry_by_key_(const ObAiDimensionKey &key) const
{
  BatchSizeEntry *entry = nullptr;
  for (int64_t i = 0; i < entries_.count() && OB_ISNULL(entry); ++i) {
    if (OB_NOT_NULL(entries_.at(i)) && entries_.at(i)->key_ == key) {
      entry = entries_.at(i);
    }
  }
  return entry;
}

int64_t ObAdaptiveBatchProcessor::get_suggested_batch_size(const ObAiDimensionKey &key,
                                                            int64_t default_batch_size)
{
  int64_t batch_size = default_batch_size;
  if (!is_inited_) {
    LOG_WARN_RET(OB_NOT_INIT, "ObAdaptiveBatchProcessor not inited");
  } else if (!key.is_valid()) {
    LOG_WARN_RET(OB_INVALID_ARGUMENT, "Invalid dimension key");
  } else {
    common::ObSpinLockGuard guard(lock_);
    BatchSizeEntry *entry = find_entry_by_key_(key);
    if (OB_NOT_NULL(entry)) {
      batch_size = entry->state_.current_batch_size_;
    }
  }
  return batch_size;
}

int ObAdaptiveBatchProcessor::report_success(const ObAiDimensionKey &key)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObAdaptiveBatchProcessor not inited", K(ret));
  } else if (!key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid dimension key", K(ret), K(key));
  } else {
    common::ObSpinLockGuard guard(lock_);
    BatchSizeEntry *entry = find_entry_by_key_(key);
    if (OB_NOT_NULL(entry)) {
      entry->state_.successful_requests_++;
      entry->state_.consecutive_failures_ = 0;
      try_increase_batch_size_(*entry);
    }
  }
  return ret;
}

int ObAdaptiveBatchProcessor::report_failure(const ObAiDimensionKey &key, bool is_batch_size_related)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObAdaptiveBatchProcessor not inited", K(ret));
  } else if (!key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid dimension key", K(ret), K(key));
  } else {
    common::ObSpinLockGuard guard(lock_);
    BatchSizeEntry *entry = find_entry_by_key_(key);
    if (OB_NOT_NULL(entry)) {
      entry->state_.failed_requests_++;
      entry->state_.consecutive_failures_++;
      try_decrease_batch_size_(*entry, is_batch_size_related);
    }
  }
  return ret;
}

int ObAdaptiveBatchProcessor::set_max_batch_size(const ObAiDimensionKey &key, int64_t max_batch_size)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObAdaptiveBatchProcessor not inited", K(ret));
  } else if (!key.is_valid() || max_batch_size <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid arguments", K(ret), K(key), K(max_batch_size));
  } else {
    common::ObSpinLockGuard guard(lock_);
    BatchSizeEntry *entry = find_entry_by_key_(key);
    if (OB_NOT_NULL(entry)) {
      entry->state_.max_batch_size_ = max_batch_size;
      if (entry->state_.current_batch_size_ > max_batch_size) {
        entry->state_.current_batch_size_ = max_batch_size;
      }
    }
  }
  return ret;
}

int ObAdaptiveBatchProcessor::get_batch_size_state(const ObAiDimensionKey &key,
                                                    ObAiBatchSizeState &state) const
{
  int ret = OB_SUCCESS;
  state.reset();
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObAdaptiveBatchProcessor not inited", K(ret));
  } else if (!key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid dimension key", K(ret), K(key));
  } else {
    common::ObSpinLockGuard guard(const_cast<common::ObSpinLock&>(lock_));
    BatchSizeEntry *entry = find_entry_by_key_(key);
    if (OB_NOT_NULL(entry)) {
      state.current_batch_size_ = entry->state_.current_batch_size_;
      state.min_batch_size_ = entry->state_.min_batch_size_;
      state.max_batch_size_ = entry->state_.max_batch_size_;
      state.successful_requests_ = entry->state_.successful_requests_;
      state.failed_requests_ = entry->state_.failed_requests_;
      state.consecutive_failures_ = entry->state_.consecutive_failures_;
      state.last_adjust_time_us_ = entry->state_.last_adjust_time_us_;
    } else {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("Batch size state not found for key", K(ret), K(key));
    }
  }
  return ret;
}

int ObAdaptiveBatchProcessor::get_or_create_entry_(const ObAiDimensionKey &key,
                                                    int64_t default_batch_size,
                                                    BatchSizeEntry *&entry)
{
  int ret = OB_SUCCESS;
  entry = find_entry_by_key_(key);

  if (OB_ISNULL(entry)) {
    void *buf = allocator_->alloc(sizeof(BatchSizeEntry));
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("Failed to allocate batch size entry", K(ret));
    } else {
      entry = new (buf) BatchSizeEntry();
      MEMCPY(entry->model_name_buf_, key.model_name_.ptr(),
             OB_MIN(key.model_name_.length(), OB_MAX_MODEL_NAME_LENGTH - 1));
      MEMCPY(entry->provider_buf_, key.provider_.ptr(),
             OB_MIN(key.provider_.length(), 63));
      entry->key_.model_name_.assign_ptr(entry->model_name_buf_,
                                          static_cast<int32_t>(strlen(entry->model_name_buf_)));
      entry->key_.provider_.assign_ptr(entry->provider_buf_,
                                        static_cast<int32_t>(strlen(entry->provider_buf_)));
      entry->key_.ai_execution_mode_ = key.ai_execution_mode_;

      entry->state_.current_batch_size_ = default_batch_size;
      entry->state_.min_batch_size_ = DEFAULT_MIN_BATCH_SIZE;
      entry->state_.max_batch_size_ = DEFAULT_MAX_BATCH_SIZE;
      entry->state_.successful_requests_ = 0;
      entry->state_.failed_requests_ = 0;
      entry->state_.consecutive_failures_ = 0;
      entry->state_.last_adjust_time_us_ = 0;

      if (OB_FAIL(entries_.push_back(entry))) {
        LOG_WARN("Failed to add entry to array", K(ret));
        entry->~BatchSizeEntry();
        allocator_->free(entry);
        entry = nullptr;
      }
    }
  }
  return ret;
}

void ObAdaptiveBatchProcessor::try_increase_batch_size_(BatchSizeEntry &entry)
{
  if (entry.state_.successful_requests_ > 0 &&
      entry.state_.successful_requests_ % SUCCESS_THRESHOLD_FOR_INCREASE == 0) {
    int64_t current_time = common::ObTimeUtility::current_time();
    if (entry.state_.current_batch_size_ < entry.state_.max_batch_size_) {
      entry.state_.current_batch_size_ =
        OB_MIN(entry.state_.current_batch_size_ + BATCH_SIZE_INCREASE_STEP,
               entry.state_.max_batch_size_);
      entry.state_.last_adjust_time_us_ = current_time;
      LOG_INFO("Increased batch size", K(entry.key_), K(entry.state_.current_batch_size_));
    }
  }
}

void ObAdaptiveBatchProcessor::try_decrease_batch_size_(BatchSizeEntry &entry, bool is_batch_size_related)
{
  if (is_batch_size_related || entry.state_.consecutive_failures_ >= MAX_CONSECUTIVE_FAILURES) {
    int64_t current_time = common::ObTimeUtility::current_time();
    if (entry.state_.current_batch_size_ > entry.state_.min_batch_size_) {
      int64_t new_batch_size = entry.state_.current_batch_size_ / BATCH_SIZE_DECREASE_FACTOR;
      new_batch_size = OB_MAX(new_batch_size, entry.state_.min_batch_size_);
      entry.state_.current_batch_size_ = new_batch_size;
      entry.state_.last_adjust_time_us_ = current_time;
      LOG_INFO("Decreased batch size", K(entry.key_), K(entry.state_.current_batch_size_),
               K(is_batch_size_related), K(entry.state_.consecutive_failures_));
    }
  }
}

//=============================================== ObAiAccessService Implementation ================================================

ObAiAccessService::ObAiAccessService()
  : is_inited_(false),
    is_running_(false),
    tenant_id_(OB_INVALID_TENANT_ID),
    allocator_(nullptr),
    sql_client_(nullptr),
    sql_proxy_(nullptr),
    scheduler_(),
    table_manager_(),
    table_poller_(nullptr),
    task_map_lock_(common::ObLatchIds::OB_TASK_SLOT_RING_LOCK)
{
}

ObAiAccessService::~ObAiAccessService()
{
  destroy();
}

int ObAiAccessService::init(ObIAllocator &allocator,
                               uint64_t tenant_id,
                               int64_t thread_num)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObAiAccessService already inited", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", K(ret), K(tenant_id));
  } else if (OB_UNLIKELY(thread_num <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid thread_num", K(ret), K(thread_num));
  } else {
    if (OB_FAIL(scheduler_.init(thread_num, DEFAULT_SCHEDULER_QUEUE_SIZE_POW2))) {
      LOG_WARN("failed to init scheduler", K(ret), K(thread_num));
    } else if (OB_FAIL(table_manager_.init(tenant_id, allocator))) {
      LOG_WARN("failed to init table manager", K(ret), K(tenant_id));
    } else if (OB_FAIL(task_map_.create(64, "AiTaskMap", "AiTaskMap", tenant_id))) {
      LOG_WARN("failed to init task map", K(ret));
    } else {
      allocator_ = &allocator;
      tenant_id_ = tenant_id;
      table_poller_ = nullptr;
      is_inited_ = true;
      // is_running_ will be set to true in start()
      LOG_INFO("ObAiAccessService init success", K(tenant_id), K(thread_num));
    }
  }
  return ret;
}

int ObAiAccessService::start()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObAiAccessService not init", K(ret), K(is_inited_));
  } else if (OB_FAIL(scheduler_.start())) {
    LOG_WARN("failed to start scheduler", K(ret));
  } else {
    is_running_ = true;
    LOG_INFO("ObAiAccessService started", K(tenant_id_));
  }
  return ret;
}

void ObAiAccessService::stop()
{
  if (is_inited_) {
    stop_table_poller();
    is_running_ = false;
    scheduler_.stop();
    LOG_INFO("ObAiAccessService stopped", K(tenant_id_));
  }
}

void ObAiAccessService::wait()
{
  if (is_inited_) {
    scheduler_.wait();
    LOG_INFO("ObAiAccessService wait completed", K(tenant_id_));
  }
}

void ObAiAccessService::destroy()
{
  if (is_inited_) {
    stop();
    wait();
    if (OB_NOT_NULL(table_poller_) && OB_NOT_NULL(allocator_)) {
      ObSystemTablePollerFactory::destroy_poller(*allocator_, table_poller_);
      table_poller_ = nullptr;
    }
    // Clean up task map — free Task objects before destroying the map
    if (task_map_.created()) {
      for (auto it = task_map_.begin(); it != task_map_.end(); ++it) {
        if (OB_NOT_NULL(it->second)) {
          it->second->~ObAiAccessTask();
          allocator_->free(it->second);
        }
      }
      task_map_.destroy();
    }
    scheduler_.destroy();
    table_manager_.destroy();
    allocator_ = nullptr;
    tenant_id_ = OB_INVALID_TENANT_ID;
    is_inited_ = false;
    is_running_ = false;
    LOG_INFO("ObAiAccessService destroyed");
  }
}

//=============================================== Table Poller Interface Implementation ================================================

int ObAiAccessService::register_table_poller_task(common::ObMySQLProxy &sql_proxy,
                                                      int64_t poll_interval_us)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObAiAccessService not init", K(ret), K(is_inited_));
  } else if (OB_NOT_NULL(table_poller_)) {
    LOG_INFO("table poller already registered", K_(tenant_id));
  } else if (OB_ISNULL(allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("allocator_ is null", K(ret));
  } else {
    sql_proxy_ = &sql_proxy;
    if (OB_FAIL(ObSystemTablePollerFactory::create_poller(*allocator_,
                                                          *this,
                                                          table_manager_,
                                                          sql_proxy,
                                                          poll_interval_us,
                                                          table_poller_))) {
      LOG_WARN("failed to create table poller", K(ret), K(poll_interval_us));
    } else if (OB_ISNULL(table_poller_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table_poller_ is null after creation", K(ret));
    } else if (OB_FAIL(table_poller_->start())) {
      LOG_WARN("failed to start table poller", K(ret));
      ObSystemTablePollerFactory::destroy_poller(*allocator_, table_poller_);
      table_poller_ = nullptr;
    } else if (OB_FAIL(scheduler_.schedule_after(*table_poller_, poll_interval_us))) {
      LOG_WARN("failed to schedule first poll", K(ret));
      table_poller_->stop();
      ObSystemTablePollerFactory::destroy_poller(*allocator_, table_poller_);
      table_poller_ = nullptr;
    } else {
      LOG_INFO("table poller registered and started", K_(tenant_id), K(poll_interval_us));
    }
  }
  return ret;
}

void ObAiAccessService::stop_table_poller()
{
  if (OB_NOT_NULL(table_poller_)) {
    table_poller_->stop();
    LOG_INFO("table poller stopped", K_(tenant_id));
  }
}

bool ObAiAccessService::is_table_poller_running() const
{
  return OB_NOT_NULL(table_poller_) && !table_poller_->is_stopped();
}

int ObAiAccessService::query_task_status(const ObString &task_id,
                                            ObAiTaskInfo &task_info)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObAiAccessService not init", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(task_id.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("task_id is empty", K(ret));
  } else {
    if (OB_ISNULL(sql_client_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sql_client_ is null, cannot query task status", K(ret), K(task_id));
    } else {
      task_info.reset();
      task_info.task_id_ = task_id;
      task_info.tenant_id_ = tenant_id_;
      if (OB_FAIL(table_manager_.query_task_status(task_id, task_info, *sql_client_))) {
        if (ret == OB_ENTRY_NOT_EXIST) {
          LOG_WARN("task not found in system table", K(ret), K(task_id));
        } else {
          LOG_WARN("failed to query task status from system table", K(ret), K(task_id));
        }
      }
    }
  }

  return ret;
}

int ObAiAccessService::release_task(const common::ObString &task_id)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObAiAccessService not init", K(ret));
  } else if (OB_UNLIKELY(task_id.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("task_id is empty", K(ret));
  } else if (OB_ISNULL(sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_proxy_ is null, cannot archive task", K(ret), K(task_id));
  } else {
    ObAiAccessTask *task = nullptr;
    {
      common::ObSpinLockGuard guard(task_map_lock_);
      if (OB_FAIL(task_map_.get_refactored(task_id, task))) {
        if (OB_HASH_NOT_EXIST == ret) {
          task = nullptr;
          ret = OB_SUCCESS;
          LOG_INFO("release_task: task not found, already released", K(task_id));
        } else {
          LOG_WARN("failed to get task from map", K(ret), K(task_id));
          task = nullptr;
        }
      } else if (OB_ISNULL(task)) {
        task = nullptr;
        ret = OB_SUCCESS;
        LOG_INFO("release_task: task not found, already released", K(task_id));
      } else {
        task->pin();
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(task)) {
      // Already released; treat as success.
    } else if (!task->is_terminal_state()) {
      ret = OB_STATE_NOT_MATCH;
      LOG_WARN("release_task called on non-terminal task", K(ret), K(task_id),
               "state", task->get_state());
      task->unpin();
    } else if (task->is_archived()) {
      LOG_INFO("release_task: task already archived, skip", K(task_id));
      task->unpin();
    } else {
      char token_usage_buf[128] = "";
      char provider_timeline_buf[128] = "";
      int64_t completion_tokens = task->get_accumulated_completion_tokens();
      int64_t prompt_tokens = task->get_accumulated_prompt_tokens();
      int64_t total_tokens = task->get_accumulated_total_tokens();
      int64_t model_wait_us = task->get_model_wait_time_us();
      snprintf(token_usage_buf, sizeof(token_usage_buf),
               "{\"completion_tokens\":%ld,\"prompt_tokens\":%ld,\"total_tokens\":%ld}",
               completion_tokens, prompt_tokens, total_tokens);
      if (model_wait_us > 0) {
        snprintf(provider_timeline_buf, sizeof(provider_timeline_buf),
                 "{\"model_wait_time_us\":%ld}", model_wait_us);
      }

      if (OB_FAIL(table_manager_.archive_task_to_history(
              task_id,
              ObString::make_string(token_usage_buf),
              ObString::make_string(provider_timeline_buf),
              *sql_proxy_))) {
        LOG_WARN("release_task: archive_task_to_history failed, marking abandoned for poller retry",
                 K(ret), K(task_id));
        task->set_abandoned();
        task->unpin();
      } else {
        if (task->set_archived()) {
          LOG_INFO("release_task: task archived, pending cleanup by poller", K(task_id));
        } else {
          LOG_INFO("release_task: task archived concurrently, skip", K(task_id));
        }
        task->unpin();
      }
    }
  }

  return ret;
}

int ObAiAccessService::abandon_task(const common::ObString &task_id)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObAiAccessService not init", K(ret));
  } else if (OB_UNLIKELY(task_id.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("task_id is empty", K(ret));
  } else {
    ObAiAccessTask *task = nullptr;
    {
      common::ObSpinLockGuard guard(task_map_lock_);
      if (OB_FAIL(task_map_.get_refactored(task_id, task))) {
        if (OB_HASH_NOT_EXIST == ret) {
          task = nullptr;
          ret = OB_SUCCESS;
          LOG_INFO("abandon_task: task not found, already released", K(task_id));
        } else {
          LOG_WARN("failed to get task from map", K(ret), K(task_id));
          task = nullptr;
        }
      } else if (OB_ISNULL(task)) {
        task = nullptr;
        ret = OB_SUCCESS;
        LOG_INFO("abandon_task: task not found, already released", K(task_id));
      } else {
        task->pin();
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(task)) {
      // Already released; treat as success.
    } else if (task->is_running_state()) {
      task->set_abandoned();    // Poller cleanup signal
      task->signal_cancel();    // AI Scheduler exit signal (memory only, no HTTP)
      task->unpin();
      LOG_INFO("abandon_task: set abandoned+cancel flags on running task", K(task_id));
    } else if (!task->is_terminal_state()) {
      LOG_WARN("abandon_task: unexpected task state", K(task_id), "state", task->get_state());
      task->unpin();
    } else if (task->is_archived()) {
      LOG_INFO("abandon_task: task already archived, skip", K(task_id));
      task->unpin();
    } else if (OB_ISNULL(sql_proxy_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sql_proxy_ is null, cannot archive abandoned task", K(ret), K(task_id));
      task->unpin();
    } else {
      char token_usage_buf[128] = "";
      char provider_timeline_buf[128] = "";
      int64_t completion_tokens = task->get_accumulated_completion_tokens();
      int64_t prompt_tokens = task->get_accumulated_prompt_tokens();
      int64_t total_tokens = task->get_accumulated_total_tokens();
      int64_t model_wait_us = task->get_model_wait_time_us();
      snprintf(token_usage_buf, sizeof(token_usage_buf),
               "{\"completion_tokens\":%ld,\"prompt_tokens\":%ld,\"total_tokens\":%ld}",
               completion_tokens, prompt_tokens, total_tokens);
      if (model_wait_us > 0) {
        snprintf(provider_timeline_buf, sizeof(provider_timeline_buf),
                 "{\"model_wait_time_us\":%ld}", model_wait_us);
      }

      if (OB_FAIL(table_manager_.archive_task_to_history(
              task_id,
              ObString::make_string(token_usage_buf),
              ObString::make_string(provider_timeline_buf),
              *sql_proxy_))) {
        LOG_WARN("abandon_task: archive_task_to_history failed", K(ret), K(task_id));
        task->set_abandoned();
        task->unpin();
      } else {
        if (task->set_archived()) {
          LOG_INFO("abandon_task: terminal task archived, pending cleanup by poller", K(task_id));
        }
        task->unpin();
      }
    }
  }

  return ret;
}

//=============================================== Batch Task Interface Implementation ================================================

int ObAiAccessService::open_batch_task(const ObAiModelEndpointInfo &endpoint_info,
                                        ObAiCommandType command_type,
                                        int64_t ddl_task_id,
                                        share::ObAiBatchTaskWriter &writer,
                                        bool allow_null_on_failure)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObAiAccessService not initialized", K(ret));
  } else {
    // Allocate a new TmpFile directory for this writer
    uint64_t tenant_id = tenant_id_;
    int64_t dir_id = -1;
    if (OB_FAIL(FILE_MANAGER_INSTANCE_WITH_MTL_SWITCH.alloc_dir(tenant_id, dir_id))) {
      LOG_WARN("failed to alloc dir for batch task writer", K(ret));
    } else if (OB_FAIL(writer.init(this, endpoint_info, command_type, ddl_task_id, dir_id, tenant_id,
                                    allow_null_on_failure))) {
      LOG_WARN("failed to init batch task writer", K(ret), K(dir_id), K(ddl_task_id));
    } else {
      LOG_DEBUG("[BATCH-FILE] opened batch task writer", K(dir_id), K(ddl_task_id));
    }
  }
  return ret;
}

int ObAiAccessService::commit_batch_task_(const share::ObBatchFileDataSegment &segment,
                                           int64_t ddl_task_id,
                                           const ObAiModelEndpointInfo &endpoint_info,
                                           ObAiCommandType command_type,
                                           common::ObString &task_id,
                                           bool allow_null_on_failure)
{
  int ret = OB_SUCCESS;
  char task_id_buf[OB_AI_MAX_TASK_ID_LENGTH];
  share::ObAiTaskInfo task_info;
  int64_t unowned_jsonl_fd = segment.fd_;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObAiAccessService not init", K(ret));
  } else if (OB_ISNULL(sql_client_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_client_ is null", K(ret));
  } else if (OB_ISNULL(allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("allocator_ is null", K(ret));
  } else if (segment.line_count_ <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("no data written to batch file", K(ret));
  } else {
    // Generate task_id
    task_info.tenant_id_ = tenant_id_;
    task_info.model_name_ = endpoint_info.get_name();
    task_info.command_type_ = command_type;
    task_info.status_ = OB_AI_TASK_STATUS_RUNNING;
    task_info.requests_handled_ = 0;
    task_info.total_requests_ = segment.line_count_;
    task_info.ddl_task_id_ = ddl_task_id;

    if (OB_FAIL(task_info.generate_task_id(task_id_buf, sizeof(task_id_buf)))) {
      LOG_WARN("failed to generate task_id", K(ret));
    } else {
      char *task_id_copy = static_cast<char *>(allocator_->alloc(task_info.task_id_.length() + 1));
      if (OB_ISNULL(task_id_copy)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate memory for task_id", K(ret));
      } else {
        MEMCPY(task_id_copy, task_info.task_id_.ptr(), task_info.task_id_.length());
        task_id_copy[task_info.task_id_.length()] = '\0';
        task_id.assign_ptr(task_id_copy, task_info.task_id_.length());
      }
    }
    // Register task to system table
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(table_manager_.register_task(task_info, *sql_client_))) {
      LOG_WARN("failed to register task", K(ret), K(task_id));
    } else {
      // Create ObAiAccessTask and initialize for FILE_UPLOADING phase
      void *buf = allocator_->alloc(sizeof(ObAiAccessTask));
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate task object", K(ret));
      } else {
        ObAiAccessTask *task = new (buf) ObAiAccessTask();
        if (OB_FAIL(task->init(*allocator_,
                               OB_AI_ACCESS_MODE_BATCH_FILE,
                               command_type,
                               segment.line_count_,
                               endpoint_info, task_id, &table_manager_, sql_client_,
                               allow_null_on_failure))) {
          LOG_WARN("failed to init task", K(ret), K(task_id));
          task->~ObAiAccessTask();
          allocator_->free(buf);
        } else {
          // Set Service pointer and JSONL fd from the writer's finished file
          task->service_ = this;
          task->jsonl_fd_ = unowned_jsonl_fd;
          unowned_jsonl_fd = -1;
          task->jsonl_size_ = segment.size_;
          task->jsonl_line_count_ = segment.line_count_;

          // Register and schedule the task
          bool task_in_map = false;
          if (OB_FAIL(register_task_object(task_id, task, task_in_map))) {
            LOG_WARN("failed to register task object", K(ret), K(task_id), K(task_in_map));
            if (!task_in_map) {
              task->~ObAiAccessTask();
              allocator_->free(buf);
            }
            // Mark the already-inserted system table record as FAILED to avoid a
            // permanent RUNNING orphan that only restores on observer restart.
            if (OB_NOT_NULL(sql_client_)) {
              int tmp_ret = table_manager_.update_task_status(
                  task_id, OB_AI_TASK_STATUS_FAILED, 0, ret,
                  ObString("Failed to schedule task after system table registration"),
                  0, ObString(), *sql_client_);
              if (OB_SUCCESS != tmp_ret) {
                LOG_WARN("failed to mark orphan task as FAILED in system table",
                         K(tmp_ret), K(task_id));
              }
            }
          } else {
            LOG_DEBUG("[BATCH-FILE] committed batch task",
                     K(task_id), K(ddl_task_id), K(segment.fd_),
                     K(segment.size_), K(segment.line_count_));
          }
        }
      }
    }
  }
  if (unowned_jsonl_fd >= 0) {
    const int remove_ret = FILE_MANAGER_INSTANCE_WITH_MTL_SWITCH.remove(tenant_id_, unowned_jsonl_fd);
    if (OB_SUCCESS != remove_ret) {
      LOG_WARN("failed to cleanup unowned batch task jsonl fd",
               K(remove_ret), K(unowned_jsonl_fd), K(segment), K_(tenant_id), K(ret));
      if (OB_SUCC(ret)) {
        ret = remove_ret;
      }
    }
  }
  return ret;
}

int ObAiAccessService::get_next_results(const common::ObString &task_id,
                                          int64_t batch_size,
                                          common::ObIArray<share::ObAiResultRow> &results,
                                          bool &has_more)
{
  int ret = OB_SUCCESS;
  results.reset();
  has_more = false;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObAiAccessService not init", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(task_id.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("task_id is empty", K(ret));
  } else {
    // First check task status
    share::ObAiTaskInfo task_info;
    if (OB_FAIL(query_task_status(task_id, task_info))) {
      LOG_WARN("failed to query task status", K(ret), K(task_id));
    } else if (task_info.status_ != OB_AI_TASK_STATUS_FINISHED) {
      ret = OB_ENTRY_NOT_EXIST;  // Results not ready yet
      LOG_DEBUG("task not finished yet", K(task_id), K(task_info.status_));
    } else {
      ObAiAccessTask *task = nullptr;
      {
        common::ObSpinLockGuard guard(task_map_lock_);
        if (OB_FAIL(task_map_.get_refactored(task_id, task))) {
          if (ret == OB_ENTRY_NOT_EXIST) {
            LOG_WARN("task object not found", K(ret), K(task_id));
          } else {
            LOG_WARN("failed to get task from map", K(ret), K(task_id));
          }
        } else if (OB_ISNULL(task)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("task is null in map", K(ret), K(task_id));
        } else {
          task->pin();
        }
      }
      if (OB_FAIL(ret)) {
        // error already logged in lock scope
      } else if (OB_ISNULL(task)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("task is null after lock", K(ret), K(task_id));
      } else if (OB_FAIL(task->get_next_result(batch_size, results, has_more))) {
        LOG_WARN("failed to get next result from task", K(ret), K(task_id), K(batch_size));
        task->unpin();
      } else {
        LOG_INFO("get_next_results: returned results",
                 K(task_id), K(batch_size), K(results.count()), K(has_more));
        task->unpin();
      }
    }
  }

  return ret;
}

int ObAiAccessService::register_task_object(const common::ObString &task_id,
                                              ObAiAccessTask *task,
                                              bool &task_in_map)
{
  int ret = OB_SUCCESS;
  task_in_map = false;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObAiAccessService not init", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(task_id.empty()) || OB_ISNULL(task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(task_id), KP(task));
  } else {
    common::ObSpinLockGuard guard(task_map_lock_);
    bool task_inserted = false;

    if (OB_FAIL(task_map_.set_refactored(task_id, task))) {
      LOG_WARN("failed to register task to map", K(ret), K(task_id));
    } else {
      task_inserted = true;
#ifdef ERRSIM
      ret = OB_E(common::EventTable::EN_AI_SERVICE_REGISTER_TASK_ERR) OB_SUCCESS;
      if (OB_FAIL(ret)) {
        LOG_WARN("[ERRSIM] fail to register task object", KR(ret), K(task_id));
      }
#endif
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(scheduler_.schedule_task(*task))) {
        LOG_WARN("failed to submit task to scheduler", K(ret), K(task_id));
      } else {
        task_in_map = true;
        LOG_INFO("registered task and submitted to scheduler", K(task_id));
      }
    }
    if (OB_FAIL(ret) && task_inserted) {
      if (OB_SUCCESS != task_map_.erase_refactored(task_id)) {
        LOG_WARN("failed to rollback registered task object from map, marking abandoned",
                 K(ret), K(task_id));
        task->set_abandoned();
        task_in_map = true;
      }
    }
  }

  return ret;
}

int ObAiAccessService::get_task_object(const common::ObString &task_id,
                                         ObAiAccessTask *&task)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObAiAccessService not init", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(task_id.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("task_id is empty", K(ret));
  } else {
    common::ObSpinLockGuard guard(task_map_lock_);
    if (OB_FAIL(task_map_.get_refactored(task_id, task))) {
      if (ret != OB_HASH_NOT_EXIST) {
        LOG_WARN("failed to get task from map", K(ret), K(task_id));
      }
    }
  }

  return ret;
}

int ObAiAccessService::get_and_pin_task_object(const common::ObString &task_id,
                                                  ObAiAccessTask *&task)
{
  int ret = OB_SUCCESS;
  task = nullptr;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObAiAccessService not init", K(ret));
  } else if (OB_UNLIKELY(task_id.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("task_id is empty", K(ret));
  } else {
    common::ObSpinLockGuard guard(task_map_lock_);
    if (OB_FAIL(task_map_.get_refactored(task_id, task))) {
      if (ret != OB_HASH_NOT_EXIST) {
        LOG_WARN("failed to get task from map", K(ret), K(task_id));
      }
    } else if (OB_NOT_NULL(task)) {
      task->pin();  // pin while holding lock, before cleanup_terminal_tasks_() can free it
    }
  }

  return ret;
}

int ObAiAccessService::unregister_task_object(const common::ObString &task_id)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObAiAccessService not init", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(task_id.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("task_id is empty", K(ret));
  } else {
    common::ObSpinLockGuard guard(task_map_lock_);
    if (OB_FAIL(task_map_.erase_refactored(task_id))) {
      if (ret != OB_HASH_NOT_EXIST) {
        LOG_WARN("failed to remove task from map", K(ret), K(task_id));
      } else {
        ret = OB_SUCCESS;  // Not found is acceptable
      }
    } else {
      LOG_INFO("unregistered task object from service", K(task_id));
    }
  }

  return ret;
}

int ObAiAccessService::collect_abandoned_tasks(
    common::ObIArray<ObAiAccessTask*> &tasks,
    common::ObIArray<common::ObString> &task_ids)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObAiAccessService not init", K(ret));
  } else {
    common::ObSpinLockGuard guard(task_map_lock_);
    auto collector = [&](const common::hash::HashMapPair<common::ObString, ObAiAccessTask*> &entry) -> int {
      ObAiAccessTask *task = entry.second;
      if (OB_NOT_NULL(task) && task->is_abandoned()) {
        task->pin();
        // Use task->get_task_id() (stored on task's local_allocator_) rather than
        // entry.first (map key), so the ObString remains valid while task is pinned.
        if (OB_FAIL(tasks.push_back(task))) {
          task->unpin();
          LOG_WARN("collect_abandoned_tasks: push_back task failed", K(ret));
        } else if (OB_FAIL(task_ids.push_back(task->get_task_id()))) {
          (void)tasks.pop_back();  // keep tasks/task_ids aligned
          task->unpin();
          LOG_WARN("collect_abandoned_tasks: push_back task_id failed", K(ret));
        }
      }
      return ret;
    };
    if (OB_FAIL(task_map_.foreach_refactored(collector))) {
      LOG_WARN("collect_abandoned_tasks: foreach_refactored failed", K(ret));
    }
    // Caller returns early on failure and will not run the processing loop; unpin any
    // tasks we did collect so pin_count does not leak and cleanup_terminal can proceed.
    if (OB_FAIL(ret)) {
      for (int64_t i = 0; i < tasks.count(); ++i) {
        ObAiAccessTask *pinned_task = tasks.at(i);
        if (OB_NOT_NULL(pinned_task)) {
          pinned_task->unpin();
        }
      }
    }
  }

  return ret;
}

int ObAiAccessService::cleanup_terminal_tasks_()
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObAiAccessService not init", K(ret));
  } else {
    // Collect archived task_ids under lock, then erase and free
    common::ObSEArray<common::ObString, 8> expired_task_ids;
    common::ObSEArray<ObAiAccessTask*, 8> expired_tasks;
    expired_task_ids.set_attr(ObMemAttr(MTL_ID(), "ExpiredTaskIds"));
    expired_tasks.set_attr(ObMemAttr(MTL_ID(), "ExpiredTasks"));
    {
      common::ObSpinLockGuard guard(task_map_lock_);
      // Use foreach_refactored to scan all entries (task_map_ is small, typically <= 8)
      auto collector = [&](const common::hash::HashMapPair<common::ObString, ObAiAccessTask*> &entry) -> int {
        ObAiAccessTask *task = entry.second;
        if (OB_NOT_NULL(task)
            && task->is_terminal_state()
            && task->is_archived()
            && task->get_pin_count() == 0) {
          // task->get_task_id() points into task's local_allocator_; valid until task is freed below.
          if (OB_FAIL(expired_task_ids.push_back(task->get_task_id()))) {
            LOG_WARN("cleanup_terminal_tasks_: push_back task_id failed", K(ret));
          } else if (OB_FAIL(expired_tasks.push_back(task))) {
            (void)expired_task_ids.pop_back();
            LOG_WARN("cleanup_terminal_tasks_: push_back task failed", K(ret));
          }
        }
        return ret;
      };
      if (OB_FAIL(task_map_.foreach_refactored(collector))) {
        LOG_WARN("foreach_refactored failed during cleanup scan", K(ret));
      }
      // Erase expired entries from map (still under lock)
      for (int64_t i = 0; i < expired_task_ids.count(); ++i) {
        task_map_.erase_refactored(expired_task_ids.at(i));
      }
    }
    // Free task objects outside lock
    for (int64_t i = 0; i < expired_tasks.count(); ++i) {
      ObAiAccessTask *task = expired_tasks.at(i);
      if (OB_NOT_NULL(task)) {
        LOG_INFO("cleanup terminal task", "task_id", expired_task_ids.at(i),
                 "state", task->get_state(), "terminal_ts", task->get_terminal_ts());
        task->~ObAiAccessTask();
        allocator_->free(task);
      }
    }
  }

  return ret;
}

//=============================================== ObAiAccessServiceManager Implementation ================================================

namespace
{
struct ServiceEntry
{
  uint64_t tenant_id_;
  ObAiAccessService *service_;
  ServiceEntry() : tenant_id_(OB_INVALID_TENANT_ID), service_(nullptr) {}
};

const int64_t MAX_SERVICE_ENTRIES = 1024;
static ServiceEntry g_service_entries[MAX_SERVICE_ENTRIES];
static common::ObSpinLock g_service_lock(common::ObLatchIds::OB_EMBEDDING_TASK_HANDLER_SPIN_LOCK);

ServiceEntry* find_entry(uint64_t tenant_id)
{
  ServiceEntry *entry = nullptr;
  for (int64_t i = 0; i < MAX_SERVICE_ENTRIES; ++i) {
    if (g_service_entries[i].tenant_id_ == tenant_id) {
      entry = &g_service_entries[i];
      break;
    }
  }
  return entry;
}

ServiceEntry* find_free_entry()
{
  ServiceEntry *entry = nullptr;
  for (int64_t i = 0; i < MAX_SERVICE_ENTRIES; ++i) {
    if (g_service_entries[i].tenant_id_ == OB_INVALID_TENANT_ID) {
      entry = &g_service_entries[i];
      break;
    }
  }
  return entry;
}
} // anonymous namespace

ObAiAccessService* ObAiAccessServiceManager::get_instance(uint64_t tenant_id)
{
  ObAiAccessService *service = nullptr;
  ObSpinLockGuard guard(g_service_lock);
  ServiceEntry *entry = find_entry(tenant_id);
  if (OB_NOT_NULL(entry)) {
    service = entry->service_;
  }
  return service;
}

int ObAiAccessServiceManager::create_instance(uint64_t tenant_id,
                                                 ObIAllocator &allocator,
                                                 int64_t thread_num)
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(g_service_lock);

  ServiceEntry *existing = find_entry(tenant_id);
  if (OB_NOT_NULL(existing)) {
    ret = OB_ENTRY_EXIST;
    LOG_WARN("service already exists for tenant", K(ret), K(tenant_id));
  } else {
    ServiceEntry *entry = find_free_entry();
    if (OB_ISNULL(entry)) {
      ret = OB_SIZE_OVERFLOW;
      LOG_WARN("no free slot for service entry", K(ret), K(tenant_id));
    } else {
      void *buf = allocator.alloc(sizeof(ObAiAccessService));
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate memory for service", K(ret));
      } else {
        ObAiAccessService *service = new (buf) ObAiAccessService();
        if (OB_FAIL(service->init(allocator, tenant_id, thread_num))) {
          LOG_WARN("failed to init service", K(ret), K(tenant_id));
          service->~ObAiAccessService();
          allocator.free(buf);
        } else if (OB_FAIL(service->start())) {
          LOG_WARN("failed to start service", K(ret), K(tenant_id));
          service->destroy();
          service->~ObAiAccessService();
          allocator.free(buf);
        } else {
          entry->tenant_id_ = tenant_id;
          entry->service_ = service;
          LOG_INFO("created AI execution service for tenant", K(tenant_id), K(thread_num));
        }
      }
    }
  }
  return ret;
}

void ObAiAccessServiceManager::destroy_instance(uint64_t tenant_id)
{
  ObSpinLockGuard guard(g_service_lock);
  ServiceEntry *entry = find_entry(tenant_id);
  if (OB_NOT_NULL(entry) && OB_NOT_NULL(entry->service_)) {
    ObAiAccessService *service = entry->service_;
    service->destroy();
    service->~ObAiAccessService();
    entry->tenant_id_ = OB_INVALID_TENANT_ID;
    entry->service_ = nullptr;
    LOG_INFO("destroyed AI execution service for tenant", K(tenant_id));
  }
}

bool ObAiAccessServiceManager::has_instance(uint64_t tenant_id)
{
  ObSpinLockGuard guard(g_service_lock);
  return OB_NOT_NULL(find_entry(tenant_id));
}

} // namespace vector_index
} // namespace oceanbase

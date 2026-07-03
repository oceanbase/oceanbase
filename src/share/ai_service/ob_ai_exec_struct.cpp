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

#include "share/ai_service/ob_ai_exec_struct.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/time/ob_time_utility.h"
#include "lib/json_type/ob_json_tree.h"
#include "lib/json_type/ob_json_common.h"
#include "share/rc/ob_tenant_base.h"

namespace oceanbase
{
namespace share
{

// String constants for enum conversion
static const char *COMMAND_TYPE_STR[] = {
  "INVALID",
  "EMBED",
  "COMPLETE",
  "RERANK"
};

static const char *SERVICE_TIER_STR[] = {
  "standard",
  "batch",
  "flex"
};

static const char *TASK_STATUS_STR[] = {
  "INVALID",
  "RESERVED_1",
  "RESERVED_2",
  "PENDING",
  "RUNNING",
  "FINISHED",
  "FAILED",
  "CANCELLED"
};

static const char *TASK_PHASE_STR[] = {
  "INIT",
  "HTTP_SENT",
  "HTTP_COMPLETED",
  "PARSED",
  "RESERVED",
  "DONE"
};

static const char *SOURCE_TYPE_STR[] = {
  "INVALID",
  "MEMORY",
  "FILE",
  "TABLE"
};

// ObAiResultItem implementation
int ObAiResultItem::deep_copy_vector(common::ObIAllocator &allocator, const float *src, int64_t size)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(src) || size <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(src), K(size));
  } else {
    float *new_vec = static_cast<float *>(allocator.alloc(size * sizeof(float)));
    if (OB_ISNULL(new_vec)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory for vector", K(ret), K(size));
    } else {
      MEMCPY(new_vec, src, size * sizeof(float));
      embedding_vector_ = new_vec;
      vector_size_ = size;
    }
  }
  return ret;
}

// ObAiTaskInfo implementation
int ObAiTaskInfo::generate_task_id(char *buf, int64_t buf_len)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || buf_len < 64) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(buf), K(buf_len));
  } else {
    // Generate UUID-like task ID: tenant_timestamp_random
    int64_t ts = common::ObTimeUtility::current_time();
    int64_t random_val = common::ObRandom::rand(0, INT64_MAX);
    int64_t pos = snprintf(buf, buf_len, "ai_%lu_%ld_%ld", tenant_id_, ts, random_val);
    if (pos < 0 || pos >= buf_len) {
      ret = OB_SIZE_OVERFLOW;
      LOG_WARN("task id too long", K(ret), K(pos), K(buf_len));
    } else {
      task_id_.assign_ptr(buf, static_cast<int32_t>(pos));
    }
  }
  return ret;
}

int ObAiTaskInfo::serialize_file_metadata(char *buf, int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || buf_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(buf), K(buf_len));
  } else {
    pos = snprintf(buf, buf_len,
        "{\"jsonl_fd\":%ld,\"jsonl_size\":%ld,\"jsonl_line_count\":%ld,"
        "\"result_fd\":%ld,\"result_size\":%ld,\"result_line_count\":%ld}",
        jsonl_fd_, jsonl_size_, jsonl_line_count_,
        result_fd_, result_size_, result_line_count_);
    if (pos < 0 || pos >= buf_len) {
      ret = OB_SIZE_OVERFLOW;
      LOG_WARN("file_metadata buffer overflow", K(ret), K(pos), K(buf_len));
    }
  }
  return ret;
}

int ObAiTaskInfo::deserialize_file_metadata(const ObString &json)
{
  int ret = OB_SUCCESS;
  if (json.empty()) {
    // Empty metadata is valid (e.g., new task)
  } else {
    // Simple sscanf-based parsing for fixed JSON format
    int matched = sscanf(json.ptr(),
        "{\"jsonl_fd\":%ld,\"jsonl_size\":%ld,\"jsonl_line_count\":%ld,"
        "\"result_fd\":%ld,\"result_size\":%ld,\"result_line_count\":%ld}",
        &jsonl_fd_, &jsonl_size_, &jsonl_line_count_,
        &result_fd_, &result_size_, &result_line_count_);
    if (matched != 6) {
      LOG_WARN("failed to parse file_metadata JSON, keeping defaults",
               K(json), K(matched));
      // Non-fatal: keep default values
    }
  }
  return ret;
}

int ObAiTaskInfo::build_remote_files_json(common::ObIAllocator &alloc,
                                         const ObString &input_file_id,
                                         const ObString &output_file_id,
                                         const ObString &error_file_id,
                                         ObString &json_out)
{
  int ret = OB_SUCCESS;
  common::ObArenaAllocator tmp_alloc("AiRmtJson", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  common::ObJsonObject obj(&tmp_alloc);

  common::ObJsonString input_node(input_file_id.ptr(), input_file_id.length());
  common::ObJsonString output_node(output_file_id.ptr(), output_file_id.length());
  common::ObJsonString error_node(error_file_id.ptr(), error_file_id.length());

  if (!input_file_id.empty()) {
    if (OB_FAIL(obj.add(common::ObString("input_file_id"), &input_node))) {
      LOG_WARN("failed to add input_file_id", K(ret));
    }
  }
  if (OB_SUCC(ret) && !output_file_id.empty()) {
    if (OB_FAIL(obj.add(common::ObString("output_file_id"), &output_node))) {
      LOG_WARN("failed to add output_file_id", K(ret));
    }
  }
  if (OB_SUCC(ret) && !error_file_id.empty()) {
    if (OB_FAIL(obj.add(common::ObString("error_file_id"), &error_node))) {
      LOG_WARN("failed to add error_file_id", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    ObJsonBuffer j_buf(&tmp_alloc);
    if (OB_FAIL(obj.print(j_buf, false))) {
      LOG_WARN("failed to print remote files json", K(ret));
    } else {
      char *dest = static_cast<char*>(alloc.alloc(j_buf.length() + 1));
      if (OB_ISNULL(dest)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else {
        MEMCPY(dest, j_buf.ptr(), j_buf.length());
        dest[j_buf.length()] = '\0';
        json_out.assign_ptr(dest, static_cast<int32_t>(j_buf.length()));
      }
    }
  }
  return ret;
}

int ObAiTaskInfo::parse_remote_files_json(const ObString &json, ObAiRemoteFilesView &out)
{
  int ret = OB_SUCCESS;
  out.reset();
  if (json.empty()) {
    // Empty is valid — all fields stay reset
  } else {
    // Manual extraction: for each known key, search the JSON string and extract
    // the value between quotes. Handles fields in any order and absent fields.
    const char *keys[]  = {"\"input_file_id\":\"",
                           "\"output_file_id\":\"", "\"error_file_id\":\""};
    ObString *targets[]  = {&out.input_file_id_,
                            &out.output_file_id_, &out.error_file_id_};
    for (int i = 0; i < 3; i++) {
      const char *p = strstr(json.ptr(), keys[i]);
      if (OB_NOT_NULL(p)) {
        p += strlen(keys[i]);  // skip to start of value
        const char *end = strchr(p, '"');
        if (OB_NOT_NULL(end)) {
          int32_t len = static_cast<int32_t>(end - p);
          if (len > 0) {
            targets[i]->assign_ptr(p, len);
          }
        }
      }
    }
  }
  return ret;
}

// ObAiExecUtils implementation
const char *ObAiExecUtils::get_command_type_str(ObAiCommandType type)
{
  if (type >= OB_AI_COMMAND_INVALID && type < OB_AI_COMMAND_MAX) {
    return COMMAND_TYPE_STR[type];
  }
  return "UNKNOWN";
}

const char *ObAiExecUtils::get_ai_service_tier_str(ObAiServiceTier tier)
{
  if (tier >= OB_AI_SERVICE_TIER_STANDARD && tier < OB_AI_SERVICE_TIER_MAX) {
    return SERVICE_TIER_STR[tier];
  }
  return "UNKNOWN";
}

const char *ObAiExecUtils::get_task_status_str(ObAiTaskStatus status)
{
  if (status >= OB_AI_TASK_STATUS_INVALID && status <= OB_AI_TASK_STATUS_CANCELLED) {
    return TASK_STATUS_STR[status];
  }
  return "UNKNOWN";
}

const char *ObAiExecUtils::get_task_phase_str(ObAiTaskPhase phase)
{
  if (phase >= OB_AI_TASK_PHASE_INIT && phase <= OB_AI_TASK_PHASE_DONE) {
    return TASK_PHASE_STR[phase];
  }
  return "UNKNOWN";
}

const char *ObAiExecUtils::get_source_type_str(ObAiSourceType type)
{
  if (type >= OB_AI_SOURCE_TYPE_INVALID && type < OB_AI_SOURCE_TYPE_MAX) {
    return SOURCE_TYPE_STR[type];
  }
  return "UNKNOWN";
}

ObAiCommandType ObAiExecUtils::str_to_command_type(const ObString &str)
{
  ObAiCommandType type = OB_AI_COMMAND_INVALID;
  for (int i = OB_AI_COMMAND_INVALID; i < OB_AI_COMMAND_MAX; ++i) {
    if (str.case_compare(COMMAND_TYPE_STR[i]) == 0) {
      type = static_cast<ObAiCommandType>(i);
      break;
    }
  }
  return type;
}

ObAiServiceTier ObAiExecUtils::str_to_ai_service_tier(const ObString &str)
{
  ObAiServiceTier tier = OB_AI_SERVICE_TIER_MAX;
  for (int i = OB_AI_SERVICE_TIER_STANDARD; i < OB_AI_SERVICE_TIER_MAX; ++i) {
    if (str.case_compare(SERVICE_TIER_STR[i]) == 0) {
      tier = static_cast<ObAiServiceTier>(i);
      break;
    }
  }
  return tier;
}

int ObAiExecUtils::build_error_detail_json(int ob_error_code,
                                           int64_t model_http_code,
                                           const common::ObString &message,
                                           char *buf, int64_t buf_len)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || buf_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid buffer", K(ret), KP(buf), K(buf_len));
  } else {
    buf[0] = '\0';
    common::ObArenaAllocator tmp_alloc("AiErrJson", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
    common::ObJsonObject obj(&tmp_alloc);
    common::ObJsonInt code_node(static_cast<int64_t>(ob_error_code));
    common::ObJsonInt http_node(model_http_code);
    const char *msg_ptr = (message.ptr() != nullptr) ? message.ptr() : "";
    const int64_t msg_len = MIN(message.length(), OB_AI_MAX_ERROR_MESSAGE_LENGTH);
    common::ObJsonString msg_node(msg_ptr, static_cast<uint64_t>(msg_len));
    if (OB_FAIL(obj.add(common::ObString("ob_error_code"), &code_node))) {
      LOG_WARN("failed to add ob_error_code", K(ret));
    } else if (OB_FAIL(obj.add(common::ObString("model_http_code"), &http_node))) {
      LOG_WARN("failed to add model_http_code", K(ret));
    } else if (OB_FAIL(obj.add(common::ObString("message"), &msg_node))) {
      LOG_WARN("failed to add message", K(ret));
    } else {
      ObJsonBuffer j_buf(&tmp_alloc);
      if (OB_FAIL(obj.print(j_buf, false))) {
        LOG_WARN("failed to print error detail json", K(ret));
      } else if (j_buf.length() >= static_cast<uint64_t>(buf_len)) {
        ret = OB_SIZE_OVERFLOW;
        LOG_WARN("error detail json too long", K(ret), K(j_buf.length()), K(buf_len));
      } else {
        MEMCPY(buf, j_buf.ptr(), j_buf.length());
        buf[j_buf.length()] = '\0';
      }
    }
  }
  return ret;
}

} // namespace share
} // namespace oceanbase

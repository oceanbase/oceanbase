/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SHARE

#include "share/ai_service/ob_ai_batch_file_writer.h"
#include "share/vector_index/ob_ai_access_service.h"
#include "lib/allocator/page_arena.h"
#include "lib/json/ob_json.h"
#include "lib/time/ob_time_utility.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/utility/ob_macro_utils.h"

namespace oceanbase
{
namespace share
{

// ObAiBatchTaskWriter implementation

ObAiBatchTaskWriter::ObAiBatchTaskWriter()
    : is_inited_(false),
      is_committed_(false),
      service_(nullptr),
      ddl_task_id_(0),
      dir_id_(-1),
      tenant_id_(OB_INVALID_TENANT_ID),
      command_type_(OB_AI_COMMAND_INVALID),
      endpoint_info_(nullptr),
      allow_null_on_failure_(false),
      current_line_count_(0),
      line_alloc_("BatchLineJson", OB_MALLOC_NORMAL_BLOCK_SIZE)
{
}

ObAiBatchTaskWriter::~ObAiBatchTaskWriter()
{
  if (is_inited_ && !is_committed_) {
    // RAII: release TmpFile fd if not committed
    jsonl_writer_.destroy();
    LOG_INFO("[BATCH-FILE] ObAiBatchTaskWriter destroyed without commit, fd released",
             K_(ddl_task_id), K_(current_line_count));
  }
}

void ObAiBatchTaskWriter::reset()
{
  // After commit(), finish() already set fd_=-1, so jsonl_writer_.reset() won't double-remove.
  // For an uncommitted writer the caller must not call reset() — use the destructor path instead.
  jsonl_writer_.reset();
  line_alloc_.reset();
  service_ = nullptr;
  endpoint_info_ = nullptr;
  ddl_task_id_ = 0;
  dir_id_ = -1;
  tenant_id_ = OB_INVALID_TENANT_ID;
  command_type_ = OB_AI_COMMAND_INVALID;
  allow_null_on_failure_ = false;
  current_line_count_ = 0;
  is_committed_ = false;
  is_inited_ = false;
}

int ObAiBatchTaskWriter::init(vector_index::ObAiAccessService *service,
                               const ObAiModelEndpointInfo &endpoint_info,
                               ObAiCommandType command_type,
                               int64_t ddl_task_id,
                               int64_t dir_id,
                               uint64_t tenant_id,
                               bool allow_null_on_failure)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObAiBatchTaskWriter already initialized", K(ret));
  } else if (OB_ISNULL(service)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("service is null", K(ret));
  } else if (OB_UNLIKELY(dir_id < 0 || OB_INVALID_TENANT_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(dir_id), K(tenant_id));
  } else if (OB_FAIL(jsonl_writer_.init(dir_id, tenant_id, "BatchTaskW"))) {
    LOG_WARN("failed to init jsonl writer", K(ret), K(dir_id), K(tenant_id));
  } else {
    service_ = service;
    endpoint_info_ = &endpoint_info;
    command_type_ = command_type;
    allow_null_on_failure_ = allow_null_on_failure;
    ddl_task_id_ = ddl_task_id;
    dir_id_ = dir_id;
    tenant_id_ = tenant_id;
    line_alloc_.set_tenant_id(tenant_id);
    current_line_count_ = 0;
    is_committed_ = false;
    is_inited_ = true;
    LOG_INFO("[BATCH-FILE] ObAiBatchTaskWriter initialized",
             K_(ddl_task_id), K_(dir_id), K_(tenant_id));
  }
  return ret;
}

int ObAiBatchTaskWriter::append(const common::ObAiBatchFileLine &line)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObAiBatchTaskWriter not initialized", K(ret));
  } else if (OB_UNLIKELY(is_committed_)) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("writer already committed", K(ret));
  } else if (OB_UNLIKELY(line.line_size_ > common::ObAiBatchFileConstraints::MAX_LINE_SIZE_BYTES)) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("line size exceeds 6MB limit", K(ret), K(line.line_size_),
             K(common::ObAiBatchFileConstraints::MAX_LINE_SIZE_BYTES));
  } else if (current_line_count_ >= common::ObAiBatchFileConstraints::MAX_LINES_PER_FILE
             || jsonl_writer_.get_size() + line.line_size_ > common::ObAiBatchFileConstraints::MAX_FILE_SIZE_BYTES) {
    ret = OB_ITER_END;
    LOG_DEBUG("[BATCH-FILE] threshold reached, need commit+re-open",
             K_(current_line_count), "file_size", jsonl_writer_.get_size(), K(line.line_size_),
             K_(ddl_task_id));
  } else {
    line_alloc_.reuse();
    common::ObString json_str;
    if (OB_FAIL(line.to_json(line_alloc_, json_str))) {
      LOG_WARN("failed to convert line to json", K(ret), K(line.custom_id_));
    } else if (OB_FAIL(jsonl_writer_.write_line(json_str))) {
      LOG_WARN("failed to write jsonl line to tmp file", K(ret),
               K(json_str.length()), K(line.custom_id_));
    } else {
      current_line_count_++;
    }
  }
  return ret;
}

int ObAiBatchTaskWriter::commit(common::ObString &task_id)
{
  int ret = OB_SUCCESS;
  task_id.reset();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObAiBatchTaskWriter not initialized", K(ret));
  } else if (OB_UNLIKELY(is_committed_)) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("writer already committed", K(ret));
  } else if (OB_ISNULL(service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("service is null", K(ret));
  } else {
    ObBatchFileDataSegment segment;
    if (OB_FAIL(jsonl_writer_.finish(segment))) {
      LOG_WARN("failed to finish jsonl writer", K(ret), K_(ddl_task_id));
    } else if (OB_FAIL(service_->commit_batch_task_(segment, ddl_task_id_,
                                                      *endpoint_info_, command_type_, task_id,
                                                      allow_null_on_failure_))) {
      LOG_WARN("failed to commit batch task", K(ret), K_(ddl_task_id));
    } else {
      is_committed_ = true;
      LOG_INFO("[BATCH-FILE] ObAiBatchTaskWriter committed",
               K(task_id), K_(ddl_task_id), K_(current_line_count),
               "file_size", segment.size_);
    }
  }
  return ret;
}

} // namespace share
} // namespace oceanbase

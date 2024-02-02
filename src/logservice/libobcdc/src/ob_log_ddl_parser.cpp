/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX OBLOG_PARSER

#include "ob_log_ddl_parser.h"

#include "ob_log_instance.h"            // IObLogErrHandler
#include "ob_log_part_trans_parser.h"   // IObLogPartTransParser
#include "ob_log_part_trans_task.h"     // PartTransTask
#include "ob_log_trace_id.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace libobcdc
{

ObLogDdlParser::ObLogDdlParser() :
    inited_(false),
    err_handler_(NULL),
    part_trans_parser_(NULL),
    push_seq_(0)
{
}

ObLogDdlParser::~ObLogDdlParser()
{
  destroy();
}

int ObLogDdlParser::init(const int64_t thread_num,
    const int64_t queue_size,
    IObLogErrHandler &err_handler,
    IObLogPartTransParser &part_trans_parser)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    LOG_ERROR("DDL parser has been initialized");
    ret = OB_INIT_TWICE;
  } else if (OB_UNLIKELY(thread_num <= 0)
      || OB_UNLIKELY(thread_num > MAX_THREAD_NUM)
      || OB_UNLIKELY(queue_size <= 0)) {
    LOG_ERROR("invalid argument", K(thread_num), LITERAL_K(MAX_THREAD_NUM),
        K(queue_size));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(DdlParserThread::init(thread_num, queue_size))) {
    LOG_ERROR("init DDL parser queue thread fail", KR(ret), K(thread_num), K(queue_size));
  } else {
    err_handler_ = &err_handler;
    part_trans_parser_ = &part_trans_parser;
    push_seq_ = 0;
    inited_ = true;

    LOG_INFO("init DDL parser succ", K(thread_num), K(queue_size));
  }

  return ret;
}

void ObLogDdlParser::destroy()
{
  DdlParserThread::destroy();

  inited_ = false;
  err_handler_ = NULL;
  part_trans_parser_ = NULL;
  push_seq_ = 0;
}

int ObLogDdlParser::start()
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("DDL parser has not been initialized");
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(DdlParserThread::start())) {
    LOG_ERROR("start DDL parser thread fail", KR(ret), "thread_num", get_thread_num());
  } else {
    LOG_INFO("start DDL parser threads succ", "thread_num", get_thread_num());
  }

  return ret;
}

void ObLogDdlParser::stop()
{
  if (inited_) {
    DdlParserThread::stop();
    LOG_INFO("stop DDL parser threads succ", "thread_num", get_thread_num());
  }
}

int ObLogDdlParser::push(PartTransTask &task, const int64_t timeout)
{
  int ret = OB_SUCCESS;
  int64_t push_hash = ATOMIC_FAA(&push_seq_, 1);

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("DDL parser has not been initialized");
    ret = OB_NOT_INIT;
  }
  // Verify that the task information is valid
  else if (OB_UNLIKELY(! task.is_task_info_valid())) {
    LOG_ERROR("invalid task", K(task));
    ret = OB_INVALID_ARGUMENT;
  }
  // Deal only DDL type tasks
  else if (OB_UNLIKELY(! task.is_ddl_trans())) {
    LOG_ERROR("task type is not supported by DDL parser", K(task));
    ret = OB_NOT_SUPPORTED;
  } else if (OB_UNLIKELY(DdlParserThread::is_stoped())) {
    LOG_INFO("DDL parser has been stoped");
    ret = OB_IN_STOP_STATE;
  } else if (OB_FAIL(DdlParserThread::push(&task, push_hash, timeout))) {
    if (OB_TIMEOUT != ret && OB_IN_STOP_STATE != ret) {
      LOG_ERROR("push task into DDL queue thread fail", KR(ret), K(task), K(push_hash));
    }
  } else {
    LOG_DEBUG("push task into DDL parser", K(push_hash), K(task));
  }

  return ret;
}

int ObLogDdlParser::get_part_trans_task_count(int64_t &task_num)
{
  return DdlParserThread::get_total_task_num(task_num);
}

int ObLogDdlParser::handle(void *data,
    const int64_t thread_index,
    volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;
  ObLogTraceIdGuard trace_guard;
  PartTransTask *task = (PartTransTask *)data;

  if (OB_UNLIKELY(! inited_) || OB_ISNULL(part_trans_parser_)) {
    LOG_ERROR("DDL parser has not been initialized", K(part_trans_parser_));
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(task)) {
    LOG_ERROR("invalid argument", KPC(task));
    ret = OB_INVALID_ARGUMENT;
  }
  // Receive only DDL type tasks
  else if (OB_UNLIKELY(! task->is_ddl_trans())) {
    LOG_ERROR("task type is not supported by DDL Parser", KPC(task));
    ret = OB_NOT_SUPPORTED;
  } else {
    LOG_DEBUG("DDL parser handle task", K(thread_index), KPC(task));

    lib::Worker::CompatMode compat_mode = lib::Worker::CompatMode::INVALID;

    if (OB_FAIL(get_tenant_compat_mode(task->get_tenant_id(), compat_mode, stop_flag))) {
      LOG_ERROR("get_tenant_compat_mode fail", KR(ret), "tenant_id", task->get_tenant_id(),
          "compat_mode", print_compat_mode(compat_mode), KPC(task));
    } else {
      lib::CompatModeGuard g(compat_mode);
      const bool is_build_baseline = false;

      // Parse DDL task
      if (OB_FAIL(part_trans_parser_->parse(*task, is_build_baseline, stop_flag))) {
        LOG_ERROR("parse DDL task fail", KR(ret), KPC(task), K(is_build_baseline),
            "compat_mode", print_compat_mode(compat_mode));
      } else {
        // The DDL task does not need to go through the formatter module, and here the formatting is set to complete directly
        // DDL Handler directly waits for formatting to complete or not
        task->set_formatted();
        // The task cannot be accessed after the marker is completed
        task = NULL;
      }
    }
  }

  // exit on fail
  if (OB_SUCCESS != ret && OB_IN_STOP_STATE != ret && NULL != err_handler_) {
    err_handler_->handle_error(ret, "DDL parser thread exits, thread_index=%ld, err=%d",
        thread_index, ret);
    stop_flag = true;
  }

  return ret;
}

} // namespace libobcdc
} // namespace oceanbase

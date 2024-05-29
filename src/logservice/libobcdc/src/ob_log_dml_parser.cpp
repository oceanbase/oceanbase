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
 *
 * DML type task parser, work thread pool
 */

#define USING_LOG_PREFIX OBLOG_PARSER

#include "ob_log_dml_parser.h"

#include "ob_log_formatter.h"           // IObLogFormatter
#include "ob_log_instance.h"            // IObLogErrHandler
#include "ob_log_part_trans_parser.h"   // IObLogPartTransParser
#include "ob_ms_queue_thread.h"         // BitSet
#include "ob_log_resource_collector.h"  // IObLogResourceCollector
#include "ob_log_trace_id.h"
#include "ob_cdc_auto_config_mgr.h"     // CDC_CFG_MGR

using namespace oceanbase::common;

namespace oceanbase
{
namespace libobcdc
{

ObLogDmlParser::ObLogDmlParser() :
    inited_(false),
    round_value_(0),
    formatter_(NULL),
    err_handler_(NULL),
    part_trans_parser_(NULL),
    log_entry_task_count_(0)
{
}

ObLogDmlParser::~ObLogDmlParser()
{
  destroy();
}

int ObLogDmlParser::init(const int64_t parser_thread_num,
    const int64_t parser_queue_size,
    IObLogFormatter &formatter,
    IObLogErrHandler &err_handler,
    IObLogPartTransParser &part_trans_parser)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(inited_)) {
    LOG_ERROR("parser has been initialized");
    ret = OB_INIT_TWICE;
  } else if (OB_UNLIKELY(parser_thread_num <= 0)
      || OB_UNLIKELY(parser_thread_num > MAX_THREAD_NUM)
      || OB_UNLIKELY(parser_queue_size <= 0)) {
    LOG_ERROR("invalid argument", K(parser_thread_num), LITERAL_K(MAX_THREAD_NUM),
        K(parser_queue_size));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(DmlParserThread::init(parser_thread_num, parser_queue_size))) {
    LOG_ERROR("init parser queue thread fail", KR(ret), K(parser_thread_num), K(parser_queue_size));
  } else {
    formatter_ = &formatter;
    err_handler_ = &err_handler;
    part_trans_parser_ = &part_trans_parser;
    log_entry_task_count_ = 0;
    inited_ = true;

    LOG_INFO("init DML parser succ", K(parser_thread_num), K(parser_queue_size));
  }

  return ret;
}

void ObLogDmlParser::destroy()
{
  DmlParserThread::destroy();

  inited_ = false;
  round_value_ = 0;
  formatter_ =  NULL;
  err_handler_ = NULL;
  part_trans_parser_ = NULL;
  log_entry_task_count_ = 0;
}

int ObLogDmlParser::start()
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("parser has not been initialized");
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(DmlParserThread::start())) {
    LOG_ERROR("start parser thread fail", KR(ret), "thread_num", get_thread_num());
  } else {
    LOG_INFO("start DML parser threads succ", "thread_num", get_thread_num());
  }

  return ret;
}

void ObLogDmlParser::stop()
{
  if (inited_) {
    DmlParserThread::stop();
    LOG_INFO("stop DML parser threads succ", "thread_num", get_thread_num());
  }
}

int ObLogDmlParser::push(ObLogEntryTask &task, const int64_t timeout)
{
  int ret = OB_SUCCESS;
  PartTransTask *part_trans_task = NULL;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("DML parser has not been initialized");
    ret = OB_NOT_INIT;
  }
  // Verify that the task information is valid
  else if (OB_UNLIKELY(! task.is_valid())) {
    LOG_ERROR("invalid task", K(task));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_ISNULL(part_trans_task = static_cast<PartTransTask *>(task.get_host()))) {
    LOG_ERROR("part_trans_task is NULL", K(part_trans_task), K(task));
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_UNLIKELY(DmlParserThread::is_stoped())) {
    LOG_INFO("DML parser has been stoped");
    ret = OB_IN_STOP_STATE;
  } else {
    uint64_t hash_value = ATOMIC_FAA(&round_value_, 1);
    const DmlRedoLogNode *redo_log_node = task.get_redo_log_node();

    if (OB_ISNULL(redo_log_node)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("redo_log_node is NULL", KR(ret));
    } else {
      if (redo_log_node->is_direct_load_inc_log() && CDC_CFG_MGR.get_direct_load_inc_thread_num() < get_thread_num()) {
        hash_value = hash_value % CDC_CFG_MGR.get_direct_load_inc_thread_num();
      }

      if (OB_FAIL(DmlParserThread::push(&task, hash_value, timeout))) {
        if (OB_TIMEOUT != ret && OB_IN_STOP_STATE != ret) {
          LOG_ERROR("push task into DML queue thread fail", KR(ret), K(task));
        }
      } else {
        ATOMIC_INC(&log_entry_task_count_);
        LOG_DEBUG("push task into DML parser", KP(&task), K(task));
      }
    }
  }

  return ret;
}

int ObLogDmlParser::get_log_entry_task_count(int64_t &task_num)
{
  int ret = OB_SUCCESS;

  task_num = ATOMIC_LOAD(&log_entry_task_count_);

  return ret;
}

int ObLogDmlParser::handle(void *data,
    const int64_t thread_index,
    volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;
  ObLogTraceIdGuard trace_guard;
  ObLogEntryTask *task = (ObLogEntryTask *)(data);
  PartTransTask *part_trans_task = NULL;
  const DmlRedoLogNode *redo_log_node = NULL;

  if (OB_UNLIKELY(! inited_) || OB_ISNULL(part_trans_parser_)) {
    LOG_ERROR("DML parser has not been initialized", K(part_trans_parser_));
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(task)) {
    LOG_ERROR("invalid argument", KPC(task));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_ISNULL(part_trans_task = static_cast<PartTransTask *>(task->get_host()))) {
    LOG_ERROR("part_trans_task is NULL", K(part_trans_task), KPC(task));
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_ISNULL(redo_log_node = task->get_redo_log_node())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("redo_log_node is NULL", K(redo_log_node), KPC(task));
  } else if (redo_log_node->is_direct_load_inc_log() && OB_FAIL(wait_until_parser_pause_down_(thread_index, stop_flag))) {
    if (OB_IN_STOP_STATE != ret) {
      LOG_ERROR("wait until parser pause down failed", KR(ret), KP(task), KPC(task));
    }
  } else {
    LOG_DEBUG("DML parser handle task", K(thread_index), KP(task), KPC(task));
    const uint64_t tenant_id = part_trans_task->get_tenant_id();
    lib::Worker::CompatMode compat_mode = lib::Worker::CompatMode::INVALID;

    if (OB_FAIL(get_tenant_compat_mode(tenant_id, compat_mode, stop_flag))) {
      LOG_ERROR("get_tenant_compat_mode fail", KR(ret), K(tenant_id),
          "compat_mode", print_compat_mode(compat_mode), KPC(task));
    } else {
      lib::CompatModeGuard g(compat_mode);

      if (OB_FAIL(part_trans_parser_->parse(*task, stop_flag))) {
        LOG_ERROR("parse task fail", KR(ret), KPC(task), "compat_mode", print_compat_mode(compat_mode));
      } else if (OB_FAIL(task->set_redo_log_parsed())) {
        LOG_ERROR("failed to set log_entry_task parsed", KR(ret), KP(task), KPC(task));
      } else if (OB_FAIL(dispatch_task_(*task, *part_trans_task, stop_flag))) {
        if (OB_IN_STOP_STATE != ret) {
          LOG_ERROR("dispatch_task_ fail", KR(ret), KPC(task), KPC(part_trans_task));
        }
      }
    }

    if (OB_SUCC(ret)) {
      ATOMIC_DEC(&log_entry_task_count_);
    }
  }

  // Failure to exit
  if (OB_SUCCESS != ret && OB_IN_STOP_STATE != ret && NULL != err_handler_) {
    err_handler_->handle_error(ret, "DML parser thread exits, thread_index=%ld, err=%d",
        thread_index, ret);
    stop_flag = true;
  }

  return ret;
}


int ObLogDmlParser::dispatch_task_(ObLogEntryTask &log_entry_task,
    PartTransTask &part_trans_task,
    volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;
  const int64_t stmt_count = log_entry_task.get_stmt_list().num_;

  if (stmt_count <= 0 ) {
    if (OB_FAIL(handle_empty_stmt_(log_entry_task, part_trans_task, stop_flag))) {
      LOG_ERROR("handle_empty_stmt_ fail", KR(ret), K(log_entry_task), K(part_trans_task));
    }
  } else {
    if (OB_FAIL(push_task_into_formatter_(log_entry_task, stop_flag))) {
      if (OB_IN_STOP_STATE != ret) {
        LOG_ERROR("push_task_into_formatter_ fail", KR(ret), K(log_entry_task), K(part_trans_task));
      }
    }
  }

  return ret;
}

int ObLogDmlParser::handle_empty_stmt_(ObLogEntryTask &log_entry_task,
    PartTransTask &part_trans_task,
    volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;
  IObLogResourceCollector *resource_collector = TCTX.resource_collector_;

  if (OB_ISNULL(resource_collector)) {
    LOG_ERROR("resource_collector is NULL");
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(log_entry_task.set_redo_log_formatted())) {
    LOG_ERROR("set_redo_log_formatted fail", KR(ret), K(log_entry_task), K(part_trans_task), K(stop_flag));
  // TODO Optimize revert_log_entry_task is synchronous interface
  } else if (OB_FAIL(resource_collector->revert_log_entry_task(&log_entry_task))) {
    if (OB_IN_STOP_STATE != ret) {
      LOG_ERROR("revert log_entry_task fail", KR(ret));
    }
  }

  return ret;
}

int ObLogDmlParser::push_task_into_formatter_(ObLogEntryTask &task, volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(formatter_)) {
    LOG_ERROR("invalid formatter", K(formatter_));
    ret = OB_INVALID_ARGUMENT;
  } else {
    const StmtList &stmt_list = task.get_stmt_list();
    IStmtTask *stmt = stmt_list.head_;

    if (OB_FAIL(formatter_->push(stmt, stop_flag))) {
      if (OB_IN_STOP_STATE != ret) {
        LOG_ERROR("formatter_ push fail", KR(ret), K(task));
      }
    } else {
      // succ
    }
  }

  return ret;
}

int ObLogDmlParser::wait_until_parser_pause_down_(const int64_t thread_index, volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(TCTX.formatter_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("formatter is null", KR(ret));
  } else {
    while (OB_SUCC(ret) && !stop_flag) {
      const double memory_usage_warn_percent = TCONF.memory_usage_warn_threshold / 100.0;
      const int64_t memory_limit = CDC_CFG_MGR.get_memory_limit();
      const int64_t memory_warn_usage = memory_limit * memory_usage_warn_percent;
      const int64_t memory_hold = lib::get_memory_hold();
      const bool touch_memory_warn_limit = (memory_hold > memory_warn_usage);

      const int64_t queue_backlog_lowest_tolerance = CDC_CFG_MGR.get_direct_load_inc_queue_backlog_lowest_tolerance();

      int64_t formatter_br_count = 0;
      int64_t formatter_log_entry_count = 0;
      int64_t formatter_stmt_in_lob_merger_count = 0;

      if (OB_FAIL(TCTX.formatter_->get_task_count(formatter_br_count, formatter_log_entry_count,
          formatter_stmt_in_lob_merger_count))) {
        LOG_ERROR("formatter get_task_count failed", KR(ret));
      } else if (touch_memory_warn_limit && formatter_br_count > queue_backlog_lowest_tolerance ) {
        const static int64_t PRINT_WAIT_PARSER_PAUSE_DOWN_TIMEOUT = 10 * _SEC_;
        if (TC_REACH_TIME_INTERVAL(PRINT_WAIT_PARSER_PAUSE_DOWN_TIMEOUT)) {
          _LOG_INFO("[STAT] [FLOW_CONTROL] NEED_SLOW_DOWN=%d MEM=%s/%s THREAD_INDEX=%ld "
              "FORMATTER=[BR:%ld, LOGENTRY:%ld, STMT_IN_LOG_MERGER: %ld] ",
              1, SIZE_TO_STR(memory_hold), SIZE_TO_STR(memory_limit), thread_index,
              formatter_br_count, formatter_log_entry_count, formatter_stmt_in_lob_merger_count);
        } else {
          _LOG_DEBUG("[STAT] [FLOW_CONTROL] NEED_SLOW_DOWN=%d MEM=%s/%s THREAD_INDEX=%ld "
              "FORMATTER=[BR:%ld, LOGENTRY:%ld, STMT_IN_LOG_MERGER: %ld] ",
              1, SIZE_TO_STR(memory_hold), SIZE_TO_STR(memory_limit), thread_index,
              formatter_br_count, formatter_log_entry_count, formatter_stmt_in_lob_merger_count);
        }
        // sleep 1ms and retry
        const static int64_t WAIT_PARSER_PAUSE_DOWN_TIME = 1 * 1000;
        ob_usleep(WAIT_PARSER_PAUSE_DOWN_TIME);
      } else {
        break;
      }
    } // end while
  }

  if (stop_flag) {
    ret = OB_IN_STOP_STATE;
  }

  return ret;
}

} // namespace libobcdc
} // namespace oceanbase

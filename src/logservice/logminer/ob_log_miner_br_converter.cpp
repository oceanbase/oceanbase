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

#define USING_LOG_PREFIX LOGMNR

#include "ob_log_miner_br_converter.h"
#include "ob_log_miner_resource_collector.h"
#include "ob_log_miner_data_manager.h"
#include "ob_log_miner_analysis_writer.h"
#include "ob_log_miner_record.h"
#include "ob_log_miner_br.h"
#include "ob_log_miner_logger.h"

namespace oceanbase
{
namespace oblogminer
{
const int64_t ObLogMinerBRConverter::PUSH_BR_TIMEOUT_TIME = 1L * 1000 * 1000;
const int64_t ObLogMinerBRConverter::BR_CONVERTER_THREAD_NUM = 1L;
const int64_t ObLogMinerBRConverter::BR_CONVERTER_QUEUE_SIZE = 100000L;

ObLogMinerBRConverter::ObLogMinerBRConverter():
    is_inited_(false),
    data_manager_(nullptr),
    writer_(nullptr),
    resource_collector_(nullptr),
    err_handle_(nullptr) { }

ObLogMinerBRConverter::~ObLogMinerBRConverter()
{
  destroy();
}

int ObLogMinerBRConverter::init(ILogMinerDataManager *data_manager,
    ILogMinerAnalysisWriter *writer,
    ILogMinerResourceCollector *resource_collector,
    ILogMinerErrorHandler *err_handle)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_ERROR("br converter has already been initialized", K(is_inited_));
  } else if (OB_ISNULL(data_manager) || OB_ISNULL(writer) || OB_ISNULL(resource_collector)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("get null components pointer", K(data_manager), K(writer), K(resource_collector));
  } else if (OB_FAIL(BRConverterThreadPool::init(BR_CONVERTER_THREAD_NUM, BR_CONVERTER_QUEUE_SIZE))) {
    LOG_ERROR("BRConverterThreads failed to init");
  } else {
    data_manager_ = data_manager;
    writer_ = writer;
    resource_collector_ = resource_collector;
    err_handle_ = err_handle;
    is_inited_ = true;
    LOG_INFO("ObLogMinerBRConverter finished to init", K(data_manager_), K(writer_),
        K(resource_collector_));
    LOGMINER_STDOUT_V("ObLogMinerBRConverter finished to init\n");
  }
  return ret;
}

int ObLogMinerBRConverter::start()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("logminer br converter is not initialized", K(is_inited_));
  } else if (OB_FAIL(BRConverterThreadPool::start())) {
    LOG_ERROR("logminer br converter failed to start");
  } else {
    // start
    LOG_INFO("ObLogMinerBRConverter starts");
  }
  return ret;
}

void ObLogMinerBRConverter::stop()
{
  BRConverterThreadPool::mark_stop_flag();
  LOG_INFO("ObLogMinerBRConverter stopped");
}

void ObLogMinerBRConverter::wait()
{
  BRConverterThreadPool::stop();
   LOG_INFO("ObLogMinerBRConverter finished to wait");
}

void ObLogMinerBRConverter::destroy()
{
  if (IS_INIT) {
    data_manager_ = nullptr;
    writer_ = nullptr;
    resource_collector_ = nullptr;
    err_handle_ = nullptr;
    BRConverterThreadPool::destroy();
    is_inited_ = false;
    LOG_INFO("ObLogMinerBRConverter destroyed");
    LOGMINER_STDOUT_V("ObLogMinerBRConverter destroyed\n");
  }
}

int ObLogMinerBRConverter::push(ObLogMinerBR *logminer_br)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObLogMinerBRConverter is not inited", K(logminer_br));
  } else if (OB_ISNULL(logminer_br)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("get a null logminer br in br converter", K(logminer_br));
  } else {
    LOG_TRACE("BRConverter push logminer_br into queue", KPC(logminer_br));
    int64_t hash_val = 0;
    RETRY_FUNC(is_stoped(), *(static_cast<ObMQThread*>(this)), push, logminer_br, hash_val, PUSH_BR_TIMEOUT_TIME);
    if (OB_SUCC(ret)) {
      if (is_stoped()) {
        ret = OB_IN_STOP_STATE;
      }
    } else {
      LOG_ERROR("push logminer br into the queue of ObMQThread failed", K(logminer_br), K(hash_val));
    }
  }


  return ret;
}

int ObLogMinerBRConverter::get_total_task_count(int64_t &record_count)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObLogMinerBRConverter is not inited", K(this));
  } else if (OB_FAIL(BRConverterThreadPool::get_total_task_num(record_count))) {
    LOG_ERROR("failed to get record count");
  }

  return ret;
}

int ObLogMinerBRConverter::handle(void *data, const int64_t thread_index, volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;
  ObLogMinerBR *br = static_cast<ObLogMinerBR*>(data);
  ObLogMinerRecord *record = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObLogMinerBRConverter is not inited", K(br), K(is_inited_));
  } else if (OB_ISNULL(br)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("get null br when handle br converter tasks", K(br));
  } else if (br->is_filtered()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("filtered br appears in converter", K(br));
  } else if (OB_FAIL(data_manager_->get_logminer_record(record))) {
    LOG_ERROR("get logminer record from data manager failed", K(record), K(data_manager_));
  } else if (OB_ISNULL(record)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("get a null record from data_manager", K(record), K(data_manager_));
  } else if (OB_FAIL(generate_logminer_record_(*br, *record))) {
    LOG_ERROR("failed to generate logminer record", K(br), K(record));
  } else if (OB_FAIL(writer_->push(record))) {
    LOG_ERROR("failed to push record into writer", K(record), K(writer_));
  }
  if (OB_SUCC(ret) && OB_NOT_NULL(br)) {
    if (OB_FAIL(resource_collector_->revert(br))) {
      LOG_ERROR("failed to revert logminer br", K(br), K(resource_collector_));
    }
  }

  if (OB_FAIL(ret)) {
    err_handle_->handle_error(ret, "ObLogMinerBRFilter exit unexpected\n");
  }
  return ret;
}

int ObLogMinerBRConverter::generate_logminer_record_(ObLogMinerBR &br,
    ObLogMinerRecord &record)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(record.init(br))) {
    LOG_ERROR("record failed to init using logminerbr", K(br), K(record));
  } else if (record.is_ddl_record() || record.is_dml_record()) {
    if (OB_FAIL(record.build_stmts(br))) {
      LOG_ERROR("failed to build stmts for record", K(record), K(br));
    }
  }
  return ret;
}

}
}
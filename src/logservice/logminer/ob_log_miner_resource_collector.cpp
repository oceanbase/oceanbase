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

#include "ob_log_miner_resource_collector.h"
#include "ob_log_miner_recyclable_task.h"
#include "ob_log_miner_br.h"
#include "ob_log_miner_record.h"
#include "ob_log_miner_batch_record.h"
#include "ob_log_miner_data_manager.h"
#include "ob_log_miner_logger.h"

namespace oceanbase
{
namespace oblogminer
{

const int64_t ObLogMinerResourceCollector::RC_THREAD_NUM = 1L;
const int64_t ObLogMinerResourceCollector::RC_QUEUE_SIZE = 100000L;

ObLogMinerResourceCollector::ObLogMinerResourceCollector():
    is_inited_(false),
    data_manager_(nullptr),
    err_handle_(nullptr)
{
}

ObLogMinerResourceCollector::~ObLogMinerResourceCollector()
{
  destroy();
}

int ObLogMinerResourceCollector::init(ILogMinerDataManager *data_manager,
    ILogMinerErrorHandler *err_handle)
{
  int ret = OB_SUCCESS;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_ERROR("Resource Collector has been initialized", K(is_inited_));
  } else if (OB_ISNULL(data_manager)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("get invalid argument when trying to initialize resource collector", K(data_manager));
  } else if (OB_FAIL(ResourceCollectorThread::init(RC_THREAD_NUM, RC_QUEUE_SIZE))) {
    LOG_ERROR("failed to init resource collector thread", K(RC_THREAD_NUM), K(RC_QUEUE_SIZE));
  } else {
    is_inited_ = true;
    data_manager_ = data_manager;
    err_handle_ = err_handle_;
    LOG_INFO("ObLogMinerResourceCollector finished to init");
    LOGMINER_STDOUT_V("ObLogMinerResourceCollector finished to init\n");
  }

  return ret;
}

int ObLogMinerResourceCollector::start()
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObLogMinerResourceCollector hasn't been initialized", K(is_inited_));
  } else if (OB_FAIL(ResourceCollectorThread::start())) {
    LOG_ERROR("ResourceCollectorThread failed to start");
  } else {
    LOG_INFO("ObLogMinerResourceCollector starts");
  }

  return ret;
}

void ObLogMinerResourceCollector::stop()
{
  ResourceCollectorThread::mark_stop_flag();
  LOG_INFO("ObLogMinerResourceCollector stopped");
}

void ObLogMinerResourceCollector::wait()
{
  ResourceCollectorThread::stop();
  LOG_INFO("ObLogMinerResourceCollector finished to wait");
}

void ObLogMinerResourceCollector::destroy()
{
  if (IS_INIT) {
    is_inited_ = false;
    data_manager_ = nullptr;
    err_handle_ = nullptr;
    LOG_INFO("ObLogMinerResourceCollector destroyed");
    LOGMINER_STDOUT_V("ObLogMinerResourceCollector destroyed\n");
  }
}

int ObLogMinerResourceCollector::revert(ObLogMinerRecyclableTask *task)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObLogMinerResourceCollector hasn't been initialized", K(is_inited_));
  } else if (OB_FAIL(push_task_(task))) {
    LOG_ERROR("failed to push task into thread queue", K(task));
  }
  return ret;
}

int ObLogMinerResourceCollector::get_total_task_count(int64_t &task_count)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObLogMinerResourceCollector hasn't been initialized", K(is_inited_));
  } else if (OB_FAIL(ResourceCollectorThread::get_total_task_num(task_count))) {
    LOG_ERROR("failed to get total task count in resource collector");
  }

  return ret;
}

int ObLogMinerResourceCollector::handle(void *data, const int64_t thread_index, volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObLogMinerResourceCollector hasn't been initialized", K(is_inited_));
  } else if (OB_ISNULL(data)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("get null data when handling data", K(data));
  } else {
    ObLogMinerRecyclableTask *task = static_cast<ObLogMinerRecyclableTask*>(data);
    if (task->is_logminer_record()) {
      ObLogMinerRecord *rec = static_cast<ObLogMinerRecord*>(task);
      if (OB_FAIL(handle_logminer_record_(rec))) {
        LOG_ERROR("failed to handle logminer record", KPC(rec));
      }
    } else if (task->is_binlog_record()) {
      ObLogMinerBR *br = static_cast<ObLogMinerBR*>(task);
      if (OB_FAIL(handle_binlog_record_(br))) {
        LOG_ERROR("failed to handle binlog record", KPC(br));
      }
    } else if (task->is_batch_record()) {
      ObLogMinerBatchRecord *batch_rec = static_cast<ObLogMinerBatchRecord*>(task);
      if (OB_FAIL(handle_batch_record_(batch_rec))) {
        LOG_ERROR("failed to handle batch record", KPC(batch_rec));
      }
    } else {
      ret = OB_NOT_SUPPORTED;
      LOG_ERROR("get unsupported task", "type", task->get_task_type());
    }
  }

  if (OB_FAIL(ret)) {
    err_handle_->handle_error(ret, "ObLogMinerResourceCollector exit unexpected\n");
  }
  return ret;
}

int ObLogMinerResourceCollector::push_task_(ObLogMinerRecyclableTask *task)
{
  int ret = OB_SUCCESS;
  const int64_t DATA_OP_TIMEOUT = 1L * 1000 * 1000;
  static int64_t seq_no = 0;
  int64_t hash_val = ATOMIC_AAF(&seq_no, 1);
  RETRY_FUNC(is_stoped(), *(static_cast<ObMQThread*>(this)), push, task, hash_val, DATA_OP_TIMEOUT);
  return ret;
}

int ObLogMinerResourceCollector::handle_binlog_record_(ObLogMinerBR *br)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(br)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("get null br in resource collector", K(br));
  } else {
    br->destroy();
    ob_free(br);
  }
  return ret;
}

int ObLogMinerResourceCollector::handle_logminer_record_(ObLogMinerRecord *logminer_rec)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(logminer_rec)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("get null logminer_rec in resource collector", K(logminer_rec));
  } else if (OB_FAIL(data_manager_->release_log_miner_record(logminer_rec))) {
    LOG_ERROR("failed to release log miner record", KPC(logminer_rec));
  }
  return ret;
}

int ObLogMinerResourceCollector::handle_batch_record_(ObLogMinerBatchRecord *batch_rec)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(batch_rec)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("get null batch record in resource collector", K(batch_rec));
  } else if (OB_FAIL(data_manager_->release_logminer_batch_record(batch_rec))) {
    LOG_ERROR("failed to release batch record", KPC(batch_rec));
  }
  return ret;
}

}
}
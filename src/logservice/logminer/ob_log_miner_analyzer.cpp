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

#include "ob_log_miner_analyzer.h"
#include "ob_log_miner_utils.h"
#include "ob_log_miner_br_producer.h"
#include "ob_log_miner_br_filter.h"
#include "ob_log_miner_br_converter.h"
#include "ob_log_miner_args.h"
#include "ob_log_miner_analysis_writer.h"
#include "ob_log_miner_data_manager.h"
#include "ob_log_miner_resource_collector.h"
#include "ob_log_miner_logger.h"

namespace oceanbase
{
namespace oblogminer
{

ObLogMinerAnalyzer::ObLogMinerAnalyzer():
    lib::ThreadPool(1),
    is_inited_(false),
    is_stopped_(false),
    start_time_(-1),
    producer_(nullptr),
    data_filter_(nullptr),
    data_converter_(nullptr),
    writer_(nullptr),
    data_manager_(nullptr),
    resource_collector_(nullptr),
    file_manager_(nullptr) { }

ObLogMinerAnalyzer::~ObLogMinerAnalyzer()
{
  destroy();
}

int ObLogMinerAnalyzer::init(const AnalyzerArgs &args, ILogMinerFileManager *file_mgr)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_ERROR("analyzer has been initialized", K(is_inited_));
  } else {
    LOGMINER_STDOUT("ObLogMinerAnalyzer init...\n");
    file_manager_ = file_mgr;
    if (OB_FAIL(lib::ThreadPool::init())) {
      LOG_ERROR("analyzer thread pool failed to init");
    } else if (OB_FAIL(init_component<ObLogMinerDataManager>(data_manager_,
        LogMinerMode::ANALYSIS, args.record_format_, args.start_time_us_, args.end_time_us_, this))) {
      LOG_ERROR("failed to init ObLogMinerDataManager", K(args));
    } else if (OB_FAIL(init_component<ObLogMinerResourceCollector>(resource_collector_,
        data_manager_, this))) {
      LOG_ERROR("failed to init ObLogMinerResourceCollector", K(args));
    } else if (OB_FAIL(init_component<ObLogMinerAnalysisWriter>(writer_,
      args.start_time_us_, data_manager_, resource_collector_, file_manager_, this))) {
      LOG_ERROR("failed to init ObLogMinerAnalysisWriter", K(args));
    } else if (OB_FAIL(init_component<ObLogMinerBRConverter>(data_converter_,
        data_manager_, writer_, resource_collector_, this))) {
      LOG_ERROR("failed to init ObLogMinerBRConverter", K(args));
    } else if (OB_FAIL(init_component<ObLogMinerBRFilter>(data_filter_,
        args.column_cond_, args.operations_, data_manager_, resource_collector_, data_converter_, this))) {
      LOG_ERROR("failed to init ObLogMinerBRFilter", K(args));
    } else if (OB_FAIL(init_component<ObLogMinerBRProducer>(producer_, args, data_filter_, this))) {
      LOG_ERROR("failed to init ObLogMinerBRProducer", K(args));
    } else {
      is_inited_ = true;
      LOG_INFO("ObLogMinerAnalyzer finished to init", K(args), KP(file_mgr));
      LOGMINER_STDOUT("ObLogMinerAnalyzer finished to init\n");
    }
  }
  return ret;
}

int ObLogMinerAnalyzer::start()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("analyzer hasn't been initialized", K(is_inited_));
  } else if (OB_FAIL(data_manager_->start())) {
    LOG_ERROR("data_manager failed to start");
  } else if (OB_FAIL(resource_collector_->start())) {
    LOG_ERROR("resource_collector failed to start");
  } else if (OB_FAIL(writer_->start())) {
    LOG_ERROR("analyzer_writer failed to start");
  } else if (OB_FAIL(data_converter_->start())) {
    LOG_ERROR("data_converter failed to start");
  } else if (OB_FAIL(data_filter_->start())) {
    LOG_ERROR("data_filter failed to start");
  } else if (OB_FAIL(producer_->start())) {
    LOG_ERROR("data_producer failed to start");
  } else if (OB_FAIL(lib::ThreadPool::start())) {
    LOG_ERROR("analyzer thread failed to start");
  } else {
    is_stopped_ = false;
    start_time_ = ObClockGenerator::getClock();
    LOG_INFO("ObLogMinerAnalyzer starts");
    LOGMINER_STDOUT("ObLogMinerAnalyzer starts\n");
  }

  return ret;
}

void ObLogMinerAnalyzer::stop()
{
  int ret = OB_SUCCESS;
  double interval_sec = (double)(ObClockGenerator::getClock() - start_time_) / (double)1000000;
  LOGMINER_STDOUT("ObLogMinerAnalyzer took %.2lfs to process\n", interval_sec);
  LOGMINER_STDOUT("ObLogMinerAnalyzer exit...\n");
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("analyzer hasn't been initialized");
  } else {
    lib::Threads::stop();
    data_filter_->stop();
    data_converter_->stop();
    writer_->stop();
    resource_collector_->stop();
    data_manager_->stop();
    // resource collector depends on producer
    producer_->stop();
    ATOMIC_STORE(&is_stopped_, true);
    stop_cond_.signal();
    LOG_INFO("ObLogMinerAnalyzer stopped");
    LOGMINER_STDOUT_V("ObLogMinerAnalyzer stopped\n");
  }
}

void ObLogMinerAnalyzer::wait()
{
  if (IS_INIT) {
    const int64_t wait_time_interval = 10L * 1000 * 1000;
    while (! ATOMIC_LOAD(&is_stopped_)) {
      stop_cond_.timedwait(wait_time_interval);
    }
    lib::Threads::wait();
    writer_->wait();
    data_converter_->wait();
    data_filter_->wait();
    data_manager_->wait();
    resource_collector_->wait();
    producer_->wait();
  }
  LOG_INFO("ObLogMinerAnalyzer finished to wait");
  LOGMINER_STDOUT_V("ObLogMinerAnalyzer finished to wait\n");
}

void ObLogMinerAnalyzer::destroy()
{
  if (IS_INIT) {
    is_inited_ = false;
    start_time_ = -1;
    lib::Threads::destroy();
    destroy_component<ObLogMinerBRProducer>(producer_);
    destroy_component<ObLogMinerBRFilter>(data_filter_);
    destroy_component<ObLogMinerBRConverter>(data_converter_);
    destroy_component<ObLogMinerAnalysisWriter>(writer_);
    destroy_component<ObLogMinerResourceCollector>(resource_collector_);
    destroy_component<ObLogMinerDataManager>(data_manager_);
    file_manager_ = nullptr;
    LOG_INFO("ObLogMinerAnalyzer destroyed");
    LOGMINER_STDOUT("ObLogMinerAnalyzer destroyed\n");
  }
}

void ObLogMinerAnalyzer::handle_error(int err_code, const char *fmt, ...)
{
  int ret = OB_SUCCESS;
  constexpr int64_t err_msg_len = 1024;
  char err_msg[err_msg_len] = {0};
  static int stop_once = 0;

  if (IS_INIT) {
    va_list arg;
    ret = err_code;
    va_start(arg, fmt);
    vsnprintf(err_msg, err_msg_len, fmt, arg);
    if (ret != OB_SUCCESS) {
      LOG_ERROR(err_msg);
      LOGMINER_STDOUT(err_msg);
      LOGMINER_STDOUT("please check log[%s] for more detail\n", ObLogMinerArgs::LOGMINER_LOG_FILE);
    } else {
      LOG_INFO(err_msg);
    }
    va_end(arg);
    if (ATOMIC_BCAS(&stop_once, 0, 1)) {
      stop();
    }
  }
}

void ObLogMinerAnalyzer::run1()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("analyzer hasn't been initialized");
  } else {
    const int64_t STAT_INTERVAL = 10L * 1000 * 1000; // 10s
    const int64_t LOOP_INTERVAL = 1L * 1000 * 1000; // 1s
    while (!has_set_stop()) {
      if (REACH_TIME_INTERVAL(STAT_INTERVAL)) {
        int64_t filter_task_cnt = 0;
        int64_t converter_task_cnt = 0;
        int64_t writer_task_cnt = 0;
        int64_t rc_task_cnt = 0;
        if (OB_FAIL(data_filter_->get_total_task_count(filter_task_cnt))) {
          LOG_ERROR("failed to get task count in data_filter");
        } else if (OB_FAIL(data_converter_->get_total_task_count(converter_task_cnt))) {
          LOG_ERROR("failed to get task count in data_converter");
        } else if (OB_FAIL(writer_->get_total_task_count(writer_task_cnt))) {
          LOG_ERROR("failed to get task count in writer");
        } else if (OB_FAIL(resource_collector_->get_total_task_count(rc_task_cnt))) {
          LOG_ERROR("failed to get task count in resource collector");
        } else {
          LOG_INFO("[ANALYZER][TASK_STAT]", "Filter", filter_task_cnt, "Converter", converter_task_cnt,
              "Writer", writer_task_cnt, "ResourceCollector", rc_task_cnt);
        }
      }
      ob_usleep(LOOP_INTERVAL);
    }
  }
}

}
}

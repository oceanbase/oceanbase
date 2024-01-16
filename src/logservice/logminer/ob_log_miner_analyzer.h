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

#ifndef OCEANBASE_LOG_MINER_ANALYZER_H_
#define OCEANBASE_LOG_MINER_ANALYZER_H_

#include "ob_log_miner_error_handler.h"
#include "ob_queue_thread.h"

namespace oceanbase
{
namespace oblogminer
{

class ILogMinerBRProducer;
class ILogMinerBRFilter;
class ILogMinerBRConverter;
class ILogMinerAnalysisWriter;
class ILogMinerResourceCollector;
class ILogMinerDataManager;
class ILogMinerFileManager;
class AnalyzerArgs;

class ObLogMinerAnalyzer: public ILogMinerErrorHandler, lib::ThreadPool {
public:
  ObLogMinerAnalyzer();
  virtual ~ObLogMinerAnalyzer();
  int init(const AnalyzerArgs &args, ILogMinerFileManager *file_mgr);
  int start();
  void stop();
  void wait();
  void destroy();
public:
  void handle_error(int err_code, const char *fmt, ...);

public:
  virtual void run1();

private:
  bool is_inited_;
  bool is_stopped_;
  int64_t start_time_;
  common::ObCond stop_cond_;
  ILogMinerBRProducer         *producer_;
  ILogMinerBRFilter           *data_filter_;
  ILogMinerBRConverter        *data_converter_;
  ILogMinerAnalysisWriter     *writer_;
  ILogMinerDataManager        *data_manager_;
  ILogMinerResourceCollector  *resource_collector_;
  ILogMinerFileManager        *file_manager_;
};

}
}

#endif
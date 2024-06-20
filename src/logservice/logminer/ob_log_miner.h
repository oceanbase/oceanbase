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

#ifndef OCEANBASE_LOG_MINER_H_
#define OCEANBASE_LOG_MINER_H_

#include <cstdint>
#include "ob_log_miner_mode.h"
#include "ob_log_miner_analyzer.h"

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

class ILogMinerFlashbackReader;
class ILogMinerRecordParser;
class ILogMinerRecordFilter;
class ILogMinerFlashbackWriter;
class ILogMinerFileManager;

class ObLogMinerArgs;
class AnalyzerArgs;
class FlashbackerArgs;

class ObLogMinerFlashbacker {
public:
  ObLogMinerFlashbacker() {}
  virtual ~ObLogMinerFlashbacker() {}
  int init(const FlashbackerArgs &args, ILogMinerFileManager *file_mgr) { return 0; }
  void run() {}
  void stop() {}
  void destroy() {}

private:
  bool is_inited_;
  ILogMinerFlashbackReader    *reader_;
  ILogMinerRecordParser       *parser_;
  ILogMinerRecordFilter       *filter_;
  ILogMinerFlashbackWriter    *writer_;
};

class ObLogMiner {
public:
  static ObLogMiner *get_logminer_instance();

public:
  ObLogMiner();
  ~ObLogMiner();

  int init(const ObLogMinerArgs &args);

  // LogMiner would stop automatically if some error occurs or has finished processing logs.
  // There is no need to implement some interfaces like wait/stop/etc.
  void run();
  void destroy();
private:
  void set_log_params_();
  int init_analyzer_(const ObLogMinerArgs &args);

private:
  bool                        is_inited_;
  bool                        is_stopped_;

  LogMinerMode                mode_;

  ObLogMinerAnalyzer          *analyzer_;
  ObLogMinerFlashbacker       *flashbacker_;
  ILogMinerFileManager        *file_manager_;
};

} // oblogminer

} // oceanbase

#endif
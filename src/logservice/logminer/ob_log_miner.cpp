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

#include "ob_log_miner.h"
#include "lib/ob_errno.h"
#include "lib/oblog/ob_log.h"
#include "ob_log_miner_args.h"
#include "lib/allocator/ob_malloc.h"
#include "ob_log_miner_file_manager.h"
#include "ob_log_miner_utils.h"
#include "ob_log_miner_logger.h"
#include "ob_log_miner_record_converter.h"
#include "ob_log_miner_timezone_getter.h"

namespace oceanbase {
namespace oblogminer {

ObLogMiner *get_logminer_instance()
{
  static ObLogMiner logminer_instance;
  return &logminer_instance;
}

ObLogMiner::ObLogMiner():
    is_inited_(false),
    is_stopped_(true),
    mode_(LogMinerMode::UNKNOWN),
    analyzer_(nullptr),
    flashbacker_(nullptr),
    file_manager_(nullptr) { }

ObLogMiner::~ObLogMiner()
{
  destroy();
}

int ObLogMiner::init(const ObLogMinerArgs &args)
{
  int ret = common::OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_ERROR("oblogminer was inited, no need to init again", K(is_inited_));
  } else {
    mode_ = args.mode_;
    LOGMINER_LOGGER.set_verbose(args.verbose_);
    if (OB_FAIL(LOGMINER_TZ.set_timezone(args.timezone_))) {
      LOG_ERROR("oblogminer logger set timezone failed", K(ret), K(args.timezone_));
    }
    if (OB_SUCC(ret)) {
      if (is_analysis_mode(mode_)) {
        if (OB_FAIL(init_analyzer_(args))) {
          LOG_ERROR("failed to initialize analyzer for analysis mode", K(args));
        }
      } else {
        // TODO: flashback mode and restart mode
        // if (OB_FAIL(init_component<ObLogMinerFileManager>(file_manager_,
        //     args.flashbacker_args_.recovery_path_, RecordFileFormat::CSV, ObLogMinerFileManager::FileMgrMode::FLASHBACK))) {
        //   LOG_ERROR("failed to initialize file_manager for flashback mode", K(args));
        // } else if (OB_FAIL(init_component<ObLogMinerFlashbacker>(flashbacker_,
        //     args.flashbacker_args_, file_manager_))) {
        //   LOG_ERROR("flashbacker failed to init", K(args));
        // } else if (OB_FAIL(file_manager_->write_config(args))) {
        //   LOG_ERROR("failed to write config", K(args));
        // }
        // is_inited_ = true;
        ret = OB_NOT_SUPPORTED;
        LOG_ERROR("get not supported mode", K(args));
      }
    }
  }
  if (OB_SUCC(ret)) {
    is_inited_ = true;
    LOG_INFO("ObLogMiner init succeed", K(args));
    LOGMINER_STDOUT("ObLogMiner init succeed\n");
  } else {
    LOG_INFO("ObLogMiner init failed", K(args));
  }
  return ret;
}

int ObLogMiner::init_analyzer_(const ObLogMinerArgs &args)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_component<ObLogMinerFileManager>(file_manager_,
      args.analyzer_args_.output_dst_, args.analyzer_args_.record_format_, ObLogMinerFileManager::FileMgrMode::ANALYZE))) {
    LOG_ERROR("failed to initialize file_manager for analysis mode", K(args));
  } else if (OB_FAIL(init_component<ObLogMinerAnalyzer>(analyzer_, args.analyzer_args_, file_manager_))) {
    LOG_ERROR("analyzer failed to init", K(args));
  } else if (OB_FAIL(file_manager_->write_config(args))) {
    LOG_ERROR("failed to write config", K(args));
  } else if (OB_FAIL(OB_LOGGER.set_mod_log_levels(args.analyzer_args_.log_level_))) {
    LOG_ERROR("failed to set mod log levels", K(args));
    LOGMINER_STDOUT("failed to set log level, please check the log level setting\n");
  }
  return ret;
}
void ObLogMiner::run() {
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObLogMiner hasn't been initiailized yet");
  } else if (is_analysis_mode(mode_)) {
    if (OB_FAIL(analyzer_->start())) {
      LOG_ERROR("analyzer failed to start");
    } else {
      analyzer_->wait();
    }
  } else if (is_flashback_mode(mode_)) {
    flashbacker_->run();
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_ERROR("get not supported mode", K(mode_));
  }
}

void ObLogMiner::destroy() {
  if (IS_INIT) {
    if (is_analysis_mode(mode_)) {
      destroy_component<ObLogMinerAnalyzer>(analyzer_);
      destroy_component<ObLogMinerFileManager>(file_manager_);
    } else if (is_flashback_mode(mode_)) {
      destroy_component<ObLogMinerFlashbacker>(flashbacker_);
      destroy_component<ObLogMinerFileManager>(file_manager_);
    }
    is_inited_ = false;
    mode_ = LogMinerMode::UNKNOWN;
    LOG_INFO("ObLogMiner destroyed");
    LOGMINER_STDOUT("ObLogMiner destroyed\n");
  }
}

} // namespace oblogminer
} // namespace oceanbase
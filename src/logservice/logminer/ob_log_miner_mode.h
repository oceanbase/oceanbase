/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_LOG_MINER_MODE_H_
#define OCEANBASE_LOG_MINER_MODE_H_

#include "lib/string/ob_string.h"

namespace oceanbase
{
namespace oblogminer
{
enum class LogMinerMode {
  UNKNOWN = 0,

  ANALYSIS = 1,
  FLASHBACK = 2,

  MAX_MODE
};

const char *logminer_mode_to_str(LogMinerMode mode);
LogMinerMode get_logminer_mode(const common::ObString &mode_str);
LogMinerMode get_logminer_mode(const char *mode_str);
bool is_logminer_mode_valid(const LogMinerMode mode);
bool is_analysis_mode(const LogMinerMode mode);
bool is_flashback_mode(const LogMinerMode mode);

}
}

#endif
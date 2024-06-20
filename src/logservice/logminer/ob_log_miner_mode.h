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
/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "ob_log_miner_mode.h"

namespace oceanbase
{
namespace oblogminer
{

const char *logminer_mode_to_str(LogMinerMode mode)
{
  const char *mode_str = nullptr;
  switch(mode) {
    case LogMinerMode::UNKNOWN: {
      mode_str = "UNKNOWN";
      break;
    }

    case LogMinerMode::ANALYSIS: {
      mode_str = "ANALYSIS";
      break;
    }

    case LogMinerMode::FLASHBACK: {
      mode_str = "FLASHBACK";
      break;
    }

    case LogMinerMode::MAX_MODE: {
      mode_str = "MAX_MODE";
      break;
    }

    default: {
      mode_str = "INVALID";
      break;
    }
  }

  return mode_str;
}

LogMinerMode get_logminer_mode(const common::ObString &mode_str)
{
  LogMinerMode mode = LogMinerMode::UNKNOWN;
  if (0 == mode_str.case_compare("analysis")) {
    mode = LogMinerMode::ANALYSIS;
  } else if (0 == mode_str.case_compare("flashback")) {
    mode = LogMinerMode::FLASHBACK;
  } else {
    // return UNKNOWN mode
  }
  return mode;
}

LogMinerMode get_logminer_mode(const char *mode_str)
{
  LogMinerMode mode = LogMinerMode::UNKNOWN;

  if (0 == strcasecmp(mode_str, "analysis")) {
    mode = LogMinerMode::ANALYSIS;
  } else if (0 == strcasecmp(mode_str, "flashback")) {
    mode = LogMinerMode::FLASHBACK;
  } else {
    // return UNKNOWN mode
  }

  return mode;
}

bool is_logminer_mode_valid(const LogMinerMode mode)
{
  return mode > LogMinerMode::UNKNOWN && mode < LogMinerMode::MAX_MODE;
}

bool is_analysis_mode(const LogMinerMode mode)
{
  return LogMinerMode::ANALYSIS == mode;
}

bool is_flashback_mode(const LogMinerMode mode)
{
  return LogMinerMode::FLASHBACK == mode;
}

}
}
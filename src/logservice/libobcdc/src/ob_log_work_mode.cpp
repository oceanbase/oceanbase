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
 * WorkingMode
 */

#include "ob_log_work_mode.h"

namespace oceanbase
{
namespace libobcdc
{
using namespace oceanbase::common;

const char *print_working_mode(const WorkingMode mode)
{
  const char *mode_str = "INVALID";

  switch (mode) {
    case MEMORY_MODE: {
      mode_str = "Memory Working Mode";
      break;
    }
    case STORAGER_MODE: {
      mode_str = "Storager Working Mode";
      break;
    }
    case AUTO_MODE: {
      mode_str = "Auto Working Mode";
      break;
    }
    default: {
      mode_str = "INVALID";
      break;
    }
  }

  return mode_str;
}

// TODO support auto mode
WorkingMode get_working_mode(const char *working_mode_str)
{
  WorkingMode ret_mode = UNKNOWN_MODE;

  if (OB_ISNULL(working_mode_str)) {
  } else {
    if (0 == strcmp("memory", working_mode_str)) {
      ret_mode = MEMORY_MODE;
    } else if (0 == strcmp("storage", working_mode_str)) {
      ret_mode = STORAGER_MODE;
    } else {
    }
  }

  return ret_mode;
}

bool is_working_mode_valid(WorkingMode mode)
{
  bool bool_ret = false;

  bool_ret = (mode > WorkingMode::UNKNOWN_MODE)
    && (mode < WorkingMode::MAX_MODE);

  return bool_ret;
}

bool is_memory_working_mode(const WorkingMode mode)
{
  return WorkingMode::MEMORY_MODE == mode;
}

bool is_storage_working_mode(const WorkingMode mode)
{
  return WorkingMode::STORAGER_MODE == mode;
}

bool is_auto_working_mode(const WorkingMode mode)
{
  return WorkingMode::AUTO_MODE == mode;
}

}
}

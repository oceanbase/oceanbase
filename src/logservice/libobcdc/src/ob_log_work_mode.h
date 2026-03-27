/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_LIBOBCDC_OB_LOG_WORK_MODE_H_
#define OCEANBASE_LIBOBCDC_OB_LOG_WORK_MODE_H_

#include "share/ob_define.h"
#include "share/ob_errno.h"

namespace oceanbase
{
namespace libobcdc
{
enum WorkingMode
{
  UNKNOWN_MODE = 0,

  MEMORY_MODE = 1,
  STORAGER_MODE = 2,
  AUTO_MODE = 3,

  MAX_MODE
};
const char *print_working_mode(const WorkingMode mode);
WorkingMode get_working_mode(const char *working_mode_str);

bool is_working_mode_valid(WorkingMode mode);
bool is_memory_working_mode(const WorkingMode mode);
bool is_storage_working_mode(const WorkingMode mode);
bool is_auto_working_mode(const WorkingMode mode);

}
}

#endif

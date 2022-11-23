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
 * WorkingMode define
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

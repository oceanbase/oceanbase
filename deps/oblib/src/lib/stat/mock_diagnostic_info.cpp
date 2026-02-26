/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define private public
#define protected public
#include "deps/oblib/src/lib/ob_lib_config.h"
#undef private
#undef protected

namespace oceanbase
{

// ATTENTION: add this cpp file as library only if compiled target doesn't need diagnose. like unittest or cdc.

__attribute__((constructor)) void init_diagnostic_info()
{
  // lib::ObLibConfig::enable_diagnose_info_ = false;
  lib::ObPerfModeGuard::PERF_MODE_VALUE = true;
  lib::ObPerfModeGuard::get_tl_instance() = true;
}

} /* namespace oceanbase */
/**
 * Copyright (c) 2024 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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
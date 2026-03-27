/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "ob_lib_config.h"
namespace oceanbase
{
namespace lib
{

bool ObLibConfig::enable_diagnose_info_ = true;
volatile bool ObLibConfig::enable_trace_log_ = true;

bool ObPerfModeGuard::PERF_MODE_VALUE = false;

} //lib
} //oceanbase

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
 */

#include "ob_lib_config.h"
namespace oceanbase
{
namespace lib
{

volatile bool ObLibConfig::enable_diagnose_info_ = true;
volatile bool ObLibConfig::enable_trace_log_ = true;
thread_local bool ObDisableDiagnoseGuard::in_disable_diagnose_guard_ = false;

} //lib
} //oceanbase

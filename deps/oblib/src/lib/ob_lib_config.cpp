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
namespace oceanbase {
namespace lib {

ObLibConfig::ObLibConfig() : enable_diagnose_info_(true), enable_trace_log_(true)
{}

ObLibConfig& ObLibConfig::get_instance()
{
  static ObLibConfig instance_;
  return instance_;
}

void ObLibConfig::reload_diagnose_info_config(const bool enable_diagnose_info)
{
  ATOMIC_SET(&enable_diagnose_info_, enable_diagnose_info);
}

void ObLibConfig::reload_trace_log_config(const bool enable_trace_log)
{
  ATOMIC_SET(&enable_trace_log_, enable_trace_log);
}

}  // namespace lib
}  // namespace oceanbase

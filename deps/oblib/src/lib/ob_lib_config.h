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

#ifndef OB_LIB_CONFIG_H_
#define OB_LIB_CONFIG_H_

#include "lib/atomic/ob_atomic.h"

namespace oceanbase
{
namespace lib
{
bool is_diagnose_info_enabled();
void reload_diagnose_info_config(const bool);
bool is_trace_log_enabled();
void reload_trace_log_config(const bool);

class ObLibConfig
{
  friend bool is_diagnose_info_enabled();
  friend void reload_diagnose_info_config(const bool);
  friend bool is_trace_log_enabled();
  friend void reload_trace_log_config(const bool);
private:
  static volatile bool enable_diagnose_info_ CACHE_ALIGNED;
  static volatile bool enable_trace_log_ CACHE_ALIGNED;
};

class ObPerfModeGuard
{
  friend bool is_diagnose_info_enabled();
  friend bool is_trace_log_enabled();
public:
  explicit ObPerfModeGuard() : old_value_(in_disable_diagnose_guard_)
  {
    in_disable_diagnose_guard_ = true;
  }
  ~ObPerfModeGuard()
  {
    in_disable_diagnose_guard_ = old_value_;
  }
private:
  static thread_local bool in_disable_diagnose_guard_;
  bool old_value_;
};

using ObDisableDiagnoseGuard = ObPerfModeGuard;

inline bool is_diagnose_info_enabled()
{
  return ObLibConfig::enable_diagnose_info_ && !ObPerfModeGuard::in_disable_diagnose_guard_;
}

inline void reload_diagnose_info_config(const bool enable_diagnose_info)
{
  ATOMIC_STORE(&ObLibConfig::enable_diagnose_info_, enable_diagnose_info);
}

inline bool is_trace_log_enabled()
{
  return ObLibConfig::enable_trace_log_ && !ObPerfModeGuard::in_disable_diagnose_guard_;
}

inline void reload_trace_log_config(const bool enable_trace_log)
{
  ATOMIC_STORE(&ObLibConfig::enable_trace_log_, enable_trace_log);
}

} //lib
} //oceanbase
#endif // OB_LIB_CONFIG_H_

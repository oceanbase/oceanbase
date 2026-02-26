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

namespace common
{
class ObBackGroundSessionGuard;
class ObDiagnosticInfoSwitchGuard;
}

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
  static bool enable_diagnose_info_ CACHE_ALIGNED;
  static volatile bool enable_trace_log_ CACHE_ALIGNED;
};

class ObPerfModeGuard
{
  friend class common::ObBackGroundSessionGuard;
  friend class common::ObDiagnosticInfoSwitchGuard;
  friend class ObEnableDiagnoseGuard;
  friend bool is_diagnose_info_enabled();
  friend bool is_trace_log_enabled();
public:
  explicit ObPerfModeGuard() : old_value_(get_tl_instance())
  {
    get_tl_instance() = true;
  }
  ~ObPerfModeGuard()
  {
    get_tl_instance() = old_value_;
  }
private:
  static bool &get_tl_instance()
  {
    static thread_local bool in_disable_diagnose_guard = PERF_MODE_VALUE;

    return in_disable_diagnose_guard;
  }
private:
  static bool PERF_MODE_VALUE;
  bool old_value_;
};

class ObEnableDiagnoseGuard
{
  friend class common::ObBackGroundSessionGuard;
  friend class common::ObDiagnosticInfoSwitchGuard;
  friend bool is_diagnose_info_enabled();
  friend bool is_trace_log_enabled();
public:
  explicit ObEnableDiagnoseGuard() : old_value_(ObPerfModeGuard::get_tl_instance())
  {
    ObPerfModeGuard::get_tl_instance() = ObPerfModeGuard::PERF_MODE_VALUE;
  }
  ~ObEnableDiagnoseGuard()
  {
    ObPerfModeGuard::get_tl_instance() = old_value_;
  }
private:
  bool old_value_;
};

using ObDisableDiagnoseGuard = ObPerfModeGuard;

inline bool is_diagnose_info_enabled()
{
  return ObLibConfig::enable_diagnose_info_ && !ObPerfModeGuard::get_tl_instance();
}

inline void reload_diagnose_info_config(const bool enable_diagnose_info)
{
  ATOMIC_STORE(&ObLibConfig::enable_diagnose_info_, enable_diagnose_info);
}

inline bool is_trace_log_enabled()
{
  bool bool_ret = ObLibConfig::enable_trace_log_;
#ifdef ENABLE_DEBUG_LOG
  if (!bool_ret) {
    bool_ret = true;
  }
#endif
  return bool_ret && !ObPerfModeGuard::get_tl_instance();
}

inline void reload_trace_log_config(const bool enable_trace_log)
{
  ATOMIC_STORE(&ObLibConfig::enable_trace_log_, enable_trace_log);
}

} //lib
} //oceanbase
#endif // OB_LIB_CONFIG_H_

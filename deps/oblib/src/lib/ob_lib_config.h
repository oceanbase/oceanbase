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

#include "lib/ob_define.h"
#include "lib/utility/ob_print_utils.h"
namespace oceanbase {
namespace lib {

class ObLibConfig {
public:
  static ObLibConfig& get_instance();
  void reload_diagnose_info_config(const bool enable_diagnose_info);
  void reload_trace_log_config(const bool enable_trace_log);
  bool is_diagnose_info_enabled() const
  {
    return enable_diagnose_info_;
  }
  bool is_trace_log_enabled() const
  {
    return enable_trace_log_;
  }

private:
  ObLibConfig();
  virtual ~ObLibConfig() = default;
  volatile bool enable_diagnose_info_;
  volatile bool enable_trace_log_;
};

inline bool is_diagnose_info_enabled()
{
  return ObLibConfig::get_instance().is_diagnose_info_enabled();
}

inline int reload_diagnose_info_config(const bool enable_diagnose_info)
{
  int ret = common::OB_SUCCESS;
  ObLibConfig::get_instance().reload_diagnose_info_config(enable_diagnose_info);
  return ret;
}

inline bool is_trace_log_enabled()
{
  return ObLibConfig::get_instance().is_trace_log_enabled();
}

inline int reload_trace_log_config(const bool enable_trace_log)
{
  int ret = common::OB_SUCCESS;
  ObLibConfig::get_instance().reload_trace_log_config(enable_trace_log);
  return ret;
}

}  // namespace lib
}  // namespace oceanbase
#endif  // OB_LIB_CONFIG_H_

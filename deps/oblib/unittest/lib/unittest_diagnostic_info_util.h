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

#ifndef OB_UNITTEST_DIAGNOSTIC_INFO_UTIL_H_
#define OB_UNITTEST_DIAGNOSTIC_INFO_UTIL_H_

#define private public
#define protected public
#include "deps/oblib/src/lib/ob_lib_config.h"
#undef private
#undef protected

namespace oceanbase
{
namespace lib
{

class ObUnitTestEnableDiagnoseGuard
{
  friend class common::ObBackGroundSessionGuard;
  friend class common::ObDiagnosticInfoSwitchGuard;
  friend bool is_diagnose_info_enabled();
  friend bool is_trace_log_enabled();
public:
  explicit ObUnitTestEnableDiagnoseGuard() : old_value_(ObPerfModeGuard::get_tl_instance())
  {
    ObPerfModeGuard::get_tl_instance() = false;
  }
  ~ObUnitTestEnableDiagnoseGuard()
  {
    ObPerfModeGuard::get_tl_instance() = old_value_;
  }
private:
  bool old_value_;
};


} /* namespace lib */
} /* namespace oceanbase */

#endif /* OB_UNITTEST_DIAGNOSTIC_INFO_UTIL_H_ */
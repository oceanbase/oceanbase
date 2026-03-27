/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX COMMON
#include "ob_tenant_errsim_event.h"

namespace oceanbase
{
using namespace lib;

namespace common
{

ObTenantErrsimEvent::ObTenantErrsimEvent()
    : timestamp_(0),
      type_(),
      errsim_error_(OB_SUCCESS),
      backtrace_()
{
}

void ObTenantErrsimEvent::reset()
{
  timestamp_ = 0;
  type_.reset();
  errsim_error_ = OB_SUCCESS;
  backtrace_.reset();
}

bool ObTenantErrsimEvent::is_valid() const
{
  return timestamp_ > 0 && type_.is_valid() && !backtrace_.is_empty();
}

void ObTenantErrsimEvent::build_event(const int32_t result)
{
  timestamp_ = ObTimeUtil::current_time();
  errsim_error_ = result;
  lbt(backtrace_.ptr(), backtrace_.capacity());
#ifdef ERRSIM
  type_ = THIS_WORKER.get_module_type();
#else
  ObErrsimModuleType tmp_type(ObErrsimModuleType::ERRSIM_MODULE_NONE);
  type_ = tmp_type;
#endif
}

} //common
} //oceanbase

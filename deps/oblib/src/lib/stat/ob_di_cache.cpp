/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX COMMON

#include "lib/stat/ob_di_cache.h"
#include "lib/stat/ob_session_stat.h"
#include "lib/utility/ob_tracepoint.h" // for ERRSIM_POINT_DEF

namespace oceanbase
{
namespace common
{

ObDISessionCollect::ObDISessionCollect()
  : session_id_(0),
    base_value_(),
    lock_()
{
}

ObDISessionCollect::~ObDISessionCollect()
{
}

void ObDISessionCollect::clean()
{
  session_id_ = 0;
  client_sid_ = INVALID_SESSID;
  base_value_.reset();
}

ObDITenantCollect::ObDITenantCollect(ObIAllocator *allocator)
  : tenant_id_(0),
    last_access_time_(0),
    base_value_(allocator)
{
}

ObDITenantCollect::~ObDITenantCollect()
{
}

void ObDITenantCollect::clean()
{
  tenant_id_ = 0;
  last_access_time_ = 0;
  base_value_.reset();
}

}
}

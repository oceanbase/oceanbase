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

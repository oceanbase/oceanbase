/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "palf_handle_impl_guard.h"
#include "palf_env_impl.h"

namespace oceanbase
{
using namespace common;
namespace palf
{

IPalfHandleImplGuard::IPalfHandleImplGuard() : palf_id_(),
                                               palf_handle_impl_(NULL),
                                               palf_env_impl_(NULL)
{
}

IPalfHandleImplGuard::~IPalfHandleImplGuard()
{
  reset();
}

bool IPalfHandleImplGuard::is_valid() const
{
  return true == is_valid_palf_id(palf_id_) && NULL != palf_handle_impl_ && NULL != palf_env_impl_;
}

void IPalfHandleImplGuard::reset()
{
  if (NULL != palf_handle_impl_ && NULL != palf_env_impl_) {
    palf_env_impl_->revert_palf_handle_impl(palf_handle_impl_);
  }
  palf_handle_impl_ = NULL;
  palf_env_impl_ = NULL;
  palf_id_ = -1;
};

} // end namespace palf
} // end namespace oceanbase

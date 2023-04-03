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

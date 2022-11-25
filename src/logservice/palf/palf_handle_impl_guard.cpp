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

namespace oceanbase
{
using namespace common;
namespace palf
{
PalfHandleImplGuard::PalfHandleImplGuard() : palf_id_(),
                                             palf_handle_impl_(NULL),
                                             palf_handle_impl_map_(NULL)
{
}

PalfHandleImplGuard::~PalfHandleImplGuard()
{
  if (NULL != palf_handle_impl_ && NULL != palf_handle_impl_map_) {
    palf_handle_impl_map_->revert(palf_handle_impl_);
  }
}

int PalfHandleImplGuard::set_palf_handle_impl(const int64_t palf_id,
                                              PalfHandleImplMap *palf_handle_impl_map)
{
  int ret = OB_SUCCESS;
  PalfHandleImpl *palf_handle_impl = NULL;
  LSKey hash_map_key(palf_id);
  if (false == is_valid_palf_id(palf_id)
      || OB_ISNULL(palf_handle_impl_map)) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(ERROR, "Invalid argument!!!", K(ret), K(palf_id), KP(palf_handle_impl_map));
  } else if (OB_FAIL(palf_handle_impl_map->get(hash_map_key, palf_handle_impl))) {
    PALF_LOG(WARN, "PalfHandleImplGuard set_palf_handle_impl_ failed", K(ret),
        K(palf_id), K(palf_handle_impl), KP(palf_handle_impl_map));
  } else {
    palf_id_ = palf_id;
    palf_handle_impl_ = palf_handle_impl;
    palf_handle_impl_map_ = palf_handle_impl_map;
    PALF_LOG(INFO, "set_palf_handle_impl success", K(ret), K(palf_id));
  }
  return ret;
}

int PalfHandleImplGuard::set_palf_handle_impl(const int64_t palf_id,
                                     PalfHandleImpl *palf_handle_impl,
                                     PalfHandleImplMap *palf_handle_impl_map)
{
  int ret = OB_SUCCESS;
  if (false == is_valid_palf_id(palf_id)
      || OB_ISNULL(palf_handle_impl)
      || OB_ISNULL(palf_handle_impl_map)) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(ERROR, "Invalid argument!!!", K(ret), K(palf_id),
        KP(palf_handle_impl), KP(palf_handle_impl_map));
  } else {
    palf_id_ = palf_id;
    palf_handle_impl_ = palf_handle_impl;
    palf_handle_impl_map_ = palf_handle_impl_map;
  }
  return ret;
}

bool PalfHandleImplGuard::is_valid() const
{
  return true == is_valid_palf_id(palf_id_) && NULL != palf_handle_impl_ && NULL != palf_handle_impl_map_;
}

} // end namespace palf
} // end namespace oceanbase

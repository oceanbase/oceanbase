/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "ob_append_callback.h"

namespace oceanbase
{
namespace logservice
{
AppendCb* AppendCbBase::__get_class_address(ObLink *ptr)
{
  return NULL != ptr ? CONTAINER_OF(ptr, AppendCb, __next_) : NULL;
}
ObLink* AppendCbBase::__get_member_address(AppendCb *ptr)
{
  return NULL != ptr ? reinterpret_cast<ObLink*>(ADDRESS_OF(ptr, AppendCb, __next_)) : NULL;
}

void AppendCb::set_cb_first_handle_ts(const int64_t ts)
{
  if (OB_INVALID_TIMESTAMP != cb_first_handle_ts_) {
    cb_first_handle_ts_ = ts;
  }
}

} // end namespace logservice
} // end napespace oceanbase

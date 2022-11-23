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

#include "ob_append_callback.h"
#include "lib/utility/ob_macro_utils.h"

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

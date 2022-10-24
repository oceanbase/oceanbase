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

#define USING_LOG_PREFIX STORAGE

#include "storage/meta_mem/ob_tablet_handle.h"
#include "storage/tablet/ob_tablet.h"

namespace oceanbase
{
namespace storage
{
ObTabletHandle::ObTabletHandle()
  : Base(),
    wash_priority_(WashTabletPriority::WTP_MAX)
{
  INIT_OBJ_LEAK_DEBUG_NODE(node_, this, share::LEAK_CHECK_OBJ_TABLET_HANDLE, MTL_ID());
}

ObTabletHandle::ObTabletHandle(const Base &other)
  : Base(other),
    wash_priority_(WashTabletPriority::WTP_MAX)
{
}

ObTabletHandle::ObTabletHandle(const ObTabletHandle &other)
  : Base(),
    wash_priority_(WashTabletPriority::WTP_MAX)
{
  INIT_OBJ_LEAK_DEBUG_NODE(node_, this, share::LEAK_CHECK_OBJ_TABLET_HANDLE, MTL_ID());
  *this = other;
}
ObTabletHandle::~ObTabletHandle()
{
}
ObTabletHandle &ObTabletHandle::operator=(const ObTabletHandle &other)
{
  if (this != &other) {
    reset();
    Base::operator=(other);
    wash_priority_ = other.wash_priority_;
  }
  return *this;
}

void ObTabletHandle::reset()
{
  if (OB_NOT_NULL(obj_)) {
    obj_->update_wash_score(calc_wash_score(wash_priority_));
  }
  wash_priority_ = WashTabletPriority::WTP_MAX;
  Base::reset();
}

int64_t ObTabletHandle::calc_wash_score(const WashTabletPriority priority) const
{
  int ret = OB_SUCCESS;
  int64_t score = INT64_MIN;
  if (OB_UNLIKELY(WashTabletPriority::WTP_HIGH != priority
               && WashTabletPriority::WTP_LOW != priority)) {
    ret = OB_NOT_SUPPORTED;
    LOG_ERROR("this priority value hasn't be supported", K(ret), KPC(this));
  } else {
    // Formula:
    //   score = (1 - priority) * t + priority * (t - INT64_MAX)
    const int64_t t = ObTimeUtility::current_time_ns();
    score = WashTabletPriority::WTP_HIGH == priority ? t : t - INT64_MAX;
  }
  return score;
}

int64_t ObTabletHandle::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(KP_(obj),
       KP_(obj_pool),
       K_(wash_priority));
  J_OBJ_END();
  return pos;
}

} // end namespace storage
} // end namespace oceanbase

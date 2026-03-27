/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "ob_handle_list.h"

namespace oceanbase
{
namespace common
{
void ObHandleList::init_handle(Handle& handle)
{
  handle.reset();
  total_list_.add(&handle.total_list_);
  ATOMIC_AAF(&total_count_, 1);
}

void ObHandleList::destroy_handle(Handle& handle)
{
  set_frozen(handle);
  total_list_.del(&handle.total_list_);
  ATOMIC_AAF(&total_count_, -1);
}

void ObHandleList::set_active(Handle& handle)
{
  if (handle.set_active()) {
    active_list_.add(&handle.active_list_, handle);
    update_hazard();
  }
  handle.set_id(alloc_id());
}

void ObHandleList::set_frozen(Handle& handle)
{
  if (handle.is_active()) {
    active_list_.del(&handle.active_list_);
    update_hazard();
  }
  handle.set_frozen();
}

void ObHandleList::update_hazard()
{
  ATOMIC_STORE(&hazard_, calc_hazard());
}

int64_t ObHandleList::calc_hazard()
{
  int64_t x = INT64_MAX;
  DLink* last = active_list_.tail_.prev_;
  if (&active_list_.head_ != last) {
    Handle* handle = CONTAINER_OF(last, Handle, active_list_);
    x = handle->get_clock();
  }
  COMMON_LOG(TRACE, "HandleList.calc_hazard", K(x));
  return x;
}

}; // end namespace common
}; // end namespace oceanbase

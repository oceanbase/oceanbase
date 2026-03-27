/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "ob_malloc_callback.h"

namespace oceanbase
{
namespace lib
{
void ObDefaultMallocCallback::operator()(const ObMemAttr& attr, int64_t used, const lib::AObject &obj)
{
  UNUSED(attr);
  UNUSED(used);
  UNUSED(obj);
}

_RLOCAL(ObMallocCallback *, malloc_callback);

void ObMallocCallback::unlink()
{
  prev_->next_ = next_;
  next_->prev_ = prev_;
  prev_ = this;
  next_ = this;
}

void ObMallocCallback::insert_before(ObMallocCallback *callback)
{
  prev_ = callback->prev_;
  next_ = callback;
  prev_->next_ = this;
  next_->prev_ = this;
}

ObMallocCallbackGuard::ObMallocCallbackGuard(ObMallocCallback& cb)
    : callback_(cb)
{
  if (OB_ISNULL(malloc_callback)) {
    malloc_callback = &callback_;
  } else {
    callback_.insert_before(malloc_callback);
  }
}

ObMallocCallbackGuard::~ObMallocCallbackGuard()
{
  if (callback_.next() == &callback_) {
    malloc_callback = nullptr;
  } else {
    callback_.unlink();
  }
}

}
}